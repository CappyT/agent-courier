package spike

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func waitGone(t *testing.T, kv jetstream.KeyValue, key string, max time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < max {
		if _, err := kv.Get(t.Context(), key); errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
			return time.Since(start)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1
}

// TestKVLeasePerKeyTTL: a lease written by the callout with Create and a
// per-key TTL, renewed with compare-and-swap, taken over after expiry.
func TestKVLeasePerKeyTTL(t *testing.T) {
	sw := startSwarm(t)
	js, _ := jetstream.New(sw.adminConn())
	ctx := t.Context()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "lease", History: 1, LimitMarkerTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	si, _ := js.Stream(ctx, "KV_lease")
	t.Logf("KV_lease: allow_msg_ttl=%v subject_delete_marker_ttl=%v allow_rollup=%v deny_purge=%v", si.CachedInfo().Config.AllowMsgTTL, si.CachedInfo().Config.SubjectDeleteMarkerTTL, si.CachedInfo().Config.AllowRollup, si.CachedInfo().Config.DenyPurge)
	const key = "acme_app"
	ttl := 2 * time.Second

	rev, err := kv.Create(ctx, key, []byte("holder=X"), jetstream.KeyTTL(ttl))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Create(ctx, key, []byte("holder=Y"), jetstream.KeyTTL(ttl)); !errors.Is(err, jetstream.ErrKeyExists) {
		t.Fatalf("Y takes a live lease: %v", err)
	}
	t.Logf("Y Create on a live lease: ErrKeyExists")

	// Renewal with kv.Update keeps no TTL: the lease would never expire.
	rev2, err := kv.Update(ctx, key, []byte("holder=X"), rev)
	if err != nil {
		t.Fatal(err)
	}
	if d := waitGone(t, kv, key, ttl+time.Second); d >= 0 {
		t.Fatalf("after kv.Update the key expired in %v", d)
	}
	t.Logf("kv.Update renewal: key still present %v after the TTL, the TTL is lost", ttl+time.Second)

	// Renewal that works: a raw publish with both expected-last-subject-
	// sequence (CAS) and Nats-TTL.
	ack, err := js.Publish(ctx, "$KV.lease."+key, []byte("holder=X"), jetstream.WithExpectLastSequencePerSubject(rev2), jetstream.WithMsgTTL(ttl))
	if err != nil {
		t.Fatal(err)
	}
	rev3 := ack.Sequence
	// A stale renewal (old revision) fails: that is the fencing check.
	if _, err := js.Publish(ctx, "$KV.lease."+key, []byte("holder=X"), jetstream.WithExpectLastSequencePerSubject(rev2), jetstream.WithMsgTTL(ttl)); err == nil {
		t.Fatal("stale CAS renewal passed")
	} else {
		t.Logf("stale CAS renewal: %v", err)
	}
	for range 3 {
		time.Sleep(ttl / 2)
		ack, err := js.Publish(ctx, "$KV.lease."+key, []byte("holder=X"), jetstream.WithExpectLastSequencePerSubject(rev3), jetstream.WithMsgTTL(ttl))
		if err != nil {
			t.Fatalf("renewal: %v", err)
		}
		rev3 = ack.Sequence
	}
	t.Logf("CAS+TTL renewal every %v for %v: lease held at rev %d", ttl/2, 3*ttl/2, rev3)
	w, _ := kv.Watch(ctx, key, jetstream.UpdatesOnly())
	start := time.Now()
	d := waitGone(t, kv, key, 3*ttl)
	if d < 0 {
		t.Fatal("lease did not expire")
	}
	t.Logf("holder stops renewing: key gone after %v (TTL %v)", d, ttl)
	select {
	case e := <-w.Updates():
		if e != nil {
			t.Logf("watcher sees op=%v rev=%d after %v", e.Operation(), e.Revision(), time.Since(start))
		}
	case <-time.After(2 * time.Second):
		t.Logf("watcher saw nothing")
	}
	_ = w.Stop()

	// Takeover by Y, then X's stale renewal fails.
	revY, err := kv.Create(ctx, key, []byte("holder=Y"), jetstream.KeyTTL(ttl))
	if err != nil {
		t.Fatalf("Y takeover: %v", err)
	}
	t.Logf("Y takeover: rev %d", revY)
	if _, err := js.Publish(ctx, "$KV.lease."+key, []byte("holder=X"), jetstream.WithExpectLastSequencePerSubject(rev3), jetstream.WithMsgTTL(ttl)); err == nil {
		t.Fatal("X renewed after takeover")
	} else {
		t.Logf("X renews after takeover: %v", err)
	}
}

// TestKVClaimBucketTTL: a claim bucket with a bucket TTL (stream max_age).
// Each write restarts the age, so plain Create/Update need no TTL header.
func TestKVClaimBucketTTL(t *testing.T) {
	sw := startSwarm(t)
	js, _ := jetstream.New(sw.adminConn())
	ctx := t.Context()
	ttl := 2 * time.Second
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "claims", History: 1, TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	const key = "acme_app.42"
	rev, err := kv.Create(ctx, key, []byte("gh=1001"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Create(ctx, key, []byte("gh=2002")); !errors.Is(err, jetstream.ErrKeyExists) {
		t.Fatalf("second claim: %v", err)
	}
	for range 3 {
		time.Sleep(ttl / 2)
		if rev, err = kv.Update(ctx, key, []byte("gh=1001"), rev); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("claim renewed with plain Update for %v with a bucket TTL of %v", 3*ttl/2, ttl)
	d := waitGone(t, kv, key, 3*ttl)
	if d < 0 {
		t.Fatal("claim did not expire")
	}
	t.Logf("claim expires %v after the last renewal", d)
	if _, err := kv.Create(ctx, key, []byte("gh=2002")); err != nil {
		t.Fatalf("claim after expiry: %v", err)
	}
	if _, err := kv.Update(ctx, key, []byte("gh=1001"), rev); !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		t.Fatalf("stale claim renewal: %v", err)
	}
	t.Logf("new claimant takes it, stale holder gets ErrKeyRevisionMismatch")
}

func kvPolicy(id string) *policy {
	return &policy{
		pub: []string{
			"$KV.state.acme_app." + id + ".>",
			"$JS.API.INFO",
			"$JS.API.STREAM.INFO.KV_state",
			"$JS.API.DIRECT.GET.KV_state.>",
		},
		sub: []string{"_INBOX_" + id + ".>"},
	}
}

// TestKVPerKeyPermissions: publish permissions on $KV.<bucket>.<project>.<key-id>.>
func TestKVPerKeyPermissions(t *testing.T) {
	for _, hardened := range []bool{false, true} {
		name := "nats.go KV bucket"
		if hardened {
			name = "hardened stream: allow_rollup_hdrs=false, deny_purge=true"
		}
		t.Run(name, func(t *testing.T) { kvPermissions(t, hardened) })
	}
}

func kvPermissions(t *testing.T, hardened bool) {
	sw := startSwarm(t)
	sw.setPolicy(kvPolicy)
	ajs, _ := jetstream.New(sw.adminConn())
	ctx := t.Context()
	if _, err := ajs.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "state", History: 1}); err != nil {
		t.Fatal(err)
	}
	if hardened {
		s, _ := ajs.Stream(ctx, "KV_state")
		cfg := s.CachedInfo().Config
		cfg.AllowRollup = false
		cfg.DenyPurge = true
		if _, err := ajs.UpdateStream(ctx, cfg); err != nil {
			t.Fatal(err)
		}
	}
	a, b := sw.mustConnect(newKey(t)), sw.mustConnect(newKey(t))
	jsA, _ := jetstream.New(a.nc)
	jsB, _ := jetstream.New(b.nc)
	kvA, err := jsA.KeyValue(ctx, "state")
	if err != nil {
		t.Fatalf("A binds the bucket: %v", err)
	}
	kvB, _ := jsB.KeyValue(ctx, "state")
	keyA := "acme_app." + a.id + ".status"
	keyB := "acme_app." + b.id + ".status"
	if _, err := kvA.Put(ctx, keyA, []byte("A ok")); err != nil {
		t.Fatalf("A puts own key: %v", err)
	}
	if _, err := kvB.Put(ctx, keyB, []byte("B ok")); err != nil {
		t.Fatalf("B puts own key: %v", err)
	}
	if e, err := kvB.Get(ctx, keyA); err != nil {
		t.Fatalf("B reads A's key through direct get: %v", err)
	} else {
		t.Logf("B reads A's key (direct get): %q", e.Value())
	}
	denied(t, b, "B puts A's key", "$KV.state."+keyA, func() error { _, err := kvB.Put(short(t), keyA, []byte("evil")); return err })
	denied(t, b, "B deletes A's key", "$KV.state."+keyA, func() error { return kvB.Delete(short(t), keyA) })
	denied(t, b, "B purges A's key", "$KV.state."+keyA, func() error { return kvB.Purge(short(t), keyA) })
	denied(t, b, "B STREAM.MSG.GET", "$JS.API.STREAM.MSG.GET.KV_state", func() error {
		_, err := b.nc.Request("$JS.API.STREAM.MSG.GET.KV_state", []byte(`{"last_by_subj":"$KV.state.`+keyA+`"}`), 700*time.Millisecond)
		return err
	})
	denied(t, b, "B STREAM.PURGE", "$JS.API.STREAM.PURGE.KV_state", func() error {
		_, err := b.nc.Request("$JS.API.STREAM.PURGE.KV_state", []byte(`{"filter":"$KV.state.`+keyA+`"}`), 700*time.Millisecond)
		return err
	})
	denied(t, b, "B STREAM.MSG.DELETE", "$JS.API.STREAM.MSG.DELETE.KV_state", func() error {
		_, err := b.nc.Request("$JS.API.STREAM.MSG.DELETE.KV_state", []byte(`{"seq":1}`), 700*time.Millisecond)
		return err
	})
	denied(t, b, "B watch (ordered consumer)", "$JS.API.CONSUMER.CREATE.KV_state.", func() error {
		w, err := kvB.Watch(short(t), "acme_app.>")
		if err != nil {
			return err
		}
		defer w.Stop()
		select {
		case <-w.Updates():
			return nil
		case <-time.After(500 * time.Millisecond):
			return errors.New("no updates")
		}
	})

	// Msg-Id dedupe across keys: B publishes on its own key with an id the
	// callout later uses on another key.
	if _, err := jsB.Publish(ctx, "$KV.state."+keyB, []byte("B"), jetstream.WithMsgID("lease-1")); err != nil {
		t.Fatal(err)
	}
	ack, err := ajs.Publish(ctx, "$KV.state.acme_app.lease", []byte("callout"), jetstream.WithMsgID("lease-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ajs.Publish(ctx, "$KV.state.acme_app.lease2", []byte("callout")); err != nil {
		t.Fatal(err)
	}
	kvAdmin, _ := ajs.KeyValue(ctx, "state")
	_, gerr := kvAdmin.Get(ctx, "acme_app.lease")
	t.Logf("callout write with a Msg-Id that B used first: duplicate=%v, get err=%v", ack.Duplicate, gerr)

	// Rollup "all" on B's own key.
	m := nats.NewMsg("$KV.state." + keyB)
	m.Data = []byte("B rolls up")
	m.Header.Set("Nats-Rollup", "all")
	_, rerr := jsB.PublishMsg(ctx, m)
	_, aerr := kvB.Get(ctx, keyA)
	if hardened {
		if rerr == nil || aerr != nil {
			t.Fatalf("hardened: rollup err %v, A's key err %v", rerr, aerr)
		}
		t.Logf("hardened: rollup-all on own key refused (%v), A's key intact", rerr)
		if err := kvA.Delete(ctx, keyA); err != nil {
			t.Fatalf("hardened: A deletes own key: %v", err)
		}
		t.Logf("hardened: kv.Delete still works; kv.Purge: %v", kvA.Purge(ctx, keyA))
		return
	}
	if rerr != nil {
		t.Fatalf("rollup-all refused on a nats.go bucket: %v", rerr)
	}
	if !errors.Is(aerr, jetstream.ErrKeyNotFound) {
		t.Fatalf("A's key after B's rollup-all: %v", aerr)
	}
	si, _ := ajs.Stream(ctx, "KV_state")
	info, _ := si.Info(ctx)
	t.Logf("SURPRISE: B's rollup-all on its own key wiped A's key (get: %v); stream now holds %d msgs", aerr, info.State.Msgs)
}
