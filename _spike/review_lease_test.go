package spike

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Every renewal changes the revision, so a leader message sent at rev R
// fails a "revision is current" check after the next renewal.
func TestReviewLeaseRevisionPerRenewal(t *testing.T) {
	sw := startSwarm(t)
	js, _ := jetstream.New(sw.adminConn())
	ctx := t.Context()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "lease", History: 1, LimitMarkerTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	const key = "acme_app"
	ttl := 2 * time.Second
	rev, err := kv.Create(ctx, key, []byte("holder=X"), jetstream.KeyTTL(ttl))
	if err != nil {
		t.Fatal(err)
	}
	revs := []uint64{rev}
	for range 3 {
		ack, err := js.Publish(ctx, "$KV.lease."+key, []byte("holder=X"), jetstream.WithExpectLastSequencePerSubject(revs[len(revs)-1]), jetstream.WithMsgTTL(ttl))
		if err != nil {
			t.Fatal(err)
		}
		revs = append(revs, ack.Sequence)
	}
	e, _ := kv.Get(ctx, key)
	t.Logf("revisions over one tenure of X: %v; current %d. A message stamped %d fails 'revision is current'.", revs, e.Revision(), revs[0])

	// TTL precision: measure several expiries.
	for i := range 3 {
		ack, err := js.Publish(ctx, "$KV.lease."+key, []byte("holder=X"), jetstream.WithExpectLastSequencePerSubject(revs[len(revs)-1]), jetstream.WithMsgTTL(ttl))
		if err != nil {
			// key may be gone after expiry: recreate
			r, err2 := kv.Create(ctx, key, []byte("holder=X"), jetstream.KeyTTL(ttl))
			if err2 != nil {
				t.Fatalf("renew %v / create %v", err, err2)
			}
			revs = append(revs, r)
		} else {
			revs = append(revs, ack.Sequence)
		}
		start := time.Now()
		d := waitGone(t, kv, key, 4*ttl)
		t.Logf("expiry %d: gone %v after write, TTL %v (late by %v)", i, d, ttl, d-ttl)
		_ = start
		// delete marker revision
		if ent, err := kv.Get(ctx, key); err != nil {
			t.Logf("  get after expiry: %v", err)
		} else {
			t.Logf("  get after expiry: op=%v rev=%d", ent.Operation(), ent.Revision())
		}
		// Re-create by the same holder: new revision, higher.
		r, err := kv.Create(ctx, key, []byte("holder=X"), jetstream.KeyTTL(ttl))
		if err != nil {
			t.Fatalf("recreate: %v", err)
		}
		t.Logf("  recreate by the same holder: rev %d (previous tenure ended at %d)", r, revs[len(revs)-1])
		revs = append(revs, r)
	}
}

// After the bucket is deleted and created again, revisions restart, so an
// old token can equal a new one.
func TestReviewLeaseRevisionAfterBucketRecreate(t *testing.T) {
	sw := startSwarm(t)
	js, _ := jetstream.New(sw.adminConn())
	ctx := t.Context()
	cfg := jetstream.KeyValueConfig{Bucket: "lease", History: 1, LimitMarkerTTL: time.Second}
	kv, _ := js.CreateKeyValue(ctx, cfg)
	r1, _ := kv.Create(ctx, "acme_app", []byte("holder=X"), jetstream.KeyTTL(time.Hour))
	if err := js.DeleteKeyValue(ctx, "lease"); err != nil {
		t.Fatal(err)
	}
	kv, _ = js.CreateKeyValue(ctx, cfg)
	r2, _ := kv.Create(ctx, "acme_app", []byte("holder=Y"), jetstream.KeyTTL(time.Hour))
	t.Logf("tenure X rev %d, after bucket recreate tenure Y rev %d: equal=%v", r1, r2, r1 == r2)
}

// A claim expires while the holder works; the holder's renewal fails, and a
// silent Create by the holder would succeed only if nobody took it.
func TestReviewClaimLapse(t *testing.T) {
	sw := startSwarm(t)
	js, _ := jetstream.New(sw.adminConn())
	ctx := t.Context()
	ttl := 2 * time.Second
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "claims", History: 1, TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	const key = "acme_app.42"
	rev, _ := kv.Create(ctx, key, []byte("gh=1001"))
	time.Sleep(ttl + 1500*time.Millisecond) // holder laptop sleeps
	if _, err := kv.Update(ctx, key, []byte("gh=1001"), rev); err != nil {
		t.Logf("holder renews after lapse, nobody took it: %v (errors.Is mismatch=%v)", err, errors.Is(err, jetstream.ErrKeyRevisionMismatch))
	}
	// Watch semantics: is there a delete marker without LimitMarkerTTL?
	if _, err := kv.Get(ctx, key); err != nil {
		t.Logf("get after max_age expiry: %v", err)
	}
	r2, err := kv.Create(ctx, key, []byte("gh=2002"))
	t.Logf("second agent claims after lapse: rev %d err %v; holder 1001 still works on #42 unless told", r2, err)
}
