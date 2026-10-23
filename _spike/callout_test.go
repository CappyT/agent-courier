package spike

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func basePolicy(id string) *policy {
	return &policy{
		pub:    []string{"courier.*." + id + ".*", "$SYS.REQ.USER.INFO"},
		sub:    []string{"_INBOX_" + id + ".>"},
		limits: jwt.NatsLimits{Subs: 5, Payload: 1024, Data: -1},
	}
}

// TestCalloutPermissionsAndLimits: the callout signs a user JWT with the
// COURIER signing key, with per-key publish and subscribe permissions and
// subs/payload limits; the callout caps connections per key itself.
func TestCalloutPermissionsAndLimits(t *testing.T) {
	sw := startSwarm(t)
	sw.setPolicy(basePolicy)
	sw.maxConns = 2
	a, b := newKey(t), newKey(t)

	ca := sw.mustConnect(a)
	resp, err := ca.nc.Request("$SYS.REQ.USER.INFO", nil, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("user info: %s", resp.Data)
	if !strings.Contains(string(resp.Data), sw.courPub) {
		t.Fatalf("not placed in COURIER: %s", resp.Data)
	}

	// Own subject: allowed. Another key's subject: violation.
	if err := ca.nc.Publish("courier.t."+ca.id+".note", []byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = ca.nc.Flush()
	if err := ca.permErr(t, 300*time.Millisecond); err != nil {
		t.Fatalf("own publish: %v", err)
	}
	_ = ca.nc.Publish("courier.t."+pubOf(b)+".note", []byte("x"))
	_ = ca.nc.Flush()
	if err := ca.permErr(t, time.Second); err == nil || !strings.Contains(err.Error(), "Permissions Violation for Publish") {
		t.Fatalf("forged publish: want violation, got %v", err)
	}
	// Subscribe outside the own inbox: violation.
	_, _ = ca.nc.SubscribeSync("courier.>")
	_ = ca.nc.Flush()
	if err := ca.permErr(t, time.Second); err == nil || !strings.Contains(err.Error(), "Permissions Violation for Subscription") {
		t.Fatalf("foreign subscribe: want violation, got %v", err)
	}

	// The user JWT says subs 5 and payload 1024, but the server drops the
	// callout JWT after it reads the permissions: user limits do not apply.
	for i := range 6 {
		if _, err := ca.nc.SubscribeSync("_INBOX_" + ca.id + ".u" + string(rune('a'+i))); err != nil {
			t.Fatal(err)
		}
	}
	_ = ca.nc.Flush()
	if err := ca.permErr(t, 300*time.Millisecond); err != nil {
		t.Fatalf("user JWT subs limit applied after all: %v", err)
	}
	t.Logf("user JWT subs=5: 6 subscriptions pass, the user limit is ignored")
	if err := ca.nc.Publish("courier.t."+ca.id+".note", make([]byte, 2000)); err != nil {
		t.Fatal(err)
	}
	_ = ca.nc.Flush()
	if err := ca.permErr(t, 300*time.Millisecond); err != nil || ca.nc.Status() != nats.CONNECTED {
		t.Fatalf("user JWT payload limit applied after all: %v %v", err, ca.nc.Status())
	}
	t.Logf("user JWT payload=1024: a 2000 byte publish passes, the user limit is ignored")

	// Account limits apply to every client of COURIER, per connection.
	sw.pushAccount(func(ac *jwt.AccountClaims) { ac.Limits.Subs = 5; ac.Limits.Payload = 1024 })
	for _, name := range []string{"first", "second"} {
		c := sw.mustConnect(newKey(t))
		var subErr error
		n := 0
		for i := range 7 {
			if _, err := c.nc.SubscribeSync("_INBOX_" + c.id + ".s" + string(rune('a'+i))); err != nil {
				t.Fatal(err)
			}
			_ = c.nc.Flush()
			if subErr = c.permErr(t, 200*time.Millisecond); subErr != nil {
				break
			}
			n++
		}
		if subErr == nil || !strings.Contains(strings.ToLower(subErr.Error()), "maximum subscriptions") {
			t.Fatalf("%s client: want a subscription limit error, got %v", name, subErr)
		}
		t.Logf("account subs=5, %s client: %d subscriptions, then %v", name, n, subErr)
	}
	closed := make(chan error, 1)
	cp := sw.mustConnect(newKey(t), nats.NoReconnect(), nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { closed <- err }))
	t.Logf("max_payload the client sees in INFO: %d", cp.nc.MaxPayload())
	err = cp.nc.Publish("courier.t."+cp.id+".note", make([]byte, 2000))
	_ = cp.nc.Flush()
	// The server may send an INFO with the account max_payload after
	// CONNECT; then the client refuses the publish itself.
	select {
	case <-func() chan struct{} {
		c := make(chan struct{})
		if errors.Is(err, nats.ErrMaxPayload) {
			close(c)
		}
		return c
	}():
		t.Logf("account payload=1024: client refuses 2000 bytes, max_payload now %d", cp.nc.MaxPayload())
	case e := <-closed:
		t.Logf("account payload=1024, 2000 byte publish: publish err %v, disconnect %v, last err %v", err, e, cp.nc.LastError())
	case e := <-cp.errs:
		t.Logf("account payload=1024, 2000 byte publish: async err %v (publish err %v)", e, err)
	case <-time.After(2 * time.Second):
		t.Fatalf("payload over the account limit passed (publish err %v)", err)
	}

	// Lowering the account subs limit dropped A, which held 7 subscriptions.
	t.Logf("after the account update A (7 subs) is %v", ca.nc.Status())

	// Connection cap per key, enforced by the callout: the third fails.
	k := newKey(t)
	c1 := sw.mustConnect(k)
	sw.mustConnect(k)
	if _, err := sw.connect(k); err == nil {
		t.Fatal("third connection of a key passed")
	} else {
		t.Logf("third connection: %v", err)
	}
	c1.nc.Close()
	start := time.Now()
	for {
		if _, err := sw.connect(k); err == nil {
			sw.mu.Lock()
			t.Logf("after a close the key connects again in %v (disconnect event %s)", time.Since(start), sw.lastDisc)
			sw.mu.Unlock()
			break
		}
		if time.Since(start) > 3*time.Second {
			t.Fatal("key cannot reconnect after closing a connection")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A client that presents key b but signs with key a: refused.
	bad := []nats.Option{nats.Nkey(pubOf(b), a.Sign), nats.NoReconnect()}
	if _, err := nats.Connect(sw.url, bad...); err == nil {
		t.Fatal("nonce signed by another key passed")
	} else {
		t.Logf("forged nkey: %v", err)
	}
}

// TestCalloutRevocation measures how a revocation reaches a live connection.
func TestCalloutRevocation(t *testing.T) {
	sw := startSwarm(t)
	banned := map[string]bool{}
	sw.setPolicy(func(k string) *policy {
		if banned[k] {
			return nil
		}
		return basePolicy(k)
	})
	a := newKey(t)
	disc := make(chan time.Time, 8)
	reconn := make(chan time.Time, 8)
	ca := sw.mustConnect(a,
		nats.ReconnectWait(100*time.Millisecond), nats.MaxReconnects(5),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				disc <- time.Now()
			}
		}),
		nats.ReconnectHandler(func(*nats.Conn) { reconn <- time.Now() }))

	// 1. Revoking the client's own nkey does nothing: the user JWT subject
	// is the ephemeral key the server generated for this connection.
	sw.pushAccount(func(ac *jwt.AccountClaims) { ac.Revoke(ca.id) })
	select {
	case <-disc:
		t.Fatal("revoking the client nkey disconnected it")
	case <-time.After(time.Second):
		t.Logf("revoking client nkey %s…: still connected (status %v)", ca.id[:8], ca.nc.Status())
	}

	// 2. Revoking the ephemeral subjects the callout issued for the key
	// disconnects at once. nats.go reconnects and the callout issues a new
	// JWT, so the callout must also refuse the key.
	sw.mu.Lock()
	eph := append([]string(nil), sw.issued[ca.id]...)
	sw.mu.Unlock()
	start := sw.pushAccount(func(ac *jwt.AccountClaims) {
		for _, e := range eph {
			ac.Revoke(e)
		}
	})
	select {
	case at := <-disc:
		t.Logf("revoke ephemeral subject -> disconnect in %v", at.Sub(start))
	case <-time.After(3 * time.Second):
		t.Fatal("revocation did not disconnect")
	}
	select {
	case at := <-reconn:
		t.Logf("without a ban, A reconnected %v after the revocation", at.Sub(start))
	case <-time.After(3 * time.Second):
		t.Fatal("no reconnect")
	}

	// 3. Ban plus revoke-all: the connection drops and stays down.
	sw.mu.Lock()
	banned[ca.id] = true
	sw.mu.Unlock()
	start = sw.pushAccount(func(ac *jwt.AccountClaims) { ac.RevokeAt(jwt.All, time.Now()) })
	select {
	case at := <-disc:
		t.Logf("revoke all -> disconnect in %v", at.Sub(start))
	case <-time.After(3 * time.Second):
		t.Fatal("revoke all did not disconnect")
	}
	select {
	case <-reconn:
		t.Fatal("banned key reconnected")
	case <-time.After(1500 * time.Millisecond):
		t.Logf("banned key stays out, status %v", ca.nc.Status())
	}
	// A new key connects after a revoke-all: revocation is "issued before".
	time.Sleep(1100 * time.Millisecond) // iat has 1s resolution
	if _, err := sw.connect(newKey(t)); err != nil {
		t.Fatalf("new key after revoke all: %v", err)
	}
}

// TestCalloutExpiry: a user JWT that expires disconnects the client. The
// client reconnects through the callout: renewal is a reconnect. With the
// callout down, nothing keeps the client connected.
func TestCalloutExpiry(t *testing.T) {
	sw := startSwarm(t)
	sw.setPolicy(func(k string) *policy { p := basePolicy(k); p.expires = 2 * time.Second; return p })
	disc := make(chan string, 16)
	reconn := make(chan time.Time, 16)
	connectedAt := time.Now()
	ca := sw.mustConnect(newKey(t),
		nats.ReconnectWait(100*time.Millisecond), nats.MaxReconnects(-1), nats.ReconnectJitter(0, 0),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				disc <- time.Now().Format("15:04:05.000") + " " + err.Error()
			}
		}),
		nats.ReconnectHandler(func(*nats.Conn) { reconn <- time.Now() }))
	sub, _ := ca.nc.SubscribeSync("_INBOX_" + ca.id + ".x")
	select {
	case d := <-disc:
		t.Logf("expiry -> %s, %v after connect", d, time.Since(connectedAt))
	case <-time.After(4 * time.Second):
		t.Fatal("JWT expiry did not disconnect")
	}
	select {
	case at := <-reconn:
		t.Logf("reconnected through the callout at %s", at.Format("15:04:05.000"))
	case <-time.After(3 * time.Second):
		t.Fatal("no reconnect after expiry")
	}
	if sub.IsValid() {
		t.Logf("subscription survives the reconnect (client resubscribes)")
	}

	// Callout down: the next expiry drops the client and it cannot return.
	_ = sw.callout.Unsubscribe()
	down := time.Now()
	select {
	case d := <-disc:
		t.Logf("callout down, expiry -> %s", d)
	case <-time.After(4 * time.Second):
		t.Fatal("no expiry")
	}
	select {
	case <-reconn:
		t.Fatal("reconnected without a callout")
	case <-time.After(3 * time.Second):
		t.Logf("callout down: client stays disconnected %v after the outage began, status %v", time.Since(down), ca.nc.Status())
	}
	_ = nkeys.PrefixByteUser
}

// TestCalloutBanByKick: a system user kicks one connection by client id;
// with the key banned in the callout, the reconnect fails. No account JWT
// update, so no operator key is needed.
func TestCalloutBanByKick(t *testing.T) {
	sw := startSwarm(t)
	var mu sync.Mutex
	banned := map[string]bool{}
	sw.setPolicy(func(k string) *policy {
		mu.Lock()
		defer mu.Unlock()
		if banned[k] {
			return nil
		}
		return basePolicy(k)
	})
	disc := make(chan time.Time, 4)
	reconn := make(chan struct{}, 4)
	ca := sw.mustConnect(newKey(t), nats.ReconnectWait(100*time.Millisecond), nats.MaxReconnects(5),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				disc <- time.Now()
			}
		}),
		nats.ReconnectHandler(func(*nats.Conn) { reconn <- struct{}{} }))
	cid, err := ca.nc.GetClientID()
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	banned[ca.id] = true
	mu.Unlock()
	sys := sw.sysConn()
	start := time.Now()
	resp, err := sys.Request(fmt.Sprintf("$SYS.REQ.SERVER.%s.KICK", sw.srv.ID()), fmt.Appendf(nil, `{"cid":%d}`, cid), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("kick response: %s", resp.Data)
	select {
	case at := <-disc:
		t.Logf("kick -> disconnect in %v", at.Sub(start))
	case <-time.After(3 * time.Second):
		t.Fatal("kick did not disconnect")
	}
	select {
	case <-reconn:
		t.Fatal("banned key reconnected after kick")
	case <-time.After(1500 * time.Millisecond):
		t.Logf("banned key stays out after kick, status %v", ca.nc.Status())
	}
}
