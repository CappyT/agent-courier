package spike

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func hostOf(url string) string { return strings.TrimPrefix(url, "nats://") }

// mitm is a hostile server: it forwards the default server's INFO (and its
// nonce) to the victim, forwards the victim's CONNECT, then injects a PUB.
func mitm(t *testing.T, upstream string, inject func() string) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			down, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", upstream)
			if err != nil {
				down.Close()
				continue
			}
			go func() { _, _ = io.Copy(down, up) }()
			go func() {
				r := bufio.NewReader(down)
				injected := false
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						up.Close()
						return
					}
					_, _ = up.Write([]byte(line))
					if !injected && strings.HasPrefix(line, "CONNECT ") {
						injected = true
						_, _ = up.Write([]byte(inject()))
					}
				}
			}()
		}
	}()
	return "nats://" + ln.Addr().String()
}

// TestRelayNonceSignature: the NATS nonce signature carries no server
// identity, so a hostile server that relays the default server's nonce
// authenticates as the victim's key on the default server.
func TestRelayNonceSignature(t *testing.T) {
	sw := startSwarm(t)
	sw.setPolicy(basePolicy)
	victim := newKey(t)
	vid := pubOf(victim)

	got := make(chan string, 4)
	adm := sw.adminConn()
	_, _ = adm.Subscribe("courier.>", func(m *nats.Msg) { got <- m.Subject + " " + string(m.Data) })
	_ = adm.Flush()

	forged := "PUB courier.t." + vid + ".handoff 6\r\nforged\r\n"
	evil := mitm(t, hostOf(sw.url), func() string { return forged })

	// The victim believes it talks to evil; it signs whatever nonce it sees.
	nc, err := nats.Connect(evil, nats.Nkey(vid, victim.Sign), nats.NoReconnect())
	if err != nil {
		t.Fatalf("victim via relay: %v", err)
	}
	defer nc.Close()
	select {
	case m := <-got:
		t.Logf("RELAY WORKS: default server accepted %q as %s…", m, vid[:8])
	case <-time.After(2 * time.Second):
		t.Fatal("no forged message")
	}

	// Fix: the client signs domain || nonce, the callout checks its own domain.
	sw.connectDomain = "courier.cappyt.sh"
	signFor := func(domain string, k nkeys.KeyPair) nats.SignatureHandler {
		return func(nonce []byte) ([]byte, error) {
			return k.Sign([]byte("agent-courier-connect-v1\n" + domain + "\n" + string(nonce)))
		}
	}
	honest, err := nats.Connect(sw.url, nats.Nkey(vid, signFor("courier.cappyt.sh", victim)), nats.NoReconnect())
	if err != nil {
		t.Fatalf("honest domain-bound connect: %v", err)
	}
	honest.Close()
	if _, err := nats.Connect(evil, nats.Nkey(vid, signFor("evil.example", victim)), nats.NoReconnect()); err == nil {
		t.Fatal("domain-bound relay passed")
	} else {
		t.Logf("domain-bound signature, relay refused: %v", err)
	}
}

func rawConnect(t *testing.T, addr string, k nkeys.KeyPair, closeEarly bool) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r := bufio.NewReader(c)
	line, _ := r.ReadString('\n')
	var info struct {
		Nonce string `json:"nonce"`
	}
	_ = json.Unmarshal([]byte(strings.TrimPrefix(line, "INFO ")), &info)
	sig, _ := k.Sign([]byte(info.Nonce))
	fmt.Fprintf(c, "CONNECT {\"verbose\":false,\"protocol\":1,\"headers\":true,\"nkey\":%q,\"sig\":%q}\r\nPING\r\n", pubOf(k), base64.RawURLEncoding.EncodeToString(sig))
	if closeEarly {
		return
	}
	_, _ = r.ReadString('\n')
}

// TestConnCountLeak: the callout counts at issue time; a client that drops
// before the response leaves no connect or disconnect event.
func TestConnCountLeak(t *testing.T) {
	sw := startSwarm(t)
	sw.setPolicy(basePolicy)
	sw.maxConns = 1
	sw.delay = 2500 * time.Millisecond
	k := newKey(t)
	_, err0 := sw.connect(k, nats.NoReconnect(), nats.Timeout(5*time.Second))
	t.Logf("connect with a callout slower than auth_timeout: %v", err0)
	time.Sleep(1500 * time.Millisecond)
	sw.mu.Lock()
	n := sw.conns[pubOf(k)]
	sw.mu.Unlock()
	t.Logf("after an aborted connect the callout counts %d live connections", n)
	sw.delay = 0
	if _, err := sw.connect(k, nats.NoReconnect()); err != nil {
		t.Logf("LEAK: honest connect now refused: %v", err)
	} else {
		t.Logf("honest connect passed")
	}
	// Ground truth: CONNZ by account and user, from the system account.
	sys := sw.sysConn()
	resp, err := sys.Request("$SYS.REQ.SERVER.PING.CONNZ", fmt.Appendf(nil, `{"acc":%q,"user":%q}`, sw.courPub, pubOf(k)), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var z struct {
		Server struct{ ID string } `json:"server"`
		Data   struct {
			NumConns int `json:"num_connections"`
		} `json:"data"`
	}
	_ = json.Unmarshal(resp.Data, &z)
	t.Logf("CONNZ for the key: %d connections on server %s…", z.Data.NumConns, z.Server.ID[:8])

	// CONNZ finds a live connection by the JWT name = courier key.
	sw.mu.Lock()
	sw.conns[pubOf(k)] = 0
	sw.mu.Unlock()
	c := sw.mustConnect(k)
	cid, _ := c.nc.GetClientID()
	resp, _ = sys.Request("$SYS.REQ.SERVER.PING.CONNZ", fmt.Appendf(nil, `{"acc":%q,"user":%q}`, sw.courPub, pubOf(k)), time.Second)
	t.Logf("CONNZ by user=%s… with live cid %d: %s", pubOf(k)[:8], cid, resp.Data[strings.Index(string(resp.Data), `"connections"`):])
}

// TestReconnectStorm: one callout subscription answers serially. N clients
// whose JWTs expire together reconnect at once.
func TestReconnectStorm(t *testing.T) {
	for _, delay := range []time.Duration{0, 2 * time.Millisecond, 5 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			sw := startSwarm(t)
			sw.setPolicy(basePolicy)
			sw.delay = delay
			const n = 1000
			keys := make([]nkeys.KeyPair, n)
			for i := range keys {
				keys[i] = newKey(t)
			}
			var ok, fail atomic.Int64
			var wg sync.WaitGroup
			start := time.Now()
			for _, k := range keys {
				wg.Add(1)
				go func() {
					defer wg.Done()
					nc, err := nats.Connect(sw.url, nats.Nkey(pubOf(k), k.Sign), nats.NoReconnect())
					if err != nil {
						fail.Add(1)
						return
					}
					ok.Add(1)
					nc.Close()
				}()
			}
			wg.Wait()
			t.Logf("callout cost %v: %d ok, %d failed of %d, in %v", delay, ok.Load(), fail.Load(), n, time.Since(start))
		})
	}
}

// TestConnectLineLimit: binding or PoW material must fit in CONNECT, which
// the server caps at max_control_line (4096 by default).
func TestConnectLineLimit(t *testing.T) {
	sw := startSwarm(t)
	sw.setPolicy(basePolicy)
	for _, size := range []int{3000, 5000} {
		_, err := nats.Connect(sw.url, nats.Token(strings.Repeat("a", size)), nats.NoReconnect())
		t.Logf("auth_token of %d bytes: %v", size, err)
	}
}

func TestPoWRate(t *testing.T) {
	buf := make([]byte, 32+32+8)
	start := time.Now()
	const n = 5_000_000
	for i := range uint64(n) {
		binary.LittleEndian.PutUint64(buf[64:], i)
		_ = sha256.Sum256(buf)
	}
	el := time.Since(start)
	t.Logf("sha256 over 72 bytes: %.1f M/s on one core", float64(n)/el.Seconds()/1e6)
}

// TestScopedSigner: a scoped account signing key bounds what a leaked key
// can grant, but not which identity it names.
func TestScopedSigner(t *testing.T) {
	sw := startSwarm(t)
	sk := kp(t, nkeys.CreateAccount)
	sw.adminConn()
	time.Sleep(1100 * time.Millisecond)
	sw.pushAccount(func(ac *jwt.AccountClaims) {
		sc := jwt.NewUserScope()
		sc.Key = pubOf(sk)
		sc.Template.Pub.Allow.Add("courier.*.{{name()}}.*")
		sc.Template.Sub.Allow.Add("_INBOX_{{name()}}.>")
		ac.SigningKeys.AddScopedSigner(sc)
	})
	// A leaked scoped key mints a user that asks for everything.
	tok, seed := userJWT(t, "UVICTIM", sk, sw.courPub, func(uc *jwt.UserClaims) { uc.Pub.Allow.Add(">") })
	if _, err := nats.Connect(sw.url, nats.UserJWTAndSeed(tok, seed), nats.NoReconnect()); err != nil {
		t.Logf("scoped key, JWT with own permissions: %v", err)
	} else {
		t.Logf("scoped key, JWT with own permissions: accepted")
	}
	tok, seed = userJWT(t, "UVICTIM", sk, sw.courPub, func(uc *jwt.UserClaims) { uc.UserPermissionLimits = jwt.UserPermissionLimits{} })
	nc, err := nats.Connect(sw.url, nats.UserJWTAndSeed(tok, seed), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 4)
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { errs <- err })
	_ = nc.Publish("courier.t.UVICTIM.note", []byte("x"))
	_ = nc.Publish("$JS.API.STREAM.DELETE.COURIER", nil)
	_ = nc.Flush()
	select {
	case err := <-errs:
		t.Logf("scoped user: forged name publish ok, stream API: %v", err)
	case <-time.After(time.Second):
		t.Logf("scoped user: no violation")
	}
}
