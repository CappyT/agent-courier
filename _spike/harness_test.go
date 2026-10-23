// Package spike holds throwaway tests for issue #1: the NATS primitives of
// the open-swarm design (docs/adr/0001-open-swarms.md).
package spike

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// policy is what the callout grants one client key. A nil policy refuses.
type policy struct {
	pub, sub []string
	limits   jwt.NatsLimits
	expires  time.Duration
}

// swarm is an operator-mode server with three accounts: SYS, AUTH (the
// callout account clients land in through the default sentinel) and COURIER
// (JetStream, where the callout puts every client).
type swarm struct {
	t   *testing.T
	srv *server.Server
	url string

	opKP, sysKP, authKP, courKP, courSK nkeys.KeyPair
	courPub                             string
	courClaims                          *jwt.AccountClaims

	mu            sync.Mutex
	policyOf      func(key string) *policy
	issued        map[string][]string // client key -> ephemeral user subjects
	maxConns      int
	conns         map[string]int // client key -> live connections (callout view)
	byEph         map[string]string
	calloutN      int
	lastDisc      string
	callout       *nats.Subscription
	connectDomain string
	delay         time.Duration
}

func kp(t *testing.T, f func() (nkeys.KeyPair, error)) nkeys.KeyPair {
	t.Helper()
	k, err := f()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func pubOf(k nkeys.KeyPair) string { p, _ := k.PublicKey(); return p }

func userJWT(t *testing.T, name string, signer nkeys.KeyPair, issuerAccount string, mod func(*jwt.UserClaims)) (string, string) {
	t.Helper()
	u := kp(t, nkeys.CreateUser)
	uc := jwt.NewUserClaims(pubOf(u))
	uc.Name = name
	uc.IssuerAccount = issuerAccount
	if mod != nil {
		mod(uc)
	}
	tok, err := uc.Encode(signer)
	if err != nil {
		t.Fatal(err)
	}
	seed, _ := u.Seed()
	return tok, string(seed)
}

func startSwarm(t *testing.T) *swarm {
	t.Helper()
	sw := &swarm{t: t, issued: map[string][]string{}, conns: map[string]int{}, byEph: map[string]string{}}
	sw.opKP = kp(t, nkeys.CreateOperator)
	sw.sysKP = kp(t, nkeys.CreateAccount)
	sw.authKP = kp(t, nkeys.CreateAccount)
	sw.courKP = kp(t, nkeys.CreateAccount)
	sw.courSK = kp(t, nkeys.CreateAccount)
	sw.courPub = pubOf(sw.courKP)

	oc := jwt.NewOperatorClaims(pubOf(sw.opKP))
	oc.Name = "spike"
	ojwt, err := oc.Encode(sw.opKP)
	if err != nil {
		t.Fatal(err)
	}
	sc := jwt.NewAccountClaims(pubOf(sw.sysKP))
	sc.Name = "SYS"
	sjwt, _ := sc.Encode(sw.opKP)

	// The callout service user, signed by the AUTH account.
	calloutJWT, calloutSeed := userJWT(t, "callout", sw.authKP, "", nil)
	cu, _ := jwt.DecodeUserClaims(calloutJWT)
	ac := jwt.NewAccountClaims(pubOf(sw.authKP))
	ac.Name = "AUTH"
	ac.EnableExternalAuthorization(cu.Subject)
	ac.Authorization.AllowedAccounts.Add(sw.courPub)
	ajwt, err := ac.Encode(sw.opKP)
	if err != nil {
		t.Fatal(err)
	}

	sw.courClaims = jwt.NewAccountClaims(sw.courPub)
	sw.courClaims.Name = "COURIER"
	sw.courClaims.SigningKeys.Add(pubOf(sw.courSK))
	sw.courClaims.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: -1, DiskStorage: -1, Streams: -1, Consumer: -1}
	cjwt, err := sw.courClaims.Encode(sw.opKP)
	if err != nil {
		t.Fatal(err)
	}

	// Every client without a JWT gets this bearer sentinel in AUTH, so the
	// callout sees it. The sentinel itself may do nothing.
	sentinel, _ := userJWT(t, "sentinel", sw.authKP, "", func(uc *jwt.UserClaims) {
		uc.BearerToken = true
		uc.Pub.Deny.Add(">")
		uc.Sub.Deny.Add(">")
	})

	dir := t.TempDir()
	conf := fmt.Sprintf(`
listen: 127.0.0.1:-1
server_name: spike
operator: %s
system_account: %s
resolver: MEM
resolver_preload: {
  %s: %s
  %s: %s
  %s: %s
}
default_sentinel: %s
jetstream: { store_dir: %q }
`, ojwt, pubOf(sw.sysKP), pubOf(sw.sysKP), sjwt, pubOf(sw.authKP), ajwt, sw.courPub, cjwt, sentinel, filepath.Join(dir, "js"))
	cf := filepath.Join(dir, "server.conf")
	if err := os.WriteFile(cf, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(cf)
	if err != nil {
		t.Fatal(err)
	}
	opts.NoLog = os.Getenv("SPIKE_LOG") == ""
	opts.Debug = os.Getenv("SPIKE_LOG") != ""
	srv, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	if !opts.NoLog {
		srv.ConfigureLogger()
	}
	go srv.Start()
	t.Cleanup(srv.Shutdown)
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	sw.srv, sw.url = srv, srv.ClientURL()

	nc, err := nats.Connect(sw.url, nats.UserJWTAndSeed(calloutJWT, calloutSeed))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	sw.callout, err = nc.Subscribe(server.AuthCalloutSubject, sw.handle)
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	// The callout tracks live connections per key through the system
	// account's disconnect events, because a user JWT has no connection limit.
	sys := sw.sysConn()
	if _, err := sys.Subscribe(fmt.Sprintf("$SYS.ACCOUNT.%s.DISCONNECT", sw.courPub), func(m *nats.Msg) {
		var ev server.DisconnectEventMsg
		if err := jsonUnmarshal(m.Data, &ev); err != nil {
			return
		}
		sw.mu.Lock()
		defer sw.mu.Unlock()
		// The server reports the user as the JWT name the callout set.
		if sw.conns[ev.Client.User] > 0 {
			sw.conns[ev.Client.User]--
		}
		sw.lastDisc = fmt.Sprintf("user=%s name_tag=%s", ev.Client.User, ev.Client.NameTag)
	}); err != nil {
		t.Fatal(err)
	}
	_ = sys.Flush()
	return sw
}

// handle is the auth callout: it checks that the client owns the nkey it
// presents, then signs a user JWT for COURIER with the account signing key.
func (sw *swarm) handle(m *nats.Msg) {
	req, err := jwt.DecodeAuthorizationRequestClaims(string(m.Data))
	if err != nil {
		sw.t.Logf("callout: bad request: %v", err)
		return
	}
	sw.mu.Lock()
	sw.calloutN++
	sw.mu.Unlock()
	refuse := func(reason string) {
		rc := jwt.NewAuthorizationResponseClaims(req.UserNkey)
		rc.Audience = req.Server.ID
		rc.Error = reason
		tok, _ := rc.Encode(sw.authKP)
		_ = m.Respond([]byte(tok))
	}
	key := req.ConnectOptions.Nkey
	pk, err := nkeys.FromPublicKey(key)
	if err != nil {
		refuse("no nkey")
		return
	}
	sig, err := base64.RawURLEncoding.DecodeString(req.ConnectOptions.SignedNonce)
	if err != nil {
		if sig, err = base64.StdEncoding.DecodeString(req.ConnectOptions.SignedNonce); err != nil {
			refuse("bad signature encoding")
			return
		}
	}
	signed := []byte(req.ClientInformation.Nonce)
	if sw.connectDomain != "" {
		signed = []byte("agent-courier-connect-v1\n" + sw.connectDomain + "\n" + req.ClientInformation.Nonce)
	}
	if sw.delay > 0 {
		time.Sleep(sw.delay)
	}
	if err := pk.Verify(signed, sig); err != nil {
		refuse("nonce signature does not match nkey")
		return
	}
	sw.mu.Lock()
	pf := sw.policyOf
	maxConns := sw.maxConns
	live := sw.conns[key]
	sw.mu.Unlock()
	var p *policy
	if pf != nil {
		p = pf(key)
	}
	if p == nil {
		refuse("refused by policy")
		return
	}
	if maxConns > 0 && live >= maxConns {
		refuse("too many connections for this key")
		return
	}
	uc := jwt.NewUserClaims(req.UserNkey)
	uc.Name = key
	uc.IssuerAccount = sw.courPub
	uc.Pub.Allow.Add(p.pub...)
	uc.Sub.Allow.Add(p.sub...)
	uc.Limits.NatsLimits = p.limits
	if p.expires > 0 {
		uc.Expires = time.Now().Add(p.expires).Unix()
	}
	ujwt, err := uc.Encode(sw.courSK)
	if err != nil {
		refuse(err.Error())
		return
	}
	rc := jwt.NewAuthorizationResponseClaims(req.UserNkey)
	rc.Audience = req.Server.ID
	rc.Jwt = ujwt
	tok, err := rc.Encode(sw.authKP)
	if err != nil {
		sw.t.Logf("callout: encode: %v", err)
		return
	}
	sw.mu.Lock()
	sw.issued[key] = append(sw.issued[key], req.UserNkey)
	sw.byEph[req.UserNkey] = key
	sw.conns[key]++
	sw.mu.Unlock()
	_ = m.Respond([]byte(tok))
}

func (sw *swarm) setPolicy(f func(key string) *policy) {
	sw.mu.Lock()
	sw.policyOf = f
	sw.mu.Unlock()
}

// sysConn connects a system account user, as the callout would to push
// account updates.
func (sw *swarm) sysConn() *nats.Conn {
	tok, seed := userJWT(sw.t, "sys", sw.sysKP, "", nil)
	nc, err := nats.Connect(sw.url, nats.UserJWTAndSeed(tok, seed))
	if err != nil {
		sw.t.Fatal(err)
	}
	sw.t.Cleanup(nc.Close)
	return nc
}

// adminConn connects an unrestricted COURIER user signed by the account
// key: the operator, and the callout when it writes leases and claims.
func (sw *swarm) adminConn() *nats.Conn {
	tok, seed := userJWT(sw.t, "admin", sw.courSK, sw.courPub, nil)
	nc, err := nats.Connect(sw.url, nats.UserJWTAndSeed(tok, seed))
	if err != nil {
		sw.t.Fatal(err)
	}
	sw.t.Cleanup(nc.Close)
	return nc
}

// pushAccount signs the COURIER claims again and sends them to the server.
func (sw *swarm) pushAccount(mod func(*jwt.AccountClaims)) time.Time {
	sw.t.Helper()
	mod(sw.courClaims)
	tok, err := sw.courClaims.Encode(sw.opKP)
	if err != nil {
		sw.t.Fatal(err)
	}
	sys := sw.sysConn()
	start := time.Now()
	resp, err := sys.Request(fmt.Sprintf("$SYS.REQ.ACCOUNT.%s.CLAIMS.UPDATE", sw.courPub), []byte(tok), 2*time.Second)
	if err != nil {
		sw.t.Fatal(err)
	}
	sw.t.Logf("claims update response: %s", resp.Data)
	return start
}

// client is one participant: an nkey and a connection through the callout.
type client struct {
	key  nkeys.KeyPair
	id   string
	nc   *nats.Conn
	errs chan error
}

func newKey(t *testing.T) nkeys.KeyPair { return kp(t, nkeys.CreateUser) }

func (sw *swarm) connect(k nkeys.KeyPair, extra ...nats.Option) (*client, error) {
	id := pubOf(k)
	c := &client{key: k, id: id, errs: make(chan error, 64)}
	opts := append([]nats.Option{
		nats.Nkey(id, k.Sign),
		nats.CustomInboxPrefix("_INBOX_" + id),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			select {
			case c.errs <- err:
			default:
			}
		}),
	}, extra...)
	nc, err := nats.Connect(sw.url, opts...)
	if err != nil {
		return nil, err
	}
	sw.t.Cleanup(nc.Close)
	c.nc = nc
	return c, nil
}

func (sw *swarm) mustConnect(k nkeys.KeyPair, extra ...nats.Option) *client {
	sw.t.Helper()
	c, err := sw.connect(k, extra...)
	if err != nil {
		sw.t.Fatal(err)
	}
	return c
}

// permErr waits for an async permission violation on c.
func (c *client) permErr(t *testing.T, within time.Duration) error {
	t.Helper()
	select {
	case err := <-c.errs:
		return err
	case <-time.After(within):
		return nil
	}
}
