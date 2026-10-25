package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/CappyT/agent-courier/internal/team"
)

const (
	testTeam  = "courier"
	testTopic = "cappyt_agent-courier"
)

// testClock is the invite clock of a node, which a test can move.
type testClock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.offset)
}

func (c *testClock) move(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offset += d
}

// syncLog is a log buffer that the services of a node write concurrently.
type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// keyHost is a host with key on loopback TCP.
func keyHost(t *testing.T, key crypto.PrivKey) host.Host {
	t.Helper()
	h, err := libp2p.New(
		libp2p.Identity(key),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.DisableRelay(),
		libp2p.DisableMetrics(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// node is a daemon in a test: a host, a member set, the join service, the
// member sync and a MsgProtocol handler that passes members frames to the
// sync.
type node struct {
	t     *testing.T
	key   crypto.PrivKey
	clock *testClock
	set   *team.Set
	log   *syncLog
	h     host.Host
	js    *JoinService
	ms    *MemberSync
}

func newNode(t *testing.T) *node {
	t.Helper()
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	n := &node{t: t, key: key, clock: &testClock{}, log: &syncLog{}}
	n.set = team.NewSet(n.clock.now)
	n.start()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("log of %s:\n%s", n.h.ID(), n.log.String())
		}
	})
	return n
}

// start runs the host and the services of n, as a daemon does when its
// first team starts.
func (n *node) start() {
	t := n.t
	t.Helper()
	log := slog.New(slog.NewTextHandler(n.log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	n.h = keyHost(t, n.key)
	n.ms = NewMemberSync(t.Context(), n.h, n.set, nil, log)
	t.Cleanup(n.ms.Close)
	var err error
	if n.js, err = NewJoinService(n.h, JoinConfig{Set: n.set, Changed: n.ms.Changed, Log: log}); err != nil {
		t.Fatal(err)
	}
	// The nodes of a test reach each other on loopback only.
	n.js.allowLoopback = true
	n.serveMsg()
}

// serveMsg sets the MsgProtocol handler of n, on its running host.
func (n *node) serveMsg() {
	m := NewMsgHandler(n.t.Context(), n.ms.IsMember, func(_ context.Context, from peer.ID, f Frame) (Frame, error) {
		if mf, ok := f.(MembersFrame); ok {
			n.ms.Receive(from, mf)
		}
		return nil, nil
	}, nil)
	n.h.SetStreamHandler(MsgProtocol, m.Handle)
}

// stop stops the host, as a machine that went off the network.
func (n *node) stop() {
	n.ms.Close()
	n.h.Close()
}

func (n *node) id() string { return n.h.ID().String() }

func (n *node) create(label string) {
	n.t.Helper()
	if _, err := n.set.Create(n.id(), testTeam, []string{testTopic}, label); err != nil {
		n.t.Fatal(err)
	}
	n.ms.Changed(testTeam)
}

// code is the invite code of n for the team, as P2P.InviteCode of the daemon
// makes it.
func (n *node) code(secret []byte, expires time.Time) (string, error) {
	return Invite{Team: testTeam, Inviter: n.h.ID(), Addrs: inviteAddrs(n.h.Addrs(), true), Secret: secret, Expires: expires}.Encode()
}

// invite returns a new code for the team and its secret.
func (n *node) invite() (string, []byte) {
	n.t.Helper()
	secret, expires, err := n.set.CreateInvite(testTeam)
	if err != nil {
		n.t.Fatal(err)
	}
	code, err := n.code(secret, expires)
	if err != nil {
		n.t.Fatal(err)
	}
	return code, secret
}

func (n *node) join(code, label string) (team.Team, error) {
	ctx, cancel := context.WithTimeout(n.t.Context(), 2*waitFor)
	defer cancel()
	return n.js.Join(ctx, code, label)
}

func (n *node) mustJoin(code, label string) {
	n.t.Helper()
	if _, err := n.join(code, label); err != nil {
		n.t.Fatalf("join as %s: %v", label, err)
	}
}

// members maps the peer ids of the team's entries to their labels, with a
// "left " prefix for a member that left. It is empty without the team.
func (n *node) members() map[string]string {
	out := map[string]string{}
	t, _ := n.set.Get(testTeam)
	for id, e := range t.Members {
		if e.Left {
			out[id] = "left " + e.Label
		} else {
			out[id] = e.Label
		}
	}
	return out
}

func (n *node) entry(peerID string) team.Entry {
	n.t.Helper()
	t, _ := n.set.Get(testTeam)
	e, ok := t.Members[peerID]
	if !ok {
		n.t.Fatalf("no entry of %s", peerID)
	}
	return e
}

// inviteOpen reports whether the invite of secret still admits a new peer.
func (n *node) inviteOpen(secret []byte) bool {
	n.t.Helper()
	_, err := n.set.CheckInvite(testTeam, secret, testPeer(n.t).String(), "zulu")
	if err != nil && !errors.Is(err, team.ErrInviteRefused) {
		n.t.Fatal(err)
	}
	return err == nil
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func wantMembers(t *testing.T, n *node, want map[string]string) {
	t.Helper()
	eventually(t, fmt.Sprintf("member list %v", want), func() bool {
		got := n.members()
		if len(got) != len(want) {
			return false
		}
		for k, v := range want {
			if got[k] != v {
				return false
			}
		}
		return true
	})
}

// noPeerIDsAbove checks that no log line above Debug names a peer id.
func noPeerIDsAbove(t *testing.T, nodes ...*node) {
	t.Helper()
	for _, n := range nodes {
		for line := range strings.Lines(n.log.String()) {
			if strings.Contains(line, "level=DEBUG") {
				continue
			}
			for _, o := range nodes {
				if strings.Contains(line, o.id()) {
					t.Errorf("log line above Debug names a peer id: %s", line)
				}
			}
		}
	}
}

func TestInviteCodeRoundTrip(t *testing.T) {
	inviter := testPeer(t)
	secret := bytes.Repeat([]byte{9}, team.SecretSize)
	addrs := []ma.Multiaddr{
		ma.StringCast("/ip4/192.0.2.1/tcp/4001"),
		ma.StringCast("/ip4/192.0.2.9/udp/4001/quic-v1/p2p/" + testPeer(t).String() + "/p2p-circuit"),
	}
	in := Invite{Team: testTeam, Inviter: inviter, Addrs: addrs, Secret: secret, Expires: time.Unix(1_800_000_000, 0)}
	code, err := in.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Trim(code, "abcdefghijklmnopqrstuvwxyz234567") != "" {
		t.Fatalf("code %q has characters outside lower-case base32", code)
	}
	for _, c := range []string{code, strings.ToUpper(code), "  " + code + "\n"} {
		out, err := DecodeInvite(c)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out.Team != in.Team || out.Inviter != in.Inviter || !bytes.Equal(out.Secret, secret) || !out.Expires.Equal(in.Expires) ||
			len(out.Addrs) != 2 || !out.Addrs[0].Equal(addrs[0]) || !out.Addrs[1].Equal(addrs[1]) {
			t.Fatalf("decoded %+v, want %+v", out, in)
		}
	}

	// The longest code fits the cap.
	long := in
	long.Team = strings.Repeat("t", 64)
	long.Addrs = nil
	for range team.MaxAddrs {
		long.Addrs = append(long.Addrs, ma.StringCast("/dns4/"+strings.Repeat("a", 240)+"/tcp/4001"))
	}
	if code, err := long.Encode(); err != nil || len(code) > MaxInviteCode {
		t.Fatalf("longest code: %d characters, %v", len(code), err)
	}
}

func TestInviteCodeRefused(t *testing.T) {
	good := Invite{Team: testTeam, Inviter: testPeer(t), Secret: make([]byte, team.SecretSize), Expires: time.Unix(1_800_000_000, 0),
		Addrs: []ma.Multiaddr{ma.StringCast("/ip4/192.0.2.1/tcp/4001")}}
	code, err := good.Encode()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := inviteEncoding.DecodeString(code)
	if err != nil {
		t.Fatal(err)
	}
	enc := inviteEncoding.EncodeToString
	bad := map[string]string{
		"empty":         "",
		"not base32":    "!!" + code,
		"too long":      strings.Repeat("a", MaxInviteCode+1),
		"version":       enc(append([]byte{2}, raw[1:]...)),
		"truncated":     enc(raw[:len(raw)-1]),
		"trailing byte": enc(append(append([]byte{}, raw...), 0)),
	}
	for name, c := range bad {
		if _, err := DecodeInvite(c); !errors.Is(err, ErrBadInviteCode) {
			t.Errorf("%s: %v, want ErrBadInviteCode", name, err)
		}
		if c != "" && strings.Contains(fmt.Sprint(err), c) {
			t.Errorf("%s: the error quotes the code", name)
		}
	}
	for name, change := range map[string]func(*Invite){
		"team rule":    func(i *Invite) { i.Team = "Bad Team" },
		"short secret": func(i *Invite) { i.Secret = i.Secret[:15] },
		"five addrs": func(i *Invite) {
			for range 4 {
				i.Addrs = append(i.Addrs, i.Addrs[0])
			}
		},
		"own p2p part": func(i *Invite) {
			i.Addrs = []ma.Multiaddr{ma.StringCast("/ip4/192.0.2.1/tcp/4001/p2p/" + i.Inviter.String())}
		},
	} {
		i := good
		change(&i)
		if _, err := i.Encode(); !errors.Is(err, ErrBadInviteCode) {
			t.Errorf("%s: %v, want ErrBadInviteCode", name, err)
		}
	}
	// inviteAddrs drops the own /p2p part and keeps at most 4 addresses.
	p := ma.StringCast("/ip4/192.0.2.1/tcp/4001/p2p/" + good.Inviter.String())
	got := inviteAddrs([]ma.Multiaddr{p, p, p, p, p}, false)
	if len(got) != team.MaxAddrs || !got[0].Equal(ma.StringCast("/ip4/192.0.2.1/tcp/4001")) {
		t.Fatalf("inviteAddrs = %v", got)
	}
}

// An invite code, its decoding and the join dial drop the addresses that
// name the joiner's own machine or network: /unix, loopback, link-local and
// unspecified addresses, and the name localhost.
func TestInviteDropsLocalAddrs(t *testing.T) {
	local := []string{"/ip4/127.0.0.1/tcp/4001", "/ip6/::1/tcp/4001", "/unix/run/courier.sock", "/ip4/169.254.169.254/tcp/80",
		"/ip6/fe80::1/tcp/4001", "/dns4/localhost/tcp/4001", "/ip4/0.0.0.0/tcp/4001", "/dns6/api.localhost/tcp/1"}
	keep := ma.StringCast("/ip4/192.0.2.1/tcp/4001")
	for _, l := range local {
		a := ma.StringCast(l)
		in := Invite{Team: testTeam, Inviter: testPeer(t), Addrs: []ma.Multiaddr{a, keep}, Secret: make([]byte, team.SecretSize),
			Expires: time.Unix(1_800_000_000, 0)}
		code, err := in.Encode()
		if err != nil {
			t.Fatal(err)
		}
		out, err := DecodeInvite(code)
		if err != nil || len(out.Addrs) != 1 || !out.Addrs[0].Equal(keep) {
			t.Errorf("%s: decoded %v, %v; want only %s", l, out.Addrs, err, keep)
		}
		if got := inviteAddrs([]ma.Multiaddr{a, keep}, false); len(got) != 1 || !got[0].Equal(keep) {
			t.Errorf("%s: inviteAddrs = %v", l, got)
		}
	}
	// A test on one machine keeps loopback, never the others.
	if got := inviteAddrs([]ma.Multiaddr{ma.StringCast(local[0]), ma.StringCast(local[2]), ma.StringCast(local[3])}, true); len(got) != 1 ||
		!got[0].Equal(ma.StringCast(local[0])) {
		t.Errorf("inviteAddrs with loopback = %v", got)
	}
}

// A code with only loopback addresses does not make the joiner dial its own
// machine: the join fails as an unreachable inviter, and no connection opens.
func TestJoinDialsNoLocalAddrs(t *testing.T) {
	a, b := newNode(t), newNode(t)
	a.create("alpha")
	code, secret := a.invite()
	b.js.allowLoopback = false
	if _, err := b.join(code, "bravo"); !errors.Is(err, ErrInviterUnreachable) {
		t.Fatalf("join with a loopback code: %v", err)
	}
	if b.h.Network().Connectedness(a.h.ID()) == network.Connected {
		t.Fatal("the joiner dialed a loopback address of the code")
	}
	if !a.inviteOpen(secret) || len(b.members()) != 0 {
		t.Fatal("the join changed a store")
	}
}

// The joiner refuses a code of its own, before it dials, also when it no
// longer holds the team.
func TestJoinOwnCode(t *testing.T) {
	b := newNode(t)
	b.create("bravo")
	code, _ := b.invite()
	if _, err := b.set.Leave(testTeam); err != nil {
		t.Fatal(err)
	}
	if _, err := b.join(code, "bravo"); !errors.Is(err, ErrBadInviteCode) {
		t.Fatalf("join with an own code: %v", err)
	}
}

// Successful join: B joins with a code from A, in a team where A already
// knows C.
func TestJoin(t *testing.T) {
	a, b, c := newNode(t), newNode(t), newNode(t)
	a.create("alpha")
	code, _ := a.invite()
	c.mustJoin(code, "charlie")
	code, secret := a.invite()

	before := uint64(time.Now().UnixMilli())
	got, err := b.join(code, "bravo")
	if err != nil {
		t.Fatal(err)
	}
	joined := got.Members[b.id()]
	if joined.Join < before {
		t.Fatalf("join counter %d before the time of the join, %d", joined.Join, before)
	}
	if got.Name != testTeam || len(got.Topics) != 1 || got.Topics[0] != testTopic {
		t.Fatalf("joined %+v", got)
	}
	wantMembers(t, b, map[string]string{a.id(): "alpha", b.id(): "bravo", c.id(): "charlie"})
	wantMembers(t, a, map[string]string{a.id(): "alpha", b.id(): "bravo", c.id(): "charlie"})

	// B set its addresses with a new clock join counter, and A takes the
	// new entry.
	if len(joined.Addrs) == 0 || joined.Left {
		t.Fatalf("B's own entry after the join: %+v", joined)
	}
	eventually(t, "A has B's addresses", func() bool { return a.entry(b.id()).Join == joined.Join })
	if a.inviteOpen(secret) {
		t.Fatal("the used invite is still open")
	}
	noPeerIDsAbove(t, a, b, c)
}

// Inviter offline: the join fails with an error that says the inviter is not
// reachable, and the code stays valid.
func TestJoinInviterOffline(t *testing.T) {
	a, b := newNode(t), newNode(t)
	a.create("alpha")
	code, secret := a.invite()
	a.stop()

	_, err := b.join(code, "bravo")
	if !errors.Is(err, ErrInviterUnreachable) || !strings.Contains(err.Error(), "not reachable") {
		t.Fatalf("join with the inviter offline: %v", err)
	}
	if !a.inviteOpen(secret) {
		t.Fatal("the code is used after a failed dial")
	}
	if len(b.members()) != 0 {
		t.Fatal("B stored the team")
	}

	// The host of A is back, on new addresses that B learns from elsewhere,
	// and the same code works.
	a.start()
	b.h.Peerstore().AddAddrs(a.h.ID(), a.h.Addrs(), time.Minute)
	b.mustJoin(code, "bravo")
	wantMembers(t, a, map[string]string{a.id(): "alpha", b.id(): "bravo"})
}

// A taken label refuses the join and leaves the code valid.
func TestJoinLabelTaken(t *testing.T) {
	a, b := newNode(t), newNode(t)
	a.create("alpha")
	code, secret := a.invite()

	if _, err := b.join(code, "alpha"); !errors.Is(err, team.ErrLabelTaken) {
		t.Fatalf("join with a taken label: %v", err)
	}
	if !a.inviteOpen(secret) || len(b.members()) != 0 {
		t.Fatal("a refused join changed a store")
	}
	b.mustJoin(code, "bravo")
}

// Rejoin after a restart: while B is connected to A, its label refuses a
// join. When B's daemon restarts, it is a new peer id, and the join with its
// old label marks the old peer id left on A and on C.
func TestJoinRejoinAfterRestart(t *testing.T) {
	a, b, c, d := newNode(t), newNode(t), newNode(t), newNode(t)
	a.create("alpha")
	code, _ := a.invite()
	b.mustJoin(code, "bravo")
	code, _ = a.invite()
	c.mustJoin(code, "charlie")
	reconnect(t, b, a)

	code, secret := a.invite()
	if _, err := d.join(code, "bravo"); !errors.Is(err, team.ErrLabelTaken) {
		t.Fatalf("join with the label of a connected member: %v", err)
	}
	if !a.inviteOpen(secret) {
		t.Fatal("a refused join used the code")
	}

	b.stop()
	eventually(t, "A sees B disconnected", func() bool { return a.h.Network().Connectedness(b.h.ID()) != network.Connected })
	restarted := newNode(t)
	restarted.mustJoin(code, "bravo")
	want := map[string]string{a.id(): "alpha", b.id(): "left bravo", c.id(): "charlie", restarted.id(): "bravo"}
	wantMembers(t, a, want)
	wantMembers(t, c, want)
	wantMembers(t, restarted, want)
}

// Code used twice: the second machine's join fails with the one refusal.
func TestJoinCodeReused(t *testing.T) {
	a, b, d := newNode(t), newNode(t), newNode(t)
	a.create("alpha")
	code, _ := a.invite()
	b.mustJoin(code, "bravo")

	if _, err := d.join(code, "delta"); !errors.Is(err, team.ErrInviteRefused) {
		t.Fatalf("join with a used code: %v", err)
	}
	if _, ok := a.members()[d.id()]; ok || len(d.members()) != 0 {
		t.Fatal("the reused code let D in")
	}
}

// Expired code: the joiner reads the expiry in the code and fails before it
// dials, with an error that says the code expired. The inviter checks its
// own record too, with the one refusal.
func TestJoinExpired(t *testing.T) {
	a, b := newNode(t), newNode(t)
	a.create("alpha")
	secret, _, err := a.set.CreateInvite(testTeam)
	if err != nil {
		t.Fatal(err)
	}
	old, err := a.code(secret, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.join(old, "bravo"); !errors.Is(err, ErrInviteExpired) || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("join with an expired code: %v", err)
	}
	if b.h.Network().Connectedness(a.h.ID()) == network.Connected {
		t.Fatal("the joiner dialed with an expired code")
	}

	code, _ := a.invite()
	a.clock.move(25 * time.Hour)
	if _, err := b.join(code, "bravo"); !errors.Is(err, team.ErrInviteRefused) {
		t.Fatalf("join 25 hours later: %v", err)
	}
	if _, ok := a.members()[b.id()]; ok {
		t.Fatal("the expired invite let B in")
	}
}

// A tampered secret, and a code that names another team of the inviter, get
// the one refusal.
func TestJoinTamperedAndWrongTeam(t *testing.T) {
	a, b := newNode(t), newNode(t)
	a.create("alpha")
	if _, err := a.set.Create(a.id(), "other", []string{"cappyt_other"}, "alpha"); err != nil {
		t.Fatal(err)
	}
	code, secret := a.invite()
	inv, err := decodeInvite(code, true)
	if err != nil {
		t.Fatal(err)
	}
	tampered := inv
	tampered.Secret = bytes.Clone(secret)
	tampered.Secret[0] ^= 1
	other := inv
	other.Team = "other"
	for name, i := range map[string]Invite{"tampered secret": tampered, "wrong team": other} {
		c, err := i.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.join(c, "bravo"); !errors.Is(err, team.ErrInviteRefused) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if !a.inviteOpen(secret) {
		t.Fatal("a refused code used the invite")
	}
	b.mustJoin(code, "bravo")
}

// A topic that another team of the joiner owns refuses the join on the
// joiner, before the inviter writes anything: the code stays valid.
func TestJoinTopicTaken(t *testing.T) {
	a, b := newNode(t), newNode(t)
	a.create("alpha")
	if _, err := b.set.Create(b.id(), "mine", []string{testTopic}, "bravo"); err != nil {
		t.Fatal(err)
	}
	code, secret := a.invite()
	if _, err := b.join(code, "bravo"); !errors.Is(err, team.ErrTopicTaken) {
		t.Fatalf("join with a taken topic: %v", err)
	}
	if !a.inviteOpen(secret) {
		t.Fatal("the invite is used")
	}
	if _, ok := a.members()[b.id()]; ok {
		t.Fatal("the inviter added B")
	}
}

// openJoin opens a raw join stream from h to n.
func openJoin(t *testing.T, h host.Host, n *node) network.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	if err := h.Connect(ctx, peer.AddrInfo{ID: n.h.ID(), Addrs: n.h.Addrs()}); err != nil {
		t.Fatal(err)
	}
	s, err := h.NewStream(ctx, n.h.ID(), JoinProtocol)
	if err != nil {
		t.Fatal(err)
	}
	s.SetDeadline(time.Now().Add(waitFor))
	t.Cleanup(func() { s.Reset() })
	return s
}

// A join frame that names a peer id is refused: the inviter takes the
// joiner's peer id only from the connection.
func TestJoinForgedPeerID(t *testing.T) {
	a, c := newNode(t), newNode(t)
	a.create("alpha")
	_, secret := a.invite()
	e := newHost(t)

	body := fmt.Sprintf(`{"type":"join","team":%q,"secret":%q,"label":"echo","peer_id":%q}`,
		testTeam, base64.StdEncoding.EncodeToString(secret), c.id())
	s := openJoin(t, e, a)
	if _, err := exchange(s, frameOf(body)); err == nil {
		t.Fatal("a join frame with a peer id was answered")
	} else {
		wantReset(t, err, network.StreamProtocolViolation)
	}
	if !a.inviteOpen(secret) || len(a.members()) != 1 {
		t.Fatal("the forged join changed the inviter's member set")
	}

	// The same secret in a valid frame admits the peer of the connection.
	s = openJoin(t, e, a)
	rw := joinIO{s: s, end: time.Now().Add(waitFor), frame: waitFor}
	if err := rw.write(joinFrame{Team: testTeam, Secret: secret, Label: "echo"}); err != nil {
		t.Fatal(err)
	}
	if f, err := rw.read(); err != nil || f.(joinReplyFrame).Result != joinOffer {
		t.Fatalf("reply %v, %v", f, err)
	}
	if err := rw.write(joinAcceptFrame{}); err != nil {
		t.Fatal(err)
	}
	f, err := rw.read()
	if err != nil {
		t.Fatal(err)
	}
	if mf := f.(MembersFrame); mf.Entries[0].PeerID != e.ID().String() || mf.Entries[0].Left || mf.Entries[1].PeerID != a.id() {
		t.Fatalf("entries %+v, want the connection's peer, then A", mf.Entries)
	}
	wantMembers(t, a, map[string]string{a.id(): "alpha", e.ID().String(): "echo"})
}

// A stranger that never joins: its join stream with a wrong secret is
// refused, its msg stream is reset before a byte is read, and its members
// frame changes nothing.
func TestStrangerNeverJoins(t *testing.T) {
	a := newNode(t)
	a.create("alpha")
	s := newNode(t)

	rw := joinIO{s: openJoin(t, s.h, a), end: time.Now().Add(waitFor), frame: waitFor}
	if err := rw.write(joinFrame{Team: testTeam, Secret: make([]byte, team.SecretSize), Label: "sierra"}); err != nil {
		t.Fatal(err)
	}
	if f, err := rw.read(); err != nil || f.(joinReplyFrame).Result != joinRefused {
		t.Fatalf("reply to a wrong secret: %v, %v", f, err)
	}
	// An unknown team gets the same refusal, so a stranger learns no names.
	rw = joinIO{s: openJoin(t, s.h, a), end: time.Now().Add(waitFor), frame: waitFor}
	if err := rw.write(joinFrame{Team: "nosuch", Secret: make([]byte, team.SecretSize), Label: "sierra"}); err != nil {
		t.Fatal(err)
	}
	if f, err := rw.read(); err != nil || f.(joinReplyFrame).Result != joinRefused {
		t.Fatalf("reply for an unknown team: %v, %v", f, err)
	}

	// S sends an entry that adds itself.
	entry := team.Entry{PeerID: s.id(), Label: "sierra", Join: 1}
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	ms, err := s.h.NewStream(ctx, a.h.ID(), MsgProtocol)
	if err != nil {
		t.Fatal(err)
	}
	_, err = exchange(ms, encode(t, MembersFrame{Team: testTeam, Entries: []team.Entry{entry}}))
	wantReset(t, err, network.StreamGated)
	// Receive ignores it too, whatever handler passes it.
	a.ms.Receive(s.h.ID(), MembersFrame{Team: testTeam, Entries: []team.Entry{entry}})
	if got := a.members(); len(got) != 1 {
		t.Fatalf("the stranger changed the member list: %v", got)
	}
}

// The join handler caps the streams of one peer and bounds a silent stream
// with the frame timeout.
func TestJoinStreamLimits(t *testing.T) {
	a := newNode(t)
	a.create("alpha")
	a.js.frameTimeout = 300 * time.Millisecond
	e := newHost(t)

	first := openJoin(t, e, a)
	// Make sure the handler holds the first stream before the second opens.
	if _, err := first.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	second := openJoin(t, e, a)
	if _, err := exchange(second, frameOf(`{}`)); err == nil {
		t.Fatal("a second join stream of one peer was served")
	} else {
		wantReset(t, err, network.StreamResourceLimitExceeded)
	}
	start := time.Now()
	_, err := first.Read(make([]byte, 1))
	wantReset(t, err, network.StreamProtocolViolation)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("a silent join stream lived %v", d)
	}
}

// fakeInviter serves JoinProtocol on h with an offer and the members frames
// that frames returns, to test what the joiner accepts.
func fakeInviter(t *testing.T, h host.Host, frames func(joiner peer.ID) []Frame) {
	h.SetStreamHandler(JoinProtocol, func(s network.Stream) {
		defer s.Close()
		rw := joinIO{s: s, end: time.Now().Add(waitFor), frame: waitFor}
		if _, err := rw.read(); err != nil {
			return
		}
		if err := rw.write(joinReplyFrame{Result: joinOffer, Topics: []string{testTopic}}); err != nil {
			return
		}
		if _, err := rw.read(); err != nil {
			return
		}
		for _, f := range frames(s.Conn().RemotePeer()) {
			if err := rw.write(f); err != nil {
				return
			}
		}
	})
}

// The joiner checks the inviter's list: a list without the inviter or
// without the joiner with its label, or frames of another team, fail the
// join and store nothing.
func TestJoinChecksList(t *testing.T) {
	a, b := newNode(t), newNode(t)
	a.create("alpha")
	own := a.entry(a.id())
	joinerEntry := func(j peer.ID, label string) team.Entry {
		return team.Entry{PeerID: j.String(), Label: label, Join: 1}
	}
	cases := map[string]func(joiner peer.ID) []Frame{
		"no own entry": func(peer.ID) []Frame { return []Frame{MembersFrame{Team: testTeam, Entries: []team.Entry{own}}} },
		"no inviter": func(j peer.ID) []Frame {
			return []Frame{MembersFrame{Team: testTeam, Entries: []team.Entry{joinerEntry(j, "bravo")}}}
		},
		"inviter left": func(j peer.ID) []Frame {
			left := own
			left.Left = true
			return []Frame{MembersFrame{Team: testTeam, Entries: []team.Entry{joinerEntry(j, "bravo"), left}}}
		},
		"own entry left": func(j peer.ID) []Frame {
			e := joinerEntry(j, "bravo")
			e.Left = true
			return []Frame{MembersFrame{Team: testTeam, Entries: []team.Entry{e, own}}}
		},
		"other label": func(j peer.ID) []Frame {
			return []Frame{MembersFrame{Team: testTeam, Entries: []team.Entry{joinerEntry(j, "other"), own}}}
		},
		"other team": func(j peer.ID) []Frame {
			return []Frame{MembersFrame{Team: "other", Entries: []team.Entry{joinerEntry(j, "bravo"), own}}}
		},
		"no list": func(peer.ID) []Frame { return nil },
	}
	for name, frames := range cases {
		fakeInviter(t, a.h, frames)
		code, _ := a.invite()
		if _, err := b.join(code, "bravo"); !errors.Is(err, ErrJoinFailed) {
			t.Errorf("%s: %v, want ErrJoinFailed", name, err)
		}
		if len(b.members()) != 0 {
			t.Fatalf("%s: B stored the team", name)
		}
	}
	// The honest reply passes.
	fakeInviter(t, a.h, func(j peer.ID) []Frame {
		return []Frame{MembersFrame{Team: testTeam, Entries: []team.Entry{joinerEntry(j, "bravo"), own}}}
	})
	code, _ := a.invite()
	b.mustJoin(code, "bravo")
}

// The inviter takes only join_accept after its offer: another frame resets the
// stream, and the code stays valid.
func TestJoinAcceptFrameType(t *testing.T) {
	a := newNode(t)
	a.create("alpha")
	_, secret := a.invite()
	e := newHost(t)
	rw := joinIO{s: openJoin(t, e, a), end: time.Now().Add(waitFor), frame: waitFor}
	req := joinFrame{Team: testTeam, Secret: secret, Label: "echo"}
	if err := rw.write(req); err != nil {
		t.Fatal(err)
	}
	if f, err := rw.read(); err != nil || f.(joinReplyFrame).Result != joinOffer {
		t.Fatalf("reply %v, %v", f, err)
	}
	if err := rw.write(req); err != nil {
		t.Fatal(err)
	}
	_, err := rw.read()
	wantReset(t, err, network.StreamProtocolViolation)
	if !a.inviteOpen(secret) || len(a.members()) != 1 {
		t.Fatal("a join without join_accept changed the inviter's member set")
	}
}

// A join frame carries a secret of team.SecretSize bytes: the joiner does
// not send another, and the inviter resets a stream that does.
func TestJoinFrameSecretSize(t *testing.T) {
	for _, n := range []int{team.SecretSize - 1, team.SecretSize + 1} {
		if err := (joinFrame{Team: testTeam, Secret: make([]byte, n), Label: "echo"}).Validate(); err == nil {
			t.Errorf("a secret of %d bytes passed", n)
		}
	}
	a := newNode(t)
	a.create("alpha")
	body := fmt.Sprintf(`{"type":"join","team":%q,"secret":%q,"label":"echo"}`, testTeam,
		base64.StdEncoding.EncodeToString(make([]byte, team.SecretSize-1)))
	_, err := exchange(openJoin(t, newHost(t), a), frameOf(body))
	wantReset(t, err, network.StreamProtocolViolation)
}

// The inviter serves at most MaxJoinStreams join streams at once, from all
// peers, and resets the others before it reads a byte.
func TestJoinStreamTotalCap(t *testing.T) {
	a := newNode(t)
	a.create("alpha")
	for range MaxJoinStreams {
		s := openJoin(t, newHost(t), a)
		if _, err := s.Write([]byte{0}); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "the inviter holds every stream", func() bool {
		a.js.mu.Lock()
		defer a.js.mu.Unlock()
		return a.js.total == MaxJoinStreams
	})
	_, err := exchange(openJoin(t, newHost(t), a), frameOf(`{}`))
	wantReset(t, err, network.StreamResourceLimitExceeded)
}

// The joiner reads at most maxJoinFrames members frames: an inviter that
// sends more fails the join.
func TestJoinMembersFrameCap(t *testing.T) {
	a, b := newNode(t), newNode(t)
	a.create("alpha")
	own := a.entry(a.id())
	frames := func(n int) func(peer.ID) []Frame {
		return func(j peer.ID) []Frame {
			out := []Frame{MembersFrame{Team: testTeam, Entries: []team.Entry{{PeerID: j.String(), Label: "bravo", Join: 1}, own}}}
			for len(out) < n {
				out = append(out, MembersFrame{Team: testTeam, Entries: []team.Entry{own}})
			}
			return out
		}
	}
	fakeInviter(t, a.h, frames(maxJoinFrames+1))
	code, _ := a.invite()
	if _, err := b.join(code, "bravo"); !errors.Is(err, ErrJoinFailed) {
		t.Fatalf("%d members frames: %v, want ErrJoinFailed", maxJoinFrames+1, err)
	}
	if len(b.members()) != 0 {
		t.Fatal("B stored the team")
	}
	fakeInviter(t, a.h, frames(maxJoinFrames))
	code, _ = a.invite()
	b.mustJoin(code, "bravo")
}

// DecodeInvite refuses a code over MaxInviteCode characters before it decodes
// a byte.
func TestDecodeInviteLengthCap(t *testing.T) {
	long := strings.Repeat("a", MaxInviteCode+8)
	if _, err := DecodeInvite(long); !errors.Is(err, ErrBadInviteCode) {
		t.Fatalf("a code of %d characters: %v", len(long), err)
	}
	if n := testing.AllocsPerRun(10, func() { DecodeInvite(long) }); n != 0 {
		t.Fatalf("DecodeInvite of a code over the cap allocates %v times: it decoded the code", n)
	}
}
