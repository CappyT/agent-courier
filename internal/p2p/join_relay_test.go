//go:build unix

package p2p

import (
	"context"
	"crypto/rand"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/CappyT/agent-courier/internal/team"
)

// relayNode is a node on a Host without listen addresses that holds a
// reservation on the relays of relays, as the daemon wires it: peers reach
// it only through a circuit, which is a limited connection.
func relayNode(t *testing.T, relays []string) (*node, *Host) {
	t.Helper()
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	n := &node{t: t, key: key, clock: &testClock{}, log: &syncLog{}}
	n.set = team.NewSet(n.clock.now)
	h := startTestHost(t, nil, relays, &members{}, func(c *HostConfig) {
		c.Key = key
		c.IsMember = func(id peer.ID) bool { return n.set.IsMember(id.String()) }
		c.MemberOf = func(id peer.ID, name string) bool { return n.set.MemberOf(id.String(), name) }
	})
	n.h = h.Host()
	log := slog.New(slog.NewTextHandler(n.log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	n.ms = NewMemberSync(t.Context(), n.h, n.set, nil, log)
	// Without limited connections a send waits for a direct connection
	// until its timeout: keep the failure short.
	n.ms.timeout = waitFor
	t.Cleanup(n.ms.Close)
	if n.js, err = NewJoinService(n.h, JoinConfig{Set: n.set, Addrs: func() []ma.Multiaddr { return h.AdvertiseAddrs("") },
		Changed: n.ms.Changed, Log: log}); err != nil {
		t.Fatal(err)
	}
	n.js.allowLoopback = true
	n.js.dialTimeout = waitFor
	n.serveMsg()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("log of %s:\n%s", n.h.ID(), n.log.String())
		}
	})
	return n, h
}

// The inviter and the joiner listen on no address. The joiner knows only the
// circuit address of the inviter's invite code, so every stream between them
// crosses a limited connection: the join, the member list sync both ways,
// and the leave of the joiner.
func TestJoinThroughRelay(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	entry := relayEntryOf(t, r, "tcp")
	a, ah := relayNode(t, []string{entry})
	b, bh := relayNode(t, []string{entry})
	waitCircuit(t, ah)
	waitCircuit(t, bh)
	a.create("alpha")

	secret, expires, err := a.set.CreateInvite(testTeam)
	if err != nil {
		t.Fatal(err)
	}
	inv := Invite{Team: testTeam, Inviter: a.h.ID(), Addrs: ah.AdvertiseAddrs(""), Secret: secret, Expires: expires}
	if len(inv.Addrs) == 0 {
		t.Fatal("the invite code has no address")
	}
	for _, addr := range inv.Addrs {
		if _, err := addr.ValueForProtocol(ma.P_CIRCUIT); err != nil {
			t.Fatalf("invite address %v is not a circuit address", addr)
		}
	}
	code, err := inv.Encode()
	if err != nil {
		t.Fatal(err)
	}
	b.mustJoin(code, "bravo")
	if !relayed(bh, a.h.ID()) {
		t.Fatal("the connection to the inviter is not relayed")
	}
	all := map[string]string{a.id(): "alpha", b.id(): "bravo"}
	wantMembers(t, a, all)
	wantMembers(t, b, all)

	// The joiner's member list sync: after the join, B sets its own circuit
	// address and sends its entry to A.
	own := b.entry(b.id())
	if len(own.Addrs) == 0 {
		t.Fatalf("B's own entry after the join has no address: %+v", own)
	}
	eventually(t, "A has B's addresses", func() bool { return a.entry(b.id()).Join == own.Join })

	// The inviter's member list sync: a change on A reaches B.
	e := team.Entry{PeerID: testPeer(t).String(), Label: "echo", Join: 1}
	if _, err := a.set.Merge(testTeam, b.id(), []team.Entry{e}); err != nil {
		t.Fatal(err)
	}
	a.ms.Changed(testTeam)
	all[e.PeerID] = "echo"
	wantMembers(t, b, all)

	// The leave of B reaches A.
	left, err := b.set.Leave(testTeam)
	if err != nil {
		t.Fatal(err)
	}
	// echo exists only in the member lists.
	if n := b.ms.SendLeft(left); n != 1 {
		t.Fatalf("SendLeft did not reach %d members, want 1: echo", n)
	}
	all[b.id()] = "left bravo"
	wantMembers(t, a, all)
	if a.ms.IsMember(b.h.ID()) {
		t.Fatal("A still takes B as a member after the leave")
	}
	// The access rule holds on a limited connection: A refuses B's streams.
	ctx, cancel := context.WithTimeout(network.WithAllowLimitedConn(t.Context(), "test"), waitFor)
	defer cancel()
	s, err := b.h.NewStream(ctx, a.h.ID(), MsgProtocol)
	if err != nil {
		t.Fatalf("open a msg stream: %v", err)
	}
	defer s.Reset()
	s.SetDeadline(time.Now().Add(waitFor))
	_, err = exchange(s, encode(t, MembersFrame{Team: testTeam, Entries: []team.Entry{own}}))
	wantReset(t, err, network.StreamGated)
	if !relayed(bh, a.h.ID()) {
		t.Fatal("a direct connection replaced the relayed one")
	}
	noPeerIDsAbove(t, a, b)
}
