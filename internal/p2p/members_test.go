package p2p

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/CappyT/agent-courier/internal/team"
)

// msgStream opens a MsgProtocol stream from n to o and sends one members
// frame with n's own entry. It returns the error of the exchange: nil when o
// took the stream, a reset when the access rule refused it.
func msgStream(t *testing.T, n, o *node, own team.Entry) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	if err := n.h.Connect(ctx, peer.AddrInfo{ID: o.h.ID(), Addrs: o.h.Addrs()}); err != nil {
		t.Fatal(err)
	}
	s, err := n.h.NewStream(ctx, o.h.ID(), MsgProtocol)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Reset()
	s.SetDeadline(time.Now().Add(waitFor))
	_, err = exchange(s, encode(t, MembersFrame{Team: testTeam, Entries: []team.Entry{own}}))
	return err
}

func disconnect(a, b *node) {
	a.h.Network().ClosePeer(b.h.ID())
	b.h.Network().ClosePeer(a.h.ID())
}

func reconnect(t *testing.T, from, to *node) {
	t.Helper()
	disconnect(from, to)
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	if err := from.h.Connect(ctx, peer.AddrInfo{ID: to.h.ID(), Addrs: to.h.Addrs()}); err != nil {
		t.Fatal(err)
	}
}

// Member announces a new member: A adds B through an invite, and C, a
// member, adds B when it hears it from A. A sends the list after the change
// to the members it reaches, on each connection to a member, and after a
// change it hears from a member.
func TestMembersPropagation(t *testing.T) {
	a, b, c := newNode(t), newNode(t), newNode(t)
	a.create("alpha")
	code, _ := a.invite()
	c.mustJoin(code, "charlie")
	all := map[string]string{a.id(): "alpha", c.id(): "charlie"}
	wantMembers(t, c, all)

	// After a local change: A pushes the list with B to C at once.
	code, _ = a.invite()
	b.mustJoin(code, "bravo")
	all[b.id()] = "bravo"
	wantMembers(t, c, all)
	wantMembers(t, b, all)

	// On connect: C cannot take frames while A adds D, so the push fails.
	// When C connects to A again, A sends its list and C adds D.
	disconnect(a, c)
	c.h.RemoveStreamHandler(MsgProtocol)
	d := newNode(t)
	code, _ = a.invite()
	d.mustJoin(code, "delta")
	all[d.id()] = "delta"
	wantMembers(t, a, all)
	time.Sleep(200 * time.Millisecond)
	if _, ok := c.members()[d.id()]; ok {
		t.Fatal("C got D without a handler")
	}
	c.serveMsg()
	reconnect(t, c, a)
	wantMembers(t, c, all)
	noPeerIDsAbove(t, a, b, c, d)

	// A change that A hears from a member goes on to the other members: C
	// learns E from A, though only A heard it from B.
	e := team.Entry{PeerID: testPeer(t).String(), Label: "echo", Join: 1}
	a.ms.Receive(b.h.ID(), MembersFrame{Team: testTeam, Entries: []team.Entry{e}})
	all[e.PeerID] = "echo"
	wantMembers(t, c, all)
}

// Resend: C missed a change and keeps its connection to A. The next Resend
// of A sends the list again, and C learns the change.
func TestMembersResend(t *testing.T) {
	a, c := newNode(t), newNode(t)
	a.create("alpha")
	code, _ := a.invite()
	c.mustJoin(code, "charlie")
	reconnect(t, c, a)

	c.h.RemoveStreamHandler(MsgProtocol)
	e := team.Entry{PeerID: testPeer(t).String(), Label: "echo", Join: 1}
	if _, err := a.set.Merge(testTeam, c.id(), []team.Entry{e}); err != nil {
		t.Fatal(err)
	}
	a.ms.Changed(testTeam)
	time.Sleep(200 * time.Millisecond)
	if _, ok := c.members()[e.PeerID]; ok {
		t.Fatal("C got the change without a handler")
	}
	c.serveMsg()
	a.ms.Resend()
	wantMembers(t, c, map[string]string{a.id(): "alpha", c.id(): "charlie", e.PeerID: "echo"})
}

// Outsider sends a member list: a member of one team who sends a list of
// another team of the receiver is ignored.
func TestMembersFromOutsideTheTeam(t *testing.T) {
	a, c := newNode(t), newNode(t)
	a.create("alpha")
	code, _ := a.invite()
	c.mustJoin(code, "charlie")
	if _, err := a.set.Create(a.id(), "other", []string{"cappyt_other"}, "alpha"); err != nil {
		t.Fatal(err)
	}
	if !a.ms.IsMember(c.h.ID()) || a.ms.MemberOf(c.h.ID(), "other") {
		t.Fatal("C is a member of courier only")
	}
	add := team.Entry{PeerID: testPeer(t).String(), Label: "sierra", Join: 1}
	a.ms.Receive(c.h.ID(), MembersFrame{Team: "other", Entries: []team.Entry{add}})
	if ot, _ := a.set.Get("other"); len(ot.Members) != 1 {
		t.Fatalf("a member of another team changed the list of other: %v", ot.Members)
	}
}

// Member leaves: B forgets the team at once and tells A, which refuses B's
// streams from then on. C, which missed the notice, learns it from A, and a
// stale list of C that still holds B does not bring B back on A. B's code
// admits no one.
func TestMembersLeave(t *testing.T) {
	a, b, c := newNode(t), newNode(t), newNode(t)
	a.create("alpha")
	code, _ := a.invite()
	b.mustJoin(code, "bravo")
	code, _ = a.invite()
	c.mustJoin(code, "charlie")
	all := map[string]string{a.id(): "alpha", b.id(): "bravo", c.id(): "charlie"}
	for _, n := range []*node{a, b, c} {
		wantMembers(t, n, all)
	}
	eventually(t, "B's entry carries its addresses everywhere", func() bool {
		return len(a.entry(b.id()).Addrs) > 0 && len(c.entry(b.id()).Addrs) > 0
	})
	stale := c.entry(b.id())
	ownB := b.entry(b.id())
	harvested, _ := b.invite()

	// C cannot take frames for now, so it does not hear the leave.
	c.h.RemoveStreamHandler(MsgProtocol)
	left, err := b.set.Leave(testTeam)
	if err != nil {
		t.Fatal(err)
	}
	b.ms.SendLeft(left)
	if b.set.Len() != 0 || b.ms.IsMember(a.h.ID()) {
		t.Fatal("B kept the team after the leave")
	}
	afterLeave := map[string]string{a.id(): "alpha", b.id(): "left bravo", c.id(): "charlie"}
	wantMembers(t, a, afterLeave)
	wantReset(t, msgStream(t, b, a, ownB), network.StreamGated)
	if _, err := newNode(t).join(harvested, "delta"); !errors.Is(err, team.ErrInviteRefused) {
		t.Fatalf("join with a code of the machine that left: %v", err)
	}
	if !c.ms.IsMember(b.h.ID()) {
		t.Fatal("C dropped B before it heard the leave")
	}

	// A stale list from C: A keeps B out.
	a.ms.Receive(c.h.ID(), MembersFrame{Team: testTeam, Entries: []team.Entry{stale}})
	if a.ms.IsMember(b.h.ID()) {
		t.Fatal("a stale list brought B back")
	}

	c.serveMsg()
	reconnect(t, c, a)
	wantMembers(t, c, afterLeave)
	eventually(t, "C refuses B", func() bool { return !c.ms.IsMember(b.h.ID()) })
}
