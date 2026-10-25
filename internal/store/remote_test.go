package store

import (
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/team"
)

func remoteMsg(c *clock, origin, sender string, to, summary string) message.Message {
	m := msg(c, sender, message.KindNote, to, summary)
	m.Origin = origin
	return m
}

// Scenarios "Address a remote agent" and "Sent to a team", on the side of
// the sender: a broadcast on a team topic reaches the local agents, and a
// <label>/<agent> recipient none of them. team.TestRoute covers the
// refused recipients.
func TestSendTeamRoutes(t *testing.T) {
	s, c := setup(t)
	r := team.Route{Team: "courier", Peers: []team.Queued{{PeerID: "peerb", Member: "bob"}}}
	sent, err := s.SendTeam(t.Context(), msg(c, "a", message.KindNote, "", "to everyone"), r)
	if err != nil {
		t.Fatal(err)
	}
	want(t, sent.Recipients, "b", "c")
	if len(sent.Queued) != 0 {
		t.Fatalf("the store queued %+v", sent.Queued)
	}
	remote := msg(c, "a", message.KindAsk, "bob/claude-agent-courier", "to bob's agent")
	if sent, err := s.SendTeam(t.Context(), remote, r); err != nil || len(sent.Recipients) != 0 {
		t.Fatalf("remote recipient: %+v %v", sent, err)
	}
	if n := count(t, s, `SELECT count(*) FROM messages WHERE id = '`+remote.ID.String()+`'`); n != 1 {
		t.Fatalf("the remote message stored %d times", n)
	}
	// On the topic of no team, a <label>/<agent> recipient is an unknown
	// agent of this machine.
	if _, err := s.SendTeam(t.Context(), msg(c, "a", message.KindNote, "bob/claude", "x"), team.Route{}); !errors.Is(err, ErrUnknownRecipient) {
		t.Fatalf("remote recipient without a team: %v", err)
	}
}

func count(t *testing.T, s *Store, query string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// A received message keeps its id unless a message here has it: then it gets
// a new local id, so a member cannot suppress another member's message, or
// a local one, by reusing its id.
func TestReceiveReusedID(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	m := remoteMsg(c, "peerb", "bob/claude", "", "hello")
	sent, err := s.Receive(ctx, m)
	if err != nil || sent.Message.ID != m.ID {
		t.Fatalf("first receive: %+v %v", sent, err)
	}
	want(t, sent.Recipients, "a", "b", "c")

	forged := remoteMsg(c, "peerc", "carol/claude", "", "carol's")
	forged.ID = m.ID
	got, err := s.Receive(ctx, forged)
	if err != nil || got.Message.ID == m.ID {
		t.Fatalf("receive of a reused id: %+v %v", got.Message, err)
	}
	local := msg(c, "a", message.KindNote, "", "local")
	send(t, s, local)
	same := remoteMsg(c, "peerb", "bob/claude", "", "same id as local")
	same.ID = local.ID
	if got, err := s.Receive(ctx, same); err != nil || got.Message.ID == local.ID {
		t.Fatalf("receive with a local id: %+v %v", got.Message, err)
	}
	want(t, summaries(t, s, "b"), "hello", "carol's", "local", "same id as local")
}

// Scenario "Remote budget" for the pending caps: the cap per sender counts
// every agent of one member, so new agent names do not pass it.
func TestReceivePendingCapPerMember(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	for i := range MaxPendingFromSender {
		if _, err := s.Receive(ctx, remoteMsg(c, "peerb", "bob/agent-"+strconv.Itoa(i), "b", "n")); err != nil {
			t.Fatal(err)
		}
	}
	_, err := s.Receive(ctx, remoteMsg(c, "peerb", "bob/fresh-name", "b", "one more"))
	if _, ok := errors.AsType[*QueueFullError](err); !ok {
		t.Fatalf("direct message over the cap of a member: %v", err)
	}
	sent, err := s.Receive(ctx, remoteMsg(c, "peerb", "bob/other-name", "", "broadcast"))
	if err != nil {
		t.Fatal(err)
	}
	want(t, sent.Skipped, "b")
	// Another member, and a local agent with the same name, have their own cap.
	if _, err := s.Receive(ctx, remoteMsg(c, "peerc", "carol/agent-0", "b", "carol")); err != nil {
		t.Fatalf("another member: %v", err)
	}
	send(t, s, msg(c, "c", message.KindNote, "b", "local"))
}

// A remote message keeps the rules of a local one: its recipient is known
// on the topic, and a reply answers an ask on it. A remote sender needs no
// registration.
func TestReceiveRules(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	if _, err := s.Receive(ctx, remoteMsg(c, "peerb", "bob/claude", "nobody", "x")); !errors.Is(err, ErrUnknownRecipient) {
		t.Fatalf("unknown recipient: %v", err)
	}
	reply := remoteMsg(c, "peerb", "bob/claude", "", "x")
	reply.Kind, reply.Re = message.KindReply, message.NewID(c.t)
	if _, err := s.Receive(ctx, reply); !errors.Is(err, ErrNoAsk) {
		t.Fatalf("reply without ask: %v", err)
	}
	for _, bad := range []message.Message{
		remoteMsg(c, "peer b", "bob/claude", "", "bad origin"),
		remoteMsg(c, "peerb", "claude", "", "sender without member"),
		remoteMsg(c, "peerb", "bob/claude", "carol/claude", "recipient on a third machine"),
		remoteMsg(c, "", "bob/claude", "", "no origin"),
	} {
		if _, err := s.Receive(ctx, bad); err == nil {
			t.Errorf("%s: stored", bad.Summary)
		}
	}
	for _, p := range pending(t, s, "a") {
		if p.Kind != message.KindOnline {
			t.Fatalf("a refused message is pending: %+v", p)
		}
	}
	ok := remoteMsg(c, "peerb", "bob/claude", "a", "ok")
	if _, err := s.Receive(ctx, ok); err != nil {
		t.Fatal(err)
	}
	ps := pending(t, s, "a")
	if last := ps[0]; last.Origin != "peerb" || last.Sender != "bob/claude" || last.Summary != "ok" {
		t.Fatalf("pending %+v", last)
	}
}

// A broadcast that reaches no agent here is not stored.
func TestReceiveBroadcastNoAgent(t *testing.T) {
	s, c := setup(t)
	m := remoteMsg(c, "peerb", "bob/claude", "", "nobody listens")
	m.Topic = "empty"
	sent, err := s.Receive(t.Context(), m)
	if err != nil || !sent.Dropped || len(sent.Recipients) != 0 {
		t.Fatalf("receive: %+v %v", sent, err)
	}
	if n := count(t, s, `SELECT count(*) FROM messages WHERE summary = 'nobody listens'`); n != 0 {
		t.Fatalf("stored %d", n)
	}
	if got := summaries(t, s, "a"); slices.Contains(got, "nobody listens") {
		t.Fatalf("pending %v", got)
	}
}
