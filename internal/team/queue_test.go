package team

import (
	"errors"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
)

const (
	qBob   = "12D3KooWbob111111"
	qCarol = "12D3KooWcarol22222"
)

// queueSet is a set with team courier on t1, with the members bob and
// carol, and team other on o1, with bob.
func queueSet(t *testing.T) (*Set, *clock) {
	t.Helper()
	s, c := newSet(t)
	mustCreate(t, s, "pa", "courier", "alpha", "t1")
	mustCreate(t, s, "pa", "other", "alpha", "o1")
	for _, name := range []string{"courier", "other"} {
		secret, _, err := s.CreateInvite(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.AcceptInvite(name, secret, qBob, "bob"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Merge("courier", qBob, []Entry{{PeerID: qCarol, Label: "carol", Join: 1}}); err != nil {
		t.Fatal(err)
	}
	return s, c
}

var qSeq byte

func qMsg(topic, summary string) message.Message {
	qSeq++
	return message.Message{ID: message.ID{15: qSeq}, Sender: "claude",
		Fields: message.Fields{Topic: topic, Kind: message.KindNote, Summary: summary}}
}

// enqueue routes and queues a broadcast on topic.
func enqueue(t *testing.T, s *Set, topic, summary string) message.Message {
	t.Helper()
	m := qMsg(topic, summary)
	r, err := s.Route(topic, "")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Enqueue(r, m); len(got) != len(r.Peers) {
		t.Fatalf("queued %+v of %+v", got, r.Peers)
	}
	return m
}

// batchOf is the batch of peer in batches.
func batchOf(batches []Batch, peer string) []Item {
	for _, b := range batches {
		if b.PeerID == peer {
			return b.Items
		}
	}
	return nil
}

func TestBackoff(t *testing.T) {
	want := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second,
		160 * time.Second, 320 * time.Second, 600 * time.Second, 600 * time.Second}
	for i, w := range want {
		if got := Backoff(i + 1); got != w {
			t.Errorf("backoff after %d attempts: %v, want %v", i+1, got, w)
		}
	}
	if Backoff(1000) != QueueMaxBackoff {
		t.Error("no cap")
	}
}

// Scenarios "Member briefly offline" and "Receipt lost", on the side of the
// sender, with an injected clock: an item is due at once, then waits 5 s
// doubling to 10 min after each send without a receipt, and goes only on a
// receipt of its own member.
func TestQueueBackoffAndReceipt(t *testing.T) {
	s, c := queueSet(t)
	t0 := c.t
	m := enqueue(t, s, "t1", "hello")
	batches, _, ok := s.Take(nil)
	if len(batchOf(batches, qBob)) != 1 || len(batchOf(batches, qCarol)) != 1 || ok {
		t.Fatalf("first take: %+v, next %v", batches, ok)
	}
	if it := batchOf(batches, qBob)[0]; it.Team != "courier" || it.Message.ID != m.ID || it.Attempts != 0 {
		t.Fatalf("item %+v", it)
	}
	// Receipt lost: no item goes without a receipt, and each retry waits
	// longer, up to the cap.
	at, wait := t0, QueueMinBackoff
	for attempt := 1; attempt <= 10; attempt++ {
		batches, next, ok := s.Take(nil)
		if len(batches) != 0 || !ok || !next.Equal(at.Add(wait)) {
			t.Fatalf("attempt %d: %+v, next %v (%v), want %v", attempt, batches, next, ok, at.Add(wait))
		}
		c.t = next.Add(-time.Millisecond)
		if batches, _, _ := s.Take(nil); len(batches) != 0 {
			t.Fatalf("attempt %d: due before the backoff ended", attempt)
		}
		c.t, at = next, next
		batches, _, _ = s.Take(nil)
		if got := batchOf(batches, qBob); len(got) != 1 || got[0].Attempts != attempt {
			t.Fatalf("retry %d: %+v", attempt, batches)
		}
		wait = min(2*wait, QueueMaxBackoff)
	}
	if wait != QueueMaxBackoff {
		t.Fatalf("the backoff did not reach the cap: %v", wait)
	}
	// A receipt deletes only the item of the member that sent it.
	if s.Delivered(qCarol, qMsg("t1", "other").ID) {
		t.Fatal("a receipt for another id deleted an item")
	}
	if !s.Delivered(qCarol, m.ID) || s.Delivered(qCarol, m.ID) {
		t.Fatal("carol's receipt did not delete her item once")
	}
	if s.QueueLen(qBob) != 1 || s.QueueLen(qCarol) != 0 {
		t.Fatalf("queue bob %d, carol %d", s.QueueLen(qBob), s.QueueLen(qCarol))
	}
	if !s.Delivered(qBob, m.ID) || s.QueueLen(qBob) != 0 {
		t.Fatal("bob's receipt")
	}
	if _, _, ok := s.Take(nil); ok {
		t.Fatal("an empty queue has a next attempt")
	}
}

// A member that connects makes its items due at once.
func TestQueueMakeDue(t *testing.T) {
	s, c := queueSet(t)
	enqueue(t, s, "t1", "x")
	s.Take(nil)
	c.t = c.t.Add(time.Second)
	s.MakeDue(qCarol)
	batches, next, _ := s.Take(nil)
	if len(batches) != 1 || batches[0].PeerID != qCarol || !next.Equal(c.t.Add(4*time.Second)) {
		t.Fatalf("after carol connected: %+v, next %v", batches, next)
	}
}

// Scenario "one member's backlog never delays another member": one take
// gives each member at most QueueBatch items, oldest first over its teams,
// and skips a busy member, whose items keep their attempt.
func TestQueueFair(t *testing.T) {
	s, _ := queueSet(t)
	first := enqueue(t, s, "o1", "bob first")
	for i := range 3 * QueueBatch {
		enqueue(t, s, "o1", "bob "+string(rune('a'+i%26)))
	}
	enqueue(t, s, "t1", "both")
	batches, _, _ := s.Take(nil)
	bob, carol := batchOf(batches, qBob), batchOf(batches, qCarol)
	if len(bob) != QueueBatch || bob[0].Message.ID != first.ID || len(carol) != 1 || carol[0].Message.Summary != "both" {
		t.Fatalf("bob %d items, carol %+v", len(bob), carol)
	}
	// bob's send runs: the next take leaves his items alone, and his
	// items give no next attempt.
	busy := func(p string) bool { return p == qBob }
	if batches, _, ok := s.Take(busy); len(batches) != 0 || !ok {
		t.Fatalf("take with bob busy: %+v, next %v", batches, ok)
	}
	m := enqueue(t, s, "t1", "carol again")
	batches, _, ok := s.Take(busy)
	if len(batches) != 1 || batches[0].PeerID != qCarol || batches[0].Items[0].Message.ID != m.ID || ok {
		t.Fatalf("take with bob busy: %+v, next %v", batches, ok)
	}
	batches, _, _ = s.Take(nil)
	bob = batchOf(batches, qBob)
	if len(bob) != QueueBatch || bob[0].Attempts != 0 || bob[0].Message.ID == first.ID {
		t.Fatalf("bob's second batch: %d items, first %+v", len(bob), bob[0])
	}
}

// Scenario "Member left", and the end of a team: the items and the received
// ids of a member go when it leaves, all of a team when this machine leaves
// it, and a team or member that is gone queues nothing.
func TestQueueDropped(t *testing.T) {
	s, _ := queueSet(t)
	enqueue(t, s, "t1", "courier")
	enqueue(t, s, "o1", "other")
	wire := qMsg("t1", "from carol").ID
	if dup, err := s.BeginReceive("courier", qCarol, wire); dup || err != nil {
		t.Fatal(dup, err)
	}
	s.EndReceive("courier", qCarol, wire, wire, true)
	if s.QueueLen(qBob) != 2 || s.QueueLen(qCarol) != 1 {
		t.Fatalf("queue bob %d, carol %d", s.QueueLen(qBob), s.QueueLen(qCarol))
	}

	if _, err := s.Merge("courier", qBob, []Entry{{PeerID: qCarol, Label: "carol", Join: 1, Left: true}}); err != nil {
		t.Fatal(err)
	}
	if s.QueueLen(qCarol) != 0 || s.QueueLen(qBob) != 2 {
		t.Fatalf("after carol left: bob %d, carol %d", s.QueueLen(qBob), s.QueueLen(qCarol))
	}
	r := Route{Team: "courier", Peers: []Queued{{PeerID: qCarol, Member: "carol"}}}
	if got, _ := s.Enqueue(r, qMsg("t1", "late")); len(got) != 0 || s.QueueLen(qCarol) != 0 {
		t.Fatalf("a member that left got %+v", got)
	}
	// carol joins again: what she sent before is new again.
	secret, _, _ := s.CreateInvite("courier")
	if _, err := s.AcceptInvite("courier", secret, qCarol, "carol"); err != nil {
		t.Fatal(err)
	}
	if dup, err := s.BeginReceive("courier", qCarol, wire); dup || err != nil {
		t.Fatalf("after carol joined again: dup %v, %v", dup, err)
	}

	if _, err := s.Leave("courier"); err != nil {
		t.Fatal(err)
	}
	batches, _, _ := s.Take(nil)
	if got := batchOf(batches, qBob); len(got) != 1 || got[0].Team != "other" {
		t.Fatalf("after the leave of courier: %+v", batches)
	}
	if len(s.seen) != 0 || len(s.wire) != 0 {
		t.Fatalf("received ids of courier stay: %v %v", s.seen, s.wire)
	}
	if got, _ := s.Enqueue(Route{Team: "courier", Peers: []Queued{{PeerID: qBob}}}, qMsg("t1", "x")); len(got) != 0 {
		t.Fatalf("a team this machine left queued %+v", got)
	}
	s.EndReceive("courier", qCarol, wire, wire, true)
	if len(s.seen) != 0 || len(s.wire) != 0 {
		t.Fatal("a receive that ended after the team kept its id")
	}
	if _, err := s.Leave("other"); err != nil {
		t.Fatal(err)
	}
	if len(s.queue) != 0 || len(s.rate) != 0 {
		t.Fatalf("the set holds %d queues and %d rates with no team", len(s.queue), len(s.rate))
	}
}

// Scenario "Receipt lost", on the side of the receiver: an id that a member
// sent before is a duplicate for DedupeTTL, keyed by the member,
// and a second copy that comes while the first is being stored waits.
func TestReceiveDedupe(t *testing.T) {
	s, _ := queueSet(t)
	wire := qMsg("t1", "x").ID
	if dup, err := s.BeginReceive("courier", qBob, wire); dup || err != nil {
		t.Fatal(dup, err)
	}
	if _, err := s.BeginReceive("courier", qBob, wire); !errors.Is(err, ErrReceiveBusy) {
		t.Fatalf("a second copy while the first runs: %v", err)
	}
	// carol's message with the same id is hers.
	if dup, err := s.BeginReceive("courier", qCarol, wire); dup || err != nil {
		t.Fatal(dup, err)
	}
	s.EndReceive("courier", qCarol, wire, wire, false)
	if dup, err := s.BeginReceive("courier", qCarol, wire); dup || err != nil {
		t.Fatalf("an id the store did not keep is not free: %v %v", dup, err)
	}
	s.EndReceive("courier", qBob, wire, wire, true)
	if dup, err := s.BeginReceive("courier", qBob, wire); !dup || err != nil {
		t.Fatalf("bob's copy after the store: dup %v, %v", dup, err)
	}
	if _, err := s.BeginReceive("courier", "12D3KooWdave44444", wire); !errors.Is(err, ErrNotMember) {
		t.Fatalf("a stranger: %v", err)
	}
	if _, err := s.BeginReceive("other", qCarol, wire); !errors.Is(err, ErrNotMember) {
		t.Fatalf("a member of another team: %v", err)
	}
}

// A received id stays a duplicate, with its wire id, until DedupeTTL after
// its receive ended.
func TestReceiveDedupeTTL(t *testing.T) {
	s, c := queueSet(t)
	wire, local := qMsg("t1", "ask").ID, qMsg("t1", "local id").ID
	if _, err := s.BeginReceive("courier", qBob, wire); err != nil {
		t.Fatal(err)
	}
	s.EndReceive("courier", qBob, wire, local, true)
	c.t = c.t.Add(DedupeTTL - time.Second)
	if dup, err := s.BeginReceive("courier", qBob, wire); !dup || err != nil {
		t.Fatalf("before DedupeTTL: dup %v, %v", dup, err)
	}
	c.t = c.t.Add(time.Second)
	if dup, err := s.BeginReceive("courier", qBob, wire); dup || err != nil {
		t.Fatalf("at DedupeTTL: dup %v, %v", dup, err)
	}
	if _, ok := s.wire["courier"][local]; ok {
		t.Fatal("the wire id of an expired record stays")
	}
}

// A member holds at most QueueMax items: Enqueue skips it and reports it
// full, Full names it, and the other members still get the message.
func TestQueueMax(t *testing.T) {
	s, _ := queueSet(t)
	r, err := s.Route("t1", "bob/claude")
	if err != nil {
		t.Fatal(err)
	}
	for range QueueMax {
		if q, full := s.Enqueue(r, qMsg("t1", "x")); len(q) != 1 || len(full) != 0 {
			t.Fatalf("queued %+v, full %+v", q, full)
		}
	}
	if f := s.Full(r); len(f) != 1 || f[0].PeerID != qBob || f[0].Member != "bob" {
		t.Fatalf("Full: %+v", f)
	}
	all, _ := s.Route("t1", "")
	q, full := s.Enqueue(all, qMsg("t1", "one more"))
	if len(q) != 1 || q[0].PeerID != qCarol || len(full) != 1 || full[0].PeerID != qBob {
		t.Fatalf("queued %+v, full %+v", q, full)
	}
	if s.QueueLen(qBob) != QueueMax || s.QueueLen(qCarol) != 1 {
		t.Fatalf("bob %d, carol %d", s.QueueLen(qBob), s.QueueLen(qCarol))
	}
}

// One member's messages, stored or not, are capped per hour over all its
// teams; a duplicate gets its receipt over the cap, and the other members
// keep their own count.
func TestReceiveRate(t *testing.T) {
	s, c := queueSet(t)
	t0 := c.t
	var first message.ID
	for i := range MaxReceivedPerHour {
		name := []string{"courier", "other"}[i%2]
		wire := message.NewID(t0.Add(time.Duration(i) * time.Millisecond))
		if i == 0 {
			first = wire
		}
		if dup, err := s.BeginReceive(name, qBob, wire); dup || err != nil {
			t.Fatalf("message %d: %v %v", i, dup, err)
		}
		s.EndReceive(name, qBob, wire, message.ID{}, true)
		c.t = c.t.Add(time.Second)
	}
	over := message.NewID(c.t)
	if _, err := s.BeginReceive("other", qBob, over); !errors.Is(err, ErrReceiveRate) {
		t.Fatalf("over the cap: %v", err)
	}
	if dup, err := s.BeginReceive("courier", qBob, first); !dup || err != nil {
		t.Fatalf("a duplicate over the cap: %v %v", dup, err)
	}
	if dup, err := s.BeginReceive("courier", qCarol, over); dup || err != nil {
		t.Fatalf("carol: %v %v", dup, err)
	}
	c.t = t0.Add(time.Hour)
	if dup, err := s.BeginReceive("other", qBob, over); dup || err != nil {
		t.Fatalf("an hour after the first: %v %v", dup, err)
	}
}

// A reply to an ask that a member sent carries the wire id of the ask, also
// when the store gave the ask a new local id. A reply to a message of this
// machine keeps its id.
func TestEnqueueWireID(t *testing.T) {
	s, _ := queueSet(t)
	wire, local := qMsg("t1", "ask").ID, qMsg("t1", "local id").ID
	if _, err := s.BeginReceive("courier", qBob, wire); err != nil {
		t.Fatal(err)
	}
	s.EndReceive("courier", qBob, wire, local, true)
	own := qMsg("t1", "own ask").ID
	for _, tc := range []struct{ re, want message.ID }{{local, wire}, {own, own}} {
		reply := qMsg("t1", "answer")
		reply.Kind, reply.Re = message.KindReply, tc.re
		r, _ := s.Route("t1", "")
		s.Enqueue(r, reply)
		batches, _, _ := s.Take(nil)
		if got := batchOf(batches, qBob); len(got) != 1 || got[0].Message.Re != tc.want {
			t.Fatalf("re %v: %+v", tc.re, got)
		}
	}
}
