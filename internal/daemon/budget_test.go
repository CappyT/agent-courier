//go:build unix

package daemon

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/store"
)

var budgetT0 = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

// msg is the params of a send on t1. to is empty for a broadcast.
func msg(kind, to string) map[string]any {
	f := map[string]any{"topic": "t1", "kind": kind, "summary": "s"}
	if to != "" {
		f["to"] = to
	}
	return f
}

// sendN sends n messages from p and returns their ids.
func sendN(p *peer, n int, f map[string]any) []string {
	p.t.Helper()
	var ids []string
	for range n {
		ids = append(ids, p.send(f).ID)
	}
	return ids
}

// wantPushes reads one deliver per id and checks it is the push of that id.
func wantPushes(p *peer, ids []string) {
	p.t.Helper()
	for _, id := range ids {
		if got := p.deliver(); got.Attrs["id"] != id {
			p.t.Fatalf("%s: deliver %v, want the push of %s", p.name, got.Attrs, id)
		}
	}
}

// wantBudget reads the next deliver and checks it is the budget notice that
// sender passed b in the session of p.
func wantBudget(p *peer, sender string, b *budget) {
	p.t.Helper()
	got := p.deliver()
	want := map[string]string{"topic": "t1", "sender": sender, "kind": "budget", "to": p.name}
	if !maps.Equal(got.Attrs, want) {
		p.t.Fatalf("%s: deliver attrs %v, want %v", p.name, got.Attrs, want)
	}
	named := sender
	if b.session {
		named = ""
	}
	if text := message.RenderBudget(named, p.name, b.text); got.Text != text {
		p.t.Errorf("budget text %q, want %q", got.Text, text)
	}
	if !strings.Contains(got.Text, "call pending") || !strings.Contains(got.Text, b.text) ||
		!strings.Contains(got.Text, "holds these messages") || b.session && strings.Contains(got.Text, sender+" passed") {
		p.t.Errorf("budget text %q does not name the budget and pending", got.Text)
	}
	if notice := "courier: budget from " + sender + " to " + p.name; got.Notice != notice {
		p.t.Errorf("budget notice %q, want %q", got.Notice, notice)
	}
}

// Scenario "Flood from one sender".
func TestFloodFromOneSender(t *testing.T) {
	clock := &fakeClock{t: budgetT0}
	d := startDaemonWith(t, clock.now, nil)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	var ids []string
	for i := range 15 {
		clock.set(budgetT0.Add(time.Duration(i) * 3 * time.Second))
		ids = append(ids, a.send(msg("note", "b")).ID)
	}
	wantPushes(b, ids[:10])
	wantBudget(b, "a", &senderBudget)
	b.noDeliver()

	res := b.pending()
	if res.Total != 15 {
		t.Fatalf("b holds %d messages, want 15", res.Total)
	}
	for _, id := range ids[10:] {
		if !strings.Contains(res.Text, id) {
			t.Errorf("%s is not in the pending list of b", id)
		}
	}
	waitFor(t, "the pushes recorded", func() bool {
		ps, _, err := d.store.Pending(context.Background(), "b", 0)
		if err != nil {
			t.Fatal(err)
		}
		pushed := 0
		for _, p := range ps {
			if p.Session != "" {
				pushed++
				if slices.Contains(ids[10:], p.ID.String()) {
					t.Fatalf("%s over the budget was recorded as pushed", p.ID)
				}
			}
		}
		return pushed == 10
	})
}

// The sender budget counts the pushes of the last 10 minutes, not of a fixed
// window.
func TestBudgetWindowSlides(t *testing.T) {
	clock := &fakeClock{t: budgetT0}
	d := startDaemonWith(t, clock.now, nil)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	wantPushes(b, sendN(a, 5, msg("note", "")))
	clock.set(budgetT0.Add(6 * time.Minute))
	wantPushes(b, sendN(a, 5, msg("note", "")))
	clock.set(budgetT0.Add(11 * time.Minute))
	ids := sendN(a, 6, msg("note", ""))
	wantPushes(b, ids[:5])
	wantBudget(b, "a", &senderBudget)
	b.noDeliver()
}

// A sender gets one budget notice per budget window, and one more when the
// window has passed since the last.
func TestBudgetNoticeOncePerWindow(t *testing.T) {
	clock := &fakeClock{t: budgetT0}
	tick := &manualTicker{}
	d := startDaemonTicked(t, clock.now, tick)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	ids := sendN(a, 12, msg("note", ""))
	wantPushes(b, ids[:10])
	wantBudget(b, "a", &senderBudget)
	b.noDeliver()

	clock.set(budgetT0.Add(5 * time.Minute))
	ids = append(ids, a.send(msg("note", "")).ID)
	tick.fire()
	b.noDeliver()

	// Room in the pending cap of b for the next 11.
	for _, id := range ids[:10] {
		if err := b.ack(id); err != nil {
			t.Fatal(err)
		}
	}
	clock.set(budgetT0.Add(senderBudget.window))
	ids = append(ids, sendN(a, 11, msg("note", ""))...)
	b.noDeliver()
	tick.fire()
	wantPushes(b, ids[10:20])
	wantBudget(b, "a", &senderBudget)
	b.noDeliver()
}

// Scenario "Notes do not bury a handoff". The broadcast budget of the
// session gives one notice in its window, whatever the number of senders.
func TestNotesDoNotBuryHandoff(t *testing.T) {
	d := startDaemon(t)
	b := d.join(t, "b", "t1")
	names := []string{"a", "c", "d", "e"}
	for i := range 20 {
		names = append(names, "f"+strconv.Itoa(i))
	}
	senders := map[string]*peer{}
	for _, n := range names {
		senders[n] = d.join(t, n, "t1")
	}
	b.onlines(names...)
	for _, n := range []string{"c", "d", "e"} {
		wantPushes(b, sendN(senders[n], 10, msg("note", "")))
	}
	for _, n := range names[4:] {
		senders[n].send(msg("note", ""))
	}
	wantBudget(b, "f0", &broadcastBudget)
	handoff := senders["a"].send(msg("handoff", "b"))
	d.wantDeliver(t, b.deliver(), "b", handoff.ID)
	b.noDeliver()
}

func TestDirectBudgets(t *testing.T) {
	t.Run("one sender pushes at most 10 direct messages in an hour and keeps its order", func(t *testing.T) {
		clock := &fakeClock{t: budgetT0}
		tick := &manualTicker{}
		d := startDaemonTicked(t, clock.now, tick)
		a := d.join(t, "a", "t1")
		c := d.join(t, "c", "t1")
		b := d.join(t, "b", "t1")
		wantPushes(b, sendN(a, 10, msg("note", "b")))
		clock.set(budgetT0.Add(15 * time.Minute))
		handoff := a.send(msg("handoff", "b")).ID
		wantBudget(b, "a", &senderDirectBudget)
		wantPushes(b, []string{c.send(msg("handoff", "b")).ID})
		broadcast := a.send(msg("note", "")).ID
		tick.fire()
		b.noDeliver()
		clock.set(budgetT0.Add(time.Hour + time.Minute))
		tick.fire()
		wantPushes(b, []string{handoff, broadcast})
		b.noDeliver()
	})

	t.Run("all senders push at most 30 direct messages in an hour", func(t *testing.T) {
		d := startDaemon(t)
		b := d.join(t, "b", "t1")
		names := []string{"a", "c", "d", "e", "f"}
		var ps []*peer
		for _, n := range names {
			ps = append(ps, d.join(t, n, "t1"))
		}
		b.onlines(names...)
		for _, p := range ps[:3] {
			wantPushes(b, sendN(p, 10, msg("note", "b")))
		}
		ps[3].send(msg("handoff", "b"))
		wantBudget(b, "e", &directBudget)
		wantPushes(b, []string{ps[4].send(msg("note", "")).ID})
		b.noDeliver()
	})
}

// The direct budget of a sender counts only its direct messages.
func TestSenderDirectBudgetIgnoresBroadcasts(t *testing.T) {
	clock := &fakeClock{t: budgetT0}
	d := startDaemonWith(t, clock.now, nil)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	wantPushes(b, sendN(a, 6, msg("note", "")))
	clock.set(budgetT0.Add(senderBudget.window + time.Minute))
	wantPushes(b, sendN(a, 10, msg("note", "b")))
	b.noDeliver()
}

// Onlines pass no budget and use none.
func TestOnlinesNotBudgeted(t *testing.T) {
	d := startDaemon(t)
	b := d.join(t, "b", "t1")
	var names []string
	var ys []*peer
	for i := range broadcastBudget.limit + 5 {
		names = append(names, "y"+strconv.Itoa(i))
		ys = append(ys, d.join(t, names[i], "t1"))
	}
	b.onlines(names...)
	wantPushes(b, sendN(ys[0], 10, msg("note", "")))
	b.noDeliver()
}

// The pushes after register apply the budgets: a message over budget waits,
// and the next session pushes it.
func TestReplayRespectsBudgets(t *testing.T) {
	d := startDaemon(t)
	a := d.join(t, "a", "t1")
	d.join(t, "b", "t1").leave()
	ids := sendN(a, 15, msg("note", "b"))

	b := d.join(t, "b", "t1")
	wantPushes(b, ids[:10])
	wantBudget(b, "a", &senderBudget)
	b.noDeliver()
	for _, id := range ids[:10] {
		if err := b.ack(id); err != nil {
			t.Fatal(err)
		}
	}
	b.leave()

	b = d.join(t, "b", "t1")
	wantPushes(b, ids[10:])
	b.noDeliver()
}

// A held message reaches the live session when its window slides, without a
// new register, and a newer message of its sender comes after it.
func TestHeldReofferedInSession(t *testing.T) {
	clock := &fakeClock{t: budgetT0}
	tick := &manualTicker{}
	d := startDaemonTicked(t, clock.now, tick)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	ids := sendN(a, 11, msg("note", "b"))
	wantPushes(b, ids[:10])
	wantBudget(b, "a", &senderBudget)
	for _, id := range ids[:10] {
		if err := b.ack(id); err != nil {
			t.Fatal(err)
		}
	}
	clock.set(budgetT0.Add(2 * time.Minute))
	handoff := a.send(msg("handoff", "b")).ID
	tick.fire()
	b.noDeliver()
	clock.set(budgetT0.Add(budgetWindow + time.Minute))
	tick.fire()
	wantPushes(b, []string{ids[10], handoff})
	b.noDeliver()
}

// A held message that the agent acknowledges is not pushed when its window
// slides.
func TestAckedHeldNotReoffered(t *testing.T) {
	clock := &fakeClock{t: budgetT0}
	tick := &manualTicker{}
	d := startDaemonTicked(t, clock.now, tick)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	ids := sendN(a, 11, msg("note", ""))
	wantPushes(b, ids[:10])
	wantBudget(b, "a", &senderBudget)
	if err := b.ack(ids[10]); err != nil {
		t.Fatal(err)
	}
	clock.set(budgetT0.Add(budgetWindow))
	tick.fire()
	b.noDeliver()
}

// unitSession is a registered session of b with a fake connection, a store
// on a fake clock where a and b follow t1, and a manual re-offer ticker.
type unitSession struct {
	t     *testing.T
	st    *store.Store
	clock *fakeClock
	tick  *manualTicker
	srv   *Server
	conn  *fakeConn
	ss    *session
}

func newUnitSession(t *testing.T) *unitSession {
	t.Helper()
	u := &unitSession{t: t, clock: &fakeClock{t: budgetT0}, tick: &manualTicker{}, conn: &fakeConn{}}
	u.st = openStore(t, u.clock.now)
	for _, n := range []string{"a", "b"} {
		if _, err := u.st.RegisterTopics(context.Background(), n, []string{"t1"}); err != nil {
			t.Fatal(err)
		}
	}
	u.srv = New(u.st, testLogger(t))
	u.srv.reofferTick = u.tick.start
	u.ss = &session{id: "s1", conn: u.conn, name: "b", topics: []string{"t1"}, out: newOutbox("b")}
	u.ss.out.behind = false
	return u
}

// send stores a message from a, to b or to everyone, and pushes it.
func (u *unitSession) send(to string) message.Message {
	u.t.Helper()
	now := u.st.Now()
	sent, err := u.st.Send(context.Background(), message.Message{ID: message.NewID(now), Sender: "a", Created: now,
		Fields: message.Fields{Topic: "t1", Kind: message.KindNote, Summary: "x", To: to}})
	if err != nil {
		u.t.Fatal(err)
	}
	u.srv.push(u.ss, u.ss.out, sent.Message)
	return sent.Message
}

// onlines pushes n onlines that no store row backs.
func (u *unitSession) onlines(n int) {
	now := u.st.Now()
	for i := range n {
		u.srv.push(u.ss, u.ss.out, message.Message{ID: message.NewID(now), Sender: "o" + strconv.Itoa(i), Created: now,
			Fields: message.Fields{Topic: "t1", Kind: message.KindOnline, Summary: "online"}})
	}
}

func (u *unitSession) start() {
	u.srv.wg.Go(func() { u.srv.drain(context.Background(), u.ss, u.ss.out) })
	u.t.Cleanup(func() {
		close(u.ss.out.done)
		u.srv.wg.Wait()
	})
}

func (u *unitSession) writes() []string {
	u.conn.mu.Lock()
	defer u.conn.mu.Unlock()
	var out []string
	for _, w := range u.conn.writes {
		out = append(out, string(w))
	}
	return out
}

func (u *unitSession) waitWrites(n int) []string {
	u.t.Helper()
	waitFor(u.t, strconv.Itoa(n)+" writes", func() bool { return len(u.writes()) >= n })
	return u.writes()
}

// quiet ticks the writer twice, so it has handled the first tick, and checks
// that it queued and wrote nothing more than n lines.
func (u *unitSession) quiet(n int) {
	u.t.Helper()
	u.tick.fire()
	u.tick.fire()
	if w, q := len(u.writes()), len(u.ss.out.queue); w != n || q != 0 {
		u.t.Fatalf("%d writes and %d queued, want %d and none", w, q, n)
	}
}

func (u *unitSession) pushes() int {
	u.ss.out.mu.Lock()
	defer u.ss.out.mu.Unlock()
	return len(u.ss.out.pushes)
}

// A held message stays held through the catch-up after a full queue and
// while its window is full, and is pushed once when the window has room.
func TestHeldUntilWindowHasRoom(t *testing.T) {
	u := newUnitSession(t)
	var held message.Message
	for range senderBudget.limit + 1 {
		held = u.send("")
	}
	u.onlines(pushQueue - senderBudget.limit - 1 + 1)
	if !u.ss.out.behind {
		t.Fatal("the queue did not fill")
	}
	u.start()
	u.waitWrites(pushQueue)
	u.quiet(pushQueue)
	u.clock.set(budgetT0.Add(5 * time.Minute))
	u.quiet(pushQueue)
	u.clock.set(budgetT0.Add(senderBudget.window))
	u.tick.fire()
	w := u.waitWrites(pushQueue + 1)
	if !strings.Contains(w[pushQueue], held.ID.String()) {
		t.Fatalf("write %s, want the push of %s", w[pushQueue], held.ID)
	}
	u.quiet(pushQueue + 1)
}

// A budgeted push that finds the queue full counts nothing, and the
// catch-up pushes it.
func TestBudgetedPushAfterFullQueue(t *testing.T) {
	u := newUnitSession(t)
	u.onlines(pushQueue)
	m := u.send("b")
	if n := u.pushes(); n != 0 || len(u.ss.out.held) != 0 || !u.ss.out.behind {
		t.Fatalf("after a full queue: %d pushes counted, %d held, behind %v", n, len(u.ss.out.held), u.ss.out.behind)
	}
	u.start()
	w := u.waitWrites(pushQueue + 1)
	if !strings.Contains(w[pushQueue], m.ID.String()) {
		t.Fatalf("write %s, want the push of %s", w[pushQueue], m.ID)
	}
	u.quiet(pushQueue + 1)
	if n := u.pushes(); n != 1 {
		t.Errorf("%d pushes counted, want 1", n)
	}
}

// A budget notice that finds the queue full is owed, once, and the writer
// pushes it after the catch-up.
func TestOwedNotice(t *testing.T) {
	u := newUnitSession(t)
	for range senderBudget.limit {
		u.send("")
	}
	u.onlines(pushQueue - senderBudget.limit)
	u.send("")
	u.send("")
	if n := len(u.ss.out.owed); n != 1 {
		t.Fatalf("%d notices owed, want 1", n)
	}
	u.start()
	w := u.waitWrites(pushQueue + 1)
	if !strings.Contains(w[pushQueue], `"kind":"budget"`) || !strings.Contains(w[pushQueue], `"sender":"a"`) {
		t.Fatalf("write %s, want the budget notice of a", w[pushQueue])
	}
	u.quiet(pushQueue + 1)
}

// Scenarios "Sender queue full" and "Broadcast with one full recipient".
func TestPendingCaps(t *testing.T) {
	d := startDaemon(t)
	a := d.join(t, "a", "t1")
	d.join(t, "b", "t1").leave()
	sendN(a, store.MaxPendingFromSender, msg("note", "b"))

	t.Run("sender queue full", func(t *testing.T) {
		a.t = t
		wantError(t, a.rpc("send", msg("note", "b")).Error, codeQueueFull, "the queue of b from a is full")
		if n := pendingCount(t, d.store, "b"); n != store.MaxPendingFromSender {
			t.Errorf("b holds %d messages, want %d", n, store.MaxPendingFromSender)
		}
	})

	t.Run("broadcast with one full recipient", func(t *testing.T) {
		a.t = t
		c := d.join(t, "c", "t1")
		res := a.send(msg("note", ""))
		if !slices.Equal(res.Recipients, []string{"c"}) || !slices.Equal(res.Skipped, []string{"b"}) ||
			!strings.Contains(res.Text, "Skipped, their pending queue is full: b.") {
			t.Fatalf("recipients %v skipped %v text %q, want c reached and b skipped", res.Recipients, res.Skipped, res.Text)
		}
		d.wantDeliver(t, c.deliver(), "c", res.ID)
		ps, _, err := d.store.Pending(context.Background(), "b", 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range ps {
			if p.ID.String() == res.ID {
				t.Errorf("the broadcast is pending for b")
			}
		}
	})
}
