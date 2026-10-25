//go:build unix

package daemon

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/store"
)

// pushQueue is the most deliver notifications that wait for the writer of
// one session. A push that finds the queue full marks the session behind,
// and the writer pushes the pending messages again once it emptied the queue.
const pushQueue = 256

// outbox holds the pushes of one registered session. Its writer drains queue
// after the register response, so a sender never waits on a recipient.
type outbox struct {
	agent string
	queue chan delivery
	done  chan struct{}

	mu     sync.Mutex
	queued map[message.ID]struct{}
	// behind is true while pending messages may miss a push to this session:
	// before the first replay, and after a push found the queue full.
	behind bool
	// pushes are the budgeted pushes of the last budgetWindow, oldest first.
	pushes []pushed
	// held are the messages over a budget, or behind a held message of their
	// sender, oldest first by the time this machine stored them. The writer offers them again when budgets have room.
	held []message.Message
	// noticed is when the budget notice of a key was queued.
	noticed map[noticeKey]time.Time
	// owed are the budget notices that found the queue full.
	owed []owedNotice
	// onlines are the times of the remote onlines pushed in the last
	// window of remoteOnlineBudget, by senderKey: the peer id of the
	// member.
	onlines map[string][]time.Time
}

// budget caps the pushes into one session in a sliding window. A session
// budget counts the pushes of all senders.
type budget struct {
	text    string
	limit   int
	window  time.Duration
	session bool
}

var (
	senderBudget       = budget{"10 pushes per sender in 10 minutes", 10, 10 * time.Minute, false}
	senderDirectBudget = budget{"10 direct messages per sender in one hour", 10, time.Hour, false}
	directBudget       = budget{"30 direct messages in one hour", 30, time.Hour, true}
	broadcastBudget    = budget{"30 broadcasts in one hour", 30, time.Hour, true}
)

// budgetWindow is the longest window of a budget.
const budgetWindow = time.Hour

// reofferEvery is how often the writer offers the held messages again when
// no push wakes it.
const reofferEvery = time.Minute

// pushed is a budgeted push. sender is the senderKey of its message.
type pushed struct {
	at     time.Time
	sender string
	direct bool
}

// senderKey is what the per-sender budgets and the order of held messages
// count by: the agent name for a message of this machine, the peer id for a
// message from a member. So a member cannot pass a per-sender budget by
// naming a new agent for each message. A space is in no agent name.
func senderKey(m message.Message) string {
	if m.Origin != "" {
		return "member " + m.Origin
	}
	return m.Sender
}

// noticeKey names the notices that go once per window. sender is empty for
// a session budget.
type noticeKey struct {
	budget *budget
	sender string
}

type owedNotice struct {
	key noticeKey
	d   delivery
}

type offerResult int

const (
	offerQueued offerResult = iota
	offerFull
	offerHeld
)

// delivery is one deliver line. id is the stored message, zero for a notice
// that no store row backs.
type delivery struct {
	id   message.ID
	note notification
}

type notification struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  deliverParams `json:"params"`
}

type deliverParams struct {
	Text   string            `json:"text"`
	Notice string            `json:"notice"`
	Attrs  map[string]string `json:"attrs"`
}

func newOutbox(agent string) *outbox {
	return &outbox{agent: agent, queue: make(chan delivery, pushQueue), done: make(chan struct{}),
		queued: map[message.ID]struct{}{}, behind: true, noticed: map[noticeKey]time.Time{}, onlines: map[string][]time.Time{}}
}

// remoteOnlineBudget caps the onlines from one member's machine that one
// session gets pushed. Onlines from another person's machine are outside the
// same-user boundary of the local mode.
var remoteOnlineBudget = budget{"30 onlines per member in one hour", 30, time.Hour, false}

// offerOnline offers the push of the online m. A local online has no cap.
// A remote one, with an origin, is held when its member used the budget of
// the session at the time of clock: it stays pending, and a later replay
// offers it again. The budget counts by senderKey, the peer id, so a member
// cannot pass it under another label.
func (out *outbox) offerOnline(m message.Message, clock func() time.Time) offerResult {
	out.mu.Lock()
	defer out.mu.Unlock()
	if m.Origin == "" {
		if !out.offerLocked(delivery{m.ID, deliverOf(m)}) {
			return offerFull
		}
		return offerQueued
	}
	now := clock()
	member := senderKey(m)
	times := slices.DeleteFunc(out.onlines[member], func(at time.Time) bool { return now.Sub(at) >= remoteOnlineBudget.window })
	if len(times) >= remoteOnlineBudget.limit {
		out.onlines[member] = times
		return offerHeld
	}
	if !out.offerLocked(delivery{m.ID, deliverOf(m)}) {
		out.onlines[member] = times
		return offerFull
	}
	out.onlines[member] = append(times, now)
	maps.DeleteFunc(out.onlines, func(_ string, ts []time.Time) bool {
		return len(ts) == 0 || now.Sub(ts[len(ts)-1]) >= remoteOnlineBudget.window
	})
	return offerQueued
}

func (out *outbox) has(id message.ID) bool {
	out.mu.Lock()
	defer out.mu.Unlock()
	_, ok := out.queued[id]
	return ok
}

// offer queues d without waiting. On a full queue it marks the outbox
// behind and returns false.
func (out *outbox) offer(d delivery) bool {
	out.mu.Lock()
	defer out.mu.Unlock()
	return out.offerLocked(d)
}

func (out *outbox) offerLocked(d delivery) bool {
	if _, ok := out.queued[d.id]; ok && d.id != (message.ID{}) {
		return true
	}
	select {
	case out.queue <- d:
		if d.id != (message.ID{}) {
			out.queued[d.id] = struct{}{}
		}
		return true
	default:
		out.behind = true
		return false
	}
}

// offerBudgeted offers the push of m within the push budgets of the session
// at now. It holds m when m passes a budget or its sender has held
// messages, and then returns the budget m passes, nil when only the order
// holds it. A held message stays out of queued. The first time a budget is
// passed in its window, a budget notice is queued, or owed on a full queue.
func (out *outbox) offerBudgeted(m message.Message, now time.Time) (offerResult, *budget) {
	out.mu.Lock()
	defer out.mu.Unlock()
	if _, ok := out.queued[m.ID]; ok {
		return offerQueued, nil
	}
	if slices.ContainsFunc(out.held, func(h message.Message) bool { return h.ID == m.ID }) {
		return offerHeld, nil
	}
	out.forget(now)
	b := out.over(m, now)
	if b == nil && !slices.ContainsFunc(out.held, func(h message.Message) bool { return senderKey(h) == senderKey(m) }) {
		if !out.offerLocked(delivery{m.ID, deliverOf(m)}) {
			return offerFull, nil
		}
		out.pushes = append(out.pushes, pushed{now, senderKey(m), m.To != ""})
		return offerQueued, nil
	}
	// By the local time, then the id: a member chooses the id of its
	// message, so the id alone would let it jump the queue.
	i, _ := slices.BinarySearchFunc(out.held, m, func(h, m message.Message) int {
		return cmp.Or(h.Created.Compare(m.Created), bytes.Compare(h.ID[:], m.ID[:]))
	})
	out.held = slices.Insert(out.held, i, m)
	if b != nil {
		out.notify(m, b, now)
	}
	return offerHeld, b
}

// reoffer offers the held messages again, oldest first, when their budgets
// have room at the time of clock. A sender keeps its order: after one of its
// messages stays held, its newer ones stay held too. A message that still
// passes a budget gets its notice when none went in the budget's window.
func (out *outbox) reoffer(clock func() time.Time) {
	out.mu.Lock()
	defer out.mu.Unlock()
	if len(out.held) == 0 {
		return
	}
	now := clock()
	out.forget(now)
	blocked := map[string]bool{}
	kept := out.held[:0]
	for i, m := range out.held {
		if blocked[senderKey(m)] {
			kept = append(kept, m)
			continue
		}
		if b := out.over(m, now); b != nil {
			blocked[senderKey(m)] = true
			kept = append(kept, m)
			out.notify(m, b, now)
			continue
		}
		if !out.offerLocked(delivery{m.ID, deliverOf(m)}) {
			kept = append(kept, out.held[i:]...)
			break
		}
		out.pushes = append(out.pushes, pushed{now, senderKey(m), m.To != ""})
	}
	clear(out.held[len(kept):])
	out.held = kept
}

// unhold drops a held message that the agent acknowledged.
func (out *outbox) unhold(id message.ID) {
	out.mu.Lock()
	defer out.mu.Unlock()
	out.held = slices.DeleteFunc(out.held, func(h message.Message) bool { return h.ID == id })
}

// offerOwed queues the owed budget notices, oldest first, at the time of
// clock, until the queue is full.
func (out *outbox) offerOwed(clock func() time.Time) {
	out.mu.Lock()
	defer out.mu.Unlock()
	if len(out.owed) == 0 {
		return
	}
	now := clock()
	for i, o := range out.owed {
		if !out.offerLocked(o.d) {
			out.owed = slices.Delete(out.owed, 0, i)
			return
		}
		out.noticed[o.key] = now
	}
	out.owed = nil
}

// notify queues the notice that m passed b, unless a notice of the same key
// went in the window of b or is owed. On a full queue the notice is owed.
func (out *outbox) notify(m message.Message, b *budget, now time.Time) {
	k := noticeKey{budget: b}
	if !b.session {
		k.sender = senderKey(m)
	}
	if last, ok := out.noticed[k]; ok && now.Sub(last) < b.window {
		return
	}
	if slices.ContainsFunc(out.owed, func(o owedNotice) bool { return o.key == k }) {
		return
	}
	d := budgetNotice(m, out.agent, b)
	if out.offerLocked(d) {
		out.noticed[k] = now
		return
	}
	out.owed = append(out.owed, owedNotice{k, d})
}

// forget drops the pushes and notices older than budgetWindow.
func (out *outbox) forget(now time.Time) {
	out.pushes = slices.DeleteFunc(out.pushes, func(p pushed) bool { return now.Sub(p.at) >= budgetWindow })
	maps.DeleteFunc(out.noticed, func(_ noticeKey, at time.Time) bool { return now.Sub(at) >= budgetWindow })
}

// over returns the first push budget of the session that m would pass at
// now, or nil.
func (out *outbox) over(m message.Message, now time.Time) *budget {
	var fromSender, directFromSender, directs, broadcasts int
	key := senderKey(m)
	for _, p := range out.pushes {
		if p.sender == key {
			if now.Sub(p.at) < senderBudget.window {
				fromSender++
			}
			if p.direct {
				directFromSender++
			}
		}
		if p.direct {
			directs++
		} else {
			broadcasts++
		}
	}
	direct := m.To != ""
	switch {
	case fromSender >= senderBudget.limit:
		return &senderBudget
	case direct && directFromSender >= senderDirectBudget.limit:
		return &senderDirectBudget
	case direct && directs >= directBudget.limit:
		return &directBudget
	case !direct && broadcasts >= broadcastBudget.limit:
		return &broadcastBudget
	}
	return nil
}

// budgetNotice is the deliver that tells recipient that m passed b. It has
// no id: no store row backs it. The text names the sender of m only for a
// per-sender budget: for a message from a member, the member, because its
// budget counts every agent of that machine.
func budgetNotice(m message.Message, recipient string, b *budget) delivery {
	from := m.Sender
	if m.Origin != "" {
		from, _, _ = strings.Cut(m.Sender, "/")
	}
	n := message.Message{Sender: from, Fields: message.Fields{Topic: m.Topic, Kind: message.KindBudget, To: recipient}}
	attrs := map[string]string{"topic": m.Topic, "sender": from, "kind": string(message.KindBudget), "to": recipient}
	sender := from
	if b.session {
		sender = ""
	}
	return delivery{note: notification{JSONRPC: "2.0", Method: "deliver", Params: deliverParams{
		Text: message.RenderBudget(sender, recipient, b.text), Notice: message.Notice(n), Attrs: attrs}}}
}

// catchUp reports whether the writer must push the pending messages again:
// the outbox is behind and its queue is empty. It clears behind.
func (out *outbox) catchUp() bool {
	out.mu.Lock()
	defer out.mu.Unlock()
	if !out.behind || len(out.queue) > 0 {
		return false
	}
	out.behind = false
	return true
}

func deliverOf(m message.Message) notification {
	to := m.To
	if to == "" {
		to = message.Everyone
	}
	attrs := map[string]string{"id": m.ID.String(), "topic": m.Topic, "sender": m.Sender, "kind": string(m.Kind), "to": to}
	if m.Issue > 0 {
		attrs["issue"] = strconv.Itoa(m.Issue)
	}
	if m.Re != (message.ID{}) {
		attrs["re"] = m.Re.String()
	}
	return notification{JSONRPC: "2.0", Method: "deliver",
		Params: deliverParams{Text: message.Render(m), Notice: message.Notice(m), Attrs: attrs}}
}

// pushTo pushes m to the live session of agent, when it has one that
// finished its registration.
func (s *Server) pushTo(agent string, m message.Message) {
	s.mu.Lock()
	ss := s.names[agent]
	var out *outbox
	if ss != nil {
		out = ss.out
	}
	s.mu.Unlock()
	if out != nil {
		s.push(ss, out, m)
	}
}

// push queues m for the session ss. Every push goes through it, and it
// queues a message at most once per session. A push it does not queue
// leaves the message in pending.
func (s *Server) push(ss *session, out *outbox, m message.Message) {
	if out.has(m.ID) {
		return
	}
	res := offerFull
	if m.Kind == message.KindOnline {
		if res = out.offerOnline(m, s.store.Now); res == offerHeld {
			// The sender is another member's text: only the cap is logged.
			s.log.Debug("remote online over the cap, it stays pending", "session", ss.id, "agent", out.agent,
				"id", m.ID, "cap", remoteOnlineBudget.text)
			return
		}
	} else {
		var b *budget
		if res, b = out.offerBudgeted(m, s.store.Now()); res == offerHeld {
			why := "a held message of the same sender"
			if b != nil {
				why = b.text
			}
			// senderKey: the agent name of a member's message is its text.
			s.log.Debug("push held, the message stays pending", "session", ss.id, "agent", out.agent,
				"sender", senderKey(m), "id", m.ID, "reason", why)
			return
		}
	}
	if res == offerFull {
		s.log.Warn("push queue full, the session is behind", "session", ss.id, "agent", out.agent, "id", m.ID)
	}
}

// afterRegister runs after the register response is written. It starts the
// writer of the session's pushes, which first pushes the pending messages.
func (s *Server) afterRegister(ctx context.Context, ss *session) {
	out := ss.out
	s.wg.Go(func() { s.drain(ctx, ss, out) })
}

// replay pushes the pending messages of the agent that this session has not
// received, direct ones first, oldest first.
func (s *Server) replay(ctx context.Context, ss *session, out *outbox) {
	ps, _, err := s.store.Pending(ctx, out.agent, 0)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Error("pending for a push failed", "session", ss.id, "agent", out.agent, "err", err)
		}
		return
	}
	for _, p := range ps {
		if p.Session != ss.id {
			s.push(ss, out, p.Message)
		}
	}
}

// drain writes the queued pushes of ss and records each one it wrote. When
// the session is behind and its queue is empty, it replays the pending
// messages and offers the owed notices first. It offers the held messages
// again after each push, after that catch-up, and on each tick. A failed
// write closes the connection, as for a response.
func (s *Server) drain(ctx context.Context, ss *session, out *outbox) {
	tick, stop := s.reofferTick()
	defer stop()
	for {
		if out.catchUp() {
			s.replay(ctx, ss, out)
			out.offerOwed(s.store.Now)
			out.reoffer(s.store.Now)
		}
		var d delivery
		select {
		case <-out.done:
			return
		case <-tick:
			out.reoffer(s.store.Now)
			continue
		case d = <-out.queue:
		}
		if err := ss.write(d.note); err != nil {
			if errors.Is(err, errLineCap) {
				s.log.Error("push over the line cap, the message stays pending", "session", ss.id, "id", d.id)
				continue
			}
			s.log.Warn("push failed, closing", "session", ss.id, "err", err)
			return
		}
		if d.id != (message.ID{}) {
			if err := s.store.MarkPushed(ctx, out.agent, ss.id, d.id); err != nil &&
				!errors.Is(err, store.ErrNotPending) && ctx.Err() == nil {
				s.log.Error("recording a push failed", "session", ss.id, "id", d.id, "err", err)
			}
		}
		out.reoffer(s.store.Now)
	}
}

// announce pushes the online message that store.RegisterTopics stored to
// the live sessions of its recipients.
func (s *Server) announce(online store.Sent) {
	s.log.Debug("online stored", "agent", online.Message.Sender, "topic", online.Message.Topic,
		"id", online.Message.ID, "recipients", len(online.Recipients))
	for _, r := range online.Recipients {
		s.pushTo(r, online.Message)
	}
}
