package store

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/team"
)

const topic = "cappyt_agent-courier"

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newClock() *clock { return &clock{time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)} }

func open(t *testing.T, path string, c *clock) *Store {
	t.Helper()
	s, err := Open(path, c.now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// setup opens a store in a temporary directory with agents a, b and c on topic.
func setup(t *testing.T) (*Store, *clock) {
	t.Helper()
	c := newClock()
	s := open(t, filepath.Join(t.TempDir(), "state", "courier.db"), c)
	for _, a := range []string{"a", "b", "c"} {
		register(t, s, topic, a)
	}
	return s, c
}

func register(t *testing.T, s *Store, topic, agent string) *Sent {
	t.Helper()
	online, err := s.Register(t.Context(), topic, agent)
	if err != nil {
		t.Fatal(err)
	}
	return online
}

func msg(c *clock, sender string, kind message.Kind, to, summary string) message.Message {
	c.t = c.t.Add(time.Second)
	return message.Message{ID: message.NewID(c.t), Sender: sender, Created: c.t,
		Fields: message.Fields{Topic: topic, Kind: kind, To: to, Summary: summary}}
}

func send(t *testing.T, s *Store, m message.Message) Sent {
	t.Helper()
	got, err := s.Send(t.Context(), m)
	if err != nil {
		t.Fatalf("send %s: %v", m.Summary, err)
	}
	return got
}

func pending(t *testing.T, s *Store, agent string) []Pending {
	t.Helper()
	ps, total, err := s.Pending(t.Context(), agent, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != len(ps) {
		t.Fatalf("total %d, listed %d", total, len(ps))
	}
	return ps
}

// summaries returns the summaries pending for agent in order, without the
// online messages that registrations send.
func summaries(t *testing.T, s *Store, agent string) []string {
	t.Helper()
	var out []string
	for _, p := range pending(t, s, agent) {
		if p.Kind != message.KindOnline {
			out = append(out, p.Summary)
		}
	}
	return out
}

// onlines returns the senders of the online messages pending for agent.
func onlines(t *testing.T, s *Store, agent string) []string {
	t.Helper()
	var out []string
	for _, p := range pending(t, s, agent) {
		if p.Kind == message.KindOnline {
			out = append(out, p.Sender)
		}
	}
	return out
}

func want(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestSendListAck stores, lists and acknowledges messages, and covers the
// scenarios of "Delivery scope" and "Acknowledgement".
func TestSendListAck(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()

	note := msg(c, "a", message.KindNote, "", "broadcast")
	want(t, send(t, s, note).Recipients, "b", "c")
	handoff := msg(c, "a", message.KindHandoff, "b", "direct")
	handoff.Issue, handoff.Link = 4, "https://github.com/CappyT/agent-courier/issues/4"
	sent := send(t, s, handoff)
	want(t, sent.Recipients, "b")
	if sent.Message.ID != handoff.ID || sent.Skipped != nil {
		t.Fatalf("sent %+v", sent)
	}

	want(t, summaries(t, s, "a"))
	want(t, summaries(t, s, "b"), "direct", "broadcast")
	want(t, summaries(t, s, "c"), "broadcast")

	ps, total, err := s.Pending(ctx, "b", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 1 || total != 3 {
		t.Fatalf("limit 1: listed %d, total %d", len(ps), total)
	}
	got := ps[0]
	if got.ID != handoff.ID || got.Sender != "a" || got.Fields != handoff.Fields || !got.Created.Equal(handoff.Created) || got.Session != "" {
		t.Fatalf("pending %+v, want %+v", got, handoff)
	}
	if got.Created.Location() != time.UTC {
		t.Fatalf("created in %v, want UTC", got.Created.Location())
	}

	if _, err := s.Send(ctx, msg(c, "a", message.KindHandoff, "nobody", "x")); !errors.Is(err, ErrUnknownRecipient) {
		t.Fatalf("send to an unknown agent: %v", err)
	}
	register(t, s, "other-topic", "d")
	if _, err := s.Send(ctx, msg(c, "a", message.KindHandoff, "d", "x")); !errors.Is(err, ErrUnknownRecipient) {
		t.Fatalf("send to an agent of another topic: %v", err)
	}

	if err := s.Ack(ctx, "c", handoff.ID); !errors.Is(err, ErrNotPending) {
		t.Fatalf("c acknowledges b's message: %v", err)
	}
	want(t, summaries(t, s, "b"), "direct", "broadcast")
	if err := s.Ack(ctx, "b", handoff.ID); err != nil {
		t.Fatal(err)
	}
	want(t, summaries(t, s, "b"), "broadcast")
	if err := s.Ack(ctx, "b", handoff.ID); !errors.Is(err, ErrNotPending) {
		t.Fatalf("second ack: %v", err)
	}
	if err := s.Ack(ctx, "b", note.ID); err != nil {
		t.Fatal(err)
	}
	want(t, summaries(t, s, "b"))
	want(t, summaries(t, s, "c"), "broadcast")
}

// TestSendChecksTheSender covers the scenario "Sender outside the topic" of
// "Delivery scope", and a message to the sender itself.
func TestSendChecksTheSender(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	elsewhere := msg(c, "a", message.KindNote, "", "x")
	elsewhere.Topic = "other-topic"
	register(t, s, "other-topic", "b")
	if _, err := s.Send(ctx, elsewhere); !errors.Is(err, ErrSenderNotOnTopic) {
		t.Fatalf("send on a topic a does not follow: %v", err)
	}
	if _, err := s.Send(ctx, msg(c, "a", message.KindHandoff, "a", "x")); err == nil || !strings.Contains(err.Error(), "its sender") {
		t.Fatalf("send to self: %v", err)
	}
	bad := msg(c, "a", message.KindNote, "", "x")
	bad.Sender = "A B"
	if _, err := s.Send(ctx, bad); err == nil {
		t.Fatal("stored a message with an invalid sender")
	}
	want(t, summaries(t, s, "b"))
}

// TestSendReplyNeedsAsk covers the scenario "Reply without a valid ask" of
// "Message kinds".
func TestSendReplyNeedsAsk(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	ask := msg(c, "a", message.KindAsk, "", "which branch?")
	send(t, s, ask)
	note := msg(c, "a", message.KindNote, "", "fyi")
	send(t, s, note)
	register(t, s, "other", "a")
	register(t, s, "other", "b")
	elsewhere := msg(c, "a", message.KindAsk, "", "other topic")
	elsewhere.Topic = "other"
	send(t, s, elsewhere)

	reply := func(re message.ID) message.Message {
		m := msg(c, "b", message.KindReply, "a", "main")
		m.Re = re
		return m
	}
	for name, re := range map[string]message.ID{
		"no such message":      message.NewID(c.t),
		"re names a note":      note.ID,
		"ask on another topic": elsewhere.ID,
	} {
		if _, err := s.Send(ctx, reply(re)); !errors.Is(err, ErrNoAsk) {
			t.Errorf("%s: %v", name, err)
		}
	}
	want(t, send(t, s, reply(ask.ID)).Recipients, "a")
}

// TestPendingOrder covers the scenario of "Pending list".
func TestPendingOrder(t *testing.T) {
	s, c := setup(t)
	send(t, s, msg(c, "a", message.KindNote, "", "first broadcast"))
	send(t, s, msg(c, "c", message.KindNote, "", "second broadcast"))
	send(t, s, msg(c, "a", message.KindHandoff, "b", "direct"))
	want(t, summaries(t, s, "b"), "direct", "first broadcast", "second broadcast")

	for _, limit := range []int{0, -1} {
		ps, total, err := s.Pending(t.Context(), "b", limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(ps) != total || total != 4 {
			t.Fatalf("limit %d: listed %d, total %d, want all 4 with the online of c", limit, len(ps), total)
		}
	}
}

// TestMarkPushed records which session received a push, so the daemon pushes
// a message once per session and again to a new session ("Redelivery").
func TestMarkPushed(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	m := msg(c, "a", message.KindDone, "b", "done")
	send(t, s, m)
	session := func() string {
		t.Helper()
		for _, p := range pending(t, s, "b") {
			if p.ID == m.ID {
				return p.Session
			}
		}
		t.Fatal("message not pending")
		return ""
	}
	if err := s.MarkPushed(ctx, "b", "session-1", m.ID); err != nil {
		t.Fatal(err)
	}
	if got := session(); got != "session-1" {
		t.Fatalf("session %q", got)
	}
	// A new session of b receives the message, and then holds it as pushed.
	if err := s.MarkPushed(ctx, "b", "session-2", m.ID); err != nil {
		t.Fatal(err)
	}
	if got := session(); got != "session-2" {
		t.Fatalf("after the push to a new session: session %q", got)
	}
	if err := s.MarkPushed(ctx, "c", "session-3", m.ID); !errors.Is(err, ErrNotPending) {
		t.Fatalf("push to a non-recipient: %v", err)
	}
	if err := s.Ack(ctx, "b", m.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkPushed(ctx, "b", "session-3", m.ID); !errors.Is(err, ErrNotPending) {
		t.Fatalf("push of an acknowledged message: %v", err)
	}
}

// TestRegisterOnline covers the scenarios of "Presence" that the store
// decides: one online per hour per agent and topic, stored with the
// registration.
func TestRegisterOnline(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	t0 := c.t
	online := register(t, s, topic, "e")
	if online == nil {
		t.Fatal("first registration sends no online")
	}
	want(t, online.Recipients, "a", "b", "c")
	if m := online.Message; m.Kind != message.KindOnline || m.Sender != "e" || m.Topic != topic || m.To != "" {
		t.Fatalf("online %+v", m)
	}
	want(t, onlines(t, s, "a"), "b", "c", "e")

	c.t = t0.Add(10 * time.Minute)
	if register(t, s, topic, "e") != nil {
		t.Fatal("registration after 10 minutes sends an online")
	}
	agents, err := s.Agents(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if e := agents[3]; e.Name != "e" || !e.Registered.Equal(c.t) || !e.Online.Equal(t0) {
		t.Fatalf("quick registration does not refresh the registration time: %+v", e)
	}
	if register(t, s, "other", "e") == nil {
		t.Fatal("first registration on another topic sends no online")
	}

	c.t = t0.Add(time.Hour)
	if register(t, s, topic, "e") == nil {
		t.Fatal("registration an hour after the last online sends no online")
	}
	agents, err = s.Agents(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, a := range agents {
		names = append(names, a.Name)
	}
	want(t, names, "a", "b", "c", "e")
	if e := agents[3]; !e.Registered.Equal(c.t) || !e.Online.Equal(c.t) || e.Topic != topic || e.Registered.Location() != time.UTC {
		t.Fatalf("agent %+v", e)
	}
	for _, bad := range [][2]string{{"../etc", "e"}, {topic, message.Everyone}, {topic, "E"}} {
		if _, err := s.Register(ctx, bad[0], bad[1]); err == nil || !strings.Contains(err.Error(), "must match") {
			t.Errorf("register %q: %v", bad, err)
		}
	}
}

func TestRegisterTopicsIsAtomic(t *testing.T) {
	s, _ := setup(t)
	ctx := t.Context()
	if _, err := s.db.ExecContext(ctx, `CREATE TRIGGER fail BEFORE INSERT ON agents WHEN NEW.topic = 'second' AND NEW.name = 'e'
		BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterTopics(ctx, "e", []string{topic, "second"}); err == nil {
		t.Fatal("RegisterTopics succeeded over the injected failure")
	}
	agents, err := s.Agents(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 3 {
		t.Errorf("agents on %s after the failure: %+v, want a, b and c", topic, agents)
	}
	want(t, onlines(t, s, "a"), "b", "c")

	if _, err := s.db.ExecContext(ctx, `DROP TRIGGER fail`); err != nil {
		t.Fatal(err)
	}
	sent, err := s.RegisterTopics(ctx, "e", []string{topic, "second"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || sent[0].Message.Topic != topic || sent[1].Message.Topic != "second" {
		t.Fatalf("onlines %+v, want one per topic in order", sent)
	}
	want(t, sent[0].Recipients, "a", "b", "c")
	if _, err := s.RegisterTopics(ctx, "e", []string{topic, "../etc"}); err == nil || !strings.Contains(err.Error(), "must match") {
		t.Errorf("RegisterTopics with an invalid topic: %v", err)
	}
}

// TestOnlineReplacesOnline covers the scenario "Online not acknowledged" of
// "Presence".
func TestOnlineReplacesOnline(t *testing.T) {
	s, c := setup(t)
	var last *Sent
	for range 25 {
		c.t = c.t.Add(time.Hour)
		if online := register(t, s, topic, "b"); online != nil {
			last = online
		}
	}
	want(t, onlines(t, s, "a"), "c", "b")
	for _, p := range pending(t, s, "a") {
		if p.Sender == "b" && p.ID != last.Message.ID {
			t.Fatalf("a holds online %s, want the last one %s", p.ID, last.Message.ID)
		}
	}
	// An acknowledged online stays acknowledged; the next one is pending again.
	if err := s.Ack(t.Context(), "a", last.Message.ID); err != nil {
		t.Fatal(err)
	}
	c.t = c.t.Add(time.Hour)
	register(t, s, topic, "b")
	want(t, onlines(t, s, "a"), "c", "b")
}

// TestPendingCaps covers the scenarios of "Pending caps".
func TestPendingCaps(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	// b already holds the online of c: it must not count.
	for i := range MaxPendingFromSender {
		send(t, s, msg(c, "c", message.KindNote, "b", "n"+strconv.Itoa(i)))
	}
	_, err := s.Send(ctx, msg(c, "c", message.KindNote, "b", "one more"))
	full, ok := errors.AsType[*QueueFullError](err)
	if !ok || full.Recipient != "b" || full.Sender != "c" || full.Total {
		t.Fatalf("direct message to a full queue: %v", err)
	}
	if !strings.Contains(err.Error(), "the queue of b from c is full") {
		t.Fatalf("error %q", err)
	}

	sent := send(t, s, msg(c, "c", message.KindNote, "", "broadcast"))
	want(t, sent.Recipients, "a")
	want(t, sent.Skipped, "b")
	want(t, summaries(t, s, "a"), "broadcast")
	if n := len(summaries(t, s, "b")); n != MaxPendingFromSender {
		t.Fatalf("b holds %d messages from c", n)
	}

	// Onlines pass a full queue and do not fill it.
	c.t = c.t.Add(time.Hour)
	want(t, register(t, s, topic, "c").Recipients, "a", "b")
	if err := s.Ack(ctx, "b", pending(t, s, "b")[0].ID); err != nil {
		t.Fatal(err)
	}
	send(t, s, msg(c, "c", message.KindNote, "b", "room again"))
}

func TestPendingTotalCap(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	senders := MaxPendingTotal / MaxPendingFromSender
	for i := range senders + 1 {
		register(t, s, topic, "s"+strconv.Itoa(i))
	}
	for i := range senders {
		for range MaxPendingFromSender {
			send(t, s, msg(c, "s"+strconv.Itoa(i), message.KindNote, "b", "x"))
		}
	}
	last := "s" + strconv.Itoa(senders)
	_, err := s.Send(ctx, msg(c, last, message.KindNote, "b", "x"))
	if full, ok := errors.AsType[*QueueFullError](err); !ok || !full.Total || full.Recipient != "b" {
		t.Fatalf("direct message past the total cap: %v", err)
	}
	if !strings.Contains(err.Error(), "the queue of b is full") {
		t.Fatalf("error %q", err)
	}
	sent := send(t, s, msg(c, last, message.KindNote, "", "broadcast"))
	if !slices.Contains(sent.Skipped, "b") || slices.Contains(sent.Recipients, "b") {
		t.Fatalf("broadcast past the total cap: %+v", sent)
	}
}

// TestRetention covers "Retention" with an injected clock.
func TestRetention(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	register(t, s, topic, "idle")
	old := msg(c, "a", message.KindNote, "", "old")
	send(t, s, old)
	oldAsk := msg(c, "a", message.KindAsk, "", "old ask")
	send(t, s, oldAsk)

	c.t = c.t.Add(29 * 24 * time.Hour)
	for _, a := range []string{"a", "b", "c"} {
		register(t, s, topic, a)
	}
	recent := msg(c, "a", message.KindNote, "", "recent")
	send(t, s, recent)

	c.t = c.t.Add(2 * 24 * time.Hour)
	want(t, summaries(t, s, "b"), "recent")
	if err := s.Ack(ctx, "b", old.ID); !errors.Is(err, ErrNotPending) {
		t.Fatalf("ack of a 31-day-old message: %v", err)
	}
	if err := s.MarkPushed(ctx, "b", "session-1", old.ID); !errors.Is(err, ErrNotPending) {
		t.Fatalf("push of a 31-day-old message: %v", err)
	}
	reply := msg(c, "b", message.KindReply, "a", "too late")
	reply.Re = oldAsk.ID
	if _, err := s.Send(ctx, reply); !errors.Is(err, ErrNoAsk) {
		t.Fatalf("reply to a 31-day-old ask: %v", err)
	}
	if _, err := s.Send(ctx, msg(c, "a", message.KindHandoff, "idle", "x")); !errors.Is(err, ErrUnknownRecipient) {
		t.Fatalf("send to an agent idle for 31 days: %v", err)
	}
	if _, err := s.Send(ctx, msg(c, "idle", message.KindNote, "", "x")); !errors.Is(err, ErrSenderNotOnTopic) {
		t.Fatalf("send from an agent idle for 31 days: %v", err)
	}
	want(t, send(t, s, msg(c, "a", message.KindNote, "", "now")).Recipients, "b", "c")
	agents, err := s.Agents(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 3 {
		t.Fatalf("agents %+v, want a, b and c", agents)
	}

	if err := s.Cleanup(ctx); err != nil {
		t.Fatal(err)
	}
	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		if err := s.db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	cutoff := c.t.Add(-Retention).UnixMilli()
	if n := count(`SELECT count(*) FROM messages WHERE created < ? OR id IN (?, ?)`, cutoff, old.ID.String(), oldAsk.ID.String()); n != 0 {
		t.Errorf("%d expired messages after cleanup", n)
	}
	if n := count(`SELECT count(*) FROM deliveries WHERE message_id NOT IN (SELECT id FROM messages)`); n != 0 {
		t.Errorf("%d deliveries of deleted messages", n)
	}
	if n := count(`SELECT count(*) FROM agents WHERE topic = ?`, topic); n != 3 {
		t.Errorf("%d agents after cleanup, want a, b and c", n)
	}
	want(t, summaries(t, s, "b"), "recent", "now")
}

// TestTouchKeepsLiveAgents covers the scenario "Long session" of "Retention".
func TestTouchKeepsLiveAgents(t *testing.T) {
	s, c := setup(t)
	ctx := t.Context()
	register(t, s, topic, "long")
	for range 31 {
		c.t = c.t.Add(24 * time.Hour)
		if err := s.Touch(ctx, topic, "long"); err != nil {
			t.Fatal(err)
		}
		if err := s.Touch(ctx, topic, "a"); err != nil {
			t.Fatal(err)
		}
	}
	want(t, send(t, s, msg(c, "a", message.KindHandoff, "long", "still here")).Recipients, "long")
	if _, err := s.Send(ctx, msg(c, "a", message.KindHandoff, "b", "x")); !errors.Is(err, ErrUnknownRecipient) {
		t.Fatalf("send to b, idle for 31 days: %v", err)
	}
	// Touch sends no online, and a later registration still does.
	want(t, onlines(t, s, "long"))
	if register(t, s, topic, "long") == nil {
		t.Fatal("registration 31 days after the last online sends none: Touch moved the online time")
	}
	if err := s.Touch(ctx, topic, "new"); err != nil {
		t.Fatal(err)
	}
	if register(t, s, topic, "new") == nil {
		t.Fatal("registration after Touch of a new agent sends no online")
	}
	if err := s.Touch(ctx, "../etc", "a"); err == nil {
		t.Fatal("touched an invalid topic")
	}
}

// TestPendingSurvivesReopen is task 4.3: a pending message survives closing
// and reopening the store.
func TestPendingSurvivesReopen(t *testing.T) {
	c := newClock()
	path := filepath.Join(t.TempDir(), "courier.db")
	s, err := Open(path, c.now)
	if err != nil {
		t.Fatal(err)
	}
	register(t, s, topic, "a")
	register(t, s, topic, "b")
	ask := msg(c, "a", message.KindAsk, "", "which branch?")
	send(t, s, ask)
	m := msg(c, "b", message.KindReply, "a", "main")
	m.Re, m.Issue, m.Link = ask.ID, 9, "https://example.com/9"
	send(t, s, m)
	var mode string
	if err := s.db.QueryRowContext(t.Context(), `PRAGMA journal_mode`).Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal mode %q, %v", mode, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, path, c)
	ps, total, err := s.Pending(t.Context(), "a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || ps[0].ID != m.ID {
		t.Fatalf("after reopen: total %d, %+v", total, ps)
	}
	if got := ps[0]; got.Sender != m.Sender || got.Fields != m.Fields || !got.Created.Equal(m.Created) {
		t.Fatalf("after reopen %+v, want %+v", got, m)
	}
	want(t, summaries(t, s, "b"), "which branch?")
}

// TestOpenPathWithURICharacters opens a path that a raw URI would cut at ?
// or #.
func TestOpenPathWithURICharacters(t *testing.T) {
	c := newClock()
	path := filepath.Join(t.TempDir(), "a?b#c %d", "courier.db")
	s := open(t, path, c)
	register(t, s, topic, "a")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() == 0 {
		t.Fatalf("database at %s: %v", path, err)
	}
	s = open(t, path, c)
	if agents, err := s.Agents(t.Context(), topic); err != nil || len(agents) != 1 {
		t.Fatalf("agents after reopen %+v, %v", agents, err)
	}
}

// TestWritesWaitForTheLock holds the write lock on a second connection for a
// second, after a write, and checks that Register and Send wait for it
// instead of failing.
func TestWritesWaitForTheLock(t *testing.T) {
	c := newClock()
	path := filepath.Join(t.TempDir(), "courier.db")
	s := open(t, path, c)
	register(t, s, topic, "a")
	register(t, s, topic, "b")
	other, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { other.Close() })

	hold := func(name string) {
		t.Helper()
		conn, err := other.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(t.Context(), `BEGIN IMMEDIATE`); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(t.Context(), `INSERT INTO agents (topic, name, registered, online) VALUES ('held', ?, 1, 1)`, name); err != nil {
			t.Fatal(err)
		}
		go func() {
			defer conn.Close()
			time.Sleep(time.Second)
			conn.ExecContext(t.Context(), `COMMIT`)
		}()
	}

	hold("x")
	start := time.Now()
	c.t = c.t.Add(time.Hour)
	if _, err := s.Register(t.Context(), topic, "a"); err != nil {
		t.Fatalf("register under a held lock: %v", err)
	}
	if d := time.Since(start); d < 900*time.Millisecond {
		t.Fatalf("register returned after %v, before the lock was free", d)
	}

	hold("y")
	start = time.Now()
	if _, err := s.Send(t.Context(), msg(c, "a", message.KindNote, "b", "x")); err != nil {
		t.Fatalf("send under a held lock: %v", err)
	}
	if d := time.Since(start); d < 900*time.Millisecond {
		t.Fatalf("send returned after %v, before the lock was free", d)
	}
}

func TestOpenSetsModes(t *testing.T) {
	old := syscall.Umask(0o022)
	defer syscall.Umask(old)
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "courier.db")
	c := newClock()
	s := open(t, path, c)
	register(t, s, topic, "a")
	for p, want := range map[string]os.FileMode{dir: 0o700 | os.ModeDir, path: 0o600, path + "-wal": 0o600} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode() != want {
			t.Errorf("%s has mode %v, want %v", filepath.Base(p), fi.Mode(), want)
		}
	}
	s.Close()

	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	open(t, path, c)
	if fi, err := os.Stat(path); err != nil || fi.Mode() != 0o600 {
		t.Fatalf("existing file: mode %v, %v", fi.Mode(), err)
	}
}

// A courier.db with the team tables, the outbox and the seen table of an
// earlier release opens without them, and a send on a team topic works.
func TestOpenDropsTeamTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "courier.db")
	old, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`CREATE TABLE messages (id TEXT PRIMARY KEY, topic TEXT NOT NULL, sender TEXT NOT NULL,
		recipient TEXT NOT NULL, kind TEXT NOT NULL, summary TEXT NOT NULL, issue INTEGER NOT NULL, link TEXT NOT NULL,
		re TEXT NOT NULL, created INTEGER NOT NULL, origin TEXT NOT NULL DEFAULT '', wire_id TEXT NOT NULL DEFAULT '') STRICT;
		CREATE INDEX messages_wire ON messages (origin, wire_id) WHERE origin <> '';
		CREATE TABLE teams (name TEXT PRIMARY KEY, relays TEXT NOT NULL DEFAULT '[]', created INTEGER NOT NULL) STRICT;
		CREATE TABLE team_topics (topic TEXT PRIMARY KEY, team TEXT NOT NULL REFERENCES teams (name) ON DELETE CASCADE) STRICT;
		CREATE TABLE members (team TEXT NOT NULL REFERENCES teams (name) ON DELETE CASCADE, peer_id TEXT NOT NULL) STRICT;
		CREATE TABLE invites (secret_hash BLOB PRIMARY KEY, team TEXT NOT NULL REFERENCES teams (name) ON DELETE CASCADE) STRICT;
		CREATE TABLE outbox (message_id TEXT NOT NULL REFERENCES messages (id) ON DELETE CASCADE,
			team TEXT NOT NULL REFERENCES teams (name) ON DELETE CASCADE, peer_id TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
			next_attempt INTEGER NOT NULL, created INTEGER NOT NULL, PRIMARY KEY (message_id, peer_id)) STRICT, WITHOUT ROWID;
		CREATE TABLE seen (peer_id TEXT NOT NULL, message_id TEXT NOT NULL, received INTEGER NOT NULL,
			PRIMARY KEY (peer_id, message_id)) STRICT, WITHOUT ROWID;
		CREATE INDEX seen_peer ON seen (peer_id, received);
		INSERT INTO teams (name, created) VALUES ('courier', 1);`); err != nil {
		t.Fatal(err)
	}
	old.Close()
	c := newClock()
	s := open(t, path, c)
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM sqlite_master
		WHERE name IN ('teams', 'team_topics', 'members', 'invites', 'outbox', 'seen', 'seen_peer', 'messages_wire')`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d team tables stay, %v", n, err)
	}
	register(t, s, topic, "a")
	m := msg(c, "a", message.KindNote, "bob/claude", "x")
	if sent, err := s.SendTeam(t.Context(), m, team.Route{Team: "courier"}); err != nil || len(sent.Recipients) != 0 {
		t.Fatalf("send to a remote agent: %+v %v", sent, err)
	}
}
