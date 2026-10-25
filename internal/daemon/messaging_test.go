//go:build unix

package daemon

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/version"
)

// peer is a registered client that keeps the deliver notifications it reads
// while it waits for a response.
type peer struct {
	*client
	name  string
	srv   *Server
	inbox []deliverParams
}

type deliverLine struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  deliverParams `json:"params"`
}

// readAny reads one line: a response, or a deliver notification.
func (p *peer) readAny(timeout time.Duration) (resp, *deliverParams, bool) {
	p.t.Helper()
	p.conn.SetReadDeadline(time.Now().Add(timeout))
	b, err := p.r.ReadBytes('\n')
	if err != nil {
		return resp{}, nil, false
	}
	var members map[string]jsontext.Value
	if err := json.Unmarshal(b, &members); err != nil {
		p.t.Fatalf("line %s: %v", b, err)
	}
	if _, ok := members["method"]; !ok {
		return parseResp(p.t, b), nil, true
	}
	var n deliverLine
	if err := json.Unmarshal(b, &n, json.RejectUnknownMembers(true)); err != nil || n.JSONRPC != "2.0" || n.Method != "deliver" {
		p.t.Fatalf("not a deliver notification: %s (%v)", b, err)
	}
	return resp{}, &n.Params, true
}

func (p *peer) rpc(method string, params any) resp {
	p.t.Helper()
	p.next++
	req := map[string]any{"jsonrpc": "2.0", "id": p.next, "method": method}
	if params != nil {
		req["params"] = params
	}
	b, err := json.Marshal(req)
	if err != nil {
		p.t.Fatal(err)
	}
	p.line(string(b) + "\n")
	for {
		r, d, ok := p.readAny(5 * time.Second)
		if !ok {
			p.t.Fatalf("no response to %s", method)
		}
		if d != nil {
			if method == "register" {
				p.t.Fatalf("deliver before the register response: %+v", d)
			}
			p.inbox = append(p.inbox, *d)
			continue
		}
		if string(r.ID) != strconv.Itoa(p.next) {
			p.t.Fatalf("response id %s, want %d", r.ID, p.next)
		}
		return r
	}
}

// deliver returns the next deliver notification.
func (p *peer) deliver() deliverParams {
	p.t.Helper()
	d, ok := p.deliverWithin(5 * time.Second)
	if !ok {
		p.t.Fatalf("%s: no deliver notification", p.name)
	}
	return d
}

// deliverWithin returns the next deliver notification, if one arrives
// within timeout.
func (p *peer) deliverWithin(timeout time.Duration) (deliverParams, bool) {
	p.t.Helper()
	if len(p.inbox) > 0 {
		d := p.inbox[0]
		p.inbox = p.inbox[1:]
		return d, true
	}
	r, d, ok := p.readAny(timeout)
	if !ok {
		return deliverParams{}, false
	}
	if d == nil {
		p.t.Fatalf("%s: unexpected response %+v", p.name, r)
	}
	return *d, true
}

// noDeliver checks that no deliver arrives within a short time.
func (p *peer) noDeliver() {
	p.t.Helper()
	if len(p.inbox) > 0 {
		p.t.Fatalf("%s: unexpected deliver %+v", p.name, p.inbox[0])
	}
	if _, d, ok := p.readAny(150 * time.Millisecond); ok {
		p.t.Fatalf("%s: unexpected line, deliver %+v", p.name, d)
	}
}

// onlines reads one deliver per sender and checks it is the online of
// that sender.
func (p *peer) onlines(senders ...string) {
	p.t.Helper()
	for _, s := range senders {
		if got := p.deliver(); got.Attrs["kind"] != "online" || got.Attrs["sender"] != s {
			p.t.Fatalf("%s: deliver %v, want the online of %s", p.name, got.Attrs, s)
		}
	}
}

func (p *peer) leave() {
	p.t.Helper()
	p.conn.Close()
	waitFree(p.t, p.srv, p.name)
}

func (p *peer) send(f map[string]any) sendResult {
	p.t.Helper()
	r := p.rpc("send", f)
	if r.Error != nil {
		p.t.Fatalf("%s send %v: %+v", p.name, f, r.Error)
	}
	var res sendResult
	if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
		p.t.Fatal(err)
	}
	return res
}

func (p *peer) pending() pendingResult {
	p.t.Helper()
	r := p.rpc("pending", nil)
	if r.Error != nil {
		p.t.Fatalf("pending: %+v", r.Error)
	}
	var res pendingResult
	if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
		p.t.Fatal(err)
	}
	return res
}

func (p *peer) ack(id string) *rpcError {
	p.t.Helper()
	r := p.rpc("ack", map[string]any{"id": id})
	if r.Error == nil {
		var res textResult
		if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
			p.t.Fatal(err)
		}
		if want := "Acknowledged " + strings.ToUpper(id) + "."; res.Text != want {
			p.t.Errorf("ack text %q, want %q", res.Text, want)
		}
	}
	return r.Error
}

// join registers a session as name on topics.
func (d *testDaemon) join(t *testing.T, name string, topics ...string) *peer {
	t.Helper()
	p := &peer{client: d.dial(t), name: name, srv: d.srv}
	r := p.rpc("register", registerParams{Harness: "claude", Version: version.Version, Cwd: "/", Name: name, Topics: topics})
	if r.Error != nil {
		t.Fatalf("register %s: %+v", name, r.Error)
	}
	var res registerResult
	if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
		t.Fatal(err)
	}
	if res.Name != name {
		t.Fatalf("registered as %s, want %s", res.Name, name)
	}
	return p
}

// stored returns the message id as the store holds it for agent.
func (d *testDaemon) stored(t *testing.T, agent, id string) message.Message {
	t.Helper()
	ps, _, err := d.store.Pending(context.Background(), agent, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ps {
		if p.ID.String() == id {
			return p.Message
		}
	}
	t.Fatalf("%s is not pending for %s", id, agent)
	return message.Message{}
}

// wantDeliver checks that got is the deliver of the stored message id.
func (d *testDaemon) wantDeliver(t *testing.T, got deliverParams, agent, id string) {
	t.Helper()
	m := d.stored(t, agent, id)
	if got.Text != message.Render(m) || got.Notice != message.Notice(m) {
		t.Errorf("deliver text %q notice %q, want %q and %q", got.Text, got.Notice, message.Render(m), message.Notice(m))
	}
	want := map[string]string{"id": id, "topic": m.Topic, "sender": m.Sender, "kind": string(m.Kind), "to": cmpTo(m.To)}
	if m.Issue > 0 {
		want["issue"] = strconv.Itoa(m.Issue)
	}
	if m.Re != (message.ID{}) {
		want["re"] = m.Re.String()
	}
	if !maps.Equal(got.Attrs, want) {
		t.Errorf("attrs %v, want %v", got.Attrs, want)
	}
}

func cmpTo(to string) string {
	if to == "" {
		return message.Everyone
	}
	return to
}

func pendingCount(t *testing.T, st *store.Store, agent string) int {
	t.Helper()
	_, n, err := st.Pending(context.Background(), agent, 0)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSendParams(t *testing.T) {
	d := startDaemon(t)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	d.join(t, "c", "t2")
	a.onlines("b")
	note := func(extra map[string]any) map[string]any {
		f := map[string]any{"topic": "t1", "kind": "note", "summary": "hello"}
		maps.Copy(f, extra)
		return f
	}
	for _, tc := range []struct {
		name   string
		params any
		code   int
		in     string
	}{
		{"not an object", []int{1}, codeInvalidParams, "JSON object"},
		{"no params", nil, codeInvalidParams, "topic"},
		{"unknown member", note(map[string]any{"x": 1}), codeInvalidParams, "only the fields"},
		{"agent sends online", note(map[string]any{"kind": "online"}), codeInvalidParams, "online comes only from the daemon"},
		{"agent sends budget", note(map[string]any{"kind": "budget"}), codeInvalidParams, "kind must be one of"},
		{"summary too long", note(map[string]any{"summary": strings.Repeat("x", 1001)}), codeInvalidParams, "the limit is 1000"},
		{"non-https link", note(map[string]any{"link": "file:///etc/passwd"}), codeInvalidParams, "https"},
		{"to everyone", note(map[string]any{"to": "everyone"}), codeInvalidParams, "to:"},
		{"re on a note", note(map[string]any{"re": message.NewID(time.Now()).String()}), codeInvalidParams, "re is the id"},
		{"to the sender", note(map[string]any{"to": "a"}), codeInvalidParams, "cannot go to its sender"},
		{"sender never known on the topic", note(map[string]any{"topic": "t2"}), codeNotOnTopic, "not known on topic t2"},
		{"unknown recipient", note(map[string]any{"to": "c"}), codeUnknownRecipient, "c is not an agent known on the topic"},
		{"reply without a valid ask", note(map[string]any{"kind": "reply", "re": message.NewID(time.Now()).String()}), codeNoAsk, "no ask"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a.t = t
			wantError(t, a.rpc("send", tc.params).Error, tc.code, tc.in)
		})
	}
	a.t = t
	if n := pendingCount(t, d.store, "b"); n != 0 {
		t.Fatalf("b holds %d messages after refused sends", n)
	}

	t.Run("client sends a sender field", func(t *testing.T) {
		a.t, b.t = t, t
		res := a.send(note(map[string]any{"sender": "b", "to": "b"}))
		if m := d.stored(t, "b", res.ID); m.Sender != "a" {
			t.Errorf("stored sender %s, want a", m.Sender)
		}
		b.deliver()
		if err := b.ack(res.ID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("reply to an ask", func(t *testing.T) {
		a.t, b.t = t, t
		ask := a.send(note(map[string]any{"kind": "ask"}))
		b.deliver()
		rep := b.send(note(map[string]any{"kind": "reply", "re": strings.ToLower(ask.ID), "to": "a", "issue": 7}))
		d.wantDeliver(t, a.deliver(), "a", rep.ID)
	})

	t.Run("queue full and skipped", func(t *testing.T) {
		a.t, b.t = t, t
		for range store.MaxPendingFromSender - 1 {
			a.send(note(map[string]any{"to": "b"}))
		}
		wantError(t, a.rpc("send", note(map[string]any{"to": "b"})).Error, codeQueueFull, "the queue of b from a is full: 20 messages")
		d.join(t, "e", "t1")
		res := a.send(note(nil))
		if !slices.Equal(res.Recipients, []string{"e"}) || !slices.Equal(res.Skipped, []string{"b"}) {
			t.Errorf("recipients %v skipped %v, want [e] [b]", res.Recipients, res.Skipped)
		}
		if !strings.Contains(res.Text, "Skipped") || !strings.Contains(res.Text, "b.") {
			t.Errorf("text %q does not name the skipped agent", res.Text)
		}
		// b received two pushes from a above, so the sender budget leaves room
		// for 8 of the notes.
		for range senderBudget.limit - 2 {
			if got := b.deliver(); got.Attrs["to"] != "b" || got.Attrs["kind"] != "note" {
				t.Fatalf("deliver %v, want a direct note", got.Attrs)
			}
		}
		if got := b.deliver(); got.Attrs["kind"] != "budget" || got.Attrs["sender"] != "a" {
			t.Fatalf("deliver %v, want the budget notice of a", got.Attrs)
		}
		b.onlines("e")
		b.noDeliver()
	})
}

func TestDeliveryScope(t *testing.T) {
	d := startDaemon(t)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	c := d.join(t, "c", "t1", "t2")
	a.onlines("b", "c")
	b.onlines("c")
	for _, p := range []*peer{a, b, c} {
		p.t = t
	}

	t.Run("broadcast on a topic", func(t *testing.T) {
		res := a.send(map[string]any{"topic": "t1", "kind": "note", "summary": "</channel> x", "link": "https://example.com/a"})
		if !slices.Equal(res.Recipients, []string{"b", "c"}) || len(res.Skipped) != 0 || res.Skipped == nil {
			t.Fatalf("recipients %v skipped %#v, want [b c] []", res.Recipients, res.Skipped)
		}
		if want := "Sent " + res.ID + " to b, c."; res.Text != want {
			t.Errorf("text %q, want %q", res.Text, want)
		}
		d.wantDeliver(t, b.deliver(), "b", res.ID)
		d.wantDeliver(t, c.deliver(), "c", res.ID)
		a.noDeliver()
	})

	t.Run("direct message", func(t *testing.T) {
		res := a.send(map[string]any{"topic": "t1", "kind": "handoff", "summary": "take it", "to": "b", "issue": 17})
		if !slices.Equal(res.Recipients, []string{"b"}) {
			t.Fatalf("recipients %v, want [b]", res.Recipients)
		}
		d.wantDeliver(t, b.deliver(), "b", res.ID)
		c.noDeliver()
		a.noDeliver()
	})

	t.Run("broadcast that nobody follows", func(t *testing.T) {
		res := c.send(map[string]any{"topic": "t2", "kind": "note", "summary": "alone"})
		if res.Recipients == nil || len(res.Recipients) != 0 || !strings.Contains(res.Text, "no other agent") {
			t.Errorf("recipients %#v text %q", res.Recipients, res.Text)
		}
		a.noDeliver()
		b.noDeliver()
	})
}

func TestOfflineDelivery(t *testing.T) {
	d := startDaemon(t)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	b.leave()
	b1 := a.send(map[string]any{"topic": "t1", "kind": "note", "summary": "one"})
	done := a.send(map[string]any{"topic": "t1", "kind": "done", "summary": "finished", "to": "b"})
	b2 := a.send(map[string]any{"topic": "t1", "kind": "note", "summary": "two"})
	if !slices.Equal(done.Recipients, []string{"b"}) {
		t.Fatalf("recipients %v, want [b]", done.Recipients)
	}

	// join fails when a deliver comes before the register response.
	b = d.join(t, "b", "t1")
	for _, id := range []string{done.ID, b1.ID, b2.ID} {
		d.wantDeliver(t, b.deliver(), "b", id)
	}
	b.noDeliver()
}

func TestPendingAndAck(t *testing.T) {
	d := startDaemon(t)
	// c and a register first, so b holds no online.
	c := d.join(t, "c", "t1")
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	first := a.send(map[string]any{"topic": "t1", "kind": "note", "summary": "first broadcast"})
	second := a.send(map[string]any{"topic": "t1", "kind": "note", "summary": "second broadcast"})
	direct := a.send(map[string]any{"topic": "t1", "kind": "ask", "summary": "direct", "to": "b"})

	t.Run("order", func(t *testing.T) {
		b.t = t
		res := b.pending()
		if res.Total != 3 {
			t.Fatalf("total %d, want 3", res.Total)
		}
		i, j, k := strings.Index(res.Text, direct.ID), strings.Index(res.Text, first.ID), strings.Index(res.Text, second.ID)
		if i < 0 || !(i < j && j < k) {
			t.Errorf("pending text does not list direct, first, second in order:\n%s", res.Text)
		}
		ps, _, _ := d.store.Pending(context.Background(), "b", 0)
		ms := make([]message.Message, len(ps))
		for n, p := range ps {
			ms[n] = p.Message
		}
		if res.Text != message.RenderPending(ms, 3) {
			t.Errorf("pending text is not message.RenderPending of the pending list")
		}
	})

	t.Run("acknowledge someone else's message", func(t *testing.T) {
		c.t = t
		wantError(t, c.rpc("ack", map[string]any{"id": direct.ID}).Error, codeNotPending, "not pending for this agent")
		if n := pendingCount(t, d.store, "b"); n != 3 {
			t.Errorf("b holds %d messages, want 3", n)
		}
	})

	t.Run("acknowledge", func(t *testing.T) {
		b.t = t
		if err := b.ack(strings.ToLower(direct.ID)); err != nil {
			t.Fatal(err)
		}
		res := b.pending()
		if res.Total != 2 || strings.Contains(res.Text, direct.ID) {
			t.Errorf("after ack: total %d, text:\n%s", res.Total, res.Text)
		}
		wantError(t, b.rpc("ack", map[string]any{"id": direct.ID}).Error, codeNotPending, "not pending")
	})

	t.Run("bad params", func(t *testing.T) {
		b.t = t
		wantError(t, b.rpc("ack", map[string]any{"id": "x<y>"}).Error, codeInvalidParams, "id:")
		wantError(t, b.rpc("ack", map[string]any{}).Error, codeInvalidParams, "id:")
		wantError(t, b.rpc("ack", map[string]any{"id": 1}).Error, codeInvalidParams, "ack params")
		wantError(t, b.rpc("pending", map[string]any{"x": 1}).Error, codeInvalidParams, "no params")
		wantError(t, b.rpc("agents", map[string]any{"x": 1}).Error, codeInvalidParams, "no params")
	})
}

func TestRedelivery(t *testing.T) {
	d := startDaemon(t)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	res := a.send(map[string]any{"topic": "t1", "kind": "note", "summary": "once", "to": "b"})
	d.wantDeliver(t, b.deliver(), "b", res.ID)

	t.Run("same session", func(t *testing.T) {
		b.t = t
		b.pending()
		other := a.send(map[string]any{"topic": "t1", "kind": "note", "summary": "other", "to": "b"})
		if got := b.deliver(); got.Attrs["id"] != other.ID {
			t.Errorf("deliver of %s, want only %s", got.Attrs["id"], other.ID)
		}
		b.noDeliver()
		if err := b.ack(other.ID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("new session", func(t *testing.T) {
		b.t = t
		b.leave()
		b = d.join(t, "b", "t1")
		d.wantDeliver(t, b.deliver(), "b", res.ID)
		b.noDeliver()
	})
}

// A push that the live send and the pushes after register would both make
// reaches the session once, and a message this session already received is
// not pushed again.
func TestPushAtMostOnce(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "courier.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.RegisterTopics(ctx, "a", []string{"t1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RegisterTopics(ctx, "b", []string{"t1"}); err != nil {
		t.Fatal(err)
	}
	var ms []message.Message
	for range 2 {
		now := st.Now()
		sent, err := st.Send(ctx, message.Message{ID: message.NewID(now), Sender: "a", Created: now,
			Fields: message.Fields{Topic: "t1", Kind: message.KindNote, Summary: "x", To: "b"}})
		if err != nil {
			t.Fatal(err)
		}
		ms = append(ms, sent.Message)
	}
	if err := st.MarkPushed(ctx, "b", "s1", ms[0].ID); err != nil {
		t.Fatal(err)
	}

	srv := New(st, testLogger(t))
	conn := &fakeConn{}
	ss := &session{id: "s1", conn: conn, name: "b", topics: []string{"t1"}, out: newOutbox("b")}
	srv.afterRegister(ctx, ss)
	srv.push(ss, ss.out, ms[1])
	waitFor(t, "a push", func() bool {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		return len(conn.writes) > 0
	})
	time.Sleep(100 * time.Millisecond)
	close(ss.out.done)
	srv.wg.Wait()
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.writes) != 1 || !strings.Contains(string(conn.writes[0]), ms[1].ID.String()) {
		t.Fatalf("writes %q, want one push of %s", conn.writes, ms[1].ID)
	}
	ps, _, err := st.Pending(ctx, "b", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ps {
		if p.Session != "s1" {
			t.Errorf("%s records session %q, want s1", p.ID, p.Session)
		}
	}
}

// blockConn is a connection whose writes block until it is closed.
type blockConn struct {
	fakeConn
	closed chan struct{}
}

func (c *blockConn) Write([]byte) (int, error) {
	<-c.closed
	return 0, context.Canceled
}

// A recipient that does not read never stalls the sender: push only queues,
// and a full queue leaves the message pending.
func TestPushDoesNotBlock(t *testing.T) {
	srv := New(nil, testLogger(t))
	conn := &blockConn{closed: make(chan struct{})}
	ss := &session{id: "s1", conn: conn, out: newOutbox("b")}
	ss.out.behind = false
	srv.wg.Go(func() { srv.drain(context.Background(), ss, ss.out) })
	start := time.Now()
	now := time.Now()
	// Onlines pass no budget, so they can fill the queue.
	for i := range 2 * pushQueue {
		srv.push(ss, ss.out, message.Message{ID: message.NewID(now), Sender: "a" + strconv.Itoa(i), Created: now,
			Fields: message.Fields{Topic: "t1", Kind: message.KindOnline, Summary: "online"}})
	}
	if el := time.Since(start); el > time.Second {
		t.Errorf("%d pushes took %v", 2*pushQueue, el)
	}
	if n := len(ss.out.queued); n > pushQueue+1 {
		t.Errorf("%d pushes queued, want at most %d", n, pushQueue+1)
	}
	close(conn.closed)
	srv.wg.Wait()
	if !ss.failed.Load() {
		t.Error("a failed push write left the session open")
	}
}

// A failed push write closes the session: it handles no more lines.
func TestPushWriteFailure(t *testing.T) {
	srv := New(openStore(t, nil), testLogger(t))
	conn := &fakeConn{fail: true}
	ss := &session{id: "s1", conn: conn, name: "b", out: newOutbox("b")}
	ss.out.behind = false
	now := time.Now()
	srv.push(ss, ss.out, message.Message{ID: message.NewID(now), Sender: "a", Created: now,
		Fields: message.Fields{Topic: "t1", Kind: message.KindNote, Summary: "x", To: "b"}})
	srv.drain(context.Background(), ss, ss.out)
	if err := srv.handle(context.Background(), ss, []byte(`{"jsonrpc":"2.0","id":1,"method":"pending"}`)); err == nil {
		t.Fatal("the session handled a line after a failed write")
	}
	if len(conn.writes) != 1 {
		t.Errorf("%d writes, want only the failed push", len(conn.writes))
	}
}

// serveAt runs a daemon on the database at dbPath until stop.
func serveAt(t *testing.T, dbPath string) (*testDaemon, func()) {
	t.Helper()
	dir := shortTempDir(t)
	st, err := store.Open(dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := Listen(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatal(err)
	}
	srv := New(st, testLogger(t))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	return &testDaemon{sock: filepath.Join(dir, "run", SocketName), dbPath: dbPath, store: st, srv: srv}, func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
		st.Close()
	}
}

func TestPersistenceAcrossRestarts(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "courier.db")
	d, stop := serveAt(t, dbPath)
	a := d.join(t, "a", "t1")
	d.join(t, "b", "t1").leave()
	res := a.send(map[string]any{"topic": "t1", "kind": "done", "summary": "before the restart", "to": "b"})
	stop()

	d, stop = serveAt(t, dbPath)
	defer stop()
	b := d.join(t, "b", "t1")
	d.wantDeliver(t, b.deliver(), "b", res.ID)
}

func TestPresence(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

	t.Run("agent comes online", func(t *testing.T) {
		d := startDaemon(t)
		a := d.join(t, "a", "t1")
		d.join(t, "b", "t1", "t2")
		got := a.deliver()
		if got.Attrs["kind"] != "online" || got.Attrs["sender"] != "b" || got.Attrs["to"] != message.Everyone || got.Attrs["topic"] != "t1" {
			t.Fatalf("deliver attrs %v, want an online from b to everyone on t1", got.Attrs)
		}
		d.wantDeliver(t, got, "a", got.Attrs["id"])
		a.noDeliver()
	})

	t.Run("quick restart", func(t *testing.T) {
		clock := &fakeClock{t: t0}
		d := startDaemonWith(t, clock.now, nil)
		a := d.join(t, "a", "t1")
		b := d.join(t, "b", "t1")
		a.deliver()
		b.leave()
		clock.set(t0.Add(10 * time.Minute))
		d.join(t, "b", "t1")
		a.noDeliver()
	})

	t.Run("online not acknowledged", func(t *testing.T) {
		clock := &fakeClock{t: t0}
		d := startDaemonWith(t, clock.now, nil)
		d.join(t, "a", "t1").leave()
		for h := range 25 {
			clock.set(t0.Add(time.Duration(h) * time.Hour))
			d.join(t, "b", "t1").leave()
		}
		if n := pendingCount(t, d.store, "a"); n != 1 {
			t.Fatalf("a holds %d messages, want one online", n)
		}
		a := d.join(t, "a", "t1")
		if got := a.deliver(); got.Attrs["kind"] != "online" || got.Attrs["sender"] != "b" {
			t.Errorf("deliver attrs %v, want the online of b", got.Attrs)
		}
		a.noDeliver()
	})
}

func TestAgents(t *testing.T) {
	d := startDaemon(t)
	a := d.join(t, "a", "t1", "t2")
	d.join(t, "b", "t1")
	d.join(t, "c", "t1").leave()
	d.join(t, "d", "t2")
	d.join(t, "x", "t3")

	agents := func(p *peer) agentsResult {
		t.Helper()
		r := p.rpc("agents", nil)
		if r.Error != nil {
			t.Fatal(r.Error)
		}
		var res agentsResult
		if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
			t.Fatal(err)
		}
		return res
	}
	res := agents(a)
	want := []agentEntry{{"t1", "a", true}, {"t1", "b", true}, {"t1", "c", false}, {"t2", "a", true}, {"t2", "d", true}}
	if !slices.Equal(res.Agents, want) || res.Total != 5 {
		t.Fatalf("agents %v total %d, want %v 5", res.Agents, res.Total, want)
	}
	if !strings.Contains(res.Text, "t1 c offline") || !strings.Contains(res.Text, "t2 d live") || strings.Contains(res.Text, "not shown") {
		t.Errorf("text:\n%s", res.Text)
	}

	ctx := context.Background()
	for i := range 120 {
		if err := d.store.Touch(ctx, "t4", "y"+strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	res = agents(d.join(t, "z", "t4"))
	if len(res.Agents) != maxAgents || res.Total != 121 || !strings.Contains(res.Text, "21 more not shown.") {
		t.Errorf("%d agents, total %d, text ends %q", len(res.Agents), res.Total, res.Text[max(0, len(res.Text)-40):])
	}
}

// More pending messages than the push queue holds reach a new session, each
// once and within the push budgets: every online, the direct messages up to
// the budgets, and one budget notice per sender. The replay after the full
// queue pushes no held message and no second notice.
func TestReplayBeyondQueue(t *testing.T) {
	d := startDaemon(t)
	ctx := context.Background()
	const onlines = 300
	// Each y shares only its own topic with b, so its online goes to b alone.
	var topics []string
	for i := range onlines {
		topics = append(topics, "u"+strconv.Itoa(i))
	}
	if _, err := d.store.RegisterTopics(ctx, "b", topics); err != nil {
		t.Fatal(err)
	}
	for i := range onlines {
		if _, err := d.store.RegisterTopics(ctx, "y"+strconv.Itoa(i), topics[i:i+1]); err != nil {
			t.Fatal(err)
		}
	}
	const senders = store.MaxPendingTotal / store.MaxPendingFromSender
	for i := range senders {
		for range store.MaxPendingFromSender {
			now := d.store.Now()
			if _, err := d.store.Send(ctx, message.Message{ID: message.NewID(now), Sender: "y" + strconv.Itoa(i), Created: now,
				Fields: message.Fields{Topic: topics[i], Kind: message.KindNote, Summary: "x", To: "b"}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if n := pendingCount(t, d.store, "b"); n != onlines+store.MaxPendingTotal {
		t.Fatalf("b holds %d messages, want %d", n, onlines+store.MaxPendingTotal)
	}
	b := d.join(t, "b", "t1")
	// y0 to y2 push 10 each and pass the sender budget, with a notice each;
	// y3 passes the direct budget of the session, with the one notice of
	// that budget.
	want := map[string]int{"online": onlines, "note": directBudget.limit, "budget": 4}
	got := map[string]int{}
	seen := map[string]bool{}
	for range onlines + directBudget.limit + 4 {
		dl := b.deliver()
		key := dl.Attrs["id"]
		if dl.Attrs["kind"] == "budget" {
			key = "budget from " + dl.Attrs["sender"]
		}
		if seen[key] {
			t.Fatalf("%s pushed twice", key)
		}
		seen[key] = true
		got[dl.Attrs["kind"]]++
	}
	b.noDeliver()
	if !maps.Equal(got, want) {
		t.Errorf("pushes by kind %v, want %v", got, want)
	}
}

// An agent follows every topic it is known on, also topics that only an
// earlier session registered.
func TestFollowsKnownTopics(t *testing.T) {
	d := startDaemon(t)
	a := d.join(t, "a", "t1")
	d.join(t, "b", "t1", "t2").leave()
	a.onlines("b")
	b := d.join(t, "b", "t2")
	ask := a.send(map[string]any{"topic": "t1", "kind": "ask", "summary": "ok?", "to": "b"})
	d.wantDeliver(t, b.deliver(), "b", ask.ID)
	rep := b.send(map[string]any{"topic": "t1", "kind": "reply", "summary": "yes", "re": ask.ID, "to": "a"})
	d.wantDeliver(t, a.deliver(), "a", rep.ID)

	c := d.join(t, "c", "t3")
	wantError(t, c.rpc("send", map[string]any{"topic": "t1", "kind": "note", "summary": "x"}).Error,
		codeNotOnTopic, "not known on topic t1")
}

func TestSendResultCap(t *testing.T) {
	d := startDaemon(t)
	ctx := context.Background()
	for i := range 150 {
		if err := d.store.Touch(ctx, "t1", fmt.Sprintf("%s%03d", strings.Repeat("x", 45), i)); err != nil {
			t.Fatal(err)
		}
	}
	a := d.join(t, "a", "t1")
	r := a.rpc("send", map[string]any{"topic": "t1", "kind": "note", "summary": "all"})
	if r.Error != nil {
		t.Fatal(r.Error)
	}
	var res sendResult
	if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
		t.Fatal(err)
	}
	if len(res.Recipients) != maxAgents || res.Total != 150 || !strings.HasSuffix(res.Text, " and 140 more.") {
		t.Errorf("%d recipients, total %d, text %q", len(res.Recipients), res.Total, res.Text)
	}
	if len(r.Result) >= MaxLine/4 {
		t.Errorf("result of %d bytes", len(r.Result))
	}
}

// trySend sends from a goroutine other than the test's: it reports errors
// instead of failing the test, and skips deliver notifications.
func trySend(p *peer, id int, f map[string]any) (string, error) {
	b, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": "send", "params": f})
	if err != nil {
		return "", err
	}
	if _, err := p.conn.Write(append(b, '\n')); err != nil {
		return "", err
	}
	for {
		p.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := p.r.ReadBytes('\n')
		if err != nil {
			return "", err
		}
		var r struct {
			Method string     `json:"method"`
			Result sendResult `json:"result"`
			Error  *rpcError  `json:"error"`
		}
		if err := json.Unmarshal(line, &r); err != nil {
			return "", err
		}
		if r.Method != "" {
			continue
		}
		if r.Error != nil {
			return "", fmt.Errorf("send: %d %s", r.Error.Code, r.Error.Message)
		}
		return r.Result.ID, nil
	}
}

// Several senders push to b while b registers again and again: no session
// receives a message twice, and every message reaches b. Then, with one
// session, the messages sent during it reach that session within the
// budgets.
func TestConcurrentPushes(t *testing.T) {
	d := startDaemon(t)
	const senders, perSender = 4, 15
	var ps []*peer
	for i := range senders {
		ps = append(ps, d.join(t, "s"+strconv.Itoa(i), "t1"))
	}
	d.join(t, "b", "t1").leave()
	burst := func(phase int, ps []*peer, perSender int) (func() []string, chan error) {
		ids := make(chan string, len(ps)*perSender)
		errs := make(chan error, len(ps))
		var wg sync.WaitGroup
		for _, p := range ps {
			wg.Go(func() {
				for n := range perSender {
					id, err := trySend(p, phase*1000+n+1, map[string]any{"topic": "t1", "kind": "note", "summary": "m", "to": "b"})
					if err != nil {
						errs <- err
						return
					}
					ids <- id
				}
			})
		}
		done := make(chan error, 1)
		go func() {
			wg.Wait()
			close(errs)
			done <- <-errs
		}()
		return func() []string {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			close(ids)
			var out []string
			for id := range ids {
				out = append(out, id)
			}
			return out
		}, done
	}

	wait, done := burst(1, ps, perSender)
	got := map[string]bool{}
	for round := 0; ; round++ {
		finished := len(done) > 0
		b := d.join(t, "b", "t1")
		seen := map[string]bool{}
		for {
			dl, ok := b.deliverWithin(50 * time.Millisecond)
			if !ok {
				break
			}
			if dl.Attrs["kind"] == "budget" {
				continue
			}
			id := dl.Attrs["id"]
			if seen[id] {
				t.Fatalf("round %d: %s pushed twice to one session", round, id)
			}
			seen[id], got[id] = true, true
			if err := b.ack(id); err != nil {
				t.Fatalf("ack %s: %+v", id, err)
			}
		}
		b.leave()
		if finished && pendingCount(t, d.store, "b") == 0 {
			break
		}
		if round > 500 {
			t.Fatal("b still holds messages")
		}
	}
	for _, id := range wait() {
		if !got[id] {
			t.Errorf("%s never reached b", id)
		}
	}

	// Four senders with 10 direct messages each pass the direct budget of
	// the session: 30 pushes and its one notice.
	b := d.join(t, "b", "t1")
	wait, _ = burst(2, ps, senderBudget.limit)
	ids := wait()
	seen := map[string]bool{}
	notices := 0
	for range directBudget.limit + 1 {
		dl := b.deliver()
		if dl.Attrs["kind"] == "budget" {
			notices++
			continue
		}
		id := dl.Attrs["id"]
		if seen[id] || !slices.Contains(ids, id) {
			t.Fatalf("unexpected or repeated push of %s", id)
		}
		seen[id] = true
	}
	b.noDeliver()
	if notices != 1 {
		t.Errorf("%d budget notices, want 1", notices)
	}
}

// A message carries the time of the store's clock, so retention counts on
// one clock.
func TestMessageClock(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: t0}
	d := startDaemonWith(t, clock.now, nil)
	a := d.join(t, "a", "t1")
	b := d.join(t, "b", "t1")
	a.onlines("b")
	res := a.send(map[string]any{"topic": "t1", "kind": "note", "summary": "x", "to": "b"})
	if m := d.stored(t, "b", res.ID); !m.Created.Equal(t0) {
		t.Fatalf("created %v, want %v", m.Created, t0)
	}
	b.deliver()

	clock.set(t0.Add(store.Retention + 24*time.Hour))
	if p := b.pending(); p.Total != 0 {
		t.Errorf("a 31-day-old message is pending: %+v", p)
	}
	wantError(t, b.rpc("ack", map[string]any{"id": res.ID}).Error, codeNotPending, "not pending")
}

// A deliver without a store id, as the budget notice is, is written
// and not recorded as a push.
func TestDrainNotice(t *testing.T) {
	srv := New(nil, testLogger(t))
	conn := &fakeConn{}
	ss := &session{id: "s1", conn: conn, name: "b", out: newOutbox("b")}
	ss.out.behind = false
	if !ss.out.offer(delivery{note: notification{JSONRPC: "2.0", Method: "deliver"}}) {
		t.Fatal("notice not queued")
	}
	if len(ss.out.queued) != 0 {
		t.Errorf("a notice entered queued")
	}
	srv.wg.Go(func() { srv.drain(context.Background(), ss, ss.out) })
	waitFor(t, "the notice", func() bool {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		return len(conn.writes) == 1
	})
	close(ss.out.done)
	srv.wg.Wait()
}

// A broadcast that skips more than 100 full queues lists 100 of them and
// counts all.
func TestSendSkippedCap(t *testing.T) {
	d := startDaemon(t)
	ctx := context.Background()
	const n = 120
	for i := range n {
		if err := d.store.Touch(ctx, "t1", fmt.Sprintf("%s%03d", strings.Repeat("x", 45), i)); err != nil {
			t.Fatal(err)
		}
	}
	a := d.join(t, "a", "t1")
	for range store.MaxPendingFromSender {
		a.send(map[string]any{"topic": "t1", "kind": "note", "summary": "fill"})
	}
	r := a.rpc("send", map[string]any{"topic": "t1", "kind": "note", "summary": "one more"})
	if r.Error != nil {
		t.Fatal(r.Error)
	}
	var res sendResult
	if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
		t.Fatal(err)
	}
	if len(res.Recipients) != 0 || res.Total != 0 || len(res.Skipped) != maxAgents || res.SkipTotal != n ||
		!strings.HasSuffix(res.Text, " and 110 more.") {
		t.Errorf("recipients %d/%d, skipped %d/%d, text %q", len(res.Recipients), res.Total, len(res.Skipped), res.SkipTotal, res.Text)
	}
	if len(r.Result) >= MaxLine/4 {
		t.Errorf("result of %d bytes", len(r.Result))
	}
}
