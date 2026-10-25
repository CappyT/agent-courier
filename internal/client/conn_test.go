//go:build unix

package client

import (
	"bufio"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/daemon"
	"github.com/CappyT/agent-courier/internal/version"
)

type fakeRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      jsontext.Value `json:"id"`
	Method  string         `json:"method"`
	Params  jsontext.Value `json:"params"`
}

// peer is the daemon side of a connection in a test.
type peer struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func newPeer(t *testing.T, c net.Conn) *peer {
	t.Cleanup(func() { c.Close() })
	return &peer{t: t, conn: c, r: bufio.NewReaderSize(c, daemon.MaxLine+2)}
}

func (p *peer) read() fakeRequest {
	p.t.Helper()
	p.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	line, err := p.r.ReadBytes('\n')
	if err != nil {
		p.t.Fatalf("read: %v", err)
	}
	var req fakeRequest
	if err := json.Unmarshal(line, &req, json.RejectUnknownMembers(true)); err != nil || req.JSONRPC != "2.0" {
		p.t.Fatalf("request %s: %v", line, err)
	}
	return req
}

func (p *peer) write(s string) {
	p.t.Helper()
	p.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := p.conn.Write([]byte(s + "\n")); err != nil {
		p.t.Fatalf("write: %v", err)
	}
}

func (p *peer) noRequest() {
	p.t.Helper()
	p.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if line, err := p.r.ReadBytes('\n'); err == nil {
		p.t.Fatalf("unexpected request %s", line)
	}
}

func pipe(t *testing.T, onDeliver func(Deliver)) (*Conn, *peer) {
	a, b := net.Pipe()
	cn := NewConn(a, onDeliver)
	t.Cleanup(func() { cn.Close() })
	return cn, newPeer(t, b)
}

func TestCallRoutesResponsesAndDeliveries(t *testing.T) {
	delivered := make(chan Deliver, 1)
	cn, p := pipe(t, func(d Deliver) { delivered <- d })
	type res struct {
		Text string `json:"text"`
	}
	done := make(chan error, 2)
	var r1, r2 res
	go func() { done <- cn.Call(context.Background(), "pending", nil, &r1) }()
	req1 := p.read()
	go func() { done <- cn.Call(context.Background(), "ack", map[string]string{"id": "x"}, &r2) }()
	req2 := p.read()
	if req1.Method != "pending" || len(req1.Params) != 0 || req2.Method != "ack" || string(req2.Params) != `{"id":"x"}` {
		t.Fatalf("requests %+v %+v", req1, req2)
	}
	if string(req1.ID) == string(req2.ID) {
		t.Fatalf("two calls share the id %s", req1.ID)
	}
	p.write(`{"jsonrpc":"2.0","method":"deliver","params":{"text":"t","notice":"n","attrs":{"id":"1","kind":"note"}}}`)
	p.write(`{"jsonrpc":"2.0","id":` + string(req2.ID) + `,"error":{"code":-32013,"message":"not pending"}}`)
	p.write(`{"jsonrpc":"2.0","id":` + string(req1.ID) + `,"result":{"text":"none"}}`)
	for range 2 {
		if err := <-done; err != nil {
			if e, ok := errors.AsType[*RPCError](err); !ok || e.Code != -32013 || e.Error() != "not pending" {
				t.Fatalf("call error %v", err)
			}
		}
	}
	if r1.Text != "none" {
		t.Errorf("result %+v", r1)
	}
	select {
	case d := <-delivered:
		if d.Text != "t" || d.Notice != "n" || d.Attrs["kind"] != "note" {
			t.Errorf("deliver %+v", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no deliver")
	}
}

func TestBrokenConnectionFailsWaitingCall(t *testing.T) {
	cn, p := pipe(t, nil)
	done := make(chan error, 1)
	go func() { done <- cn.Call(context.Background(), "send", map[string]string{"topic": "t"}, nil) }()
	p.read()
	p.conn.Close()
	if err := <-done; !errors.Is(err, ErrUnreachable) {
		t.Fatalf("waiting call: %v, want ErrUnreachable", err)
	}
	<-cn.Done()
	if err := cn.Call(context.Background(), "pending", nil, nil); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("call after the break: %v", err)
	}
}

func TestLineCap(t *testing.T) {
	cn, p := pipe(t, nil)
	err := cn.Call(context.Background(), "send", map[string]string{"summary": strings.Repeat("a", daemon.MaxLine)}, nil)
	if err == nil || errors.Is(err, ErrUnreachable) {
		t.Fatalf("over-long request: %v", err)
	}
	go p.conn.Write([]byte(strings.Repeat("a", daemon.MaxLine+1) + "\n"))
	select {
	case <-cn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a line over the cap did not close the connection")
	}
}

func TestParamsFromEnv(t *testing.T) {
	t.Setenv("AGENT_COURIER_NAME", "rev")
	t.Setenv("AGENT_COURIER_TOPICS", "a, b,,c ")
	p := ParamsFromEnv("claude", "/w")
	if p.Harness != "claude" || p.Cwd != "/w" || p.Name != "rev" || !slices.Equal(p.Topics, []string{"a", "b", "c"}) {
		t.Fatalf("%+v", p)
	}
	t.Setenv("AGENT_COURIER_NAME", "")
	t.Setenv("AGENT_COURIER_TOPICS", "")
	if p := ParamsFromEnv("opencode", "/w"); p.Name != "" || p.Topics != nil {
		t.Fatalf("%+v", p)
	}
}

type registerParams struct {
	Harness string   `json:"harness"`
	Version string   `json:"version"`
	Cwd     string   `json:"cwd"`
	Name    string   `json:"name"`
	Topics  []string `json:"topics"`
}

// fakeListener is a fake daemon on the socket of the test's socket
// directory. Its accepts arrive on a channel, so a test sees each connection.
type fakeListener struct {
	t     *testing.T
	conns chan net.Conn
}

func listenFake(t *testing.T) *fakeListener {
	t.Helper()
	dir, _ := setupEnv(t)
	if err := daemon.PrivateDir(dir); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", filepath.Join(dir, daemon.SocketName))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	f := &fakeListener{t: t, conns: make(chan net.Conn, 16)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.conns <- c
		}
	}()
	return f
}

func (f *fakeListener) accept() *peer {
	f.t.Helper()
	select {
	case c := <-f.conns:
		return newPeer(f.t, c)
	case <-time.After(10 * time.Second):
		f.t.Fatal("the client did not connect")
		return nil
	}
}

// none checks that no client connects within d.
func (f *fakeListener) none(d time.Duration) {
	f.t.Helper()
	select {
	case <-f.conns:
		f.t.Fatal("unexpected connection")
	case <-time.After(d):
	}
}

func (p *peer) register() registerParams {
	p.t.Helper()
	req := p.read()
	var rp registerParams
	if req.Method != "register" || json.Unmarshal(req.Params, &rp) != nil {
		p.t.Fatalf("first request %+v", req)
	}
	return rp
}

func registered(name string) string {
	return `{"jsonrpc":"2.0","id":1,"result":{"session":"s","name":"` + name + `","topics":["t"],"protocol":"p-` + name + `","version":"` + version.Version + `"}}`
}

// TestSessionReconnect runs a session against a fake daemon that breaks the
// connection while a call waits.
func TestSessionReconnect(t *testing.T) {
	f := listenFake(t)
	accept := f.accept
	register := func(p *peer) registerParams { return p.register() }

	registered := make(chan Registration, 2)
	s := Start(Config{
		Params:       RegisterParams{Harness: "claude", Cwd: "/w", Topics: []string{"t"}},
		OnRegistered: func(r Registration) { registered <- r },
	})
	t.Cleanup(func() { s.Close() })

	first := accept()
	if rp := register(first); rp.Version != version.Version || rp.Harness != "claude" || rp.Cwd != "/w" || rp.Name != "" || !slices.Equal(rp.Topics, []string{"t"}) {
		t.Fatalf("register params %+v", rp)
	}
	first.write(`{"jsonrpc":"2.0","id":1,"result":{"session":"s1","name":"claude-w","topics":["t"],"protocol":"p1","version":"` + version.Version + `"}}`)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if reg, err := s.Registration(ctx); err != nil || reg.Name != "claude-w" {
		t.Fatalf("registration %+v, %v", reg, err)
	}

	if r := <-registered; r.Name != "claude-w" || r.Protocol != "p1" {
		t.Fatalf("first OnRegistered %+v", r)
	}

	done := make(chan error, 1)
	go func() { done <- s.Call(ctx, "send", map[string]string{"topic": "t"}, nil) }()
	if req := first.read(); req.Method != "send" {
		t.Fatalf("request %+v", req)
	}
	first.conn.Close()
	if err := <-done; !errors.Is(err, ErrUnreachable) || err.Error() != "the courier daemon is not reachable" {
		t.Fatalf("call that waited on the broken connection: %v", err)
	}

	second := accept()
	if rp := register(second); rp.Name != "claude-w" || !slices.Equal(rp.Topics, []string{"t"}) {
		t.Fatalf("reconnect register params %+v, want the name and topics the session had", rp)
	}
	second.write(`{"jsonrpc":"2.0","id":1,"result":{"session":"s2","name":"claude-w-2","topics":["t"],"protocol":"p2","version":"` + version.Version + `"}}`)
	select {
	case r := <-registered:
		if r.Name != "claude-w-2" || r.Protocol != "p2" {
			t.Fatalf("registered %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no OnRegistered after the daemon assigned another name")
	}
	second.noRequest()
}

func TestSessionWithoutDaemonFailsCalls(t *testing.T) {
	setupEnv(t)
	// ErrNotExist instead of the fake daemon: no daemon starts.
	executable = func() (string, error) { return "", os.ErrNotExist }
	t.Cleanup(func() { executable = testExecutable })
	s := Start(Config{Params: RegisterParams{Harness: "claude", Cwd: "/"}})
	t.Cleanup(func() { s.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := s.Call(ctx, "pending", nil, nil); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("call without a daemon: %v", err)
	}
}

func TestDeliverWithNullParamsIsDropped(t *testing.T) {
	delivered := make(chan Deliver, 2)
	cn, p := pipe(t, func(d Deliver) { delivered <- d })
	p.write(`{"jsonrpc":"2.0","method":"deliver","params":null}`)
	p.write(`{"jsonrpc":"2.0","method":"deliver"}`)
	p.write(`{"jsonrpc":"2.0","method":"deliver","params":{"text":"real"}}`)
	select {
	case d := <-delivered:
		if d.Text != "real" {
			t.Fatalf("deliver %+v, want only the one with params", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no deliver")
	}
	select {
	case <-cn.Done():
		t.Fatal("a deliver without params broke the connection")
	default:
	}
}

func TestRegisterRefusedStopsReconnects(t *testing.T) {
	f := listenFake(t)
	const mismatch = "version mismatch: the client is 0.0.0, the daemon is 9.9.9; the daemon exits 60 seconds after its last session closes, then the next client starts its own release"
	refuse := `{"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"` + mismatch + `"}}`
	s := Start(Config{Params: RegisterParams{Harness: "claude", Cwd: "/w", Topics: []string{"t"}}})
	t.Cleanup(func() { s.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first := f.accept()
	first.register()
	first.write(refuse)
	_, err := s.Registration(ctx)
	if e, ok := errors.AsType[*RPCError](err); !ok || e.Code != -32004 || err.Error() != mismatch {
		t.Fatalf("Registration error %v, want the -32004 message unchanged", err)
	}
	f.none(2 * time.Second)

	done := make(chan error, 1)
	go func() { done <- s.Call(ctx, "pending", nil, nil) }()
	second := f.accept()
	second.register()
	second.write(refuse)
	if err := <-done; err == nil || err.Error() != mismatch {
		t.Fatalf("call while the daemon refuses: %v, want the -32004 message", err)
	}
	f.none(500 * time.Millisecond)

	type res struct {
		Total int `json:"total"`
	}
	var r res
	go func() { done <- s.Call(ctx, "pending", nil, &r) }()
	third := f.accept()
	third.register()
	third.write(registered("claude-w"))
	if req := third.read(); req.Method != "pending" {
		t.Fatalf("request %+v", req)
	} else {
		third.write(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":{"total":3}}`)
	}
	if err := <-done; err != nil || r.Total != 3 {
		t.Fatalf("call once the daemon accepts: %+v, %v", r, err)
	}
	if reg, err := s.Registration(ctx); err != nil || reg.Name != "claude-w" {
		t.Fatalf("registration %+v, %v", reg, err)
	}
}

func TestReconnectOrder(t *testing.T) {
	f := listenFake(t)
	var mu sync.Mutex
	var events []string
	record := func(e string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}
	got := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(events)
	}
	s := Start(Config{
		Params: RegisterParams{Harness: "claude", Cwd: "/w", Topics: []string{"t"}},
		OnDeliver: func(d Deliver) {
			if d.Text == "old" {
				// The new connection must wait for the old one's handler.
				time.Sleep(300 * time.Millisecond)
			}
			record(d.Text)
		},
		OnRegistered: func(r Registration) { record("registered " + r.Name) },
	})
	t.Cleanup(func() { s.Close() })

	first := f.accept()
	first.register()
	first.write(registered("claude-w"))
	first.write(`{"jsonrpc":"2.0","method":"deliver","params":{"text":"old"}}`)
	first.conn.Close()

	second := f.accept()
	second.register()
	second.write(registered("claude-w-2") + "\n" + `{"jsonrpc":"2.0","method":"deliver","params":{"text":"new"}}`)
	want := []string{"registered claude-w", "old", "registered claude-w-2", "new"}
	for deadline := time.Now().Add(5 * time.Second); len(got()) < len(want); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			break
		}
	}
	if !slices.Equal(got(), want) {
		t.Fatalf("events %q, want %q", got(), want)
	}
}

func TestCallAfterBreakWaitsForNextConnection(t *testing.T) {
	f := listenFake(t)
	s := Start(Config{Params: RegisterParams{Harness: "claude", Cwd: "/w", Topics: []string{"t"}}})
	t.Cleanup(func() { s.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first := f.accept()
	first.register()
	first.write(registered("claude-w"))
	if _, err := s.Registration(ctx); err != nil {
		t.Fatal(err)
	}
	first.conn.Close()

	done := make(chan error, 1)
	go func() { done <- s.Call(ctx, "pending", nil, nil) }()
	second := f.accept()
	second.register()
	second.write(registered("claude-w"))
	req := second.read()
	if req.Method != "pending" {
		t.Fatalf("request %+v", req)
	}
	second.write(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":{}}`)
	if err := <-done; err != nil {
		t.Fatalf("a call that never left the broken connection: %v", err)
	}
	second.noRequest()
}

func TestUnsentCallOnClosedConn(t *testing.T) {
	cn, _ := pipe(t, nil)
	cn.Close()
	if sent, err := cn.call(context.Background(), "pending", nil, nil); sent || !errors.Is(err, ErrUnreachable) {
		t.Fatalf("call on a closed connection: sent %v, %v", sent, err)
	}
}
