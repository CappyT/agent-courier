//go:build unix

package mcpserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/daemon"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/version"
)

func TestMain(m *testing.M) {
	// A client that finds no daemon starts its own executable as the daemon.
	// Here that is the test binary: it must not run the tests again.
	if len(os.Args) == 2 && os.Args[1] == "daemon" {
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// rpcMsg is one JSON-RPC line of the MCP output.
type rpcMsg struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      jsontext.Value `json:"id,omitzero"`
	Method  string         `json:"method,omitzero"`
	Params  jsontext.Value `json:"params,omitzero"`
	Result  jsontext.Value `json:"result,omitzero"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitzero"`
}

// syncBuffer is a bytes.Buffer safe for concurrent use.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// mcpClient drives Serve over pipes the way Claude Code drives the binary
// over stdio. Every line of the output must be a JSON-RPC 2.0 message.
type mcpClient struct {
	t      *testing.T
	in     *io.PipeWriter
	msgs   chan rpcMsg
	stdout *syncBuffer
	stderr *syncBuffer
	notes  []rpcMsg
	nextID int
}

func startMCP(t *testing.T, cfg Config) *mcpClient {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	c := &mcpClient{t: t, in: inW, msgs: make(chan rpcMsg, 64), stdout: &syncBuffer{}, stderr: &syncBuffer{}}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.NewTextHandler(c.stderr, nil))
	}
	done := make(chan error, 1)
	go func() {
		done <- Serve(context.Background(), cfg, inR, outW)
		outW.Close()
	}()
	read := make(chan struct{})
	go func() {
		defer close(read)
		defer close(c.msgs)
		sc := bufio.NewScanner(outR)
		sc.Buffer(nil, 1<<20)
		for sc.Scan() {
			c.stdout.Write(append(sc.Bytes(), '\n'))
			var m rpcMsg
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil || m.JSONRPC != "2.0" || (m.Method == "" && len(m.ID) == 0) {
				t.Errorf("stdout carries a line that is not JSON-RPC 2.0: %q", sc.Bytes())
				continue
			}
			c.msgs <- m
		}
	}()
	t.Cleanup(func() {
		inW.Close()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("Serve did not return after stdin closed")
			return
		}
		go func() {
			for range c.msgs {
			}
		}()
		<-read
	})
	return c
}

func (c *mcpClient) write(v any) {
	c.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.in.Write(append(b, '\n')); err != nil {
		c.t.Fatal(err)
	}
}

func (c *mcpClient) recv(timeout time.Duration) (rpcMsg, bool) {
	select {
	case m, ok := <-c.msgs:
		return m, ok
	case <-time.After(timeout):
		return rpcMsg{}, false
	}
}

// call sends a request and returns its result. Notifications that arrive
// meanwhile wait in notes.
func (c *mcpClient) call(method string, params any, result any) {
	c.t.Helper()
	c.nextID++
	id := c.nextID
	req := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		req["params"] = params
	}
	c.write(req)
	for {
		m, ok := c.recv(20 * time.Second)
		if !ok {
			c.t.Fatalf("no response to %s", method)
		}
		if m.Method != "" {
			c.notes = append(c.notes, m)
			continue
		}
		var got int
		if json.Unmarshal(m.ID, &got) != nil || got != id {
			c.t.Fatalf("response with id %s, want %d", m.ID, id)
		}
		if m.Error != nil {
			c.t.Fatalf("%s: error %d %s", method, m.Error.Code, m.Error.Message)
		}
		if result != nil {
			if err := json.Unmarshal(m.Result, result); err != nil {
				c.t.Fatalf("%s result %s: %v", method, m.Result, err)
			}
		}
		return
	}
}

type initResult struct {
	ProtocolVersion string `json:"protocolVersion"`
	Instructions    string `json:"instructions"`
	Capabilities    struct {
		Experimental map[string]jsontext.Value `json:"experimental"`
		Tools        jsontext.Value            `json:"tools"`
	} `json:"capabilities"`
}

func (c *mcpClient) initialize(pv string) initResult {
	c.t.Helper()
	var res initResult
	c.call("initialize", map[string]any{
		"protocolVersion": pv,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "claude-code", "version": "test"},
	}, &res)
	return res
}

func (c *mcpClient) initialized() {
	c.write(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
}

type toolResult struct {
	Text    string
	IsError bool
}

func (c *mcpClient) tool(name string, args map[string]any) toolResult {
	c.t.Helper()
	var res struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	c.call("tools/call", map[string]any{"name": name, "arguments": args}, &res)
	if len(res.Content) != 1 || res.Content[0].Type != "text" {
		c.t.Fatalf("tools/call %s: content %+v, want one text item", name, res.Content)
	}
	return toolResult{res.Content[0].Text, res.IsError}
}

// gotEvent is the params of a channel notification, every member kept.
type gotEvent map[string]jsontext.Value

func (e gotEvent) content(t *testing.T) string {
	t.Helper()
	var s string
	if err := json.Unmarshal(e["content"], &s); err != nil {
		t.Fatalf("content %s: %v", e["content"], err)
	}
	return s
}

func (e gotEvent) meta(t *testing.T) map[string]string {
	t.Helper()
	raw, ok := e["meta"]
	if !ok {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("meta %s: %v", raw, err)
	}
	return m
}

// event returns the next channel notification, or false after timeout.
// Other notifications are skipped.
func (c *mcpClient) event(timeout time.Duration) (gotEvent, bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var m rpcMsg
		if len(c.notes) > 0 {
			m, c.notes = c.notes[0], c.notes[1:]
		} else {
			var ok bool
			if m, ok = c.recv(time.Until(deadline)); !ok {
				return nil, false
			}
		}
		if m.Method != "notifications/claude/channel" {
			continue
		}
		var e gotEvent
		if err := json.Unmarshal(m.Params, &e); err != nil {
			c.t.Fatalf("channel params %s: %v", m.Params, err)
		}
		return e, true
	}
}

func (c *mcpClient) mustEvent() gotEvent {
	c.t.Helper()
	e, ok := c.event(10 * time.Second)
	if !ok {
		c.t.Fatal("no channel event")
	}
	return e
}

// socketDir points the client at a new socket directory and the store at a
// temporary state directory. A unix socket path has at most 108 bytes, so
// t.TempDir() can be too long.
func socketDir(t *testing.T) string {
	t.Helper()
	run, err := os.MkdirTemp("", "acm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(run) })
	dir := filepath.Join(run, "agent-courier")
	t.Setenv(daemon.SocketDirEnv, dir)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	return dir
}

// startDaemon runs a real daemon in the test process.
func startDaemon(t *testing.T) {
	t.Helper()
	dir := socketDir(t)
	ln, err := daemon.Listen(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "courier.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- daemon.New(st, slog.New(slog.DiscardHandler)).Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		<-done
		st.Close()
		ln.Unlock()
	})
}

// fakeDaemon listens on the socket and lets a test play the daemon's side of
// the wire protocol.
type fakeDaemon struct {
	t     *testing.T
	conns chan *fakeConn
}

type fakeConn struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

type fakeRequest struct {
	ID     jsontext.Value `json:"id"`
	Method string         `json:"method"`
	Params jsontext.Value `json:"params,omitzero"`
}

func startFake(t *testing.T) *fakeDaemon {
	t.Helper()
	dir := socketDir(t)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", filepath.Join(dir, daemon.SocketName))
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDaemon{t: t, conns: make(chan *fakeConn, 8)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.conns <- &fakeConn{t: t, c: c, r: bufio.NewReader(c)}
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		for {
			select {
			case c := <-f.conns:
				c.c.Close()
			default:
				return
			}
		}
	})
	return f
}

func (f *fakeDaemon) accept() *fakeConn {
	f.t.Helper()
	select {
	case c := <-f.conns:
		f.t.Cleanup(func() { c.c.Close() })
		return c
	case <-time.After(10 * time.Second):
		f.t.Fatal("the client did not connect")
		return nil
	}
}

func (c *fakeConn) read() fakeRequest {
	c.t.Helper()
	c.c.SetReadDeadline(time.Now().Add(10 * time.Second))
	line, err := c.r.ReadBytes('\n')
	if err != nil {
		c.t.Fatalf("read request: %v", err)
	}
	var req fakeRequest
	if err := json.Unmarshal(line, &req); err != nil {
		c.t.Fatalf("request %q: %v", line, err)
	}
	return req
}

func (c *fakeConn) send(v any) {
	c.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.c.Write(append(b, '\n')); err != nil {
		c.t.Fatal(err)
	}
}

func (c *fakeConn) reply(req fakeRequest, result any) {
	c.t.Helper()
	c.send(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
}

func (c *fakeConn) fail(req fakeRequest, code int, msg string) {
	c.t.Helper()
	c.send(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": code, "message": msg}})
}

func (c *fakeConn) deliver(text, notice string, attrs map[string]string) {
	c.t.Helper()
	c.send(map[string]any{"jsonrpc": "2.0", "method": "deliver", "params": map[string]any{"text": text, "notice": notice, "attrs": attrs}})
}

// register reads the register request and answers it with name and
// protocol. It returns the request's params.
func (c *fakeConn) register(name, protocol string) map[string]jsontext.Value {
	c.t.Helper()
	req := c.read()
	if req.Method != "register" {
		c.t.Fatalf("first request %q, want register", req.Method)
	}
	var p map[string]jsontext.Value
	if err := json.Unmarshal(req.Params, &p); err != nil {
		c.t.Fatal(err)
	}
	c.reply(req, map[string]any{"session": "01JB8Z4Q6VJ5W3T2K9N0R7M1XC", "name": name, "topics": []string{"t"}, "protocol": protocol, "version": version.Version})
	return p
}
