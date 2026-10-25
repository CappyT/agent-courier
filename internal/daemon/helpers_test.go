//go:build unix

package daemon

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"log/slog"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/version"
)

type testDaemon struct {
	sock   string
	dbPath string
	store  *store.Store
	srv    *Server
}

// fakeClock is a clock that tests move by hand.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// shortTempDir is a temporary directory with a path short enough for a unix
// socket, which has at most 108 bytes; t.TempDir() can be longer.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ac")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// openStore opens a store in a temporary directory with the clock now.
func openStore(t *testing.T, now func() time.Time) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "courier.db"), now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func startDaemon(t *testing.T) *testDaemon { return startDaemonWith(t, nil, nil) }

// manualTicker replaces the re-offer ticker of every session writer with a
// channel that the test fires.
type manualTicker struct {
	mu    sync.Mutex
	chans map[chan time.Time]struct{}
}

func (m *manualTicker) start() (<-chan time.Time, func()) {
	c := make(chan time.Time)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.chans == nil {
		m.chans = map[chan time.Time]struct{}{}
	}
	m.chans[c] = struct{}{}
	return c, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.chans, c)
	}
}

// fire ticks every writer that is waiting for a tick within a short time.
// A writer that took the tick has handled it before it takes the next one.
func (m *manualTicker) fire() {
	m.mu.Lock()
	cs := slices.Collect(maps.Keys(m.chans))
	m.mu.Unlock()
	for _, c := range cs {
		select {
		case c <- time.Time{}:
		case <-time.After(time.Second):
		}
	}
}

// startDaemonTicked starts a daemon with the clock now whose session writers
// re-offer held messages only when tick fires.
func startDaemonTicked(t *testing.T, now func() time.Time, tick *manualTicker) *testDaemon {
	t.Helper()
	return startDaemonSrv(t, now, nil, func(srv *Server) { srv.reofferTick = tick.start })
}

// startDaemonWith starts a daemon on a store with the clock now. setup runs
// on the store before the daemon serves.
func startDaemonWith(t *testing.T, now func() time.Time, setup func(*store.Store)) *testDaemon {
	t.Helper()
	return startDaemonSrv(t, now, setup, nil)
}

// startDaemonSrv is startDaemonWith; configure runs on the server before it
// serves.
func startDaemonSrv(t *testing.T, now func() time.Time, setup func(*store.Store), configure func(*Server)) *testDaemon {
	t.Helper()
	dir := shortTempDir(t)
	dbPath := filepath.Join(t.TempDir(), "courier.db")
	st, err := store.Open(dbPath, now)
	if err != nil {
		t.Fatal(err)
	}
	if setup != nil {
		setup(st)
	}
	ln, err := Listen(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatal(err)
	}
	srv := New(st, testLogger(t))
	if configure != nil {
		configure(srv)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
		st.Close()
	})
	return &testDaemon{sock: filepath.Join(dir, "run", SocketName), dbPath: dbPath, store: st, srv: srv}
}

func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(t.Output(), &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// sqlDB opens a second connection to the database of the daemon, for checks
// and fault injection that the store API does not offer.
func (d *testDaemon) sqlDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+d.dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func count(t *testing.T, db *sql.DB, query string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

type client struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
	next int
}

type resp struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      jsontext.Value `json:"id"`
	Result  jsontext.Value `json:"result"`
	Error   *rpcError      `json:"error"`
}

func (d *testDaemon) dial(t *testing.T) *client { return d.dialFor(t, t) }

// dialFor connects a client that fails t and closes when owner ends.
func (d *testDaemon) dialFor(owner, t *testing.T) *client {
	t.Helper()
	c, err := net.Dial("unix", d.sock)
	if err != nil {
		t.Fatal(err)
	}
	owner.Cleanup(func() { c.Close() })
	return &client{t: t, conn: c, r: bufio.NewReaderSize(c, MaxLine+2)}
}

func (c *client) line(s string) {
	c.t.Helper()
	if _, err := c.conn.Write([]byte(s)); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) read() resp {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	b, err := c.r.ReadBytes('\n')
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return parseResp(c.t, b)
}

func parseResp(t *testing.T, b []byte) resp {
	t.Helper()
	var r resp
	if err := json.Unmarshal(b, &r, json.RejectUnknownMembers(true)); err != nil {
		t.Fatalf("response %s: %v", b, err)
	}
	if r.JSONRPC != "2.0" || (r.Error == nil) == (r.Result == nil) {
		t.Fatalf("not a JSON-RPC 2.0 response: %s", b)
	}
	return r
}

// noResponse checks that the daemon answers nothing within a short time.
func (c *client) noResponse() {
	c.t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if b, err := c.r.ReadBytes('\n'); err == nil {
		c.t.Fatalf("unexpected response %s", b)
	}
}

func (c *client) call(method string, params any) resp {
	c.t.Helper()
	c.next++
	req := map[string]any{"jsonrpc": "2.0", "id": c.next, "method": method}
	if params != nil {
		req["params"] = params
	}
	b, err := json.Marshal(req)
	if err != nil {
		c.t.Fatal(err)
	}
	c.line(string(b) + "\n")
	r := c.read()
	if string(r.ID) != strconv.Itoa(c.next) {
		c.t.Fatalf("response id %s, want %d", r.ID, c.next)
	}
	return r
}

// register sends p, with the daemon's version when p has none.
func (c *client) register(p registerParams) (registerResult, *rpcError) {
	c.t.Helper()
	if p.Version == "" {
		p.Version = version.Version
	}
	r := c.call("register", p)
	var res registerResult
	if r.Error == nil {
		if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
			c.t.Fatal(err)
		}
	}
	return res, r.Error
}

func (c *client) mustRegister(p registerParams) registerResult {
	c.t.Helper()
	res, rerr := c.register(p)
	if rerr != nil {
		c.t.Fatalf("register %+v: %+v", p, rerr)
	}
	return res
}

func wantError(t *testing.T, got *rpcError, code int, inMessage string) {
	t.Helper()
	if got == nil {
		t.Fatalf("no error, want code %d", code)
	}
	if got.Code != code || !strings.Contains(got.Message, inMessage) {
		t.Fatalf("error %d %q, want %d containing %q", got.Code, got.Message, code, inMessage)
	}
}

// waitFor polls cond for up to 5 seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// waitFree waits until no live session holds name.
func waitFree(t *testing.T, srv *Server, name string) {
	t.Helper()
	waitFor(t, name+" to be free", func() bool {
		srv.mu.Lock()
		defer srv.mu.Unlock()
		return srv.names[name] == nil
	})
}

// fakeConn feeds input to a session and records or fails its writes.
type fakeConn struct {
	net.Conn
	in     *strings.Reader
	fail   bool
	mu     sync.Mutex
	writes [][]byte
}

func (c *fakeConn) Read(p []byte) (int, error) { return c.in.Read(p) }

func (c *fakeConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	if c.fail {
		return 0, net.ErrClosed
	}
	return len(p), nil
}

func (c *fakeConn) Close() error                     { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }
