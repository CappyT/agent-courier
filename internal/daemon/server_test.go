//go:build unix

package daemon

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/version"
)

func TestOversizedLine(t *testing.T) {
	d := startDaemon(t)
	req := `{"jsonrpc":"2.0","id":1,"method":"pending"}`
	pad := func(n int) string { return req + strings.Repeat(" ", n-len(req)) }

	for _, end := range []string{"\n", "\r\n"} {
		t.Run("line at the cap ended by "+strconv.Quote(end), func(t *testing.T) {
			c := d.dial(t)
			c.line(pad(MaxLine) + end)
			wantError(t, c.read().Error, codeNotRegistered, "call register first")
		})
	}

	for name, line := range map[string]string{
		"one byte over the cap":     pad(MaxLine+1) + "\n",
		"over the cap, no newline":  strings.Repeat("x", MaxLine+2),
		"over the cap with a \\r":   pad(MaxLine+1) + "\r",
		"no newline before the EOF": pad(MaxLine + 1),
	} {
		t.Run(name, func(t *testing.T) {
			c := d.dial(t)
			c.line(line)
			if name == "no newline before the EOF" {
				c.conn.(*net.UnixConn).CloseWrite()
			}
			r := c.read()
			wantError(t, r.Error, codeInvalidRequest, "a line has at most 65536 bytes")
			if string(r.ID) != "null" {
				t.Errorf("id %s, want null", r.ID)
			}
			c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := c.r.ReadByte(); !errors.Is(err, io.EOF) {
				t.Fatalf("read after the error: %v, want EOF", err)
			}
		})
	}
}

func TestFraming(t *testing.T) {
	d := startDaemon(t)
	c := d.dial(t)
	cwd := clone(t, t.TempDir(), "agent-courier", originConfig)
	longID := `"` + strings.Repeat("i", MaxID-1) + `"`

	for _, tc := range []struct {
		name, line string
		id         string
		code       int
		inMessage  string
	}{
		{"not json", "not json\n", "null", codeParse, "not valid JSON"},
		{"invalid utf-8", "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"\xff\"}\n", "null", codeParse, "not valid JSON"},
		{"duplicate member", `{"jsonrpc":"2.0","id":1,"id":2,"method":"pending"}` + "\n", "null", codeParse, "repeats a member name"},
		{"batch", `[{"jsonrpc":"2.0","id":1,"method":"register"}]` + "\n", "null", codeInvalidRequest, "one JSON-RPC 2.0 request object"},
		{"not an object", `"register"` + "\n", "null", codeInvalidRequest, "one JSON-RPC 2.0 request object"},
		{"null id", `{"jsonrpc":"2.0","id":null,"method":"register"}` + "\n", "null", codeInvalidRequest, "id must be a string or a number"},
		{"object id", `{"jsonrpc":"2.0","id":{},"method":"register"}` + "\n", "null", codeInvalidRequest, "id must be a string or a number"},
		{"id over the cap", `{"jsonrpc":"2.0","id":` + longID + `,"method":"register"}` + "\n", "null", codeInvalidRequest, "id has at most 128 bytes"},
		{"unknown member", `{"jsonrpc":"2.0","id":1,"method":"register","x":1}` + "\n", "1", codeInvalidRequest, "no other member"},
		{"method not a string", `{"jsonrpc":"2.0","id":"m","method":5}` + "\n", `"m"`, codeInvalidRequest, "a method string"},
		{"old version", `{"jsonrpc":"1.0","id":"a","method":"register"}` + "\n", `"a"`, codeInvalidRequest, `"jsonrpc": "2.0"`},
		{"no method", `{"jsonrpc":"2.0","id":2}` + "\n", "2", codeInvalidRequest, "a method string"},
		{"unknown method", `{"jsonrpc":"2.0","id":3,"method":"rm"}` + "\n", "3", codeMethodNotFound, "register, send, pending, ack, agents"},
		{"send before register", `{"jsonrpc":"2.0","id":4,"method":"send","params":{}}` + "\n", "4", codeNotRegistered, "call register first"},
		{"params array", `{"jsonrpc":"2.0","id":5,"method":"register","params":[]}` + "\n", "5", codeInvalidParams, "params must be a JSON object"},
		{"unknown param", `{"jsonrpc":"2.0","id":6,"method":"register","params":{"harness":"claude","cwd":"/","sender":"x"}}` + "\n", "6", codeInvalidParams, "register params are harness, version and cwd"},
		{"wrong param type", `{"jsonrpc":"2.0","id":7,"method":"register","params":{"harness":"claude","cwd":"/","topics":"t"}}` + "\n", "7", codeInvalidParams, "topics, an array of strings"},
		{"bad harness", `{"jsonrpc":"2.0","id":8,"method":"register","params":{"harness":"Claude Code","cwd":"/"}}` + "\n", "8", codeInvalidParams, "harness must match ^[a-z0-9]{1,16}$"},
		{"harness everyone", `{"jsonrpc":"2.0","id":9,"method":"register","params":{"harness":"everyone","cwd":"/"}}` + "\n", "9", codeInvalidParams, "must not be everyone"},
		{"no harness", `{"jsonrpc":"2.0","id":10,"method":"register","params":{"cwd":"/"}}` + "\n", "10", codeInvalidParams, "harness must match"},
		{"no version", `{"jsonrpc":"2.0","id":11,"method":"register","params":{"harness":"claude","cwd":"/"}}` + "\n", "11", codeVersionMismatch, "version mismatch: the client is a version that is not valid, the daemon is " + version.Version},
		{"other version", `{"jsonrpc":"2.0","id":12,"method":"register","params":{"harness":"claude","version":"9.1.0","cwd":"/"}}` + "\n", "12", codeVersionMismatch, "the client is 9.1.0, the daemon is " + version.Version},
		{"version not valid", `{"jsonrpc":"2.0","id":13,"method":"register","params":{"harness":"claude","version":"</x>","cwd":"/"}}` + "\n", "13", codeVersionMismatch, "a version that is not valid"},
		{"relative cwd", `{"jsonrpc":"2.0","id":14,"method":"register","params":{"harness":"claude","version":"` + version.Version + `","cwd":"src"}}` + "\n", "14", codeInvalidParams, "cwd must be an absolute path"},
		{"long cwd", `{"jsonrpc":"2.0","id":15,"method":"register","params":{"harness":"claude","version":"` + version.Version + `","cwd":"/` + strings.Repeat("a", maxCwd) + `"}}` + "\n", "15", codeInvalidParams, "at most 4096 bytes"},
		{"no params", `{"jsonrpc":"2.0","id":16,"method":"register"}` + "\n", "16", codeInvalidParams, "harness must match"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c.t = t
			c.line(tc.line)
			r := c.read()
			wantError(t, r.Error, tc.code, tc.inMessage)
			if string(r.ID) != tc.id {
				t.Errorf("id %s, want %s", r.ID, tc.id)
			}
		})
	}

	c.t = t
	// Lines without an id get no response, whatever else they hold. Empty
	// lines are skipped, and \r\n ends a line too: the next response answers
	// id 20.
	c.line(`{"jsonrpc":"2.0","method":"register","params":{"harness":"claude","cwd":"/"}}` + "\n\n  \r\n")
	c.line(`{"jsonrpc":"2.0","method":5,"x":true}` + "\n")
	c.line(`{"jsonrpc":1,"params":[]}` + "\n")
	c.line(`{"jsonrpc":"2.0","id":20,"method":"register","params":{"harness":"claude","version":"` + version.Version + `","cwd":"` + cwd + `"}}` + "\r\n")
	if r := c.read(); string(r.ID) != "20" || r.Error != nil {
		t.Fatalf("register: id %s, error %+v", r.ID, r.Error)
	}
	c.next = 20

	_, rerr := c.register(registerParams{Harness: "claude", Cwd: cwd})
	wantError(t, rerr, codeAlreadyRegister, "already registered")
}

func TestNotificationsGetNoResponse(t *testing.T) {
	d := startDaemon(t)
	c := d.dial(t)
	for _, line := range []string{
		`{"jsonrpc":"2.0","method":"pending"}`,
		`{"jsonrpc":"2.0","method":"rm","extra":1}`,
		`{"jsonrpc":"1.0","method":["x"]}`,
	} {
		c.line(line + "\n")
	}
	c.noResponse()
}

// A session whose write fails handles no more lines, also lines it already
// read.
func TestWriteFailureStopsSession(t *testing.T) {
	srv := New(nil, testLogger(t))
	var in strings.Builder
	for i := range 1500 {
		in.WriteString(`{"jsonrpc":"2.0","id":` + strconv.Itoa(i) + `,"method":"ack","params":{"id":"x"}}` + "\n")
	}
	conn := &fakeConn{in: strings.NewReader(in.String()), fail: true}
	srv.serveConn(context.Background(), &session{id: "s", conn: conn})
	if len(conn.writes) != 1 {
		t.Fatalf("%d writes after the first failed write, want 1", len(conn.writes))
	}
}

func TestRespond(t *testing.T) {
	srv := New(nil, testLogger(t))

	t.Run("error messages pass through message.Inline", func(t *testing.T) {
		conn := &fakeConn{}
		zw := string(rune(0x200b))
		if err := srv.respond(&session{id: "s", conn: conn}, jsontext.Value(`7`), nil,
			&rpcError{codeInvalidParams, "a <b> & c" + zw + "\nd"}); err != nil {
			t.Fatal(err)
		}
		r := parseResp(t, conn.writes[0])
		if want := "a &lt;b&gt; &amp; c d"; r.Error.Message != want || string(r.ID) != "7" {
			t.Errorf("id %s, message %q; want 7, %q", r.ID, r.Error.Message, want)
		}
	})

	t.Run("a response over the line cap becomes an internal error with a null id", func(t *testing.T) {
		conn := &fakeConn{}
		if err := srv.respond(&session{id: "s", conn: conn}, jsontext.Value(`7`), strings.Repeat("x", MaxLine), nil); err != nil {
			t.Fatal(err)
		}
		if len(conn.writes) != 1 || len(conn.writes[0]) > MaxLine+1 {
			t.Fatalf("%d writes, first of %d bytes", len(conn.writes), len(conn.writes[0]))
		}
		r := parseResp(t, conn.writes[0])
		wantError(t, r.Error, codeInternal, "internal error")
		if string(r.ID) != "null" {
			t.Errorf("id %s, want null", r.ID)
		}
	})
}

// A client that never reads loses its connection after writeTimeout, so it
// cannot stall the daemon.
func TestWriteTimeout(t *testing.T) {
	old := writeTimeout
	writeTimeout = 50 * time.Millisecond
	t.Cleanup(func() { writeTimeout = old })
	d := startDaemon(t)
	c, err := net.Dial("unix", d.sock)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	line := []byte(`{"jsonrpc":"2.0","id":1,"method":"pending"}` + "\n")
	done := make(chan error, 1)
	go func() {
		for {
			if _, err := c.Write(line); err != nil {
				done <- err
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the daemon kept a client that does not read")
	}
}

func TestPeerOfAnotherUser(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SO_PEERCRED is Linux only")
	}
	t.Run("checkPeer", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		if checkPeer(a) == nil {
			t.Error("checkPeer accepted a connection that is not a unix socket")
		}
		ln, err := Listen(shortTempDir(t))
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		client, err := net.Dial("unix", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		c, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if err := checkPeer(c); err != nil {
			t.Errorf("checkPeer on a connection of this user: %v", err)
		}
		old := getuid
		getuid = func() int { return os.Getuid() + 1 }
		defer func() { getuid = old }()
		if err := checkPeer(c); err == nil || !strings.Contains(err.Error(), "peer uid") {
			t.Errorf("checkPeer for another uid: %v", err)
		}
	})

	t.Run("Serve refuses the connection", func(t *testing.T) {
		dir := shortTempDir(t)
		st, err := store.Open(filepath.Join(t.TempDir(), "courier.db"), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		ln, err := Listen(dir)
		if err != nil {
			t.Fatal(err)
		}
		old := getuid
		getuid = func() int { return os.Getuid() + 1 }
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- New(st, testLogger(t)).Serve(ctx, ln) }()
		defer func() {
			cancel()
			<-done
			getuid = old
		}()
		c := (&testDaemon{sock: filepath.Join(dir, SocketName)}).dial(t)
		c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if b, err := c.r.ReadBytes('\n'); !errors.Is(err, io.EOF) {
			t.Fatalf("read %q, %v; want EOF", b, err)
		}
	})
}

func TestListen(t *testing.T) {
	base := shortTempDir(t)
	dir := filepath.Join(base, "run")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(dir, SocketName)

	ln, err := Listen(dir)
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]os.FileMode{dir: 0o700, sock: 0o600, filepath.Join(dir, LockName): 0o600} {
		if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != want {
			t.Errorf("%s: %v, want mode %v", path, err, want)
		}
	}
	if _, err := Listen(dir); !errors.Is(err, ErrRunning) {
		t.Errorf("second Listen: %v, want ErrRunning", err)
	}
	ln.Close()
	if _, err := os.Lstat(sock); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("socket after Close: %v, want removed", err)
	}
	if _, err := Listen(dir); !errors.Is(err, ErrRunning) {
		t.Errorf("Listen after Close and before Unlock: %v, want ErrRunning", err)
	}
	ln.Unlock()
	next, err := Listen(dir)
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()
	ln.Unlock()
	if _, err := os.Lstat(sock); err != nil {
		t.Errorf("a second Close of the old listener removed the socket of the new one: %v", err)
	}
	next.Unlock()

	if err := os.WriteFile(sock, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	ln, err = Listen(dir)
	if err != nil {
		t.Fatalf("Listen over a stale socket file: %v", err)
	}
	ln.Unlock()

	link := filepath.Join(base, "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(link); err == nil {
		t.Error("Listen in a symlinked directory succeeded")
	}

	foreignFiles(t, "run")
	if _, err := Listen(dir); err == nil || !strings.Contains(err.Error(), "belongs to another user") {
		t.Errorf("Listen in a directory of another user: %v", err)
	}
}

// foreignFiles makes the files with these base names look like files of
// another user until the test ends.
func foreignFiles(t *testing.T, names ...string) {
	old := fileUID
	fileUID = func(fi os.FileInfo) int {
		for _, n := range names {
			if fi.Name() == n {
				return os.Getuid() + 1
			}
		}
		return old(fi)
	}
	t.Cleanup(func() { fileUID = old })
}

func TestListenConcurrent(t *testing.T) {
	dir := filepath.Join(shortTempDir(t), "run")
	const n = 20
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		winners []*Listener
		running int
	)
	start := make(chan struct{})
	for range n {
		wg.Go(func() {
			<-start
			ln, err := Listen(dir)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				winners = append(winners, ln)
			case errors.Is(err, ErrRunning):
				running++
			default:
				t.Errorf("Listen: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()
	if len(winners) != 1 || running != n-1 {
		t.Fatalf("%d winners and %d ErrRunning, want 1 and %d", len(winners), running, n-1)
	}
	defer winners[0].Close()
	c, err := net.Dial("unix", filepath.Join(dir, SocketName))
	if err != nil {
		t.Fatalf("the winner's socket did not survive the losers: %v", err)
	}
	c.Close()
}

func TestServeListenerClosed(t *testing.T) {
	dir := shortTempDir(t)
	st, err := store.Open(filepath.Join(t.TempDir(), "courier.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ln, err := Listen(dir)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- New(st, testLogger(t)).Serve(context.Background(), ln) }()
	c, err := net.Dial("unix", filepath.Join(dir, SocketName))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ln.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Errorf("Serve = %v, want net.ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after its listener closed")
	}
}

func TestMaintain(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: t0}
	ctx := context.Background()
	d := startDaemonWith(t, clock.now, func(st *store.Store) {
		for _, a := range []string{"old-a", "old-b"} {
			if _, err := st.Register(ctx, "t", a); err != nil {
				t.Fatal(err)
			}
		}
		clock.set(t0.Add(31 * 24 * time.Hour))
	})
	db := d.sqlDB(t)

	t.Run("cleanup at start", func(t *testing.T) {
		waitFor(t, "the cleanup at start", func() bool {
			return count(t, db, `SELECT count(*) FROM messages`) == 0 && count(t, db, `SELECT count(*) FROM agents`) == 0
		})
	})

	known := func(name string) bool {
		agents, err := d.store.Agents(ctx, "t")
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range agents {
			if a.Name == name {
				return true
			}
		}
		return false
	}
	t1 := clock.now()

	t.Run("hourly touch keeps live agents known", func(t *testing.T) {
		d.dialFor(t, t).mustRegister(registerParams{Harness: "claude", Cwd: "/", Name: "live", Topics: []string{"t"}})
		clock.set(t1.Add(29 * 24 * time.Hour))
		d.srv.maintain(ctx)
		clock.set(t1.Add(31 * 24 * time.Hour))
		if !known("live") {
			t.Error("a live agent touched at day 29 is unknown at day 31")
		}
	})

	t.Run("touch when a session ends", func(t *testing.T) {
		clock.set(t1)
		c := d.dial(t)
		c.mustRegister(registerParams{Harness: "claude", Cwd: "/", Name: "gone", Topics: []string{"t"}})
		clock.set(t1.Add(29 * 24 * time.Hour))
		c.conn.Close()
		waitFree(t, d.srv, "gone")
		clock.set(t1.Add(31 * 24 * time.Hour))
		if !known("gone") {
			t.Error("an agent whose session ended at day 29 is unknown at day 31")
		}
	})
}
