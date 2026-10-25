//go:build unix

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/client"
	"github.com/CappyT/agent-courier/internal/daemon"
	"github.com/CappyT/agent-courier/internal/version"
)

// startTestDaemon runs a daemon in this process on a temporary socket
// directory and store, and waits until it listens. A bridge test always has
// a listener before the bridge dials, so Dial never starts the test binary
// as a daemon.
func startTestDaemon(t *testing.T) {
	t.Helper()
	dir := filepath.Join(shortTempDir(t), "agent-courier")
	t.Setenv(daemon.SocketDirEnv, dir)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	var logs syncBuffer
	code := make(chan int, 1)
	go func() { code <- serveDaemon(ctx, &logs) }()
	t.Cleanup(func() {
		cancel()
		<-code
	})
	sock := filepath.Join(dir, daemon.SocketName)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if c, err := net.Dial("unix", sock); err == nil {
			c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no daemon at %s; logs:\n%s", sock, logs.String())
		}
	}
}

// testBridge is a running bridge with pipes as its stdin and stdout.
type testBridge struct {
	in     *io.PipeWriter
	lines  chan string
	stderr *syncBuffer
	code   chan int
}

func startBridge(t *testing.T) *testBridge {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	b := &testBridge{in: inW, lines: make(chan string, 64), stderr: &syncBuffer{}, code: make(chan int, 1)}
	go func() { b.code <- runBridge(inR, outW, b.stderr) }()
	go func() {
		r := bufio.NewReaderSize(outR, 2*daemon.MaxLine)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			b.lines <- line
		}
	}()
	t.Cleanup(func() {
		inW.Close()
		outR.Close()
	})
	return b
}

func (b *testBridge) write(t *testing.T, line string) {
	t.Helper()
	if _, err := io.WriteString(b.in, line+"\n"); err != nil {
		t.Fatalf("write to the bridge: %v; stderr:\n%s", err, b.stderr.String())
	}
}

// next returns the first line from the bridge that contains want.
func (b *testBridge) next(t *testing.T, want string) string {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case line := <-b.lines:
			if strings.Contains(line, want) {
				return line
			}
		case <-timeout:
			t.Fatalf("no line with %s from the bridge; stderr:\n%s", want, b.stderr.String())
		}
	}
}

func (b *testBridge) exit(t *testing.T) int {
	t.Helper()
	select {
	case c := <-b.code:
		return c
	case <-time.After(5 * time.Second):
		t.Fatalf("the bridge did not exit; stderr:\n%s", b.stderr.String())
		return 0
	}
}

func TestBridgeCarriesASession(t *testing.T) {
	startTestDaemon(t)
	b := startBridge(t)
	b.write(t, `{"jsonrpc":"2.0","id":1,"method":"register","params":{"harness":"opencode","version":"`+version.Version+`","cwd":"/","topics":["t"]}}`)
	if line := b.next(t, `"id":1`); !strings.Contains(line, `"name":"opencode"`) {
		t.Fatalf("register response through the bridge: %s", line)
	}

	nc, err := client.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	peer := client.NewConn(nc, nil)
	defer peer.Close()
	if _, err := peer.Register(context.Background(), client.RegisterParams{Harness: "claude", Cwd: "/", Topics: []string{"t"}}); err != nil {
		t.Fatal(err)
	}

	b.write(t, `{"jsonrpc":"2.0","id":2,"method":"send","params":{"topic":"t","kind":"ask","summary":"which branch?","to":"claude"}}`)
	if line := b.next(t, `"id":2`); !strings.Contains(line, `"recipients":["claude"]`) {
		t.Fatalf("send response through the bridge: %s", line)
	}

	var sent struct{ ID string }
	if err := peer.Call(context.Background(), "send", map[string]any{"topic": "t", "kind": "note", "summary": "main", "to": "opencode"}, &sent); err != nil {
		t.Fatal(err)
	}
	line := b.next(t, `"kind":"note"`)
	if !strings.Contains(line, `"method":"deliver"`) || !strings.Contains(line, sent.ID) {
		t.Fatalf("deliver through the bridge: %s", line)
	}

	b.in.Close()
	if c := b.exit(t); c != 0 {
		t.Fatalf("bridge after stdin closed = %d; stderr:\n%s", c, b.stderr.String())
	}
	// The bridge closed its socket, so the daemon frees the agent name.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		var res struct {
			Agents []struct {
				Name string
				Live bool
			}
		}
		if err := peer.Call(context.Background(), "agents", nil, &res); err != nil {
			t.Fatal(err)
		}
		live := false
		for _, a := range res.Agents {
			live = live || a.Name == "opencode" && a.Live
		}
		if !live {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the agent of the bridge is still live after the bridge exited")
		}
	}
}

func TestBridgeStopsALongLineFromStdin(t *testing.T) {
	startTestDaemon(t)
	b := startBridge(t)
	// A line of exactly MaxLine bytes reaches the daemon, which answers it.
	b.write(t, strings.Repeat("x", daemon.MaxLine))
	b.next(t, "-32700")
	b.write(t, strings.Repeat("x", daemon.MaxLine+1))
	if c := b.exit(t); c != 1 || !strings.Contains(b.stderr.String(), "from stdin: a line has more than 65536 bytes") {
		t.Fatalf("bridge after a long line = %d; stderr:\n%s", c, b.stderr.String())
	}
	// Forwarded, the line would draw the daemon's -32600 response.
	select {
	case line := <-b.lines:
		t.Fatalf("the bridge passed the long line on; the daemon answered %s", line)
	case <-time.After(200 * time.Millisecond):
	}
}

// fakeDaemonDir listens on the socket of a temporary socket directory and
// returns the accepted connections. prepare sets the directory up before the
// listen.
func fakeDaemonDir(t *testing.T, prepare func(dir string) string) <-chan net.Conn {
	t.Helper()
	dir := filepath.Join(shortTempDir(t), "agent-courier")
	listenDir := prepare(dir)
	t.Setenv(daemon.SocketDirEnv, dir)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	ln, err := net.Listen("unix", filepath.Join(listenDir, daemon.SocketName))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	conns := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns <- c
		}
	}()
	return conns
}

func TestBridgeStopsALongLineFromTheDaemon(t *testing.T) {
	conns := fakeDaemonDir(t, func(dir string) string {
		if err := daemon.PrivateDir(dir); err != nil {
			t.Fatal(err)
		}
		return dir
	})
	b := startBridge(t)
	var c net.Conn
	select {
	case c = <-conns:
		defer c.Close()
	case <-time.After(5 * time.Second):
		t.Fatalf("the bridge did not connect; stderr:\n%s", b.stderr.String())
	}
	ok := `{"jsonrpc":"2.0","method":"deliver","params":{}}`
	go io.WriteString(c, ok+"\n"+strings.Repeat("y", daemon.MaxLine+1)+"\n")
	b.next(t, ok)
	if code := b.exit(t); code != 1 || !strings.Contains(b.stderr.String(), "from the daemon: a line has more than 65536 bytes") {
		t.Fatalf("bridge after a long line from the daemon = %d; stderr:\n%s", code, b.stderr.String())
	}
	select {
	case line := <-b.lines:
		t.Fatalf("the bridge passed %d bytes of the long line on", len(line))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestBridgeRefusesAnUnsafeSocketDirectory(t *testing.T) {
	for name, prepare := range map[string]func(t *testing.T, dir string) string{
		"mode 0755": func(t *testing.T, dir string) string {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			return dir
		},
		"symlink": func(t *testing.T, dir string) string {
			real := filepath.Join(filepath.Dir(dir), "real")
			if err := daemon.PrivateDir(real); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, dir); err != nil {
				t.Fatal(err)
			}
			return real
		},
	} {
		t.Run(name, func(t *testing.T) {
			conns := fakeDaemonDir(t, func(dir string) string { return prepare(t, dir) })
			b := startBridge(t)
			if c := b.exit(t); c != 1 || !strings.Contains(b.stderr.String(), "refusing the courier socket directory") {
				t.Fatalf("bridge in an unsafe directory = %d; stderr:\n%s", c, b.stderr.String())
			}
			select {
			case c := <-conns:
				c.Close()
				t.Fatal("the bridge connected to a socket in an unsafe directory")
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
}

// rawBridge runs a bridge against a fake daemon in a private socket
// directory. Its stdout records every byte, so a partial line shows.
func rawBridge(t *testing.T, stdin io.Reader) (net.Conn, *syncBuffer, *syncBuffer, chan int) {
	t.Helper()
	conns := fakeDaemonDir(t, func(dir string) string {
		if err := daemon.PrivateDir(dir); err != nil {
			t.Fatal(err)
		}
		return dir
	})
	stdout, stderr, code := &syncBuffer{}, &syncBuffer{}, make(chan int, 1)
	go func() { code <- runBridge(stdin, stdout, stderr) }()
	select {
	case c := <-conns:
		t.Cleanup(func() { c.Close() })
		return c, stdout, stderr, code
	case <-time.After(5 * time.Second):
		t.Fatalf("the bridge did not connect; stderr:\n%s", stderr.String())
		return nil, nil, nil, nil
	}
}

func waitExit(t *testing.T, code chan int, stderr *syncBuffer) int {
	t.Helper()
	select {
	case c := <-code:
		return c
	case <-time.After(5 * time.Second):
		t.Fatalf("the bridge did not exit; stderr:\n%s", stderr.String())
		return 0
	}
}

// openStdin is a stdin that stays open until the test ends.
func openStdin(t *testing.T) io.Reader {
	r, w := io.Pipe()
	t.Cleanup(func() { w.Close() })
	return r
}

func TestBridgeEndsWhenTheDaemonCloses(t *testing.T) {
	c, _, stderr, code := rawBridge(t, openStdin(t))
	c.Close()
	if got := waitExit(t, code, stderr); got != 1 || !strings.Contains(stderr.String(), "the courier daemon closed the connection") {
		t.Fatalf("bridge after the daemon closed = %d; stderr:\n%s", got, stderr.String())
	}
}

func TestBridgeWritesNoPartOfALongLine(t *testing.T) {
	ok := `{"jsonrpc":"2.0","id":1,"method":"pending"}`
	// One byte over the cap fits the read buffer; three times the cap does not.
	for _, n := range []int{daemon.MaxLine + 1, 3 * daemon.MaxLine} {
		t.Run(fmt.Sprintf("%d bytes from stdin", n), func(t *testing.T) {
			c, _, stderr, code := rawBridge(t, strings.NewReader(ok+"\n"+strings.Repeat("x", n)+"\n"))
			got := make(chan []byte, 1)
			go func() {
				b, _ := io.ReadAll(c)
				got <- b
			}()
			if c := waitExit(t, code, stderr); c != 1 {
				t.Fatalf("bridge after a long line = %d; stderr:\n%s", c, stderr.String())
			}
			select {
			case b := <-got:
				if string(b) != ok+"\n" {
					t.Fatalf("the daemon got %d bytes, want only the first line", len(b))
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the bridge did not close the socket")
			}
		})
		t.Run(fmt.Sprintf("%d bytes from the daemon", n), func(t *testing.T) {
			c, stdout, stderr, code := rawBridge(t, openStdin(t))
			go io.WriteString(c, ok+"\n"+strings.Repeat("y", n)+"\n")
			if c := waitExit(t, code, stderr); c != 1 {
				t.Fatalf("bridge after a long line = %d; stderr:\n%s", c, stderr.String())
			}
			if got := stdout.String(); got != ok+"\n" {
				t.Fatalf("stdout got %d bytes, want only the first line", len(got))
			}
		})
	}
}
