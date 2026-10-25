//go:build unix

package client

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/daemon"
)

// fakeSocket listens on sock and counts the connections it accepts.
func fakeSocket(t *testing.T, sock string) <-chan net.Conn {
	t.Helper()
	ln, err := net.Listen("unix", sock)
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

func noConnection(t *testing.T, conns <-chan net.Conn) {
	t.Helper()
	select {
	case c := <-conns:
		c.Close()
		t.Fatal("the client connected before it checked the socket directory")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestDialRefusesDirectoryOfAnotherUser(t *testing.T) {
	dir, _ := setupEnv(t)
	if err := daemon.PrivateDir(dir); err != nil {
		t.Fatal(err)
	}
	conns := fakeSocket(t, filepath.Join(dir, daemon.SocketName))
	getuid = func() int { return os.Getuid() + 1 }
	t.Cleanup(func() { getuid = os.Getuid })
	if _, err := Dial(context.Background()); err == nil || !strings.Contains(err.Error(), "belongs to another user") {
		t.Fatalf("Dial in a directory of another user: %v", err)
	}
	noConnection(t, conns)
}

func TestDialRefusesSymlinkedDirectory(t *testing.T) {
	dir, _ := setupEnv(t)
	real := filepath.Join(filepath.Dir(dir), "real")
	if err := daemon.PrivateDir(real); err != nil {
		t.Fatal(err)
	}
	conns := fakeSocket(t, filepath.Join(real, daemon.SocketName))
	if err := os.Symlink(real, dir); err != nil {
		t.Fatal(err)
	}
	if _, err := Dial(context.Background()); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("Dial in a symlinked directory: %v", err)
	}
	noConnection(t, conns)
}

func TestDialRefusesOpenDirectory(t *testing.T) {
	dir, _ := setupEnv(t)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	conns := fakeSocket(t, filepath.Join(dir, daemon.SocketName))
	if _, err := Dial(context.Background()); err == nil || !strings.Contains(err.Error(), "0700") {
		t.Fatalf("Dial in a directory with mode 0755: %v", err)
	}
	noConnection(t, conns)
}

func TestDialStartsAtMostFiveDaemons(t *testing.T) {
	_, starts := setupEnv(t)
	t.Setenv(failEnv, "100")
	if _, err := executable(); err != nil {
		t.Fatal(err)
	}
	begin := time.Now()
	_, err := Dial(context.Background())
	if err == nil || !strings.Contains(err.Error(), `exited before it listened: level=ERROR msg="fake failure 5"`) {
		t.Fatalf("Dial with a daemon that always fails: %v", err)
	}
	if got := readStarts(t, starts); len(got) != MaxStarts {
		t.Fatalf("%d starts, want %d", len(got), MaxStarts)
	}
	if took := time.Since(begin); took < (MaxStarts-1)*startPause {
		t.Errorf("five starts took %v, want at least %v between them", took, startPause)
	}
}
