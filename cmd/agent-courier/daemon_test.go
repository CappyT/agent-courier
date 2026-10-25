//go:build unix

package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/daemon"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/version"
)

func TestDaemonCommand(t *testing.T) {
	// A unix socket path has at most 108 bytes, so t.TempDir() can be too long.
	run, err := os.MkdirTemp("", "ac")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(run) })
	state := t.TempDir()
	dir := filepath.Join(run, "agent-courier")
	t.Setenv(daemon.SocketDirEnv, dir)
	t.Setenv("XDG_STATE_HOME", state)
	unlocked := make(chan error, 1)
	beforeUnlock = func(st *store.Store) {
		if err := st.Cleanup(context.Background()); err == nil {
			unlocked <- errors.New("the store is open")
			return
		}
		lock, err := daemon.OpenLock(dir)
		if err != nil {
			unlocked <- err
			return
		}
		defer lock.Close()
		if err := daemon.TryLock(lock); !errors.Is(err, daemon.ErrRunning) {
			unlocked <- fmt.Errorf("the lock is free before the daemon releases it: %v", err)
			return
		}
		unlocked <- nil
	}
	t.Cleanup(func() { beforeUnlock = func(*store.Store) {} })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs syncBuffer
	code := make(chan int, 1)
	go func() { code <- serveDaemon(ctx, &logs) }()

	sock := filepath.Join(dir, daemon.SocketName)
	var conn net.Conn
	for deadline := time.Now().Add(5 * time.Second); conn == nil; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("no socket at %s; logs:\n%s", sock, logs.String())
		}
		conn, _ = net.Dial("unix", sock)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"register","params":{"harness":"claude","version":"` + version.Version + `","cwd":"/","topics":["t"]}}` + "\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || !strings.Contains(line, `"name":"claude"`) {
		t.Fatalf("register response %q, %v", line, err)
	}

	var second syncBuffer
	if c := serveDaemon(ctx, &second); c != 1 || !strings.Contains(second.String(), daemon.ErrRunning.Error()) {
		t.Errorf("second daemon = %d, logs:\n%s", c, second.String())
	}
	select {
	case err := <-unlocked:
		t.Fatalf("a second daemon that lost the lock reached the release: %v", err)
	default:
	}
	if _, err := net.Dial("unix", sock); err != nil {
		t.Errorf("the socket did not survive the second daemon: %v", err)
	}

	cancel()
	if c := <-code; c != 0 {
		t.Fatalf("serveDaemon = %d; logs:\n%s", c, logs.String())
	}
	select {
	case err := <-unlocked:
		if err != nil {
			t.Error(err)
		}
	default:
		t.Error("the daemon released the lock without the check")
	}
	lock, err := daemon.OpenLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.TryLock(lock); err != nil {
		t.Errorf("the lock after the daemon stopped: %v", err)
	}
	lock.Close()
	if _, err := os.Stat(filepath.Join(state, "agent-courier", "courier.db")); err != nil {
		t.Errorf("store: %v", err)
	}
	if !strings.Contains(logs.String(), "daemon started") {
		t.Errorf("logs miss the start line:\n%s", logs.String())
	}
}

func TestDaemonIdleExit(t *testing.T) {
	t.Setenv(daemon.SocketDirEnv, filepath.Join(shortTempDir(t), "agent-courier"))
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv(daemon.IdleGraceEnv, "200ms")
	var logs syncBuffer
	code := make(chan int, 1)
	go func() { code <- serveDaemon(context.Background(), &logs) }()
	select {
	case c := <-code:
		if c != 0 {
			t.Fatalf("serveDaemon = %d; logs:\n%s", c, logs.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the daemon did not exit when idle; logs:\n%s", logs.String())
	}
}

func TestIdleGrace(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	for env, want := range map[string]time.Duration{
		"":     daemon.DefaultIdleGrace,
		"5s":   5 * time.Second,
		"-1s":  daemon.DefaultIdleGrace,
		"0":    daemon.DefaultIdleGrace,
		"soon": daemon.DefaultIdleGrace,
	} {
		t.Setenv(daemon.IdleGraceEnv, env)
		if got := idleGrace(log); got != want {
			t.Errorf("%s=%q: idleGrace = %v, want %v", daemon.IdleGraceEnv, env, got, want)
		}
	}
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ac")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
