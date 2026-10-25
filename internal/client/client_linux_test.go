//go:build linux

package client

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/daemon"
)

func peerPID(t *testing.T, c net.Conn) int {
	t.Helper()
	pid, err := tryPeerPID(c)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func tryPeerPID(c net.Conn) (int, error) {
	raw, err := c.(*net.UnixConn).SyscallConn()
	if err != nil {
		return 0, err
	}
	var cred *syscall.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return 0, err
	}
	if credErr != nil {
		return 0, credErr
	}
	return int(cred.Pid), nil
}

// daemons lists the processes that run the built binary.
func daemons(t *testing.T) []int {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		if exe, err := os.Readlink(filepath.Join("/proc", e.Name(), "exe")); err == nil && exe == testBin {
			pids = append(pids, pid)
		}
	}
	return pids
}

// killDaemon stops the daemon with exactly this pid when the test ends.
func killDaemon(t *testing.T, pid int) {
	t.Cleanup(func() { stopDaemon(t, pid) })
}

func stopDaemon(t *testing.T, pid int) {
	t.Helper()
	if !slices.Contains(daemons(t), pid) {
		return
	}
	syscall.Kill(pid, syscall.SIGTERM)
	for deadline := time.Now().Add(5 * time.Second); slices.Contains(daemons(t), pid); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			syscall.Kill(pid, syscall.SIGKILL)
			t.Errorf("daemon %d ignored SIGTERM", pid)
			return
		}
	}
}

func TestConcurrentStartFindsOneDaemon(t *testing.T) {
	_, starts := setupEnv(t)
	if _, err := executable(); err != nil {
		t.Fatal(err)
	}
	if got := daemons(t); len(got) != 0 {
		t.Fatalf("daemons of the built binary run before the test: %v", got)
	}

	const clients = 3
	conns := make([]net.Conn, clients)
	errs := make([]error, clients)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range clients {
		wg.Go(func() {
			<-start
			conns[i], errs[i] = Dial(context.Background())
		})
	}
	close(start)
	wg.Wait()

	pids := map[int]bool{}
	for i, c := range conns {
		if errs[i] != nil {
			t.Fatalf("client %d: %v", i, errs[i])
		}
		pid := peerPID(t, c)
		killDaemon(t, pid)
		pids[pid] = true
		cn := NewConn(c, nil)
		t.Cleanup(func() { cn.Close() })
		reg, err := cn.Register(context.Background(), RegisterParams{Harness: "claude", Cwd: "/", Topics: []string{"t"}})
		if err != nil {
			t.Fatalf("client %d: register: %v", i, err)
		}
		t.Logf("client %d: daemon %d, agent %s", i, pid, reg.Name)
	}
	if len(pids) != 1 {
		t.Fatalf("the clients reached %d daemons: %v", len(pids), pids)
	}
	if got := daemons(t); len(got) != 1 || !pids[got[0]] {
		t.Fatalf("running daemons %v, want exactly the one the clients reached %v", got, pids)
	}
	// The started process holds the lock before it execs the daemon, runs in
	// its own session at / with stdio on /dev/null.
	want := "leader=true cwd=/ fd3=lock held=yes stdio=null"
	if got := readStarts(t, starts); len(got) != 1 || got[0] != want {
		t.Fatalf("starts %q, want one %q", got, want)
	}
	state, _ := daemon.StateDir()
	fi, err := os.Stat(filepath.Join(state, daemon.LogName))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("daemon log: %v, want mode 0600", err)
	}
	if b, _ := os.ReadFile(filepath.Join(state, daemon.LogName)); !strings.Contains(string(b), "daemon started") {
		t.Errorf("daemon log misses the start line:\n%s", b)
	}
}

func TestSelfExecutable(t *testing.T) {
	if got, err := selfExecutable(); err != nil || got != "/proc/self/exe" {
		t.Fatalf("selfExecutable = %q, %v", got, err)
	}
}

func TestSessionReconnectsToNewDaemon(t *testing.T) {
	setupEnv(t)
	if _, err := executable(); err != nil {
		t.Fatal(err)
	}
	params := RegisterParams{Harness: "claude", Cwd: "/", Topics: []string{"t"}}
	s := Start(Config{Params: params})
	// Close before the daemons stop, or the session starts another one.
	var started []int
	t.Cleanup(func() {
		s.Close()
		for _, pid := range started {
			stopDaemon(t, pid)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := s.Registration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldPID := s.peer()
	if oldPID == 0 {
		t.Fatal("no daemon behind the session")
	}
	started = append(started, oldPID)
	stopDaemon(t, oldPID)
	var newPID int
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the session did not reconnect")
		}
		if _, err := s.Registration(ctx); err != nil {
			t.Fatal(err)
		}
		if newPID = s.peer(); newPID != oldPID && newPID != 0 {
			break
		}
	}
	started = append(started, newPID)
	second, err := s.Registration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.Name != first.Name || !slices.Equal(second.Topics, first.Topics) || second.Session == first.Session {
		t.Fatalf("after reconnect %+v, before %+v", second, first)
	}
	err = s.Call(ctx, "agents", nil, nil)
	if _, ok := errors.AsType[*RPCError](err); err != nil && !ok {
		t.Fatalf("call on the new daemon: %v", err)
	}
}

// peer is the pid of the daemon of the current connection, 0 without a live
// one.
func (s *Session) peer() int {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	if cur == nil {
		return 0
	}
	pid, _ := tryPeerPID(cur.conn)
	return pid
}

func TestDialRetriesAfterEarlyExit(t *testing.T) {
	_, starts := setupEnv(t)
	t.Setenv(failEnv, "2")
	if _, err := executable(); err != nil {
		t.Fatal(err)
	}
	c, err := Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	killDaemon(t, peerPID(t, c))
	if got := readStarts(t, starts); len(got) != 3 {
		t.Fatalf("starts %q, want two that failed and one that listens", got)
	}
}

func TestConnectRefusesPeerOfAnotherUser(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "s")
	if len(sock) > 100 {
		sock = filepath.Join(os.TempDir(), "ac-peer-"+strconv.Itoa(os.Getpid()))
		t.Cleanup(func() { os.Remove(sock) })
	}
	conns := fakeSocket(t, sock)
	getuid = func() int { return os.Getuid() + 1 }
	t.Cleanup(func() { getuid = os.Getuid })
	if c, err := connect(sock); !errors.Is(err, errForeign) {
		if c != nil {
			c.Close()
		}
		t.Fatalf("connect to a daemon of another uid: %v", err)
	}
	c := <-conns
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatalf("the refused connection stayed open and sent %d bytes", n)
	}
}
