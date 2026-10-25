//go:build linux

package main

import (
	"bufio"
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/CappyT/agent-courier/internal/daemon"
	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/team"
)

// inetSockets are the inodes of the TCP and UDP sockets that this process
// holds open.
func inetSockets(t *testing.T) map[string]bool {
	t.Helper()
	own := map[string]bool{}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		link, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if ino, ok := strings.CutPrefix(link, "socket:["); err == nil && ok {
			own[strings.TrimSuffix(ino, "]")] = true
		}
	}
	out := map[string]bool{}
	for _, table := range []string{"tcp", "tcp6", "udp", "udp6"} {
		f, err := os.Open(filepath.Join("/proc/self/net", table))
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Scan() // header
		for sc.Scan() {
			if fields := strings.Fields(sc.Text()); len(fields) > 9 && own[fields[9]] {
				out[fields[9]] = true
			}
		}
		f.Close()
	}
	return out
}

// serveTestDaemon runs serveDaemon on temporary socket, state and config
// directories, and returns the config directory and a stop function that
// returns its exit code.
func serveTestDaemon(t *testing.T) (config string, logs *syncBuffer, stop func() int) {
	t.Helper()
	config = t.TempDir()
	t.Setenv(daemon.SocketDirEnv, filepath.Join(shortTempDir(t), "agent-courier"))
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", config)
	logs, stop = serveAgain(t)
	return config, logs, stop
}

// serveAgain runs serveDaemon on the directories of the last serveTestDaemon
// of t, and waits for its socket.
func serveAgain(t *testing.T) (logs *syncBuffer, stop func() int) {
	t.Helper()
	dir := os.Getenv(daemon.SocketDirEnv)
	ctx, cancel := context.WithCancel(context.Background())
	logs = &syncBuffer{}
	code := make(chan int, 1)
	go func() { code <- serveDaemon(ctx, logs) }()
	stop = sync.OnceValue(func() int {
		cancel()
		return <-code
	})
	t.Cleanup(func() { stop() })
	sock := filepath.Join(dir, daemon.SocketName)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if c, err := net.Dial("unix", sock); err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no socket at %s; logs:\n%s", sock, logs.String())
		}
	}
	return logs, stop
}

func waitInet(t *testing.T, what string, cond func(map[string]bool) bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if cond(inetSockets(t)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func added(before, now map[string]bool) int {
	n := 0
	for ino := range now {
		if !before[ino] {
			n++
		}
	}
	return n
}

// A daemon on a machine with no team listens only on the local socket, and
// writes nothing to the config directory.
func TestDaemonNoTeamNoNetwork(t *testing.T) {
	before := inetSockets(t)
	config, logs, stop := serveTestDaemon(t)
	// The host lifecycle checks the teams at start; give it time to act.
	time.Sleep(200 * time.Millisecond)
	for ino := range inetSockets(t) {
		if !before[ino] {
			t.Errorf("the daemon of a machine with no team holds network socket %s", ino)
		}
	}
	if files, err := os.ReadDir(config); err != nil || len(files) != 0 {
		t.Errorf("the daemon wrote %v to the config directory: %v", files, err)
	}
	if c := stop(); c != 0 {
		t.Fatalf("serveDaemon = %d; logs:\n%s", c, logs.String())
	}
	if strings.Contains(logs.String(), "p2p host started") {
		t.Errorf("logs show a p2p host:\n%s", logs.String())
	}
}

// A daemon in a team opens the sockets of its host, makes invite codes with
// its machine key, leaves the team when its context ends, and closes the
// sockets when it exits. The machine key never reaches the disk.
func TestDaemonInviteAndExit(t *testing.T) {
	before := inetSockets(t)
	config, logs, stop := serveTestDaemon(t)
	created := runCLI(t, 0, "team", "create", "courier", "--topic", "t", "--label", "alpha")
	waitInet(t, "the sockets of the host", func(now map[string]bool) bool { return added(before, now) >= 2 })
	_, id, ok := strings.Cut(created, "alpha  ")
	if !ok {
		t.Fatalf("no peer id in %q", created)
	}
	id = strings.TrimSpace(id)
	out := runCLI(t, 0, "team", "invite", "courier")
	var inv p2p.Invite
	for line := range strings.Lines(out) {
		if i, err := p2p.DecodeInvite(line); err == nil {
			inv = i
		}
	}
	if inv.Inviter.String() != id || inv.Team != "courier" {
		t.Fatalf("no invite code of %s for courier in %q", id, out)
	}
	if c := stop(); c != 0 {
		t.Fatalf("serveDaemon = %d; logs:\n%s", c, logs.String())
	}
	// A stop on a context cancel leaves the team first.
	wantIn(t, logs.String(), "team left at the stop of the daemon")
	if n := added(before, inetSockets(t)); n != 0 {
		t.Fatalf("%d network sockets open after the daemon exited; logs:\n%s", n, logs.String())
	}
	if files, err := os.ReadDir(config); err != nil || len(files) != 0 {
		t.Fatalf("the daemon wrote %v to the config directory: %v", files, err)
	}
}

// A restarted daemon belongs to no team, has a new peer id, and keeps
// nothing of the team on disk.
func TestDaemonRestartForgetsTeams(t *testing.T) {
	config, logs, stop := serveTestDaemon(t)
	peerID := func(out string) string {
		t.Helper()
		_, id, ok := strings.Cut(out, "alpha  ")
		if !ok {
			t.Fatalf("no peer id in %q", out)
		}
		return strings.TrimSpace(id)
	}
	first := peerID(runCLI(t, 0, "team", "create", "courier", "--topic", "t", "--label", "alpha"))
	runCLI(t, 0, "team", "invite", "courier")
	if c := stop(); c != 0 {
		t.Fatalf("serveDaemon = %d; logs:\n%s", c, logs.String())
	}
	logs, stop = serveAgain(t)
	wantIn(t, runCLI(t, 0, "team", "list"), "belongs to no team")
	second := peerID(runCLI(t, 0, "team", "create", "courier", "--topic", "t", "--label", "alpha"))
	if second == first {
		t.Fatalf("the restarted daemon kept peer id %s", first)
	}
	if c := stop(); c != 0 {
		t.Fatalf("serveDaemon = %d; logs:\n%s", c, logs.String())
	}
	if files, err := os.ReadDir(config); err != nil || len(files) != 0 {
		t.Fatalf("the daemon wrote %v to the config directory: %v", files, err)
	}
}

// A daemon stays while it is in a team. After its last team ends, it exits
// on idle while its context lives, and closes the sockets of its host before
// serveDaemon returns.
func TestDaemonIdleExitClosesHost(t *testing.T) {
	before := inetSockets(t)
	t.Setenv(daemon.IdleGraceEnv, "3s")
	dir := filepath.Join(shortTempDir(t), "agent-courier")
	t.Setenv(daemon.SocketDirEnv, dir)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	logs := &syncBuffer{}
	code := make(chan int, 1)
	go func() { code <- serveDaemon(t.Context(), logs) }()
	sock := filepath.Join(dir, daemon.SocketName)
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if c, err := net.Dial("unix", sock); err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no socket at %s; logs:\n%s", sock, logs.String())
		}
	}
	runCLI(t, 0, "team", "create", "courier", "--topic", "t", "--label", "alpha")
	waitInet(t, "the sockets of the host", func(now map[string]bool) bool { return added(before, now) >= 2 })
	select {
	case c := <-code:
		t.Fatalf("the daemon exited with %d while in a team; logs:\n%s", c, logs.String())
	case <-time.After(5 * time.Second):
	}
	runCLI(t, 0, "team", "leave", "courier")
	select {
	case c := <-code:
		if c != 0 {
			t.Fatalf("serveDaemon = %d; logs:\n%s", c, logs.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("the daemon did not exit on idle; logs:\n%s", logs.String())
	}
	if n := added(before, inetSockets(t)); n != 0 {
		t.Fatalf("%d network sockets open after the daemon exited on idle; logs:\n%s", n, logs.String())
	}
}

// startInviter runs the p2p side of another machine in this process, in team
// courier with label alpha, listening on every IPv4 interface: invite codes
// leave out loopback addresses.
func startInviter(t *testing.T) (*daemon.P2P, *team.Set) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "inviter.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	self, err := libp2ppeer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	set := team.NewSet(nil)
	if _, err := set.Create(self.String(), "courier", []string{"t"}, "alpha"); err != nil {
		t.Fatal(err)
	}
	n := daemon.NewP2P(st, set, slog.New(slog.DiscardHandler), daemon.P2PConfig{
		Key:    func() (crypto.PrivKey, error) { return key, nil },
		Listen: []string{"/ip4/0.0.0.0/tcp/0"},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		n.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		st.Close()
	})
	return n, set
}

// A daemon of a machine with no team joins through serveDaemon: the join
// starts its host, which stays after the join. A join that fails stops the
// host again.
func TestDaemonJoinWithoutTeam(t *testing.T) {
	inviter, inviterSet := startInviter(t)
	mustHost(t, inviter)
	secret, expires, err := inviterSet.CreateInvite("courier")
	if err != nil {
		t.Fatal(err)
	}
	code, err := inviter.InviteCode("courier", secret, expires)
	if err != nil {
		t.Fatal(err)
	}
	if inv, _ := p2p.DecodeInvite(code); len(inv.Addrs) == 0 {
		t.Skip("this machine has no IPv4 address outside loopback for the inviter")
	}

	before := inetSockets(t)
	_, logs, stop := serveTestDaemon(t)
	unreachable := p2p.Invite{Team: "courier", Inviter: mustHost(t, inviter).ID(), Secret: make([]byte, team.SecretSize),
		Expires: time.Now().Add(time.Hour), Addrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/1")}}
	bad, err := unreachable.Encode()
	if err != nil {
		t.Fatal(err)
	}
	wantIn(t, runCLI(t, 1, "team", "join", bad, "--label", "bravo"), "not reachable")
	waitInet(t, "the sockets of the failed join to close", func(now map[string]bool) bool { return added(before, now) == 0 })
	wantIn(t, logs.String(), "p2p host started", "p2p host stopped")

	wantIn(t, runCLI(t, 0, "team", "join", code, "--label", "bravo"), "courier", "alpha  ", "bravo  ")
	time.Sleep(200 * time.Millisecond)
	if strings.Count(logs.String(), "p2p host started") != 2 || strings.Count(logs.String(), "p2p host stopped") != 1 {
		t.Fatalf("the host did not stay after the join; logs:\n%s", logs.String())
	}
	if added(before, inetSockets(t)) == 0 {
		t.Fatal("no network socket after the join")
	}
	tm, _ := inviterSet.Get("courier")
	if !slices.ContainsFunc(tm.Members.Active(), func(e team.Entry) bool { return e.Label == "bravo" }) {
		t.Fatalf("the inviter's member list lacks the joiner: %+v", tm.Members)
	}
	if c := stop(); c != 0 {
		t.Fatalf("serveDaemon = %d; logs:\n%s", c, logs.String())
	}
}

func mustHost(t *testing.T, n *daemon.P2P) *p2p.Host {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); n.Host() == nil; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no p2p host")
		}
	}
	return n.Host()
}
