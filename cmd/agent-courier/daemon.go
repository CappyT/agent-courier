//go:build unix

package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"

	"github.com/CappyT/agent-courier/internal/daemon"
	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/store"
)

func runDaemon(stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveDaemon(ctx, stderr)
}

// p2pListen and p2pRelays are variables so tests keep the p2p host on
// loopback and away from the project relay.
var (
	p2pListen = p2p.DefaultListen
	p2pRelays = p2p.DefaultRelays
)

// beforeUnlock is a variable so tests can check the daemon's state at the
// moment before it releases the lock.
var beforeUnlock = func(*store.Store) {}

// serveDaemon runs the daemon until ctx ends or it is idle. A daemon that a
// client started writes its log to daemon.log in the state directory, because
// its stderr is /dev/null. A daemon started by hand logs to stderr.
func serveDaemon(ctx context.Context, stderr io.Writer) int {
	v, byClient := os.LookupEnv(daemon.LockFDEnv)
	os.Unsetenv(daemon.LockFDEnv)
	var lock *os.File
	var lockErr error
	if byClient {
		lock, lockErr = inheritLock(v)
	}
	state, stateErr := daemon.StateDir()
	out := stderr
	if byClient && stateErr == nil {
		if f, err := daemon.OpenLog(state); err == nil {
			defer f.Close()
			out = f
		}
	}
	log := slog.New(slog.NewTextHandler(out, nil))
	if stateErr != nil {
		log.Error("no state directory", "err", stateErr)
		return 1
	}
	if lockErr != nil {
		log.Error("inherited lock", "err", lockErr)
		return 1
	}
	path := filepath.Join(state, "courier.db")
	dir := daemon.SocketDir()
	var ln *daemon.Listener
	var err error
	if lock != nil {
		ln, err = daemon.ListenLocked(dir, lock)
	} else {
		ln, err = daemon.Listen(dir)
	}
	if err != nil {
		log.Error("listen", "dir", dir, "err", err)
		return 1
	}
	defer ln.Unlock()
	st, err := store.Open(path, nil)
	if err != nil {
		log.Error("open store", "path", path, "err", err)
		return 1
	}
	defer func() {
		st.Close()
		beforeUnlock(st)
	}()
	log.Info("daemon started", "socket", ln.Addr().String(), "store", path)
	srv := daemon.New(st, log)
	srv.ExitWhenIdle(idleGrace(log))
	hosts := daemon.NewP2P(st, srv.Teams(), log, daemon.P2PConfig{Key: machineKey, Listen: p2pListen(), DefaultRelays: p2pRelays()})
	srv.SetNetwork(hosts)
	srv.SetPresence(hosts)
	// The hosts outlive ctx: a daemon that stops on a signal leaves its
	// teams through them first.
	hostsCtx, stopHosts := context.WithCancel(context.WithoutCancel(ctx))
	hostsDone := make(chan struct{})
	go func() {
		hosts.Run(hostsCtx)
		close(hostsDone)
	}()
	// Runs before the store closes: Serve can return on idle while ctx lives.
	defer func() {
		hosts.LeaveAll()
		stopHosts()
		<-hostsDone
	}()
	if err := srv.Serve(ctx, ln); err != nil {
		log.Error("serve", "err", err)
		return 1
	}
	log.Info("daemon stopped")
	return 0
}

// machineKey creates a new machine key. The daemon keeps it in memory only,
// so a restarted daemon is a new peer.
func machineKey() (crypto.PrivKey, error) {
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	return key, err
}

// inheritLock is the lock file that the starting client passed as LockFD.
// It gets close-on-exec, so no process that the daemon starts holds the lock.
func inheritLock(v string) (*os.File, error) {
	if v != strconv.Itoa(daemon.LockFD) {
		return nil, fmt.Errorf("%s must be %d", daemon.LockFDEnv, daemon.LockFD)
	}
	syscall.CloseOnExec(daemon.LockFD)
	return os.NewFile(uintptr(daemon.LockFD), daemon.LockName), nil
}

// idleGrace is IdleGraceEnv as a positive duration, else DefaultIdleGrace.
func idleGrace(log *slog.Logger) time.Duration {
	v := os.Getenv(daemon.IdleGraceEnv)
	if v == "" {
		return daemon.DefaultIdleGrace
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		log.Warn("ignoring the idle grace, it is not a positive duration", "env", daemon.IdleGraceEnv, "default", daemon.DefaultIdleGrace)
		return daemon.DefaultIdleGrace
	}
	return d
}
