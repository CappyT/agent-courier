//go:build unix

// Package client connects a harness client to the courier daemon over the
// local socket, and starts the daemon when none runs. design.md "Socket and
// wire protocol" is the protocol, "On-demand start and single instance" the
// start.
package client

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/CappyT/agent-courier/internal/daemon"
)

const (
	// StartTimeout is how long Dial waits for a daemon to listen.
	StartTimeout = 5 * time.Second
	// MaxStarts is how many daemons one Dial starts at most.
	MaxStarts = 5

	startPause = 200 * time.Millisecond
	pollEvery  = 20 * time.Millisecond
)

// executable and getuid are variables so tests can start another binary and
// make the socket directory or the daemon look like another user's.
var (
	executable = selfExecutable
	getuid     = os.Getuid
)

// selfExecutable is /proc/self/exe on Linux, so a binary replaced or deleted
// by an update still starts a daemon of the client's own version.
func selfExecutable() (string, error) {
	if runtime.GOOS == "linux" {
		return "/proc/self/exe", nil
	}
	return os.Executable()
}

// Dial connects to the daemon socket of the user. When no daemon listens, it
// starts one and waits for it at most StartTimeout.
//
// Before it connects, Dial checks the socket directory, and on Linux the uid
// of the daemon. Only the holder of the daemon lock starts a daemon. It passes
// the open lock file to the daemon as a descriptor, so the lock stays taken
// from the check to the daemon's exit and a second client cannot start a
// second daemon.
func Dial(ctx context.Context) (net.Conn, error) {
	dir := daemon.SocketDir()
	sock := filepath.Join(dir, daemon.SocketName)
	switch err := daemon.CheckDir(dir, getuid()); {
	case err == nil:
		if c, err := connect(sock); err == nil || errors.Is(err, errForeign) {
			return c, err
		}
	case errors.Is(err, fs.ErrNotExist):
		if err := daemon.PrivateDir(dir); err != nil {
			return nil, err
		}
		if err := daemon.CheckDir(dir, getuid()); err != nil {
			return nil, refuse(err)
		}
	default:
		return nil, refuse(err)
	}

	ctx, cancel := context.WithTimeout(ctx, StartTimeout)
	defer cancel()
	starts := 0
	var exited <-chan struct{}
	for {
		if c, err := connect(sock); err == nil || errors.Is(err, errForeign) {
			return c, err
		}
		if exited == nil {
			if starts == MaxStarts {
				return nil, earlyExit()
			}
			var err error
			if exited, err = tryStart(dir, sock); err != nil {
				return nil, err
			}
			if exited != nil {
				starts++
			}
		}
		select {
		case <-ctx.Done():
			return nil, ErrUnreachable
		case <-exited:
			// The daemon died before it listened, for example because an
			// exiting daemon still held the lock.
			exited = nil
			if starts == MaxStarts {
				return nil, earlyExit()
			}
			select {
			case <-ctx.Done():
				return nil, earlyExit()
			case <-time.After(startPause):
			}
		case <-time.After(pollEvery):
		}
	}
}

var errForeign = errors.New("the courier socket belongs to another user")

func refuse(err error) error {
	return fmt.Errorf("refusing the courier socket directory: %w", err)
}

// earlyExit is the error of a Dial whose daemon exited before it listened,
// with the last line of the daemon's log.
func earlyExit() error {
	line := ""
	if dir, err := daemon.StateDir(); err == nil {
		line = daemon.LastLogLine(dir)
	}
	if line == "" {
		return errors.New("the courier daemon exited before it listened")
	}
	return fmt.Errorf("the courier daemon exited before it listened: %s", line)
}

// connect connects to sock and checks that a process of this user listens.
func connect(sock string) (net.Conn, error) {
	c, err := net.DialTimeout("unix", sock, time.Second)
	if err != nil {
		return nil, err
	}
	uid, ok, err := daemon.PeerUID(c)
	if err != nil {
		c.Close()
		return nil, err
	}
	if ok && uid != getuid() {
		c.Close()
		return nil, fmt.Errorf("%w: uid %d", errForeign, uid)
	}
	return c, nil
}

// tryStart takes the daemon lock and starts a daemon that inherits it. It
// returns a nil channel when it started none: another process holds the lock
// because a daemon runs, starts or exits, or a daemon listens.
func tryStart(dir, sock string) (<-chan struct{}, error) {
	lock, err := daemon.OpenLock(dir)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err := daemon.TryLock(lock); err != nil {
		if errors.Is(err, daemon.ErrRunning) {
			return nil, nil
		}
		return nil, err
	}
	if c, err := connect(sock); err == nil {
		c.Close()
		return nil, nil
	}
	exe, err := executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(exe, "daemon")
	cmd.Args[0] = "agent-courier"
	cmd.Env = append(os.Environ(), daemon.LockFDEnv+"="+strconv.Itoa(daemon.LockFD))
	// ExtraFiles[0] is descriptor 3 in the daemon.
	cmd.ExtraFiles = []*os.File{lock}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Dir = "/"
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	exited := make(chan struct{})
	go func() {
		cmd.Wait()
		close(exited)
	}()
	return exited, nil
}
