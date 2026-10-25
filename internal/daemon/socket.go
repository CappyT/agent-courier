//go:build unix

package daemon

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
)

const (
	// SocketName is the file name of the socket in the socket directory.
	SocketName = "daemon.sock"
	// LockName is the file name of the lock in the socket directory. The
	// daemon holds it from before it listens until after it has closed the
	// store.
	LockName = "daemon.lock"
	// LockFDEnv is set to LockFD in a daemon that a client started.
	LockFDEnv = "AGENT_COURIER_LOCK_FD"
	// LockFD is the descriptor of the lock file that a daemon inherits from
	// the client that started it.
	LockFD = 3
	// SocketDirEnv replaces the socket directory with an absolute path. Tests
	// set it, so they never meet the user's daemon.
	SocketDirEnv = "AGENT_COURIER_SOCKET_DIR"
)

// getuid and fileUID are variables so tests can make a peer or a file look
// like another user's.
var (
	getuid  = os.Getuid
	fileUID = func(fi fs.FileInfo) int {
		if st, ok := fi.Sys().(*syscall.Stat_t); ok {
			return int(st.Uid)
		}
		return -1
	}
)

func owned(fi fs.FileInfo) bool { return fileUID(fi) == getuid() }

// runUser and tmpRoot are variables so tests can move the roots of SocketDir.
var (
	runUser = "/run/user"
	tmpRoot = "/tmp"
)

// SocketDir depends only on the uid, so every process of the user finds the
// same daemon whatever its environment. On Linux it is
// /run/user/<uid>/agent-courier when /run/user/<uid> is a directory of the
// user, else /tmp/agent-courier-<uid>. Elsewhere it is agent-courier-<uid> in
// the temporary directory of the user. SocketDirEnv replaces it.
func SocketDir() string {
	if dir := os.Getenv(SocketDirEnv); filepath.IsAbs(dir) {
		return dir
	}
	uid := strconv.Itoa(getuid())
	if runtime.GOOS != "linux" {
		return filepath.Join(os.TempDir(), "agent-courier-"+uid)
	}
	run := filepath.Join(runUser, uid)
	if fi, err := os.Lstat(run); err == nil && fi.IsDir() && owned(fi) {
		return filepath.Join(run, "agent-courier")
	}
	return filepath.Join(tmpRoot, "agent-courier-"+uid)
}

// ErrRunning means that another daemon holds the lock of the socket directory.
var ErrRunning = errors.New("another daemon runs for this user")

// Listener is the daemon's socket. It holds the lock of the socket directory
// until Unlock.
type Listener struct {
	*net.UnixListener
	path   string
	lock   *os.File
	close  sync.Once
	unlock sync.Once
	err    error
}

// Close removes the socket and closes it, once. It keeps the lock. Only the
// lock holder removes the socket, so neither a daemon that lost the start nor
// a second Close removes the socket of another daemon.
func (l *Listener) Close() error {
	l.close.Do(func() {
		os.Remove(l.path)
		l.err = l.UnixListener.Close()
	})
	return l.err
}

// Unlock closes the listener when it is open and releases the lock, once.
// The daemon calls it last, after it has closed the store.
func (l *Listener) Unlock() {
	l.Close()
	l.unlock.Do(func() { l.lock.Close() })
}

// Listen makes dir a private directory of the user, takes the daemon lock in
// it, and listens on the socket in it. It returns ErrRunning when another
// daemon holds the lock.
func Listen(dir string) (*Listener, error) {
	if err := PrivateDir(dir); err != nil {
		return nil, err
	}
	lock, err := OpenLock(dir)
	if err != nil {
		return nil, err
	}
	return listenLocked(dir, lock)
}

// ListenLocked is Listen with a lock file that the daemon inherited from the
// client that started it. The client took the lock on that open file, so the
// lock passes to the daemon without a moment when it is free. lock must be the
// lock file of dir.
func ListenLocked(dir string, lock *os.File) (*Listener, error) {
	if err := PrivateDir(dir); err != nil {
		lock.Close()
		return nil, err
	}
	got, err := lock.Stat()
	if err != nil {
		lock.Close()
		return nil, err
	}
	want, err := os.Lstat(filepath.Join(dir, LockName))
	if err != nil || !os.SameFile(got, want) {
		lock.Close()
		return nil, fmt.Errorf("the inherited lock is not %s", filepath.Join(dir, LockName))
	}
	return listenLocked(dir, lock)
}

// CheckDir checks that dir is a real directory of uid with mode 0700,
// as PrivateDir leaves it. A client checks it before it connects, because
// another user could create the directory first and listen as a fake daemon.
// A missing dir gives an error that wraps fs.ErrNotExist.
func CheckDir(dir string, uid int) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	switch {
	case !fi.IsDir():
		return fmt.Errorf("%s is not a directory", dir)
	case fileUID(fi) != uid:
		return fmt.Errorf("%s belongs to another user", dir)
	case fi.Mode().Perm() != 0o700:
		return fmt.Errorf("%s has mode %v, not 0700", dir, fi.Mode().Perm())
	}
	return nil
}

// OpenLock opens the lock file of dir, which must exist.
func OpenLock(dir string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, LockName), os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
}

// TryLock takes the exclusive lock on f without waiting. It returns
// ErrRunning when another open file holds it. It succeeds on a file that
// already holds the lock.
func TryLock(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return ErrRunning
	}
	return err
}

func listenLocked(dir string, lock *os.File) (*Listener, error) {
	if err := TryLock(lock); err != nil {
		lock.Close()
		return nil, err
	}
	path := filepath.Join(dir, SocketName)
	ln, err := listenUnix(path)
	if err != nil {
		lock.Close()
		return nil, err
	}
	return &Listener{UnixListener: ln, path: path, lock: lock}, nil
}

func listenUnix(path string) (*net.UnixListener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	ln.SetUnlinkOnClose(false)
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		os.Remove(path)
		return nil, err
	}
	return ln, nil
}

// PrivateDir creates dir with mode 0700, or checks that an existing dir is a
// real directory of this user and sets its mode to 0700. In a shared temporary
// directory another user could create the directory first.
func PrivateDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	if !owned(fi) {
		return fmt.Errorf("%s belongs to another user", dir)
	}
	if fi.Mode().Perm() != 0o700 {
		return os.Chmod(dir, 0o700)
	}
	return nil
}
