//go:build unix

package client

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/CappyT/agent-courier/internal/daemon"
)

// A client in a test starts the test binary, which acts as a fake daemon: it
// records how it was started in the file that startsEnv names, fails the
// first failEnv starts with a line in daemon.log, and else execs the built
// agent-courier.
const (
	startsEnv  = "AC_TEST_STARTS"
	failEnv    = "AC_TEST_FAIL"
	realBinEnv = "AC_TEST_REAL_BIN"
)

var (
	buildOnce sync.Once
	binDir    string
	testBin   string
	buildErr  error
)

func init() { executable = testExecutable }

func testExecutable() (string, error) {
	buildOnce.Do(build)
	if buildErr != nil {
		return "", buildErr
	}
	return os.Executable()
}

func build() {
	binDir, buildErr = os.MkdirTemp("", "ac-bin")
	if buildErr != nil {
		return
	}
	testBin = filepath.Join(binDir, "agent-courier")
	out, err := exec.Command("go", "build", "-o", testBin, "github.com/CappyT/agent-courier/cmd/agent-courier").CombinedOutput()
	if err != nil {
		buildErr = errors.Join(err, fmt.Errorf("%s", out))
		return
	}
	// /proc/<pid>/exe holds the path without symlinks.
	if testBin, buildErr = filepath.EvalSymlinks(testBin); buildErr == nil {
		buildErr = os.Setenv(realBinEnv, testBin)
	}
}

func TestMain(m *testing.M) {
	// Never run the tests in a process that a client started as its daemon.
	if len(os.Args) == 2 && os.Args[1] == "daemon" {
		os.Exit(fakeDaemon())
	}
	code := m.Run()
	if binDir != "" {
		os.RemoveAll(binDir)
	}
	os.Exit(code)
}

func fakeDaemon() int {
	starts := os.Getenv(startsEnv)
	if starts == "" {
		return 2
	}
	f, err := os.OpenFile(starts, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return 2
	}
	fmt.Fprintln(f, startRecord())
	f.Close()
	b, _ := os.ReadFile(starts)
	n := strings.Count(string(b), "\n")
	if fail, _ := strconv.Atoi(os.Getenv(failEnv)); n <= fail {
		if state, err := daemon.StateDir(); err == nil {
			if log, err := daemon.OpenLog(state); err == nil {
				fmt.Fprintf(log, "level=ERROR msg=\"fake failure %d\"\n", n)
				log.Close()
			}
		}
		return 1
	}
	return execReal()
}

func execReal() int {
	err := syscall.Exec(os.Getenv(realBinEnv), []string{"agent-courier", "daemon"}, os.Environ())
	fmt.Fprintln(os.Stderr, err)
	return 2
}

// startRecord describes the start: the process group, the working
// directory, descriptor 3, the lock and stdio. It uses Fstat, because an
// os.File of descriptor 3 would close it when collected.
func startRecord() string {
	pid := os.Getpid()
	pgid, _ := syscall.Getpgid(0)
	cwd, _ := os.Getwd()
	dir := daemon.SocketDir()
	fd3 := "bad"
	if sameFile(daemon.LockFD, filepath.Join(dir, daemon.LockName)) {
		fd3 = "lock"
	}
	held := "no"
	if probe, err := daemon.OpenLock(dir); err == nil {
		if errors.Is(daemon.TryLock(probe), daemon.ErrRunning) {
			held = "yes"
		}
		probe.Close()
	}
	stdio := "null"
	for fd := range 3 {
		if !sameFile(fd, os.DevNull) {
			stdio = "other"
		}
	}
	return fmt.Sprintf("leader=%t cwd=%s fd3=%s held=%s stdio=%s", pgid == pid, cwd, fd3, held, stdio)
}

func sameFile(fd int, path string) bool {
	var got, want syscall.Stat_t
	return syscall.Fstat(fd, &got) == nil && syscall.Stat(path, &want) == nil &&
		got.Dev == want.Dev && got.Ino == want.Ino
}

// setupEnv points the socket directory and the store at temporary
// directories, and the fake daemon's records at a file. It returns the
// socket directory and the records file. A daemon that a failed test leaves
// behind exits by itself after the idle grace.
func setupEnv(t *testing.T) (string, string) {
	t.Helper()
	// A unix socket path has at most 108 bytes, so t.TempDir() can be too long.
	run, err := os.MkdirTemp("", "ac")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(run) })
	dir := filepath.Join(run, "agent-courier")
	starts := filepath.Join(t.TempDir(), "starts")
	t.Setenv(daemon.SocketDirEnv, dir)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Setenv(daemon.IdleGraceEnv, "10s")
	t.Setenv(startsEnv, starts)
	t.Setenv(failEnv, "0")
	return dir, starts
}

func readStarts(t *testing.T, starts string) []string {
	t.Helper()
	b, err := os.ReadFile(starts)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}
