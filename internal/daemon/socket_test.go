//go:build unix

package daemon

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestListenLocked(t *testing.T) {
	dir := filepath.Join(shortTempDir(t), "run")
	if err := PrivateDir(dir); err != nil {
		t.Fatal(err)
	}
	lock, err := OpenLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := TryLock(lock); err != nil {
		t.Fatal(err)
	}
	other, err := OpenLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := TryLock(other); !errors.Is(err, ErrRunning) {
		t.Fatalf("TryLock on a second open file: %v, want ErrRunning", err)
	}
	other.Close()

	ln, err := ListenLocked(dir, lock)
	if err != nil {
		t.Fatalf("ListenLocked with the held lock: %v", err)
	}
	if _, err := Listen(dir); !errors.Is(err, ErrRunning) {
		t.Errorf("Listen beside the inherited lock: %v, want ErrRunning", err)
	}
	ln.Unlock()
	next, err := Listen(dir)
	if err != nil {
		t.Fatalf("Listen after Unlock: %v", err)
	}
	next.Unlock()

	wrong, err := os.CreateTemp(t.TempDir(), "lock")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ListenLocked(dir, wrong); err == nil {
		t.Error("ListenLocked accepted a file that is not the lock of the directory")
	}
}

func TestSocketDir(t *testing.T) {
	t.Setenv(SocketDirEnv, "")
	uid := strconv.Itoa(os.Getuid())
	if runtime.GOOS != "linux" {
		if got, want := SocketDir(), filepath.Join(os.TempDir(), "agent-courier-"+uid); got != want {
			t.Errorf("SocketDir = %q, want %q", got, want)
		}
		return
	}
	base := shortTempDir(t)
	oldRun, oldTmp := runUser, tmpRoot
	t.Cleanup(func() { runUser, tmpRoot = oldRun, oldTmp })
	runUser, tmpRoot = filepath.Join(base, "run", "user"), filepath.Join(base, "tmp")
	fallback := filepath.Join(tmpRoot, "agent-courier-"+uid)
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(base, "xdg"))
	t.Setenv("TMPDIR", filepath.Join(base, "other"))

	if got := SocketDir(); got != fallback {
		t.Errorf("without /run/user/<uid>: SocketDir = %q, want %q", got, fallback)
	}
	run := filepath.Join(runUser, uid)
	if err := os.MkdirAll(run, 0o700); err != nil {
		t.Fatal(err)
	}
	if got, want := SocketDir(), filepath.Join(run, "agent-courier"); got != want {
		t.Errorf("SocketDir = %q, want %q", got, want)
	}
	foreignFiles(t, uid)
	if got := SocketDir(); got != fallback {
		t.Errorf("with /run/user/<uid> of another user: SocketDir = %q, want %q", got, fallback)
	}

	t.Setenv(SocketDirEnv, "/x/y")
	if got := SocketDir(); got != "/x/y" {
		t.Errorf("%s=/x/y: SocketDir = %q", SocketDirEnv, got)
	}
	t.Setenv(SocketDirEnv, "relative")
	if got := SocketDir(); got != fallback {
		t.Errorf("%s=relative: SocketDir = %q, want %q", SocketDirEnv, got, fallback)
	}
}

func TestCheckDir(t *testing.T) {
	base := shortTempDir(t)
	dir := filepath.Join(base, "run")
	if err := CheckDir(dir, os.Getuid()); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("missing dir: %v, want fs.ErrNotExist", err)
	}
	if err := PrivateDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := CheckDir(dir, os.Getuid()); err != nil {
		t.Errorf("private dir: %v", err)
	}
	if err := CheckDir(dir, os.Getuid()+1); err == nil || !strings.Contains(err.Error(), "another user") {
		t.Errorf("dir of another user: %v", err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	if err := CheckDir(link, os.Getuid()); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Errorf("symlinked dir: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := CheckDir(dir, os.Getuid()); err == nil || !strings.Contains(err.Error(), "0700") {
		t.Errorf("dir with mode 0755: %v", err)
	}
}
