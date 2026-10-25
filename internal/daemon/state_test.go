//go:build unix

package daemon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStateDir(t *testing.T) {
	t.Setenv("HOME", "/home/u")
	for env, want := range map[string]string{
		"/state":   "/state/agent-courier",
		"":         "/home/u/.local/state/agent-courier",
		"relative": "/home/u/.local/state/agent-courier",
	} {
		t.Setenv("XDG_STATE_HOME", env)
		if got, err := StateDir(); err != nil || got != want {
			t.Errorf("XDG_STATE_HOME=%q: StateDir = %q, %v; want %q", env, got, err, want)
		}
	}
}

func TestOpenLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	path := filepath.Join(dir, LogName)
	f, err := OpenLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("first\n")
	f.Close()
	if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("log: %v, want mode 0600", err)
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("state dir: %v, want mode 0700", err)
	}

	f, err = OpenLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(strings.Repeat("x", MaxLog) + "\nlast line\n\n")
	f.Close()
	if got := LastLogLine(dir); got != "last line" {
		t.Errorf("LastLogLine = %q", got)
	}

	f, err = OpenLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if fi, err := os.Stat(path); err != nil || fi.Size() != 0 {
		t.Errorf("log after rotation: %v, size %d, want empty", err, fi.Size())
	}
	old, err := os.ReadFile(path + ".1")
	if err != nil || !strings.HasPrefix(string(old), "first\n") {
		t.Errorf("rotated log: %v", err)
	}
	if got := LastLogLine(dir); got != "" {
		t.Errorf("LastLogLine of an empty log = %q", got)
	}

	os.WriteFile(path, []byte(strings.Repeat("y", 2*maxLastLine)), 0o600)
	if got := LastLogLine(dir); len(got) != maxLastLine {
		t.Errorf("LastLogLine has %d bytes, want %d", len(got), maxLastLine)
	}
	if got := LastLogLine(t.TempDir()); got != "" {
		t.Errorf("LastLogLine without a log = %q", got)
	}
}
