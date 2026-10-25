//go:build unix

package client

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// TestSessionLogsConnectCause checks that a failed connect reaches the
// configured logger with its cause, once while the cause stays the same.
func TestSessionLogsConnectCause(t *testing.T) {
	dir, _ := setupEnv(t)
	// A socket directory that other users can read fails the check.
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var buf logBuffer
	s := Start(Config{Params: RegisterParams{Harness: "claude", Cwd: "/"}, Log: slog.New(slog.NewTextHandler(&buf, nil))})
	t.Cleanup(func() { s.Close() })
	for deadline := time.Now().Add(5 * time.Second); !strings.Contains(buf.String(), "refusing the courier socket directory"); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("no connect cause in the log: %q", buf.String())
		}
	}
	// The backoff retries several times in a second; the cause is logged once.
	time.Sleep(time.Second)
	if n := strings.Count(buf.String(), "cannot register with the courier daemon"); n != 1 {
		t.Errorf("%d log lines for one cause, want 1: %q", n, buf.String())
	}
}
