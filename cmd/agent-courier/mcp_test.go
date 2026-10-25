//go:build unix

package main

import (
	"bytes"
	"encoding/json/v2"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/daemon"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// TestMCPLogsOffStdout runs the mcp command with a socket directory that
// fails the check: the cause goes to stderr, and stdout carries only MCP.
func TestMCPLogsOffStdout(t *testing.T) {
	run, err := os.MkdirTemp("", "acm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(run) })
	dir := filepath.Join(run, "agent-courier")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(daemon.SocketDirEnv, dir)
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	inR, inW := io.Pipe()
	var stdout, stderr lockedBuffer
	code := make(chan int, 1)
	go func() { code <- runMCP(inR, &stdout, &stderr) }()
	if _, err := inW.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"ping"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); !strings.Contains(stderr.String(), "refusing the courier socket directory") || !strings.Contains(stdout.String(), `"id":1`); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("stdout %q, stderr %q", stdout.String(), stderr.String())
		}
	}
	inW.Close()
	select {
	case c := <-code:
		if c != 0 {
			t.Errorf("runMCP = %d, stderr %q", c, stderr.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runMCP did not return after stdin closed")
	}
	for line := range strings.Lines(stdout.String()) {
		var m struct {
			JSONRPC string `json:"jsonrpc"`
		}
		if err := json.Unmarshal([]byte(line), &m); err != nil || m.JSONRPC != "2.0" {
			t.Errorf("stdout carries a line that is not JSON-RPC 2.0: %q", line)
		}
	}
}
