//go:build linux

package client

import (
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/daemon"
)

func TestPendingSurvivesDaemonRestart(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(t *testing.T, pid int, conns ...*Conn)
	}{
		{"idle exit", func(t *testing.T, pid int, conns ...*Conn) {
			for _, c := range conns {
				c.Close()
			}
			waitGone(t, pid)
		}},
		{"hard kill", func(t *testing.T, pid int, conns ...*Conn) {
			if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			waitGone(t, pid)
			for _, c := range conns {
				c.Close()
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupEnv(t)
			t.Setenv(daemon.IdleGraceEnv, "1s")
			if _, err := executable(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			a, oldPID := dialRegistered(t, ctx, nil, RegisterParams{Harness: "claude", Cwd: "/", Name: "alice", Topics: []string{"t"}})
			var oldGot deliveries
			b, bPID := dialRegistered(t, ctx, oldGot.add, RegisterParams{Harness: "claude", Cwd: "/", Name: "bob", Topics: []string{"t"}})
			if bPID != oldPID {
				t.Fatalf("a reached daemon %d, b reached daemon %d", oldPID, bPID)
			}
			var sent struct {
				ID         string   `json:"id"`
				Recipients []string `json:"recipients"`
			}
			if err := a.Call(ctx, "send", map[string]any{"topic": "t", "kind": "note", "summary": "survive the restart", "to": "bob"}, &sent); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(sent.Recipients, []string{"bob"}) {
				t.Fatalf("send recipients %v, want [bob]", sent.Recipients)
			}
			// The first push reaches the old session, which does not ack.
			oldGot.wait(t, ctx, sent.ID, 1)

			tc.stop(t, oldPID, a, b)

			var registered atomic.Bool
			var newGot deliveries
			var sp atomic.Pointer[Session]
			s := Start(Config{
				Params: RegisterParams{Harness: "claude", Cwd: "/", Name: "bob", Topics: []string{"t"}},
				OnDeliver: func(d Deliver) {
					if s := sp.Load(); s != nil {
						done, cancel := context.WithCancel(context.Background())
						cancel()
						if _, err := s.Registration(done); err == nil {
							registered.Store(true)
						}
					}
					newGot.add(d)
				},
			})
			sp.Store(s)
			var newPID int
			t.Cleanup(func() {
				s.Close()
				if newPID != 0 {
					stopDaemon(t, newPID)
				}
			})
			reg, err := s.Registration(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if newPID = s.peer(); newPID == 0 || newPID == oldPID {
				t.Fatalf("new session reached daemon %d, old daemon %d", newPID, oldPID)
			}
			if reg.Name != "bob" {
				t.Fatalf("new session registered as %q, want bob", reg.Name)
			}

			newGot.wait(t, ctx, sent.ID, 1)
			if !registered.Load() {
				t.Fatal("the pending message arrived before the register response")
			}
			var pending struct {
				Text  string `json:"text"`
				Total int    `json:"total"`
			}
			if err := s.Call(ctx, "pending", nil, &pending); err != nil {
				t.Fatal(err)
			}
			if pending.Total != 1 || !strings.Contains(pending.Text, sent.ID) {
				t.Fatalf("pending before ack: total %d, text %q, want %s", pending.Total, pending.Text, sent.ID)
			}
			// A second push would arrive in this time.
			time.Sleep(300 * time.Millisecond)
			if n := newGot.count(sent.ID); n != 1 {
				t.Fatalf("the new session got the message %d times, want once", n)
			}
			if err := s.Call(ctx, "ack", map[string]string{"id": sent.ID}, nil); err != nil {
				t.Fatal(err)
			}
			if err := s.Call(ctx, "pending", nil, &pending); err != nil {
				t.Fatal(err)
			}
			if pending.Total != 0 {
				t.Fatalf("pending after ack: total %d, text %q", pending.Total, pending.Text)
			}
		})
	}
}

// dialRegistered connects with autostart and registers. It stops the daemon
// it reached when the test ends.
func dialRegistered(t *testing.T, ctx context.Context, onDeliver func(Deliver), p RegisterParams) (*Conn, int) {
	t.Helper()
	c, err := Dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pid := peerPID(t, c)
	killDaemon(t, pid)
	cn := NewConn(c, onDeliver)
	t.Cleanup(func() { cn.Close() })
	reg, err := cn.Register(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if reg.Name != p.Name {
		t.Fatalf("registered as %q, want %q", reg.Name, p.Name)
	}
	return cn, pid
}

func waitGone(t *testing.T, pid int) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); slices.Contains(daemons(t), pid); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("daemon %d still runs", pid)
		}
	}
}

// deliveries records the ids of the deliver notifications of a session.
type deliveries struct {
	mu  sync.Mutex
	ids []string
}

func (d *deliveries) add(m Deliver) {
	d.mu.Lock()
	d.ids = append(d.ids, m.Attrs["id"])
	d.mu.Unlock()
}

func (d *deliveries) count(id string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, got := range d.ids {
		if got == id {
			n++
		}
	}
	return n
}

func (d *deliveries) wait(t *testing.T, ctx context.Context, id string, n int) {
	t.Helper()
	for d.count(id) < n {
		if ctx.Err() != nil {
			t.Fatalf("got %d deliveries of %s, want %d", d.count(id), id, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
