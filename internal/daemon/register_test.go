//go:build unix

package daemon

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/version"
)

func TestRegisterIdentity(t *testing.T) {
	d := startDaemon(t)
	cwd := clone(t, t.TempDir(), "agent-courier", originConfig)
	claude := registerParams{Harness: "claude", Cwd: cwd}
	top := t

	t.Run("default name", func(t *testing.T) {
		res := d.dialFor(top, t).mustRegister(claude)
		if res.Name != "claude-agent-courier" {
			t.Errorf("name %q, want claude-agent-courier", res.Name)
		}
		if _, err := message.ParseID(res.Session); err != nil {
			t.Errorf("session %q is not a ULID: %v", res.Session, err)
		}
		if res.Version != version.Version {
			t.Errorf("version %q, want %q", res.Version, version.Version)
		}
		if want := protocolFor("claude-agent-courier", []string{"cappyt_agent-courier"}); res.Protocol != want {
			t.Errorf("protocol %q, want %q", res.Protocol, want)
		}
		if res := d.dialFor(top, t).mustRegister(registerParams{Harness: "opencode", Cwd: cwd}); res.Name != "opencode-agent-courier" {
			t.Errorf("opencode name %q, want opencode-agent-courier", res.Name)
		}
	})

	t.Run("name taken by a live session", func(t *testing.T) {
		first := d.dial(t)
		first.mustRegister(registerParams{Harness: "claude", Cwd: cwd, Name: "taken"})
		for _, want := range []string{"taken-2", "taken-3"} {
			if res := d.dial(t).mustRegister(registerParams{Harness: "claude", Cwd: cwd, Name: "taken"}); res.Name != want {
				t.Errorf("name %q, want %q", res.Name, want)
			}
		}
		if res := d.dial(t).mustRegister(claude); res.Name != "claude-agent-courier-2" {
			t.Errorf("default name %q, want claude-agent-courier-2", res.Name)
		}

		first.conn.Close()
		waitFree(t, d.srv, "taken")
		if res := d.dial(t).mustRegister(registerParams{Harness: "claude", Cwd: cwd, Name: "taken"}); res.Name != "taken" {
			t.Errorf("name after the holder left %q, want taken", res.Name)
		}
	})

	t.Run("suffix keeps the length limit", func(t *testing.T) {
		long := strings.Repeat("a", 46) + "-b"
		d.dial(t).mustRegister(registerParams{Harness: "claude", Cwd: cwd, Name: long})
		res := d.dial(t).mustRegister(registerParams{Harness: "claude", Cwd: cwd, Name: long})
		if want := strings.Repeat("a", 46) + "-2"; res.Name != want {
			t.Errorf("name %q, want %q", res.Name, want)
		}
	})

	t.Run("reserved name", func(t *testing.T) {
		_, rerr := d.dial(t).register(registerParams{Harness: "claude", Cwd: cwd, Name: message.Everyone})
		wantError(t, rerr, codeInvalidParams, "must not be everyone")
	})

	t.Run("invalid name", func(t *testing.T) {
		_, rerr := d.dial(t).register(registerParams{Harness: "claude", Cwd: cwd, Name: "</channel>"})
		wantError(t, rerr, codeInvalidParams, "an agent name must match ^[a-z0-9][a-z0-9-]{0,47}$")
		if strings.Contains(rerr.Message, "channel") {
			t.Errorf("error %q quotes the value", rerr.Message)
		}
	})
}

// Sessions that ask for one name at the same moment get distinct names.
func TestRegisterConcurrent(t *testing.T) {
	d := startDaemon(t)
	const n = 20
	line := `{"jsonrpc":"2.0","id":1,"method":"register","params":{"harness":"claude","version":"` + version.Version +
		`","cwd":"/","name":"same","topics":["t"]}}` + "\n"
	names := make(chan string, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range n {
		c, err := net.Dial("unix", d.sock)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		wg.Go(func() {
			<-start
			c.SetDeadline(time.Now().Add(10 * time.Second))
			if _, err := c.Write([]byte(line)); err != nil {
				t.Error(err)
				return
			}
			b, err := bufio.NewReader(c).ReadBytes('\n')
			if err != nil {
				t.Error(err)
				return
			}
			var r struct {
				Result registerResult `json:"result"`
			}
			if err := json.Unmarshal(b, &r); err != nil {
				t.Error(err)
				return
			}
			names <- r.Result.Name
		})
	}
	close(start)
	wg.Wait()
	close(names)
	var got []string
	for name := range names {
		got = append(got, name)
	}
	slices.Sort(got)
	want := []string{"same"}
	for i := 2; i <= n; i++ {
		want = append(want, "same-"+strconv.Itoa(i))
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("names %q, want %q", got, want)
	}
}

func TestRegisterTopics(t *testing.T) {
	d := startDaemon(t)
	dir := t.TempDir()
	cwd := clone(t, dir, "agent-courier", originConfig)
	ctx := context.Background()
	top := t

	t.Run("default topic", func(t *testing.T) {
		res := d.dialFor(top, t).mustRegister(registerParams{Harness: "claude", Cwd: cwd})
		if !slices.Equal(res.Topics, []string{"cappyt_agent-courier"}) {
			t.Errorf("topics %q, want [cappyt_agent-courier]", res.Topics)
		}
		agents, err := d.store.Agents(ctx, "cappyt_agent-courier")
		if err != nil {
			t.Fatal(err)
		}
		if len(agents) != 1 || agents[0].Name != res.Name {
			t.Errorf("store agents %+v, want %s", agents, res.Name)
		}
	})

	t.Run("fork", func(t *testing.T) {
		res := d.dialFor(top, t).mustRegister(registerParams{Harness: "opencode", Cwd: clone(t, dir, "my-fork", forkConfig)})
		if !slices.Equal(res.Topics, []string{"cappyt_agent-courier"}) || res.Name != "opencode-agent-courier" {
			t.Errorf("topics %q, name %q; want [cappyt_agent-courier], opencode-agent-courier", res.Topics, res.Name)
		}
		// The second agent on the topic stores an online message for the first.
		pending, _, err := d.store.Pending(ctx, "claude-agent-courier", 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) != 1 || pending[0].Kind != message.KindOnline || pending[0].Sender != "opencode-agent-courier" {
			t.Errorf("pending of claude-agent-courier: %+v, want one online from opencode-agent-courier", pending)
		}
	})

	t.Run("invalid topic", func(t *testing.T) {
		_, rerr := d.dial(t).register(registerParams{Harness: "claude", Cwd: cwd, Topics: []string{"ok", "../etc"}})
		wantError(t, rerr, codeInvalidParams, "a topic must match ^[a-z0-9][a-z0-9_-]{0,63}$")
		if strings.Contains(rerr.Message, "etc") {
			t.Errorf("error %q quotes the value", rerr.Message)
		}
	})

	t.Run("given topics", func(t *testing.T) {
		res := d.dial(t).mustRegister(registerParams{Harness: "claude", Cwd: cwd, Topics: []string{"b-topic", "a_topic", "b-topic"}})
		if !slices.Equal(res.Topics, []string{"a_topic", "b-topic"}) {
			t.Errorf("topics %q, want [a_topic b-topic]", res.Topics)
		}
		if res.Name != "claude-agent-courier-2" {
			t.Errorf("name %q, want claude-agent-courier-2", res.Name)
		}
		if !strings.Contains(res.Protocol, "Your topics are a_topic, b-topic.") {
			t.Errorf("protocol does not name the topics: %q", res.Protocol)
		}
	})

	topics := func(n int) []string {
		var ts []string
		for i := range n {
			ts = append(ts, "t"+strconv.Itoa(i))
		}
		return ts
	}

	t.Run("16 different topics and a repeated one", func(t *testing.T) {
		res := d.dial(t).mustRegister(registerParams{Harness: "claude", Cwd: cwd, Topics: append(topics(maxTopics), "t0")})
		if len(res.Topics) != maxTopics {
			t.Errorf("%d topics, want %d", len(res.Topics), maxTopics)
		}
	})

	t.Run("17 different topics", func(t *testing.T) {
		_, rerr := d.dial(t).register(registerParams{Harness: "claude", Cwd: cwd, Topics: topics(maxTopics + 1)})
		wantError(t, rerr, codeInvalidParams, "at most 16 different topics")
	})

	t.Run("no topic from cwd", func(t *testing.T) {
		_, rerr := d.dial(t).register(registerParams{Harness: "claude", Cwd: "/"})
		wantError(t, rerr, codeInvalidParams, "no topic can be derived from cwd")
		if res := d.dial(t).mustRegister(registerParams{Harness: "claude", Cwd: "/", Topics: []string{"t"}}); res.Name != "claude" {
			t.Errorf("name %q, want claude", res.Name)
		}
	})

	// A named pipe as one of the git files must not block the registration:
	// it counts as missing.
	for _, tc := range []struct{ name, fifo, gitFile string }{
		{"named pipe as .git", ".git", ""},
		{"named pipe as config", ".git/config", ""},
		{"named pipe as commondir", "gd/commondir", "gitdir: gd\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := filepath.Join(t.TempDir(), "piped")
			if err := os.MkdirAll(filepath.Dir(filepath.Join(repo, tc.fifo)), 0o755); err != nil {
				t.Fatal(err)
			}
			if tc.gitFile != "" {
				writeFile(t, filepath.Join(repo, ".git"), tc.gitFile)
			}
			if err := syscall.Mkfifo(filepath.Join(repo, tc.fifo), 0o600); err != nil {
				t.Fatal(err)
			}
			res := d.dial(t).mustRegister(registerParams{Harness: "claude", Cwd: repo, Name: "piped"})
			if !slices.Equal(res.Topics, []string{"piped"}) {
				t.Errorf("topics %q, want [piped]", res.Topics)
			}
		})
	}
}

// A registration that fails on a later topic leaves no state behind.
func TestRegisterFailureLeavesNoState(t *testing.T) {
	d := startDaemon(t)
	ctx := context.Background()
	d.dial(t).mustRegister(registerParams{Harness: "claude", Cwd: "/", Name: "watcher", Topics: []string{"a", "b"}})
	db := d.sqlDB(t)
	if _, err := db.Exec(`CREATE TRIGGER fail BEFORE INSERT ON agents WHEN NEW.topic = 'b' AND NEW.name = 'x'
		BEGIN SELECT RAISE(ABORT, 'injected'); END`); err != nil {
		t.Fatal(err)
	}

	_, rerr := d.dial(t).register(registerParams{Harness: "claude", Cwd: "/", Name: "x", Topics: []string{"a", "b"}})
	wantError(t, rerr, codeInternal, "internal error")
	d.srv.mu.Lock()
	held := d.srv.names["x"] != nil
	d.srv.mu.Unlock()
	if held {
		t.Error("the failed registration holds the name")
	}
	for _, topic := range []string{"a", "b"} {
		agents, err := d.store.Agents(ctx, topic)
		if err != nil {
			t.Fatal(err)
		}
		if len(agents) != 1 {
			t.Errorf("agents on %s: %+v, want only watcher", topic, agents)
		}
	}
	if pending, _, _ := d.store.Pending(ctx, "watcher", 0); len(pending) != 0 {
		t.Errorf("watcher holds %d messages, want no online", len(pending))
	}

	if _, err := db.Exec(`DROP TRIGGER fail`); err != nil {
		t.Fatal(err)
	}
	if res := d.dial(t).mustRegister(registerParams{Harness: "claude", Cwd: "/", Name: "x", Topics: []string{"a", "b"}}); res.Name != "x" {
		t.Errorf("name after the failure %q, want x", res.Name)
	}
}
