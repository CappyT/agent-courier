//go:build unix

package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/store"
)

// startIdleDaemon starts a daemon that exits after grace with no client. The
// channel receives the result of Serve.
func startIdleDaemon(t *testing.T, grace time.Duration) (*testDaemon, <-chan error) {
	t.Helper()
	dir := shortTempDir(t)
	st, err := store.Open(filepath.Join(t.TempDir(), "courier.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ln, err := Listen(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatal(err)
	}
	srv := New(st, testLogger(t))
	srv.ExitWhenIdle(grace)
	srv.SetNetwork(&fakeNet{self: testSelf, set: srv.Teams()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		done <- srv.Serve(ctx, ln)
		close(exited)
	}()
	t.Cleanup(func() {
		cancel()
		<-exited
	})
	return &testDaemon{sock: filepath.Join(dir, "run", SocketName), store: st, srv: srv}, done
}

func running(done <-chan error) bool {
	select {
	case <-done:
		return false
	default:
		return true
	}
}

func waitExit(t *testing.T, done <-chan error, within time.Duration) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(within):
		t.Fatalf("the daemon still runs %v after the grace period started", within)
	}
}

func TestIdleExitLastSessionCloses(t *testing.T) {
	const grace = 300 * time.Millisecond
	d, done := startIdleDaemon(t, grace)
	c := d.dial(t)
	c.mustRegister(registerParams{Harness: "claude", Cwd: "/", Topics: []string{"t"}})
	time.Sleep(2 * grace)
	if !running(done) {
		t.Fatal("the daemon exited while a client was connected")
	}
	c.conn.Close()
	closed := time.Now()
	time.Sleep(grace / 2)
	if !running(done) {
		t.Fatal("the daemon exited before the grace period ended")
	}
	waitExit(t, done, 5*time.Second)
	if since := time.Since(closed); since < grace {
		t.Errorf("the daemon exited %v after the last session closed, before the grace of %v", since, grace)
	}
}

func TestIdleExitSessionRestartsWithinGrace(t *testing.T) {
	const grace = 400 * time.Millisecond
	d, done := startIdleDaemon(t, grace)
	first := d.dial(t)
	first.mustRegister(registerParams{Harness: "claude", Cwd: "/", Topics: []string{"t"}})
	first.conn.Close()
	waitFree(t, d.srv, "claude")

	time.Sleep(grace / 2)
	second := d.dial(t)
	if got := second.mustRegister(registerParams{Harness: "claude", Cwd: "/", Topics: []string{"t"}}); got.Name != "claude" {
		t.Fatalf("name %q, want claude", got.Name)
	}
	time.Sleep(2 * grace)
	if !running(done) {
		t.Fatal("the daemon exited although a client connected within the grace period")
	}
	second.line(`{"jsonrpc":"2.0","id":"after","method":"register","params":{}}` + "\n")
	if r := second.read(); r.Error == nil || r.Error.Code != codeAlreadyRegister {
		t.Fatalf("the daemon does not serve the new client: %+v", r)
	}

	second.conn.Close()
	waitExit(t, done, 5*time.Second)
}

func TestIdleExitWithoutAnyClient(t *testing.T) {
	_, done := startIdleDaemon(t, 100*time.Millisecond)
	waitExit(t, done, 5*time.Second)
}

func TestVersionMismatchNamesTheIdleExit(t *testing.T) {
	d, _ := startIdleDaemon(t, time.Minute)
	_, rerr := d.dial(t).register(registerParams{Harness: "claude", Version: "9.9.9", Cwd: "/"})
	wantError(t, rerr, codeVersionMismatch, "the client is 9.9.9, the daemon is ")
	wantError(t, rerr, codeVersionMismatch, "the daemon exits 60 seconds after its last session closes")

	// In a team, the daemon does not exit on idle: the message says so.
	c := d.dial(t)
	decodeTeam(t, c.call("team.create", map[string]any{"team": "courier", "topics": []string{"t"}, "label": "alpha"}))
	_, rerr = c.register(registerParams{Harness: "claude", Version: "9.9.9", Cwd: "/"})
	wantError(t, rerr, codeVersionMismatch, "the daemon is in a team, so it runs until agent-courier team leave ends its last team")
}

// Scenario "Member of a team" of "Idle exit": the daemon keeps running
// without clients while the machine is in a team, and the grace starts
// when it leaves its last team.
func TestIdleExitHeldByTeam(t *testing.T) {
	const grace = 300 * time.Millisecond
	d, done := startIdleDaemon(t, grace)
	c := d.dial(t)
	for _, name := range []string{"courier", "other"} {
		decodeTeam(t, c.call("team.create", map[string]any{"team": name, "topics": []string{name}, "label": "alpha"}))
	}
	c.conn.Close()
	time.Sleep(3 * grace)
	if !running(done) {
		t.Fatal("the daemon exited on idle while the machine was in a team")
	}

	c = d.dial(t)
	if r := c.call("team.leave", map[string]any{"team": "courier"}); r.Error != nil {
		t.Fatal(r.Error)
	}
	c.conn.Close()
	time.Sleep(3 * grace)
	if !running(done) {
		t.Fatal("the daemon exited on idle while the machine was in one more team")
	}

	// The last team ends while no client is connected: the leave runs on a
	// connection that closes before the grace, which starts at the close.
	c = d.dial(t)
	if r := c.call("team.leave", map[string]any{"team": "other"}); r.Error != nil {
		t.Fatal(r.Error)
	}
	c.conn.Close()
	closed := time.Now()
	waitExit(t, done, 5*time.Second)
	if since := time.Since(closed); since < grace {
		t.Errorf("the daemon exited %v after the last team ended, before the grace of %v", since, grace)
	}

	// The hold alone also starts the grace: a team that ends with no
	// connection open arms the timer.
	d2, done2 := startIdleDaemon(t, grace)
	set := d2.srv.Teams()
	if _, err := set.Create(testSelf, "courier", []string{"t"}, "alpha"); err != nil {
		t.Fatal(err)
	}
	d2.srv.teamsChanged()
	time.Sleep(3 * grace)
	if !running(done2) {
		t.Fatal("the daemon exited on idle while the machine was in a team")
	}
	if _, err := set.Leave("courier"); err != nil {
		t.Fatal(err)
	}
	d2.srv.teamsChanged()
	waitExit(t, done2, 5*time.Second)
}
