//go:build unix

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/daemon"
	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/team"
)

const cliSelf = "12D3KooWcli"

// cliNet is the p2p side of the daemon in the CLI tests.
type cliNet struct {
	mu      sync.Mutex
	label   string
	joinErr error
}

func (n *cliNet) Self() (string, error) { return cliSelf, nil }

func (n *cliNet) InviteCode(name string, secret []byte, expires time.Time) (string, error) {
	return "abcdefgh234567", nil
}

func (n *cliNet) Join(_ context.Context, code, label string) (team.Team, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.label = label
	if n.joinErr != nil {
		return team.Team{}, n.joinErr
	}
	return team.Team{Name: "courier", Topics: []string{"t"}, Members: team.Members{
		"12D3KooWinviter": {PeerID: "12D3KooWinviter", Label: "alpha", Join: 1},
		cliSelf:           {PeerID: cliSelf, Label: label, Join: 1},
	}}, nil
}

func (n *cliNet) TeamChanged(string) {}

func (n *cliNet) Left(team.Team) {}

func (n *cliNet) lastLabel() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.label
}

// startTeamTestDaemon runs a daemon with cliNet as its p2p side in this
// process, on a temporary socket directory and store, and returns its
// member set.
func startTeamTestDaemon(t *testing.T, net *cliNet) *team.Set {
	t.Helper()
	dir := filepath.Join(shortTempDir(t), "agent-courier")
	t.Setenv(daemon.SocketDirEnv, dir)
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	st, err := store.Open(filepath.Join(t.TempDir(), "courier.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := daemon.Listen(dir)
	if err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	srv := daemon.New(st, slog.New(slog.NewTextHandler(&logs, nil)))
	srv.SetNetwork(net)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v\n%s", err, logs.String())
		}
		ln.Unlock()
		st.Close()
	})
	return srv.Teams()
}

func runCLI(t *testing.T, wantCode int, args ...string) string {
	t.Helper()
	var out, errOut bytes.Buffer
	if code := run(args, &out, &errOut); code != wantCode {
		t.Fatalf("run %q = %d, want %d; stdout %q, stderr %q", args, code, wantCode, out.String(), errOut.String())
	}
	return out.String() + errOut.String()
}

func wantIn(t *testing.T, got string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(got, p) {
			t.Errorf("output %q does not contain %q", got, p)
		}
	}
}

func TestTeamCLI(t *testing.T) {
	set := startTeamTestDaemon(t, &cliNet{})

	wantIn(t, runCLI(t, 0, "team", "list"), "belongs to no team")
	wantIn(t, runCLI(t, 0, "team", "create", "courier", "--topic", "cappyt_agent-courier", "--label", "alpha"),
		"courier", "topics: cappyt_agent-courier", "alpha  "+cliSelf)
	wantIn(t, runCLI(t, 1, "team", "create", "--label", "alpha", "courier", "--topic", "other"), "exists already")

	// 13 members, joined with invites of this machine: the list of every
	// team shows 10, the list of one team all.
	join := func(id, label string) {
		secret, _, err := set.CreateInvite("courier")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := set.AcceptInvite("courier", secret, id, label); err != nil {
			t.Fatal(err)
		}
	}
	join("12D3KooWbeta", "beta")
	for i := range 11 {
		join(fmt.Sprintf("12D3KooWz%02d", i), fmt.Sprintf("z%02d", i))
	}
	all := runCLI(t, 0, "team", "list")
	wantIn(t, all, "alpha  "+cliSelf, "beta  12D3KooWbeta", "and 3 more: agent-courier team list courier shows them all")
	if strings.Contains(all, "z10") {
		t.Errorf("the list of every team shows more than 10 members: %q", all)
	}
	one := runCLI(t, 0, "team", "list", "courier")
	wantIn(t, one, "z10  12D3KooWz10")
	if strings.Contains(one, "more") {
		t.Errorf("the list of one team leaves members out: %q", one)
	}
	wantIn(t, runCLI(t, 1, "team", "list", "nope"), "no team with this name")

	wantIn(t, runCLI(t, 0, "team", "leave", "courier"), "Left team courier.")
	wantIn(t, runCLI(t, 0, "team", "list"), "belongs to no team")
	wantIn(t, runCLI(t, 1, "team", "leave", "courier"), "no team with this name")
}

func TestTeamCLIInviteAndJoin(t *testing.T) {
	net := &cliNet{}
	startTeamTestDaemon(t, net)
	runCLI(t, 0, "team", "create", "courier", "--topic", "t", "--label", "alpha")

	out := runCLI(t, 0, "team", "invite", "courier")
	wantIn(t, out, "Invite code for team courier", "It works once, until 20", "\nabcdefgh234567\n", "agent-courier team join")
	wantIn(t, runCLI(t, 1, "team", "invite", "nope"), "no team with this name")

	wantIn(t, runCLI(t, 0, "team", "join", "abcdefgh234567", "--label", "bravo"), "courier", "bravo  "+cliSelf, "alpha  12D3KooWinviter")
	defer func(f func() (string, error)) { hostname = f }(hostname)
	hostname = func() (string, error) { return "Laptop.local", nil }
	runCLI(t, 0, "team", "join", "abcdefgh234567")
	if got := net.lastLabel(); got != "laptop" {
		t.Fatalf("join without --label sent label %q, want the host name", got)
	}

	net.mu.Lock()
	net.joinErr = p2p.ErrInviterUnreachable
	net.mu.Unlock()
	got := runCLI(t, 1, "team", "join", "abcdefgh234567", "--label", "bravo")
	wantIn(t, got, "not reachable", "stays valid")
	if strings.Contains(got, "abcdefgh234567") {
		t.Fatalf("the error quotes the code: %q", got)
	}
}

// TestTeamCLIRealDaemon goes through serveDaemon, which creates the machine
// key in memory on the first team and writes no file for it.
func TestTeamCLIRealDaemon(t *testing.T) {
	config := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", config)
	startTestDaemon(t)
	out := runCLI(t, 0, "team", "create", "courier", "--topic", "t", "--label", "alpha")
	wantIn(t, out, "alpha  12D3KooW")
	wantIn(t, runCLI(t, 0, "team", "leave", "courier"), "Left team courier.")
	if files, err := os.ReadDir(config); err != nil || len(files) != 0 {
		t.Fatalf("the daemon wrote %v to the config directory: %v", files, err)
	}
}

// TestTeamCLIOldDaemon talks to a daemon that does not know the team methods.
func TestTeamCLIOldDaemon(t *testing.T) {
	dir := filepath.Join(shortTempDir(t), "agent-courier")
	t.Setenv(daemon.SocketDirEnv, dir)
	ln, err := daemon.Listen(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ln.Close()
		ln.Unlock()
	})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		var req struct {
			ID int `json:"id"`
		}
		line, err := bufio.NewReader(c).ReadBytes('\n')
		if err != nil || json.Unmarshal(line, &req) != nil {
			return
		}
		fmt.Fprintf(c, `{"jsonrpc":"2.0","id":%d,"error":{"code":-32601,"message":"the method must be one of register, send, pending, ack, agents"}}`+"\n", req.ID)
	}()
	wantIn(t, runCLI(t, 1, "team", "list"), "the running daemon is older than this command", "stop it")
}

func TestHostLabel(t *testing.T) {
	defer func(f func() (string, error)) { hostname = f }(hostname)
	for _, tc := range []struct {
		host string
		err  error
		want string
		in   string
	}{
		{"Box.example.org", nil, "box", ""},
		{"build-01", nil, "build-01", ""},
		{"my_host IGNORE", nil, "", "the host name is not a valid label"},
		{"", errors.New("uname failed"), "", "no host name"},
	} {
		hostname = func() (string, error) { return tc.host, tc.err }
		got, err := hostLabel()
		if got != tc.want || (tc.in == "") != (err == nil) || (err != nil && (!strings.Contains(err.Error(), tc.in) ||
			!strings.Contains(err.Error(), "--label") || strings.Contains(err.Error(), "IGNORE"))) {
			t.Errorf("hostLabel with %q, %v = %q, %v", tc.host, tc.err, got, err)
		}
	}
	hostname = func() (string, error) { return "Bad_Host", nil }
	wantIn(t, runCLI(t, 2, "team", "create", "courier", "--topic", "t"), "pass --label")
}

func TestTeamCLIUsage(t *testing.T) {
	for _, tc := range []struct {
		args []string
		code int
		in   string
	}{
		{[]string{"team"}, 2, "usage: agent-courier team"},
		{[]string{"team", "help"}, 0, "usage: agent-courier team"},
		{[]string{"team", "bogus"}, 2, `unknown team command "bogus"`},
		{[]string{"team", "create"}, 2, "team create takes 1 arguments"},
		{[]string{"team", "remove", "courier", "alpha"}, 2, `unknown team command "remove"`},
		{[]string{"team", "list", "a", "b"}, 2, "team list takes 0 or 1 arguments"},
		{[]string{"team", "leave", "--topic", "x", "courier"}, 2, "flag provided but not defined"},
		{[]string{"team", "invite"}, 2, "team invite takes 1 arguments"},
		{[]string{"team", "join"}, 2, "team join takes 1 arguments"},
		{[]string{"team", "join", "code", "--topic", "x"}, 2, "flag provided but not defined"},
	} {
		wantIn(t, runCLI(t, tc.code, tc.args...), tc.in)
	}
}
