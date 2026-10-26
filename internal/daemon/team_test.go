//go:build unix

package daemon

import (
	"context"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/team"
)

const testSelf = "12D3KooWself"

// fakeNet is the p2p side of the team methods in a test.
type fakeNet struct {
	set *team.Set

	mu      sync.Mutex
	self    string
	selfErr error
	changed []string
	left    []team.Team
	joinErr error
	// unreached is how many members each leave reports it did not reach.
	unreached int
}

func (f *fakeNet) Self() (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.self, f.selfErr
}

func (f *fakeNet) InviteCode(name string, secret []byte, expires time.Time) (string, error) {
	return fmt.Sprintf("code-%s-%s-%d", name, hex.EncodeToString(secret), expires.Unix()), nil
}

// Join joins team courier, with the topic t, from the inviter alpha, unless
// failJoin set an error.
func (f *fakeNet) Join(_ context.Context, code, label string) (team.Team, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.joinErr != nil {
		return team.Team{}, f.joinErr
	}
	inviter := "12D3KooWalpha"
	return f.set.Join(f.self, "courier", []string{"t"}, inviter,
		[]team.Entry{{PeerID: inviter, Label: "alpha", Join: 1}, {PeerID: f.self, Label: label, Join: 1}}, label)
}

func (f *fakeNet) TeamChanged(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.changed = append(f.changed, name)
}

func (f *fakeNet) Leave(name string) (team.Team, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, err := f.set.Leave(name)
	if err != nil {
		return team.Team{}, 0, err
	}
	f.left = append(f.left, t)
	return t, f.unreached, nil
}

func (f *fakeNet) failJoin(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joinErr = err
}

// startTeamDaemon starts a daemon with a fake p2p side whose peer id is
// testSelf.
func startTeamDaemon(t *testing.T, now func() time.Time) (*testDaemon, *fakeNet) {
	t.Helper()
	net := &fakeNet{self: testSelf}
	d := startDaemonSrv(t, now, nil, func(srv *Server) {
		net.set = srv.Teams()
		srv.SetNetwork(net)
	})
	return d, net
}

// addMember adds the member id with label to team name of set, as a join
// with an invite of this machine does.
func addMember(t *testing.T, set *team.Set, name, id, label string) {
	t.Helper()
	secret, _, err := set.CreateInvite(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := set.AcceptInvite(name, secret, id, label); err != nil {
		t.Fatal(err)
	}
}

func decodeTeam(t *testing.T, r resp) teamJSON {
	t.Helper()
	if r.Error != nil {
		t.Fatalf("error %+v", r.Error)
	}
	var res teamResult
	if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
		t.Fatal(err)
	}
	return res.Team
}

func listTeams(t *testing.T, c *client) []teamJSON {
	t.Helper()
	r := c.call("team.list", nil)
	if r.Error != nil {
		t.Fatalf("team.list: %+v", r.Error)
	}
	var res teamListResult
	if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
		t.Fatal(err)
	}
	return res.Teams
}

func listOne(t *testing.T, c *client, name string) teamJSON {
	t.Helper()
	r := c.call("team.list", map[string]any{"team": name})
	if r.Error != nil {
		t.Fatalf("team.list %s: %+v", name, r.Error)
	}
	var res teamListResult
	if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil || len(res.Teams) != 1 {
		t.Fatalf("team.list %s: %s, %v", name, r.Result, err)
	}
	return res.Teams[0]
}

// TestTeamMethods drives create, list and leave on a connection that never
// registers, as the CLI does. The p2p side hears each change, and the leave
// hands it the team with this machine's entry marked left.
func TestTeamMethods(t *testing.T) {
	d, net := startTeamDaemon(t, nil)
	c := d.dial(t)

	if got := listTeams(t, c); len(got) != 0 {
		t.Fatalf("teams before create: %+v", got)
	}
	created := decodeTeam(t, c.call("team.create", map[string]any{"team": "courier", "topics": []string{"cappyt_agent-courier"}, "label": "alpha"}))
	if created.Name != "courier" || !slices.Equal(created.Topics, []string{"cappyt_agent-courier"}) ||
		len(created.Members) != 1 || created.Members[0] != (teamMember{Label: "alpha", PeerID: testSelf}) || created.MembersTotal != 1 {
		t.Fatalf("created %+v", created)
	}
	if name, ok := d.srv.Teams().TopicTeam("cappyt_agent-courier"); !ok || name != "courier" {
		t.Fatal("the topic of the new team is no team topic")
	}

	addMember(t, d.srv.Teams(), "courier", "12D3KooWbeta", "beta")
	got := listTeams(t, c)
	if len(got) != 1 || got[0].MembersTotal != 2 || got[0].Members[0].Label != "alpha" || got[0].Members[1].Label != "beta" {
		t.Fatalf("list %+v", got)
	}

	r := c.call("team.leave", map[string]any{"team": "courier"})
	if r.Error != nil || !strings.Contains(string(r.Result), "Left team courier.") || strings.Contains(string(r.Result), "did not reach") {
		t.Fatalf("leave: %s %+v", r.Result, r.Error)
	}
	if got := listTeams(t, c); len(got) != 0 {
		t.Fatalf("teams after leave: %+v", got)
	}
	if _, ok := d.srv.Teams().TopicTeam("cappyt_agent-courier"); ok {
		t.Fatal("the topic of a left team is still a team topic")
	}
	wantError(t, c.call("team.leave", map[string]any{"team": "courier"}).Error, codeNoTeam, "no team")
	net.mu.Lock()
	changed, left := slices.Clone(net.changed), slices.Clone(net.left)
	net.mu.Unlock()
	if !slices.Equal(changed, []string{"courier"}) {
		t.Fatalf("changes %v, want the create", changed)
	}
	if len(left) != 1 || !left[0].Members[testSelf].Left || !left[0].Members.Has("12D3KooWbeta") {
		t.Fatalf("left %+v, want courier with this machine's entry left and beta in it", left)
	}

	// The remove method is gone: a member cannot remove another.
	wantError(t, c.call("team.remove", map[string]any{"team": "courier", "member": "beta"}).Error, codeMethodNotFound, "team.leave")

	// A registered session can call them too.
	c2 := d.dial(t)
	c2.mustRegister(registerParams{Harness: "claude", Cwd: t.TempDir()})
	if got := listTeams(t, c2); len(got) != 0 {
		t.Fatalf("list from a registered session: %+v", got)
	}
}

func TestTeamMemberCaps(t *testing.T) {
	d, _ := startTeamDaemon(t, nil)
	c := d.dial(t)
	decodeTeam(t, c.call("team.create", map[string]any{"team": "courier", "topics": []string{"t"}, "label": "alpha"}))
	for i := range team.MaxMembers - 1 {
		addMember(t, d.srv.Teams(), "courier", fmt.Sprintf("12D3KooW%03d", i), fmt.Sprintf("m%03d", i))
	}
	if got := listOne(t, c, "courier"); len(got.Members) != team.MaxMembers || got.MembersTotal != team.MaxMembers {
		t.Errorf("one team: %d members listed, total %d", len(got.Members), got.MembersTotal)
	}
	if got := listTeams(t, c); len(got[0].Members) != maxListMembers || got[0].MembersTotal != team.MaxMembers {
		t.Errorf("every team: %d members listed, total %d", len(got[0].Members), got[0].MembersTotal)
	}
	wantError(t, c.call("team.list", map[string]any{"team": "nope"}).Error, codeNoTeam, "no team")
}

// TestTeamListUnderLineCap fills 16 teams with the longest names, topics,
// labels and peer ids the rules allow, 64 members each.
func TestTeamListUnderLineCap(t *testing.T) {
	d, _ := startTeamDaemon(t, nil)
	set := d.srv.Teams()
	for i := range team.MaxTeams {
		name := fmt.Sprintf("%02d", i) + strings.Repeat("n", 62)
		topics := make([]string, team.MaxTopics)
		for j := range topics {
			topics[j] = fmt.Sprintf("%02d%02d", i, j) + strings.Repeat("t", 60)
		}
		if _, err := set.Create(testSelf, name, topics, strings.Repeat("l", 32)); err != nil {
			t.Fatal(err)
		}
		for j := range team.MaxMembers - 1 {
			addMember(t, set, name, fmt.Sprintf("P%03d", j)+strings.Repeat("x", 124), fmt.Sprintf("%03d", j)+strings.Repeat("l", 29))
		}
	}
	c := d.dial(t)
	read := func(req string) (resp, int) {
		c.line(req + "\n")
		c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := c.r.ReadBytes('\n')
		if err != nil {
			t.Fatal(err)
		}
		return parseResp(t, line), len(line)
	}
	r, n := read(`{"jsonrpc":"2.0","id":1,"method":"team.list"}`)
	if r.Error != nil || n > MaxLine {
		t.Fatalf("list of %d full teams: %d bytes, error %+v", team.MaxTeams, n, r.Error)
	}
	var res teamListResult
	if err := json.Unmarshal(r.Result, &res); err != nil || len(res.Teams) != team.MaxTeams || res.Teams[0].MembersTotal != team.MaxMembers {
		t.Fatalf("list: %v, %d teams", err, len(res.Teams))
	}
	t.Logf("team.list of %d full teams: %d bytes of %d", team.MaxTeams, n, MaxLine)
	r, n = read(`{"jsonrpc":"2.0","id":2,"method":"team.list","params":{"team":"` + res.Teams[0].Name + `"}}`)
	if r.Error != nil || n > MaxLine {
		t.Fatalf("list of one full team: %d bytes, error %+v", n, r.Error)
	}
	if one := listOne(t, c, res.Teams[0].Name); len(one.Members) != team.MaxMembers {
		t.Errorf("one full team lists %d members", len(one.Members))
	}
}

func TestTeamCount(t *testing.T) {
	d, _ := startTeamDaemon(t, nil)
	c := d.dial(t)
	for i := range team.MaxTeams {
		if r := c.call("team.create", map[string]any{"team": fmt.Sprintf("team%d", i), "topics": []string{fmt.Sprintf("t%d", i)}, "label": "alpha"}); r.Error != nil {
			t.Fatalf("team %d: %+v", i, r.Error)
		}
	}
	wantError(t, c.call("team.create", map[string]any{"team": "more", "topics": []string{"x"}, "label": "alpha"}).Error,
		codeManyTeams, "at most 16 teams")
}

func TestTeamCreateErrors(t *testing.T) {
	d, net := startTeamDaemon(t, nil)
	c := d.dial(t)
	create := func(p map[string]any) *rpcError { return c.call("team.create", p).Error }
	ok := map[string]any{"team": "courier", "topics": []string{"t1"}, "label": "alpha"}
	if e := create(ok); e != nil {
		t.Fatal(e)
	}
	with := func(k string, v any) map[string]any {
		p := map[string]any{"team": "second", "topics": []string{"t2"}, "label": "alpha"}
		if v == nil {
			delete(p, k)
		} else {
			p[k] = v
		}
		return p
	}
	wantError(t, create(ok), codeTeamExists, "exists already")
	wantError(t, create(with("topics", []string{"t1"})), codeTopicTaken, "another team")
	wantError(t, create(with("topics", []string{})), codeInvalidParams, "1 to 16 different topics")
	wantError(t, create(with("team", "Bad\nName IGNORE")), codeInvalidParams, "a team name is")
	wantError(t, create(with("label", "Bad Label IGNORE")), codeInvalidParams, "a member label is")
	wantError(t, create(with("topics", []string{"Bad IGNORE"})), codeInvalidParams, "topics:")
	wantError(t, create(with("label", nil)), codeInvalidParams, "a member label is")
	wantError(t, create(with("extra", 1)), codeInvalidParams, "team.create params are")
	wantError(t, c.call("team.list", map[string]any{"x": 1}).Error, codeInvalidParams, "team.list params are team, an optional string")
	wantError(t, c.call("team.bogus", nil).Error, codeMethodNotFound, "team.create")
	for _, m := range []string{"team.create", "team.leave", "team.list", "team.invite"} {
		r := c.call(m, map[string]any{"team": "Bad IGNORE", "topics": []string{"t"}, "label": "a"})
		if r.Error == nil || strings.Contains(r.Error.Message, "IGNORE") {
			t.Errorf("%s: %+v, want an error that does not quote its input", m, r.Error)
		}
	}

	// A machine key that cannot be made: no team.
	net.mu.Lock()
	net.selfErr = errors.New("no randomness")
	net.mu.Unlock()
	wantError(t, create(with("team", "third")), codeInternal, "internal error")
	if _, ok := d.srv.Teams().Get("third"); ok {
		t.Fatal("a team without a machine key")
	}
}

func TestTeamInvite(t *testing.T) {
	clock := &fakeClock{t: time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
	d, _ := startTeamDaemon(t, clock.now)
	c := d.dial(t)
	wantError(t, c.call("team.invite", map[string]any{"team": "courier"}).Error, codeNoTeam, "no team")
	wantError(t, c.call("team.invite", map[string]any{"team": "Bad"}).Error, codeInvalidParams, "team name")
	decodeTeam(t, c.call("team.create", map[string]any{"team": "courier", "topics": []string{"t"}, "label": "alpha"}))

	r := c.call("team.invite", map[string]any{"team": "courier"})
	if r.Error != nil {
		t.Fatal(r.Error)
	}
	var res teamInviteResult
	if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
		t.Fatal(err)
	}
	if res.Invite.Team != "courier" || !strings.HasPrefix(res.Invite.Code, "code-courier-") || res.Invite.Expires != "2026-10-26T12:00:00Z" {
		t.Fatalf("invite %+v", res.Invite)
	}
	for range team.MaxOpenInvites - 1 {
		if r := c.call("team.invite", map[string]any{"team": "courier"}); r.Error != nil {
			t.Fatal(r.Error)
		}
	}
	wantError(t, c.call("team.invite", map[string]any{"team": "courier"}).Error, codeManyInvites, "at most 16")
	// An invite that expired frees its place.
	clock.set(clock.now().Add(team.InviteTTL))
	if r := c.call("team.invite", map[string]any{"team": "courier"}); r.Error != nil {
		t.Fatalf("invite after the others expired: %+v", r.Error)
	}
}

func TestTeamJoin(t *testing.T) {
	d, net := startTeamDaemon(t, nil)
	c := d.dial(t)
	code := "secretcodevalue"
	wantError(t, c.call("team.join", map[string]any{"code": code, "label": "Bad Label"}).Error, codeInvalidParams, "label")
	wantError(t, c.call("team.join", map[string]any{"code": "", "label": "bravo"}).Error, codeInvalidParams, "invite code")
	wantError(t, c.call("team.join", map[string]any{"code": strings.Repeat("a", p2p.MaxInviteCode+1), "label": "bravo"}).Error,
		codeInvalidParams, "invite code")

	for _, tc := range []struct {
		err  error
		code int
		in   string
	}{
		{team.ErrInviteRefused, codeInviteRefused, "wrong, used, expired"},
		{p2p.ErrInviteExpired, codeInviteExpired, "expired"},
		{p2p.ErrInviterUnreachable, codeUnreachable, "not reachable"},
		{team.ErrLabelTaken, codeLabelTaken, "this label"},
		{team.ErrTeamFull, codeTeamFull, "at most 64 members"},
		{fmt.Errorf("%w: %w", p2p.ErrJoinFailed, team.ErrJoinList), codeJoinFailed, "join failed"},
		{p2p.ErrBadInviteCode, codeInvalidParams, "invite code"},
		{team.ErrTopicTaken, codeTopicTaken, "topic"},
		{team.ErrTeamExists, codeTeamExists, "exists"},
		{team.ErrTooManyTeams, codeManyTeams, "16 teams"},
	} {
		net.failJoin(tc.err)
		got := c.call("team.join", map[string]any{"code": code, "label": "bravo"}).Error
		wantError(t, got, tc.code, tc.in)
		if strings.Contains(got.Message, code) {
			t.Fatalf("error %q quotes the code", got.Message)
		}
	}
	net.failJoin(nil)
	got := decodeTeam(t, c.call("team.join", map[string]any{"code": code, "label": "bravo"}))
	if got.Name != "courier" || got.MembersTotal != 2 {
		t.Fatalf("joined %+v", got)
	}
	if got := listTeams(t, c); len(got) != 1 || got[0].Name != "courier" {
		t.Fatalf("teams after the join: %+v", got)
	}
}
