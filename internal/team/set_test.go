package team

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

// ms is the join counter that the clock of newSet writes at its start.
var ms = uint64(time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC).UnixMilli())

func newSet(t *testing.T) (*Set, *clock) {
	t.Helper()
	c := &clock{time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)}
	return NewSet(c.now), c
}

func mustCreate(t *testing.T, s *Set, self, name, label string, topics ...string) Team {
	t.Helper()
	tm, err := s.Create(self, name, topics, label)
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	return tm
}

func TestCreateRules(t *testing.T) {
	s, _ := newSet(t)
	tm := mustCreate(t, s, "pa", "courier", "alpha", "t2", "t1", "t1")
	if !slices.Equal(tm.Topics, []string{"t1", "t2"}) || len(tm.Members) != 1 || !reflect.DeepEqual(tm.Members["pa"], Entry{PeerID: "pa", Label: "alpha", Join: ms}) {
		t.Fatalf("created %+v", tm)
	}
	for _, tc := range []struct {
		name, team, label string
		topics            []string
		want              error
	}{
		{"same name", "courier", "alpha", []string{"t3"}, ErrTeamExists},
		{"topic of another team", "other", "alpha", []string{"t1"}, ErrTopicTaken},
		{"no topics", "other", "alpha", nil, ErrNoTopics},
		{"too many topics", "other", "alpha", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q"}, ErrNoTopics},
	} {
		if _, err := s.Create("pa", tc.team, tc.topics, tc.label); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
	}
	if _, err := s.Create("pa", "x", []string{"t9"}, "Alpha"); err == nil {
		t.Error("a bad label passed")
	}
	if _, err := s.Create("pb", "x", []string{"t9"}, "alpha"); err == nil {
		t.Error("a second peer id for this machine passed")
	}
	for i := s.Len(); i < MaxTeams; i++ {
		mustCreate(t, s, "pa", fmt.Sprintf("team%d", i), "alpha", fmt.Sprintf("topic%d", i))
	}
	if _, err := s.Create("pa", "one-more", []string{"z"}, "alpha"); !errors.Is(err, ErrTooManyTeams) {
		t.Errorf("team %d: err = %v", MaxTeams+1, err)
	}
	if _, ok := s.TopicTeam("t2"); !ok {
		t.Error("t2 is no team topic")
	}
}

// join runs both sides of a join in memory: inviter accepts the joiner
// self, which stores the list it gets.
func join(t *testing.T, inviter *Set, joiner *Set, self, name, label string) (Team, error) {
	t.Helper()
	secret, _, err := inviter.CreateInvite(name)
	if err != nil {
		t.Fatal(err)
	}
	topics, err := inviter.CheckInvite(name, secret, self, label)
	if err != nil {
		return Team{}, err
	}
	tm, err := inviter.AcceptInvite(name, secret, self, label)
	if err != nil {
		return Team{}, err
	}
	return joiner.Join(self, name, topics, inviter.self, tm.Members.Entries(), label)
}

func TestInviteAndJoin(t *testing.T) {
	a, ca := newSet(t)
	b, _ := newSet(t)
	mustCreate(t, a, "pa", "courier", "alpha", "t1")
	tm, err := join(t, a, b, "pb", "courier", "beta")
	if err != nil {
		t.Fatal(err)
	}
	if !tm.Members.Has("pa") || tm.Members["pb"].Join != ms || !slices.Equal(tm.Topics, []string{"t1"}) {
		t.Fatalf("joined %+v", tm)
	}
	if !a.MemberOf("pb", "courier") || !b.MemberOf("pa", "courier") || b.IsMember("pb") {
		t.Error("the member sets do not hold the other member only")
	}

	secret, _, _ := a.CreateInvite("courier")
	if _, err := a.AcceptInvite("courier", secret, "pc", "beta"); !errors.Is(err, ErrLabelTaken) {
		t.Errorf("taken label: %v", err)
	}
	if _, err := a.AcceptInvite("courier", secret, "pc", "gamma"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.AcceptInvite("courier", secret, "pd", "delta"); !errors.Is(err, ErrInviteRefused) {
		t.Errorf("used code: %v", err)
	}
	for name, try := range map[string]func() error{
		"wrong team": func() error { _, err := a.CheckInvite("other", secret, "pd", "delta"); return err },
		"short":      func() error { _, err := a.CheckInvite("courier", secret[:8], "pd", "delta"); return err },
		"own peer id": func() error {
			s, _, _ := a.CreateInvite("courier")
			_, err := a.CheckInvite("courier", s, "pa", "x")
			return err
		},
	} {
		if err := try(); !errors.Is(err, ErrInviteRefused) {
			t.Errorf("%s: %v", name, err)
		}
	}
	old, _, _ := a.CreateInvite("courier")
	ca.t = ca.t.Add(InviteTTL)
	if _, err := a.CheckInvite("courier", old, "pd", "delta"); !errors.Is(err, ErrInviteRefused) {
		t.Errorf("expired code: %v", err)
	}
	for range MaxOpenInvites {
		if _, _, err := a.CreateInvite("courier"); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := a.CreateInvite("courier"); !errors.Is(err, ErrTooManyInvites) {
		t.Errorf("invite %d: %v", MaxOpenInvites+1, err)
	}
	// A leave kills the open invites of the team.
	if _, err := a.Leave("courier"); err != nil {
		t.Fatal(err)
	}
	mustCreate(t, a, "pa", "courier", "alpha", "t1")
	if len(a.invites) != 0 {
		t.Errorf("%d invites outlived the leave", len(a.invites))
	}
}

func TestJoinNeedsInviterAndSelf(t *testing.T) {
	b, _ := newSet(t)
	inviter := Entry{PeerID: "pa", Label: "alpha", Join: 1}
	self := Entry{PeerID: "pb", Label: "beta", Join: 1}
	for name, entries := range map[string][]Entry{
		"no inviter":    {self},
		"no self":       {inviter},
		"other label":   {inviter, {PeerID: "pb", Label: "gamma", Join: 1}},
		"self left":     {inviter, {PeerID: "pb", Label: "beta", Join: 1, Left: true}},
		"inviter left":  {{PeerID: "pa", Label: "alpha", Join: 1, Left: true}, self},
		"invalid entry": {inviter, {PeerID: "pb", Label: "beta"}},
	} {
		if _, err := b.Join("pb", "courier", []string{"t1"}, "pa", entries, "beta"); !errors.Is(err, ErrJoinList) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if b.Len() != 0 {
		t.Fatal("a refused join stored a team")
	}
	mustCreate(t, b, "pb", "mine", "beta", "t1")
	if _, err := b.Join("pb", "courier", []string{"t1"}, "pa", []Entry{inviter, self}, "beta"); !errors.Is(err, ErrTopicTaken) {
		t.Errorf("topic of another team: %v", err)
	}
}

func TestMergeRules(t *testing.T) {
	a, _ := newSet(t)
	mustCreate(t, a, "pa", "courier", "alpha", "t1")
	secret, _, _ := a.CreateInvite("courier")
	a.AcceptInvite("courier", secret, "pb", "beta")

	if _, err := a.Merge("courier", "px", []Entry{{PeerID: "pc", Label: "gamma", Join: 1}}); !errors.Is(err, ErrNotMember) {
		t.Errorf("list from an outsider: %v", err)
	}
	if a.IsMember("pc") {
		t.Fatal("an outsider added a member")
	}
	if _, err := a.Merge("other", "pb", nil); !errors.Is(err, ErrNoTeam) {
		t.Errorf("unknown team: %v", err)
	}
	changed, err := a.Merge("courier", "pb", []Entry{{PeerID: "pc", Label: "gamma", Join: 1}, {PeerID: "bad"}})
	if err != nil || !changed || !a.MemberOf("pc", "courier") {
		t.Fatalf("member announces a member: changed %v err %v", changed, err)
	}

	// An entry of this machine from a member changes nothing: only this
	// machine and its inviter write it.
	changed, _ = a.Merge("courier", "pb", []Entry{{PeerID: "pa", Label: "alpha", Join: ms + 9, Left: true}})
	if own := mustGet(t, a, "courier").Members["pa"]; changed || own.Left || own.Join != ms {
		t.Errorf("own entry after a stray leave: changed %v, %+v", changed, own)
	}

	// The cap counts members that did not leave.
	var many []Entry
	for i := range MaxMembers + 5 {
		many = append(many, Entry{PeerID: fmt.Sprintf("q%03d", i), Label: fmt.Sprintf("q%03d", i), Join: 1})
	}
	a.Merge("courier", "pb", many)
	if n := len(mustGet(t, a, "courier").Members.Active()); n != MaxMembers {
		t.Errorf("%d members after a flood, want %d", n, MaxMembers)
	}
}

func mustGet(t *testing.T, s *Set, name string) Team {
	t.Helper()
	tm, ok := s.Get(name)
	if !ok {
		t.Fatalf("no team %s", name)
	}
	return tm
}

// The join counter is a clock value: the larger of one more than the held
// counter and the time in milliseconds, at an address change and a join.
func TestClockCounter(t *testing.T) {
	a, ca := newSet(t)
	mustCreate(t, a, "pa", "courier", "alpha", "t1")
	if !a.SetAddrs("courier", []string{"/ip4/10.0.0.1/tcp/1"}) || a.SetAddrs("courier", []string{"/ip4/10.0.0.1/tcp/1"}) {
		t.Fatal("SetAddrs reports the wrong change")
	}
	if own := mustGet(t, a, "courier").Members["pa"]; own.Join != ms+1 {
		t.Errorf("counter %d after an address change in the same millisecond, want %d", own.Join, ms+1)
	}
	ca.t = ca.t.Add(time.Hour)
	a.SetAddrs("courier", []string{"/ip4/10.0.0.2/tcp/1"})
	if own := mustGet(t, a, "courier").Members["pa"]; own.Join != ms+3_600_000 {
		t.Errorf("counter %d an hour later, want %d", own.Join, ms+3_600_000)
	}

	// pb left at a counter of a clock ahead of this one: its rejoin goes one
	// above it. pc left behind this clock: its rejoin takes the time.
	ahead := ms + 7_200_000
	addMember(t, a, "pb", "beta")
	addMember(t, a, "pd", "delta")
	if _, err := a.Merge("courier", "pd", []Entry{{PeerID: "pb", Label: "beta", Join: ahead, Left: true}, {PeerID: "pc", Label: "gamma", Join: 5, Left: true}}); err != nil {
		t.Fatal(err)
	}
	addMember(t, a, "pb", "beta")
	addMember(t, a, "pc", "gamma")
	tm := mustGet(t, a, "courier")
	if got := tm.Members["pb"]; got.Join != ahead+1 || got.Left {
		t.Errorf("rejoin after a leave ahead of the clock: %+v", got)
	}
	if got := tm.Members["pc"]; got.Join != ms+3_600_000 || got.Left {
		t.Errorf("rejoin after an old leave: %+v", got)
	}
}

func addMember(t *testing.T, s *Set, id, label string) {
	t.Helper()
	secret, _, err := s.CreateInvite("courier")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcceptInvite("courier", secret, id, label); err != nil {
		t.Fatalf("join of %s: %v", id, err)
	}
}

// Rejoin after a restart: the label of a member that is not connected goes
// to the joiner, and that member is marked left in the same step. A
// connected member, and this machine, keep their labels.
func TestRejoinTakesLabelOfGoneMember(t *testing.T) {
	a, _ := newSet(t)
	mustCreate(t, a, "pa", "courier", "alpha", "t1")
	addMember(t, a, "pb", "beta")
	online := map[string]bool{"pb": true}
	a.SetConnected(func(id string) bool { return online[id] })

	secret, _, _ := a.CreateInvite("courier")
	for _, tc := range []struct{ id, label string }{{"pb2", "beta"}, {"pb2", "alpha"}} {
		if _, err := a.CheckInvite("courier", secret, tc.id, tc.label); !errors.Is(err, ErrLabelTaken) {
			t.Errorf("label %s of a connected member: %v", tc.label, err)
		}
		if _, err := a.AcceptInvite("courier", secret, tc.id, tc.label); !errors.Is(err, ErrLabelTaken) {
			t.Errorf("accept with label %s of a connected member: %v", tc.label, err)
		}
	}

	online["pb"] = false
	if _, err := a.CheckInvite("courier", secret, "pb2", "beta"); err != nil {
		t.Fatalf("label of a member without a connection: %v", err)
	}
	if mustGet(t, a, "courier").Members["pb"].Left {
		t.Fatal("CheckInvite changed the list")
	}
	tm, err := a.AcceptInvite("courier", secret, "pb2", "beta")
	if err != nil {
		t.Fatal(err)
	}
	if old := tm.Members["pb"]; !old.Left || old.Join != ms {
		t.Errorf("old peer id after the rejoin: %+v", old)
	}
	if got := tm.Members["pb2"]; got.Label != "beta" || got.Left {
		t.Errorf("new peer id after the rejoin: %+v", got)
	}
	if a.IsMember("pb") || !a.MemberOf("pb2", "courier") {
		t.Error("the member set did not follow the rejoin")
	}

	// Without a connection source every member counts as connected.
	a.SetConnected(nil)
	secret, _, _ = a.CreateInvite("courier")
	if _, err := a.AcceptInvite("courier", secret, "pb3", "beta"); !errors.Is(err, ErrLabelTaken) {
		t.Errorf("no connection source: %v", err)
	}
}

// A full team admits a rejoin that takes the label of a gone member: the
// gone member does not count against the cap.
func TestRejoinInFullTeam(t *testing.T) {
	a, _ := newSet(t)
	mustCreate(t, a, "pa", "courier", "alpha", "t1")
	for i := range MaxMembers - 1 {
		addMember(t, a, fmt.Sprintf("p%03d", i), fmt.Sprintf("m%03d", i))
	}
	a.SetConnected(func(string) bool { return false })
	secret, _, _ := a.CreateInvite("courier")
	if _, err := a.AcceptInvite("courier", secret, "pnew", "newcomer"); !errors.Is(err, ErrTeamFull) {
		t.Fatalf("join of a new label into a full team: %v", err)
	}
	if _, err := a.AcceptInvite("courier", secret, "p000b", "m000"); err != nil {
		t.Fatalf("rejoin into a full team: %v", err)
	}
	if n := len(mustGet(t, a, "courier").Members.Active()); n != MaxMembers {
		t.Errorf("%d members after the rejoin, want %d", n, MaxMembers)
	}
}

// Join keeps at most MaxMembers members of the inviter's list, this machine
// and the inviter always among them.
func TestJoinCapsMembers(t *testing.T) {
	b, _ := newSet(t)
	entries := []Entry{{PeerID: "pa", Label: "alpha", Join: 1}, {PeerID: "pz", Label: "zulu", Join: 1}}
	for i := range MaxMembers + 5 {
		entries = append(entries, Entry{PeerID: fmt.Sprintf("q%03d", i), Label: fmt.Sprintf("q%03d", i), Join: 1})
	}
	tm, err := b.Join("pz", "courier", []string{"t1"}, "pa", entries, "zulu")
	if err != nil {
		t.Fatal(err)
	}
	if n := len(tm.Members.Active()); n != MaxMembers {
		t.Errorf("%d members after the join, want %d", n, MaxMembers)
	}
	if !tm.Members.Has("pa") || !tm.Members.Has("pz") {
		t.Error("the cap dropped the inviter or this machine")
	}
}

func TestLeave(t *testing.T) {
	a, _ := newSet(t)
	mustCreate(t, a, "pa", "courier", "alpha", "t1")
	secret, _, _ := a.CreateInvite("courier")
	a.AcceptInvite("courier", secret, "pb", "beta")
	tm, err := a.Leave("courier")
	if err != nil || !tm.Members["pa"].Left || !tm.Members.Has("pb") {
		t.Fatalf("left %+v, %v", tm, err)
	}
	if a.Len() != 0 || a.IsMember("pb") {
		t.Error("the team outlived the leave")
	}
	if _, ok := a.TopicTeam("t1"); ok {
		t.Error("the topic outlived the leave")
	}
	if _, err := a.Leave("courier"); !errors.Is(err, ErrNoTeam) {
		t.Errorf("second leave: %v", err)
	}
}

func TestRoute(t *testing.T) {
	a, _ := newSet(t)
	mustCreate(t, a, "pa", "courier", "alpha", "t1")
	a.Merge("courier", "pa", nil) // own lists are not merged: no-op
	secret, _, _ := a.CreateInvite("courier")
	a.AcceptInvite("courier", secret, "12D3KooWbob111111", "bob")
	a.Merge("courier", "12D3KooWbob111111", []Entry{
		{PeerID: "12D3KooWbob222222", Label: "bob", Join: 1},
		{PeerID: "12D3KooWcarol33333", Label: "carol", Join: 1, Left: true},
	})
	for _, tc := range []struct {
		topic, to string
		want      []string
		err       error
	}{
		{"local", "", nil, nil},
		{"local", "bob/claude", nil, ErrNotTeamTopic},
		{"t1", "", []string{"bob~111111", "bob~222222"}, nil},
		{"t1", "claude", nil, nil},
		{"t1", "bob/claude", nil, ErrAmbiguousRecipient},
		{"t1", "bob~222222/claude", []string{"bob~222222"}, nil},
		{"t1", "carol/claude", nil, ErrUnknownRemote},
		{"t1", "alpha/claude", nil, ErrUnknownRemote},
	} {
		r, err := a.Route(tc.topic, tc.to)
		if !errors.Is(err, tc.err) {
			t.Errorf("%s %s: err %v, want %v", tc.topic, tc.to, err, tc.err)
			continue
		}
		var got []string
		for _, q := range r.Peers {
			got = append(got, q.Member)
		}
		slices.Sort(got)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s %s: route %v, want %v", tc.topic, tc.to, got, tc.want)
		}
		if err == nil && strings.HasPrefix(tc.topic, "t") != (r.Team == "courier") {
			t.Errorf("%s: team %q", tc.topic, r.Team)
		}
	}
}
