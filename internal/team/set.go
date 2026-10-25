package team

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
)

const (
	// InviteTTL is how long an invite code works.
	InviteTTL = 24 * time.Hour
	// MaxOpenInvites is the most unused invites that have not expired this
	// machine holds for one team.
	MaxOpenInvites = 16
	// SecretSize is the bytes of an invite secret: 128 bits.
	SecretSize = 16
)

var (
	ErrTeamExists     = errors.New("a team with this name exists already")
	ErrTooManyTeams   = fmt.Errorf("a machine follows at most %d teams", MaxTeams)
	ErrNoTeam         = errors.New("this machine belongs to no team with this name")
	ErrTopicTaken     = errors.New("a topic belongs to one team: a topic of the new team belongs to another team already")
	ErrNoTopics       = fmt.Errorf("a team has 1 to %d different topics", MaxTopics)
	ErrTooManyInvites = fmt.Errorf("a member holds at most %d unused invite codes that have not expired for one team", MaxOpenInvites)
	// ErrInviteRefused is the one refusal for a wrong, used or expired secret,
	// or a secret of another team, so a guesser learns nothing from it.
	ErrInviteRefused = errors.New("the inviter refused the invite code: it is wrong, used, expired or for another team")
	ErrLabelTaken    = errors.New("another member of the team has this label: join with another label")
	ErrTeamFull      = fmt.Errorf("a team holds at most %d members", MaxMembers)
	ErrJoinList      = errors.New("the member list of the inviter lacks the inviter, or this machine with its label")
	// ErrNotMember refuses a member list from a peer outside the team.
	ErrNotMember = errors.New("the peer is not a member of the team")

	ErrNotTeamTopic       = errors.New("a recipient on the machine of another member needs a topic of a team this machine is a member of")
	ErrUnknownRemote      = errors.New("the recipient names no other member of the team of the topic")
	ErrAmbiguousRecipient = errors.New("two members of the team of the topic hold the label of the recipient")
)

// Team is a copy of a team of this machine. Topics are sorted.
type Team struct {
	Name    string
	Topics  []string
	Members Members
}

type state struct {
	topics  []string
	members Members
}

func (t *state) copy(name string) Team {
	return Team{Name: name, Topics: slices.Clone(t.topics), Members: t.members.clone()}
}

type invite struct {
	team    string
	expires time.Time
}

// Set holds the teams of this machine. It is the one member set of the
// daemon: the access rule, the join protocol, messaging and presence read
// it. It is safe for concurrent use. The methods that read answer from maps
// built at each change, so a stranger's stream costs a map lookup.
type Set struct {
	now func() time.Time

	mu sync.RWMutex
	// connected reports whether this machine has a connection to a peer;
	// nil counts every peer as connected.
	connected func(peerID string) bool
	// self is the peer id of this machine, set by the first Create or Join.
	self  string
	teams map[string]*state
	// invites are the open invites of this machine, by the SHA-256 hash of
	// their secret.
	invites map[[sha256.Size]byte]invite
	// peers are the teams of each other member, and topics the team of each
	// team topic.
	peers  map[string]map[string]bool
	topics map[string]string
	// The send queue and the receive dedupe, in queue.go. queue holds the
	// items for each member, oldest first, at most QueueMax; seen the ids
	// each member sent in each team in the last DedupeTTL; wire the wire id of each stored message of a member, by
	// team and local id; rate the times of the messages each member sent
	// in the last hour.
	queue map[string][]*item
	seen  map[inboxKey]map[message.ID]received
	wire  map[string]map[message.ID]message.ID
	rate  map[string][]time.Time
}

// NewSet returns an empty set. now is the clock of the invites and the send
// queue; nil means time.Now.
func NewSet(now func() time.Time) *Set {
	if now == nil {
		now = time.Now
	}
	return &Set{now: now, teams: map[string]*state{}, invites: map[[sha256.Size]byte]invite{},
		peers: map[string]map[string]bool{}, topics: map[string]string{},
		queue: map[string][]*item{}, seen: map[inboxKey]map[message.ID]received{},
		wire: map[string]map[message.ID]message.ID{}, rate: map[string][]time.Time{}}
}

// index rebuilds peers and topics, and drops what the send queue and the
// receive dedupe hold for a peer that is no longer a member. s.mu is held.
func (s *Set) index() {
	s.peers, s.topics = map[string]map[string]bool{}, map[string]string{}
	for name, t := range s.teams {
		for _, topic := range t.topics {
			s.topics[topic] = name
		}
		for id, e := range t.members {
			if e.Left || id == s.self {
				continue
			}
			if s.peers[id] == nil {
				s.peers[id] = map[string]bool{}
			}
			s.peers[id][name] = true
		}
	}
	s.prune()
}

// SetConnected sets the source of the connections of this machine to other
// peers, which a join reads: the label of a member that is not connected
// goes to the joiner.
func (s *Set) SetConnected(connected func(peerID string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connected = connected
}

// clock is the next join counter of an entry that holds held: the larger of
// held+1 and the current Unix time in milliseconds, so a join after a leave
// wins over the leave on every member. The callers keep held below
// math.MaxUint64.
func (s *Set) clock(held uint64) uint64 {
	return max(held+1, uint64(max(s.now().UnixMilli(), 0)))
}

// setSelf records the peer id of this machine. s.mu is held.
func (s *Set) setSelf(self string) error {
	if err := CheckPeerID(self); err != nil {
		return err
	}
	if s.self != "" && s.self != self {
		return errors.New("the peer id of this machine changed while it is in a team")
	}
	s.self = self
	return nil
}

// Len is the number of teams.
func (s *Set) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.teams)
}

// List is a copy of every team, sorted by name.
func (s *Set) List() []Team {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Team, 0, len(s.teams))
	for _, name := range slices.Sorted(maps.Keys(s.teams)) {
		out = append(out, s.teams[name].copy(name))
	}
	return out
}

// Get is a copy of team name.
func (s *Set) Get(name string) (Team, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.teams[name]
	if !ok {
		return Team{}, false
	}
	return t.copy(name), true
}

// IsMember reports whether peerID is a member of a team of this machine, and
// not this machine.
func (s *Set) IsMember(peerID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.peers[peerID]) > 0
}

// MemberOf reports whether peerID is a member of team name, and not this
// machine.
func (s *Set) MemberOf(peerID, name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peers[peerID][name]
}

// Peers are the other members of every team, sorted.
func (s *Set) Peers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Sorted(maps.Keys(s.peers))
}

// TeamsOf are the teams that peerID shares with this machine, sorted.
func (s *Set) TeamsOf(peerID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Sorted(maps.Keys(s.peers[peerID]))
}

// TopicTeam is the team of topic, when topic is a team topic.
func (s *Set) TopicTeam(topic string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	name, ok := s.topics[topic]
	return name, ok
}

// Sender renders the agent of the member peerID of team name as
// <label>/<agent>, as Members.Sender does. It fails when peerID is not a
// member of the team.
func (s *Set) Sender(name, peerID, agent string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.teams[name]
	if !ok || !t.members.Has(peerID) || peerID == s.self {
		return "", false
	}
	return t.members.Sender(peerID, agent), true
}

// Addrs are the addresses of the member peerID in the list of team name.
func (s *Set) Addrs(name, peerID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if t, ok := s.teams[name]; ok {
		return slices.Clone(t.members[peerID].Addrs)
	}
	return nil
}

// checkTopics returns topics sorted and without duplicates, when no other
// team owns one. s.mu is held.
func (s *Set) checkTopics(topics []string) ([]string, error) {
	topics = slices.Compact(slices.Sorted(slices.Values(topics)))
	if len(topics) == 0 || len(topics) > MaxTopics {
		return nil, ErrNoTopics
	}
	for _, t := range topics {
		if err := message.CheckTopic(t); err != nil {
			return nil, err
		}
		if _, ok := s.topics[t]; ok {
			return nil, ErrTopicTaken
		}
	}
	return topics, nil
}

// checkNew checks that this machine can hold one more team, name. s.mu is
// held.
func (s *Set) checkNew(name string) error {
	if err := CheckName(name); err != nil {
		return err
	}
	if _, ok := s.teams[name]; ok {
		return ErrTeamExists
	}
	if len(s.teams) >= MaxTeams {
		return ErrTooManyTeams
	}
	return nil
}

// Create creates team name with topics, and this machine, self, with label as
// its only member, with a clock join counter.
func (s *Set) Create(self, name string, topics []string, label string) (Team, error) {
	if err := CheckLabel(label); err != nil {
		return Team{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNew(name); err != nil {
		return Team{}, err
	}
	topics, err := s.checkTopics(topics)
	if err != nil {
		return Team{}, err
	}
	if err := s.setSelf(self); err != nil {
		return Team{}, err
	}
	t := &state{topics: topics, members: Members{self: {PeerID: self, Label: label, Join: s.clock(0)}}}
	s.teams[name] = t
	s.index()
	return t.copy(name), nil
}

// Leave marks the entry of this machine in team name left and forgets the
// team and its invites. It returns the team with the left entry, which the
// caller sends to the members it can reach.
func (s *Set) Leave(name string) (Team, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.teams[name]
	if !ok {
		return Team{}, ErrNoTeam
	}
	own := t.members[s.self]
	own.Left = true
	t.members[s.self] = own
	delete(s.teams, name)
	maps.DeleteFunc(s.invites, func(_ [sha256.Size]byte, inv invite) bool { return inv.team == name })
	s.index()
	return t.copy(name), nil
}

// SetAddrs sets the addresses of this machine's entry in team name, and
// reports whether they changed. A change sets a new clock join counter, so
// the new addresses win the merge on every member.
func (s *Set) SetAddrs(name string, addrs []string) bool {
	if len(addrs) > MaxAddrs {
		addrs = addrs[:MaxAddrs]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.teams[name]
	if !ok {
		return false
	}
	own, ok := t.members[s.self]
	if !ok || slices.Equal(own.Addrs, addrs) || own.Join == math.MaxUint64 {
		return false
	}
	own.Addrs, own.Join = slices.Clone(addrs), s.clock(own.Join)
	if own.Validate() != nil {
		return false
	}
	t.members[s.self] = own
	return true
}

// Merge merges entries that the member from sent into the list of team name,
// and reports whether the list changed. It refuses the whole list with
// ErrNotMember when from is not a member of the team. It skips an entry that
// breaks a rule of Entry, one that would add a member to a full team, and
// the entry of this machine: only this machine and its inviter write it.
func (s *Set) Merge(name, from string, entries []Entry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.teams[name]
	if !ok {
		return false, ErrNoTeam
	}
	if from == s.self || !t.members.Has(from) {
		return false, ErrNotMember
	}
	changed := false
	members := t.members.count()
	for _, e := range entries {
		if e.Validate() != nil || e.PeerID == s.self {
			continue
		}
		old, known := t.members[e.PeerID]
		if known && old.Compare(e) >= 0 {
			continue
		}
		if !e.Left && (!known || old.Left) {
			if members >= MaxMembers {
				continue
			}
			members++
		} else if e.Left && known && !old.Left {
			members--
		}
		t.members[e.PeerID] = e
		changed = true
	}
	if changed {
		s.index()
	}
	return changed, nil
}

// CreateInvite makes a new invite for team name and returns its secret and
// expiry. Only the SHA-256 hash of the secret stays in memory.
func (s *Set) CreateInvite(name string) (secret []byte, expires time.Time, err error) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.teams[name]; !ok {
		return nil, time.Time{}, ErrNoTeam
	}
	open := 0
	for h, inv := range s.invites {
		switch {
		case !now.Before(inv.expires):
			delete(s.invites, h)
		case inv.team == name:
			open++
		}
	}
	if open >= MaxOpenInvites {
		return nil, time.Time{}, ErrTooManyInvites
	}
	secret = make([]byte, SecretSize)
	rand.Read(secret)
	// Whole seconds, the precision of an invite code.
	expires = now.Add(InviteTTL).Truncate(time.Second)
	s.invites[sha256.Sum256(secret)] = invite{team: name, expires: expires}
	return secret, expires, nil
}

// CheckInvite checks, without a change, the join of joiner with label and
// secret into team name, and returns the topics of the team. It fails with
// ErrInviteRefused for a secret that is not an open invite of the team, and
// only then with ErrLabelTaken or ErrTeamFull. A label of a member that is
// not connected to this machine is free: that member left with its old peer
// id, for example in a restart.
func (s *Set) CheckInvite(name string, secret []byte, joiner, label string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, _, _, err := s.checkInvite(name, secret, joiner, label)
	if err != nil {
		return nil, err
	}
	return slices.Clone(t.topics), nil
}

// AcceptInvite is CheckInvite, then it uses the invite and adds joiner with
// label, with a clock join counter. In the same step it marks left the
// member that is not connected and held label. It returns the team after
// the join.
func (s *Set) AcceptInvite(name string, secret []byte, joiner, label string) (Team, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, hash, gone, err := s.checkInvite(name, secret, joiner, label)
	if err != nil {
		return Team{}, err
	}
	delete(s.invites, hash)
	if e, ok := t.members[gone]; ok {
		e.Left = true
		t.members[gone] = e
	}
	t.members[joiner] = Entry{PeerID: joiner, Label: label, Join: s.clock(t.members[joiner].Join)}
	s.index()
	return t.copy(name), nil
}

// checkInvite checks the secret before anything else, so every failure
// before it is ErrInviteRefused. gone is the member that is not connected
// and holds label, which the join marks left. s.mu is held.
func (s *Set) checkInvite(name string, secret []byte, joiner, label string) (t *state, hash [sha256.Size]byte, gone string, err error) {
	if len(secret) != SecretSize || CheckName(name) != nil || CheckPeerID(joiner) != nil {
		return nil, hash, "", ErrInviteRefused
	}
	hash = sha256.Sum256(secret)
	inv, ok := s.invites[hash]
	t = s.teams[name]
	if !ok || inv.team != name || !s.now().Before(inv.expires) || t == nil || joiner == s.self ||
		t.members[joiner].Join == math.MaxUint64 {
		return nil, hash, "", ErrInviteRefused
	}
	if err := CheckLabel(label); err != nil {
		return nil, hash, "", err
	}
	members := 0
	for id, e := range t.members {
		if e.Left || id == joiner {
			continue
		}
		if e.Label == label {
			if id == s.self || s.connected == nil || s.connected(id) {
				return nil, hash, "", ErrLabelTaken
			}
			gone = id
			continue
		}
		members++
	}
	if members >= MaxMembers {
		return nil, hash, "", ErrTeamFull
	}
	return t, hash, gone, nil
}

// CheckJoin checks, without a change, that this machine can join team name
// with topics: it holds no team of that name, follows fewer than MaxTeams
// teams, and no other team owns one of the topics. With nil topics it checks
// the name and the count only, as a joiner does before it dials the
// inviter.
func (s *Set) CheckJoin(name string, topics []string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.checkNew(name); err != nil {
		return err
	}
	if topics == nil {
		return nil
	}
	_, err := s.checkTopics(topics)
	return err
}

// Join stores team name with topics and the member list that inviter sent at
// the join of this machine, self, with label. It checks as CheckJoin does.
// The list must hold inviter, and self with label, as members, else Join
// fails with ErrJoinList and stores nothing. It skips the entries that break
// a rule of Entry, and the members past MaxMembers.
func (s *Set) Join(self, name string, topics []string, inviter string, entries []Entry, label string) (Team, error) {
	members := Members{}
	for _, e := range entries {
		if e.Validate() == nil {
			members.Merge(e)
		}
	}
	own := members[self]
	if !members.Has(inviter) || inviter == self || own.Left || own.Label != label {
		return Team{}, ErrJoinList
	}
	for _, e := range members.Active() {
		if members.count() <= MaxMembers {
			break
		}
		if e.PeerID != self && e.PeerID != inviter {
			delete(members, e.PeerID)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkNew(name); err != nil {
		return Team{}, err
	}
	topics, err := s.checkTopics(topics)
	if err != nil {
		return Team{}, err
	}
	if err := s.setSelf(self); err != nil {
		return Team{}, err
	}
	t := &state{topics: topics, members: members}
	s.teams[name] = t
	s.index()
	return t.copy(name), nil
}

// Queued is a member that a send on a team topic queues a message for: its
// peer id, and its label as Members.Member renders it.
type Queued struct {
	PeerID string
	Member string
}

// Route is where a send on a team topic goes beyond this machine: the team,
// and the members that get the message.
type Route struct {
	Team  string
	Peers []Queued
}

// Route resolves the recipient to of a send on topic. On a topic of no team,
// it returns an empty route, or ErrNotTeamTopic when to is
// <member label>/<agent>. On a team topic, the route holds every other
// member for a broadcast, the one member that to names for
// <member label>/<agent>, and none for a local agent. A label that names no
// other member, or two of them without the peer id suffix, fails with
// ErrUnknownRemote or ErrAmbiguousRecipient.
func (s *Set) Route(topic, to string) (Route, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	remote := strings.Contains(to, "/")
	name, ok := s.topics[topic]
	if !ok {
		if remote {
			return Route{}, ErrNotTeamTopic
		}
		return Route{}, nil
	}
	m := s.teams[name].members
	r := Route{Team: name}
	var ids []string
	switch {
	case remote:
		id, err := resolve(m, s.self, to)
		if err != nil {
			return Route{}, err
		}
		ids = []string{id}
	case to == "":
		for _, e := range m.Active() {
			if e.PeerID != s.self {
				ids = append(ids, e.PeerID)
			}
		}
	}
	for _, id := range ids {
		r.Peers = append(r.Peers, Queued{PeerID: id, Member: m.Member(id)})
	}
	return r, nil
}

// resolve returns the peer id of the other member of m that the address to,
// <label>[~<suffix>]/<agent>, names.
func resolve(m Members, self, to string) (string, error) {
	member, _, err := message.SplitAddress(to)
	if err != nil || member == "" {
		return "", ErrUnknownRemote
	}
	label, suffix, _ := strings.Cut(member, "~")
	var found []string
	for id, e := range m {
		if id != self && !e.Left && e.Label == label && strings.HasSuffix(id, suffix) {
			found = append(found, id)
		}
	}
	switch len(found) {
	case 0:
		return "", ErrUnknownRemote
	case 1:
		return found[0], nil
	}
	return "", ErrAmbiguousRecipient
}
