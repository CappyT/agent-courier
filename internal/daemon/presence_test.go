//go:build linux

package daemon

import (
	"bytes"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/team"
)

// lockedBuffer is a log sink that goroutines share.
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

// machine is a daemon with its p2p side, its host on loopback.
type machine struct {
	*testDaemon
	p   *P2P
	key crypto.PrivKey
	id  libp2ppeer.ID
	// self is the peer id of the machine key.
	self string
}

// startMachine starts a daemon whose setup gets its member set and the peer
// id of key, and its p2p side with the host running. log is the log of both; nil discards
// the log of the p2p side, because the host makes it the log of libp2p,
// which is global and outlives the test.
func startMachine(t *testing.T, now func() time.Time, key crypto.PrivKey, log *slog.Logger, setup func(*team.Set, string)) *machine {
	t.Helper()
	id, err := libp2ppeer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	self := id.String()
	srvLog, p2pLog := log, log
	if log == nil {
		srvLog, p2pLog = testLogger(t), slog.New(slog.DiscardHandler)
	}
	m := &machine{key: key, self: self}
	m.testDaemon = startDaemonSrv(t, now, nil, func(srv *Server) {
		srv.log = srvLog
		setup(srv.Teams(), self)
		m.p = NewP2P(srv.store, srv.Teams(), p2pLog, P2PConfig{
			Key:    func() (crypto.PrivKey, error) { return key, nil },
			Listen: []string{"/ip4/127.0.0.1/tcp/0"},
		})
		srv.SetPresence(m.p)
	})
	t.Cleanup(m.p.stop)
	m.p.sync(t.Context())
	if m.p.Host() == nil {
		t.Fatal("no host")
	}
	m.id = m.p.Host().ID()
	return m
}

// connect dials a from b over TCP on loopback.
func connect(t *testing.T, a, b *machine) {
	t.Helper()
	var addrs []ma.Multiaddr
	for _, addr := range a.p.Host().Host().Network().ListenAddresses() {
		if _, err := addr.ValueForProtocol(ma.P_TCP); err == nil {
			addrs = append(addrs, addr)
		}
	}
	if err := b.p.Host().Host().Connect(t.Context(), libp2ppeer.AddrInfo{ID: a.id, Addrs: addrs}); err != nil {
		t.Fatal(err)
	}
}

// feedWithoutConnection lets the tests of m deliver presence frames from
// peers that are not connected, and returns the switch of that liveness.
func feedWithoutConnection(m *machine) *atomic.Bool {
	var up atomic.Bool
	up.Store(true)
	m.p.remote.mu.Lock()
	defer m.p.remote.mu.Unlock()
	m.p.remote.ps.alive = func(libp2ppeer.ID) bool { return up.Load() }
	return &up
}

func listAgents(p *peer) agentsResult {
	p.t.Helper()
	r := p.rpc("agents", nil)
	if r.Error != nil {
		p.t.Fatalf("agents: %+v", r.Error)
	}
	var res agentsResult
	if err := json.Unmarshal(r.Result, &res, json.RejectUnknownMembers(true)); err != nil {
		p.t.Fatal(err)
	}
	return res
}

func listed(res agentsResult, e agentEntry) bool { return slices.Contains(res.Agents, e) }

// newTeam creates team name with topics, self with label as its creator, and
// adds members as joins with invites of self do.
func newTeam(t *testing.T, set *team.Set, self, name, label string, topics []string, members ...team.Entry) {
	t.Helper()
	if _, err := set.Create(self, name, topics, label); err != nil {
		t.Fatal(err)
	}
	for _, e := range members {
		addMember(t, set, name, e.PeerID, e.Label)
	}
}

// memberEntry is the member list entry of the machine with key, with label.
func memberEntry(t *testing.T, label string, key crypto.PrivKey) team.Entry {
	t.Helper()
	id, err := libp2ppeer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return team.Entry{PeerID: id.String(), Label: label, Join: 1}
}

// memberLeaves merges into the member set of m the entry of the member id
// marked left, as id sends it when it leaves team name.
func memberLeaves(t *testing.T, m *machine, name string, id libp2ppeer.ID) {
	t.Helper()
	tm, ok := m.p.set.Get(name)
	if !ok {
		t.Fatalf("no team %s", name)
	}
	e := tm.Members[id.String()]
	e.Left = true
	if changed, err := m.p.set.Merge(name, id.String(), []team.Entry{e}); err != nil || !changed {
		t.Fatalf("leave of %s: changed %v, %v", id, changed, err)
	}
	m.p.Changed()
}

// Scenario "Member comes online": Bob's daemon connects to A with an
// OpenCode session open on a shared topic, and agents on A lists
// bob/opencode-agent-courier as online. A's session gets its online through
// the push path. The presence follows the session, and ends when the
// connection does. No agent name of Bob's machine reaches A's log.
func TestRemotePresence(t *testing.T) {
	aKey, _ := newKey(t)
	bKey, _ := newKey(t)
	var aLog lockedBuffer
	a := startMachine(t, nil, aKey, slog.New(slog.NewTextHandler(&aLog, &slog.HandlerOptions{Level: slog.LevelDebug})),
		func(set *team.Set, self string) {
			newTeam(t, set, self, "courier", "alpha", []string{"t"}, memberEntry(t, "bob", bKey))
		})
	b := startMachine(t, nil, bKey, nil, func(set *team.Set, self string) {
		newTeam(t, set, self, "courier", "bob", []string{"t"}, memberEntry(t, "alpha", aKey))
	})
	alice := a.join(t, "claude-agent-courier", "t")
	bob := b.join(t, "opencode-agent-courier", "t")
	connect(t, a, b)

	remote := agentEntry{Topic: "t", Name: "bob/opencode-agent-courier", Live: true}
	eventually(t, "A to list Bob's agent", func() bool { return listed(listAgents(alice), remote) })
	eventually(t, "B to list A's agent", func() bool {
		return listed(listAgents(bob), agentEntry{Topic: "t", Name: "alpha/claude-agent-courier", Live: true})
	})
	res := listAgents(alice)
	if !strings.Contains(res.Text, "t bob/opencode-agent-courier live") {
		t.Errorf("agents text %q does not show Bob's agent as live", res.Text)
	}
	got := alice.deliver()
	if got.Attrs["kind"] != "online" || got.Attrs["sender"] != "bob/opencode-agent-courier" || got.Attrs["topic"] != "t" {
		t.Fatalf("deliver %v, want the online of bob/opencode-agent-courier on t", got.Attrs)
	}
	a.wantDeliver(t, got, "claude-agent-courier", got.Attrs["id"])
	if !strings.HasPrefix(got.Text, "bob/opencode-agent-courier -> everyone: online on t") {
		t.Errorf("online text %q", got.Text)
	}

	bob.leave()
	eventually(t, "A to drop the agent of the closed session", func() bool { return !listed(listAgents(alice), remote) })
	bob = b.join(t, "opencode-agent-courier", "t")
	eventually(t, "A to list Bob's agent again", func() bool { return listed(listAgents(alice), remote) })
	// One online per agent and topic in store.OnlineEvery, as for a local
	// agent.
	alice.noDeliver()

	b.p.stop()
	eventually(t, "A to drop Bob's presence with the connection", func() bool { return !listed(listAgents(alice), remote) })
	if strings.Contains(aLog.String(), "opencode-agent-courier") {
		t.Error("an agent name of Bob's machine reached A's log")
	}
}

// The access rule of presence: an entry counts only when its sender is a
// member of the team that owns its topic, at most presencePerTopic per
// member and topic. Topics of other teams and of no team are ignored. The
// presence of a member ends when it leaves the team and after presenceTTL
// without a frame. agents lists the local agents first, at most
// presenceListed remote agents per member and topic, within its cap.
func TestPresenceAccessRule(t *testing.T) {
	clock := &fakeClock{t: budgetT0}
	key, _ := newKey(t)
	bKey, bob := newKey(t)
	cKey, carol := newKey(t)
	a := startMachine(t, clock.now, key, nil, func(set *team.Set, self string) {
		newTeam(t, set, self, "courier", "alpha", []string{"t"}, memberEntry(t, "bob", bKey))
		newTeam(t, set, self, "other", "alpha", []string{"o"}, memberEntry(t, "carol", cKey))
	})
	w := a.join(t, "watcher", "local", "o", "t")
	feedWithoutConnection(a)
	deliver := a.p.deliver(a.p.current().members)
	send := func(from libp2ppeer.ID, entries ...p2p.PresenceEntry) {
		t.Helper()
		if _, err := deliver(t.Context(), from, p2p.PresenceFrame{Agents: entries}); err != nil {
			t.Fatal(err)
		}
	}
	// kept are the entries that the table holds for id.
	kept := func(id libp2ppeer.ID, entries ...p2p.PresenceEntry) {
		t.Helper()
		a.p.remote.mu.Lock()
		defer a.p.remote.mu.Unlock()
		var got []p2p.PresenceEntry
		if rp := a.p.remote.peers[id]; rp != nil {
			got = slices.SortedFunc(maps.Keys(rp.agents), comparePresence)
		}
		if !slices.Equal(got, entries) {
			t.Errorf("the table holds %v, want %v", got, entries)
		}
	}
	want := func(topic string, names ...string) {
		t.Helper()
		if got := a.p.RemoteAgents(topic); !slices.Equal(got, names) {
			t.Errorf("RemoteAgents(%s) = %v, want %v", topic, got, names)
		}
	}

	t.Run("not a member of the team", func(t *testing.T) {
		send(carol, p2p.PresenceEntry{Topic: "t", Agent: "x"}, p2p.PresenceEntry{Topic: "o", Agent: "y"})
		want("t")
		want("o", "carol/y")
		kept(carol, p2p.PresenceEntry{Topic: "o", Agent: "y"})
	})
	t.Run("topic of another team or of none", func(t *testing.T) {
		send(bob, p2p.PresenceEntry{Topic: "t", Agent: "a"}, p2p.PresenceEntry{Topic: "o", Agent: "b"},
			p2p.PresenceEntry{Topic: "local", Agent: "c"})
		want("t", "bob/a")
		want("o", "carol/y")
		want("local")
		kept(bob, p2p.PresenceEntry{Topic: "t", Agent: "a"})
	})
	t.Run("onlines of the kept entries only", func(t *testing.T) {
		w.onlines("carol/y", "bob/a")
		w.noDeliver()
		if res := w.pending(); res.Total != 2 {
			t.Errorf("watcher holds %d messages, want the 2 onlines:\n%s", res.Total, res.Text)
		}
	})
	t.Run("agents lists local agents first and remote ones within their caps", func(t *testing.T) {
		var many []p2p.PresenceEntry
		for i := range presencePerTopic + 50 {
			many = append(many, p2p.PresenceEntry{Topic: "o", Agent: fmt.Sprintf("agent-%03d", i)})
		}
		send(carol, many...)
		a.p.remote.mu.Lock()
		held := len(a.p.remote.peers[carol].agents)
		a.p.remote.mu.Unlock()
		if held != presencePerTopic {
			t.Errorf("the table holds %d of carol's agents, want the cap of %d", held, presencePerTopic)
		}
		if got := a.p.RemoteAgents("o"); len(got) != presenceListed || got[0] != "carol/agent-000" {
			t.Errorf("RemoteAgents(o) lists %d from %v, want the first %d", len(got), got[:min(len(got), 1)], presenceListed)
		}
		res := listAgents(w)
		// watcher on 3 topics, bob/a on t, carol's listed agents on o.
		if wantTotal := 3 + 1 + presenceListed; res.Total != wantTotal {
			t.Errorf("agents counts %d, want %d", res.Total, wantTotal)
		}
		for i := range maxAgents {
			if _, err := a.store.RegisterTopics(t.Context(), fmt.Sprintf("local-%03d", i), []string{"o"}); err != nil {
				t.Fatal(err)
			}
		}
		res = listAgents(w)
		wantTotal := 3 + maxAgents + 1 + presenceListed
		if res.Total != wantTotal || len(res.Agents) != maxAgents {
			t.Errorf("agents lists %d of %d, want %d of %d", len(res.Agents), res.Total, maxAgents, wantTotal)
		}
		if i := slices.IndexFunc(res.Agents, func(e agentEntry) bool { return strings.Contains(e.Name, "/") }); i >= 0 {
			t.Errorf("agents lists the remote agent %s and hides local ones", res.Agents[i].Name)
		}
		if !strings.Contains(res.Text, fmt.Sprintf("%d more not shown.", wantTotal-maxAgents)) {
			t.Errorf("agents text does not count the entries over the cap:\n%s", res.Text)
		}
	})
	t.Run("member leaves the team", func(t *testing.T) {
		memberLeaves(t, a, "courier", bob)
		want("t")
	})
	t.Run("expiry", func(t *testing.T) {
		clock.set(budgetT0.Add(presenceTTL - time.Second))
		if len(a.p.RemoteAgents("o")) == 0 {
			t.Fatal("carol's presence expired before presenceTTL")
		}
		clock.set(budgetT0.Add(presenceTTL))
		want("o")
		a.p.remote.prune(a.store.Now(), a.p.IsMember)
		a.p.remote.mu.Lock()
		defer a.p.remote.mu.Unlock()
		if rp := a.p.remote.peers[carol]; rp == nil || len(rp.agents) != 0 {
			t.Errorf("prune left carol's expired agents, or dropped her online times: %+v", rp)
		}
		if _, ok := a.p.remote.peers[bob]; ok {
			t.Error("prune kept a peer outside the member set")
		}
	})
}

// Remote onlines pass the single push path with a cap per member: a session
// gets at most 30 onlines from one member in an hour, and the others stay
// pending. Another member, and a local agent, have their own budget. The cap
// counts by peer id: a new label for the same member does not pass it.
func TestRemoteOnlineCapPerMember(t *testing.T) {
	clock := &fakeClock{t: budgetT0}
	d := startDaemonWith(t, clock.now, nil)
	w := d.join(t, "watcher", "t")
	ctx := t.Context()
	var flood []string
	for i := range remoteOnlineBudget.limit + 5 {
		name := fmt.Sprintf("bob/agent-%02d", i)
		flood = append(flood, name)
		d.srv.remoteOnline(ctx, "t", name, "peerb")
	}
	d.srv.remoteOnline(ctx, "t", "bobby/renamed", "peerb")
	d.srv.remoteOnline(ctx, "t", "carol/agent", "peerc")
	w.onlines(flood[:remoteOnlineBudget.limit]...)
	w.onlines("carol/agent")
	d.join(t, "local", "t")
	w.onlines("local")
	w.noDeliver()
	// One pending online per member and topic: the last of each.
	if res := w.pending(); res.Total != 3 || !strings.Contains(res.Text, "bobby/renamed") || strings.Contains(res.Text, flood[0]) {
		t.Errorf("watcher holds %d messages, want the last online of each member and the local one:\n%s", res.Total, res.Text)
	}
	ps, _, err := d.store.Pending(ctx, "watcher", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ps {
		if strings.Contains(p.Sender, "/") != (p.Origin != "") || p.Sender == "carol/agent" && p.Origin != "peerc" {
			t.Errorf("online of %s has origin %q", p.Sender, p.Origin)
		}
	}

	clock.set(budgetT0.Add(remoteOnlineBudget.window))
	d.srv.remoteOnline(ctx, "t", "bob/later", "peerb")
	w.onlines("bob/later")
	w.noDeliver()
}

// The daemon creates at most remoteOnlineBudget.limit remote onlines per
// member in an hour. It applies at most presenceFrameBurst frames of a
// member at once, and the latest frame of a burst later. A frame that
// arrives after the member disconnected, or while the host stops, counts
// for nothing.
func TestPresenceFrameBudgetAndLiveness(t *testing.T) {
	key, _ := newKey(t)
	bKey, bob := newKey(t)
	a := startMachine(t, nil, key, nil, func(set *team.Set, self string) {
		newTeam(t, set, self, "courier", "alpha", []string{"t"}, memberEntry(t, "bob", bKey))
	})
	w := a.join(t, "watcher", "t")
	up := feedWithoutConnection(a)
	deliver := a.p.deliver(a.p.current().members)
	send := func(agents ...string) {
		t.Helper()
		f := p2p.PresenceFrame{Agents: []p2p.PresenceEntry{}}
		for _, ag := range agents {
			f.Agents = append(f.Agents, p2p.PresenceEntry{Topic: "t", Agent: ag})
		}
		if _, err := deliver(t.Context(), bob, f); err != nil {
			t.Fatal(err)
		}
	}
	listedNow := func() []string { return a.p.RemoteAgents("t") }

	t.Run("onlines per member", func(t *testing.T) {
		var first, second []string
		for i := range remoteOnlineBudget.limit + 10 {
			first = append(first, fmt.Sprintf("a-%02d", i))
			second = append(second, fmt.Sprintf("b-%02d", i))
		}
		send(first...)
		send(second...)
		a.p.remote.mu.Lock()
		created := len(a.p.remote.peers[bob].onlines)
		a.p.remote.mu.Unlock()
		if created != remoteOnlineBudget.limit {
			t.Errorf("the daemon created %d onlines of bob, want %d", created, remoteOnlineBudget.limit)
		}
		var want []string
		for _, ag := range first[:remoteOnlineBudget.limit] {
			want = append(want, "bob/"+ag)
		}
		w.onlines(want...)
		w.noDeliver()
		if res := w.pending(); res.Total != 1 {
			t.Errorf("watcher holds %d onlines of bob on t, want 1:\n%s", res.Total, res.Text)
		}
	})
	t.Run("the latest frame of a burst", func(t *testing.T) {
		last := fmt.Sprintf("burst-%02d", presenceFrameBurst+4)
		for i := range presenceFrameBurst + 5 {
			send(fmt.Sprintf("burst-%02d", i))
		}
		if got := listedNow(); slices.Equal(got, []string{"bob/" + last}) {
			t.Fatal("a frame over the budget counted at once")
		}
		eventually(t, "the latest frame of the burst", func() bool { return slices.Equal(listedNow(), []string{"bob/" + last}) })
	})
	t.Run("frame after the member disconnected", func(t *testing.T) {
		up.Store(false)
		a.p.remote.drop(bob)
		// A token is back, so only the connection check stops the frame.
		time.Sleep(time.Second/presenceFrameRate + 100*time.Millisecond)
		send("late")
		if got := listedNow(); len(got) != 0 {
			t.Errorf("a frame after the disconnect counted: %v", got)
		}
		up.Store(true)
	})
	t.Run("frame while the host stops", func(t *testing.T) {
		a.p.stop()
		send("after-stop")
		if got := listedNow(); len(got) != 0 {
			t.Errorf("a frame after the host stopped counted: %v", got)
		}
	})
}

// The store refuses a remote online whose sender does not have the form of
// team.Members.Sender, without quoting it.
func TestRemoteOnlineSenderForm(t *testing.T) {
	st := openStore(t, nil)
	if _, err := st.RegisterTopics(t.Context(), "watcher", []string{"t"}); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"bob/agent", "bob~AbC123/agent"} {
		if _, err := st.RemoteOnline(t.Context(), "t", s, "peerb"); err != nil {
			t.Errorf("RemoteOnline(%q): %v", s, err)
		}
	}
	for _, s := range []string{"zq", "/zq", "bob/", "Bob/zq", "bob/zq/x", "bob~abc/zq", "bob~/zq", "bob/<zq>"} {
		_, err := st.RemoteOnline(t.Context(), "t", s, "peerb")
		if err == nil {
			t.Errorf("RemoteOnline(%q) stored", s)
			continue
		}
		if strings.Contains(err.Error(), s) {
			t.Errorf("the error %q quotes the sender", err)
		}
	}
	for _, origin := range []string{"", "peer b"} {
		if _, err := st.RemoteOnline(t.Context(), "t", "bob/agent", origin); err == nil {
			t.Errorf("RemoteOnline with origin %q stored", origin)
		}
	}
}

// The presence frame of a machine with the most live agents holds as many
// as fit the frame cap, at most presencePerTopic per topic, and only on the
// topics of the teams that the member shares.
func TestPresenceFrameFits(t *testing.T) {
	_, self := newKey(t)
	_, bob := newKey(t)
	set := team.NewSet(nil)
	p := NewP2P(nil, set, slog.New(slog.DiscardHandler), P2PConfig{})
	topics := make([]string, team.MaxTopics)
	live := map[string][]string{"other": {"x"}}
	for i := range topics {
		topics[i] = fmt.Sprintf("%02d", i) + strings.Repeat("t", 62)
		for j := range presencePerTopic + 20 {
			live[topics[i]] = append(live[topics[i]], fmt.Sprintf("%03d", j)+strings.Repeat("a", 45))
		}
	}
	newTeam(t, set, self.String(), "courier", "alpha", topics, team.Entry{PeerID: bob.String(), Label: "bob"})
	newTeam(t, set, self.String(), "other", "alpha", []string{"other"})
	f, dropped := p.presenceFor(bob, live)
	if len(f.Agents) == 0 || dropped != team.MaxTopics*(presencePerTopic+20)-len(f.Agents) {
		t.Fatalf("%d agents and %d dropped", len(f.Agents), dropped)
	}
	var buf bytes.Buffer
	if err := p2p.WriteFrame(&buf, f); err != nil {
		t.Fatalf("the packed frame breaks the wire protocol: %v", err)
	}
	if slices.ContainsFunc(f.Agents, func(e p2p.PresenceEntry) bool { return e.Topic == "other" }) {
		t.Error("the frame holds a topic of a team that the member does not share")
	}
	small, dropped := p.presenceFor(bob, map[string][]string{topics[0]: {"a", "b"}, "other": {"x"}})
	if dropped != 0 || !slices.Equal(small.Agents, []p2p.PresenceEntry{{Topic: topics[0], Agent: "a"}, {Topic: topics[0], Agent: "b"}}) {
		t.Errorf("frame %v, dropped %d", small.Agents, dropped)
	}
}
