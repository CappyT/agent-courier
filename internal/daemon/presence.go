//go:build unix

package daemon

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"

	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/team"
)

const (
	// presenceRefresh is how often the daemon sends its presence to the
	// connected members without a change.
	presenceRefresh = 2 * time.Minute
	// presenceTTL is how long the presence of a member stays without a new
	// frame: two refreshes can get lost.
	presenceTTL = 5 * time.Minute
	// presenceTimeout bounds the send of one presence frame.
	presenceTimeout = 10 * time.Second
	// presencePerTopic is the most agents of one member on one topic that
	// a presence frame carries and that the daemon keeps.
	presencePerTopic = p2p.MaxPresence / team.MaxTopics
	// presenceEnvelope is room for the type and the brackets of a presence
	// frame around its entries.
	presenceEnvelope = 64
	// presenceListed is the most agents of one member on one topic that
	// agents lists.
	presenceListed = 20
	// Presence frames a member may send: presenceFrameRate a second, with a
	// burst of presenceFrameBurst.
	presenceFrameRate  = 1
	presenceFrameBurst = 16
)

// SetPresence connects the server and the p2p side for remote presence:
// the server lists the remote agents of p in agents and tells p when its
// live agents change; p sends them to the members and stores the onlines of
// remote agents through the server. Call it before Serve and Run.
func (s *Server) SetPresence(p *P2P) {
	s.presence = p
	p.local = s
}

// presenceChanged tells the p2p side that the live agents changed.
func (s *Server) presenceChanged() {
	if s.presence != nil {
		s.presence.LocalChanged()
	}
}

// liveAgents lists, by topic and sorted, the agents that a session holds.
func (s *Server) liveAgents() map[string][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	live := map[string][]string{}
	for name, ss := range s.names {
		for _, t := range ss.topics {
			live[t] = append(live[t], name)
		}
	}
	for _, names := range live {
		slices.Sort(names)
	}
	return live
}

// remoteOnline stores the online of the remote agent sender on topic, from
// the member whose peer id is origin, and pushes it to the live recipients,
// through the single push path and its cap per member.
func (s *Server) remoteOnline(ctx context.Context, topic, sender, origin string) {
	sent, err := s.store.RemoteOnline(ctx, topic, sender, origin)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Error("store a remote online", "topic", topic, "err", err)
		}
		return
	}
	for _, r := range sent.Recipients {
		s.pushTo(r, sent.Message)
	}
}

// remoteAgents lists the live remote agents on topic, sorted.
func (s *Server) remoteAgents(topic string) []string {
	if s.presence == nil {
		return nil
	}
	return s.presence.RemoteAgents(topic)
}

// remotePeer is the presence of one member.
type remotePeer struct {
	// agents are the agents online on its machine, from its last frame.
	agents map[p2p.PresenceEntry]bool
	// expires is when agents goes stale without a new frame.
	expires time.Time
	// onlines are when the daemon last stored the online of an agent, so it
	// stores at most one per agent and topic in store.OnlineEvery.
	onlines map[p2p.PresenceEntry]time.Time
	// tokens and last are the token bucket of presence frames, on the wall
	// clock. pending is the latest frame over the budget, which a timer
	// applies when a token is back, because the sender sends only changes.
	tokens  float64
	last    time.Time
	pending map[p2p.PresenceEntry]bool
}

// remoteTable is the presence of the members, in memory.
type remoteTable struct {
	mu    sync.Mutex
	peers map[libp2ppeer.ID]*remotePeer
	// ps is the presence sender of the running node, nil while no node
	// runs. A frame counts only while ps runs and its sender is connected.
	ps *presenceSender
}

// attach makes ps the presence sender of the running node.
func (rt *remoteTable) attach(ps *presenceSender) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.ps = ps
}

// set replaces the presence of id with agents until expires, and returns
// the sender of the node and the agents whose online is due: new since the
// last frame, without an online in store.OnlineEvery, and within
// remoteOnlineBudget for id. It ignores the frame when no node runs or id is
// not connected, so a frame that a closing stream delivered does not
// outlive the connection. Over the frame budget at wall, it keeps agents as
// the pending frame and returns how long to wait before setting it, once
// per wait.
func (rt *remoteTable) set(id libp2ppeer.ID, agents map[p2p.PresenceEntry]bool, now, expires, wall time.Time) (ps *presenceSender, due []p2p.PresenceEntry, wait time.Duration) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.ps == nil || rt.ps.ctx.Err() != nil || !rt.ps.alive(id) {
		return nil, nil, 0
	}
	if rt.peers == nil {
		rt.peers = map[libp2ppeer.ID]*remotePeer{}
	}
	rp := rt.peers[id]
	if rp == nil {
		rp = &remotePeer{onlines: map[p2p.PresenceEntry]time.Time{}, tokens: presenceFrameBurst, last: wall}
		rt.peers[id] = rp
	}
	rp.tokens = min(presenceFrameBurst, rp.tokens+wall.Sub(rp.last).Seconds()*presenceFrameRate)
	rp.last = wall
	if rp.tokens < 1 {
		waiting := rp.pending != nil
		rp.pending = agents
		if waiting {
			return nil, nil, 0
		}
		return nil, nil, time.Duration((1 - rp.tokens) / presenceFrameRate * float64(time.Second))
	}
	rp.tokens--
	rp.pending = nil
	before := rp.agents
	if !now.Before(rp.expires) {
		before = nil
	}
	// An online time lives store.OnlineEvery, the window of
	// remoteOnlineBudget: len(rp.onlines) counts the onlines of the window.
	maps.DeleteFunc(rp.onlines, func(_ p2p.PresenceEntry, at time.Time) bool { return now.Sub(at) >= store.OnlineEvery })
	for _, e := range slices.SortedFunc(maps.Keys(agents), comparePresence) {
		if before[e] {
			continue
		}
		if _, ok := rp.onlines[e]; ok || len(rp.onlines) >= remoteOnlineBudget.limit {
			continue
		}
		rp.onlines[e] = now
		due = append(due, e)
	}
	rp.agents, rp.expires = agents, expires
	return rt.ps, due, 0
}

// takePending returns and forgets the pending frame of id.
func (rt *remoteTable) takePending(id libp2ppeer.ID) map[p2p.PresenceEntry]bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rp := rt.peers[id]
	if rp == nil {
		return nil
	}
	agents := rp.pending
	rp.pending = nil
	return agents
}

// drop forgets the agents and the pending frame of id, and keeps its online
// times.
func (rt *remoteTable) drop(id libp2ppeer.ID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rp := rt.peers[id]; rp != nil {
		rp.agents, rp.pending = nil, nil
	}
}

// prune forgets the stale agents, the old online times, and the peers left
// with neither or outside the member set.
func (rt *remoteTable) prune(now time.Time, isMember func(libp2ppeer.ID) bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	maps.DeleteFunc(rt.peers, func(id libp2ppeer.ID, rp *remotePeer) bool {
		if !now.Before(rp.expires) {
			rp.agents = nil
		}
		maps.DeleteFunc(rp.onlines, func(_ p2p.PresenceEntry, at time.Time) bool { return now.Sub(at) >= store.OnlineEvery })
		return !isMember(id) || len(rp.agents) == 0 && len(rp.onlines) == 0 && rp.pending == nil
	})
}

// clear forgets every agent and pending frame, and the presence sender,
// when the host stops.
func (rt *remoteTable) clear() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.ps = nil
	for _, rp := range rt.peers {
		rp.agents, rp.pending = nil, nil
	}
}

func comparePresence(a, b p2p.PresenceEntry) int {
	return cmp.Or(cmp.Compare(a.Topic, b.Topic), cmp.Compare(a.Agent, b.Agent))
}

// receivePresence takes the presence frame of the member from. It checks the
// frame again, and keeps an entry only when from is a member of the team
// that owns its topic, at most presencePerTopic per topic: it ignores the
// topics of other teams and the topics of no team. Then it stores the online
// of each new agent. No peer text reaches the log.
func (p *P2P) receivePresence(ctx context.Context, from libp2ppeer.ID, f p2p.PresenceFrame) error {
	if err := f.Validate(); err != nil {
		return err
	}
	kept := map[p2p.PresenceEntry]bool{}
	perTopic := map[string]int{}
	ignored := 0
	for _, e := range f.Agents {
		name, ok := p.set.TopicTeam(e.Topic)
		if !ok || !p.MemberOf(from, name) || perTopic[e.Topic] >= presencePerTopic {
			ignored++
			continue
		}
		perTopic[e.Topic]++
		kept[e] = true
	}
	if ignored > 0 {
		p.log.Debug("presence entries ignored", "peer", from, "ignored", ignored, "kept", len(kept))
	}
	p.setPresence(from, kept)
	return nil
}

// setPresence sets the presence of the member id to agents, and stores the
// online of each new agent. Over the frame budget of id, a timer sets the
// latest frame later.
func (p *P2P) setPresence(id libp2ppeer.ID, agents map[p2p.PresenceEntry]bool) {
	now := p.store.Now()
	ps, due, wait := p.remote.set(id, agents, now, now.Add(cmp.Or(p.presenceTTL, presenceTTL)), time.Now())
	if wait > 0 {
		time.AfterFunc(wait, func() {
			if pending := p.remote.takePending(id); pending != nil {
				p.setPresence(id, pending)
			}
		})
	}
	if ps == nil || p.local == nil {
		return
	}
	for _, e := range due {
		if sender, ok := p.remoteSender(id, e); ok {
			p.local.remoteOnline(ps.ctx, e.Topic, sender, id.String())
		}
	}
}

// remoteSender renders the agent of e on the machine id as <label>/<agent>,
// with the member list of the team of its topic. It fails when id is not a
// member of that team, so a member's presence on a team ends when it leaves
// the team, whatever the table holds.
func (p *P2P) remoteSender(id libp2ppeer.ID, e p2p.PresenceEntry) (string, bool) {
	name, ok := p.set.TopicTeam(e.Topic)
	if !ok {
		return "", false
	}
	return p.set.Sender(name, id.String(), e.Agent)
}

// RemoteAgents lists, sorted, the agents online on topic on the machines of
// the members of its team, as <label>/<agent>, the first presenceListed of
// each member. A member's presence ends when its last connection closes,
// after presenceTTL without a frame, or when it leaves the team:
// remoteSender checks the member list.
func (p *P2P) RemoteAgents(topic string) []string {
	if _, ok := p.set.TopicTeam(topic); !ok {
		return nil
	}
	now := p.store.Now()
	p.remote.mu.Lock()
	defer p.remote.mu.Unlock()
	var out []string
	for id, rp := range p.remote.peers {
		if !now.Before(rp.expires) {
			continue
		}
		var mine []string
		for e := range rp.agents {
			if e.Topic != topic {
				continue
			}
			if sender, ok := p.remoteSender(id, e); ok {
				mine = append(mine, sender)
			}
		}
		slices.Sort(mine)
		out = append(out, mine[:min(len(mine), presenceListed)]...)
	}
	slices.Sort(out)
	return out
}

// LocalChanged sends the presence of this machine to every connected member,
// after a session took or left an agent name.
func (p *P2P) LocalChanged() {
	if n := p.current(); n != nil && n.presence != nil {
		n.presence.kickAll()
	}
}

// presenceSender sends the presence of this machine to the members over the
// host of one node: when a member connects, when the live agents change and
// every presenceRefresh. It forgets a member's presence when the member's
// last connection closes.
type presenceSender struct {
	p       *P2P
	h       host.Host
	ctx     context.Context
	notifee *network.NotifyBundle
	// alive reports whether a member is connected. A field, so tests can
	// feed frames without a connection.
	alive func(libp2ppeer.ID) bool

	mu     sync.Mutex
	closed bool
	busy   map[libp2ppeer.ID]bool
	again  map[libp2ppeer.ID]bool
	wg     sync.WaitGroup
}

// startPresence starts the presence sender of a node. ctx ends with the node.
func (p *P2P) startPresence(ctx context.Context, h host.Host) *presenceSender {
	ps := &presenceSender{p: p, h: h, ctx: ctx, busy: map[libp2ppeer.ID]bool{}, again: map[libp2ppeer.ID]bool{}}
	ps.alive = func(id libp2ppeer.ID) bool { return connected(h.Network(), id) }
	ps.notifee = &network.NotifyBundle{
		ConnectedF: func(_ network.Network, c network.Conn) {
			if id := c.RemotePeer(); p.IsMember(id) {
				ps.kick(id)
			}
		},
		DisconnectedF: func(n network.Network, c network.Conn) {
			if id := c.RemotePeer(); !connected(n, id) {
				p.remote.drop(id)
			}
		},
	}
	h.Network().Notify(ps.notifee)
	p.remote.attach(ps)
	ps.wg.Go(ps.refresh)
	return ps
}

func connected(n network.Network, id libp2ppeer.ID) bool {
	c := n.Connectedness(id)
	return c == network.Connected || c == network.Limited
}

// refresh sends the presence to the connected members and prunes the table
// every presenceRefresh, until the node ends.
func (ps *presenceSender) refresh() {
	t := time.NewTicker(cmp.Or(ps.p.presenceRefresh, presenceRefresh))
	defer t.Stop()
	for {
		select {
		case <-ps.ctx.Done():
			return
		case <-t.C:
		}
		ps.p.remote.prune(ps.p.store.Now(), ps.p.IsMember)
		ps.kickAll()
	}
}

// close stops the sends and waits for the running ones. P2P.stop forgets
// the presence of the members after the host closes.
func (ps *presenceSender) close() {
	ps.h.Network().StopNotify(ps.notifee)
	ps.mu.Lock()
	ps.closed = true
	ps.mu.Unlock()
	ps.wg.Wait()
}

// kickAll sends the presence to every connected member.
func (ps *presenceSender) kickAll() {
	for _, s := range ps.p.set.Peers() {
		if id, err := libp2ppeer.Decode(s); err == nil && connected(ps.h.Network(), id) {
			ps.kick(id)
		}
	}
}

// kick sends the presence to id, unless a send to id runs: then that send
// runs once more when it ends, so the last frame holds the last state and
// frames to one member never overtake each other.
func (ps *presenceSender) kick(id libp2ppeer.ID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.closed {
		return
	}
	if ps.busy[id] {
		ps.again[id] = true
		return
	}
	ps.busy[id] = true
	ps.wg.Go(func() {
		for {
			if err := ps.send(id); err != nil {
				ps.p.log.Debug("presence send failed", "peer", id, "err", err)
			}
			ps.mu.Lock()
			if !ps.again[id] || ps.closed {
				delete(ps.busy, id)
				delete(ps.again, id)
				ps.mu.Unlock()
				return
			}
			delete(ps.again, id)
			ps.mu.Unlock()
		}
	})
}

// send writes the presence frame for id on one MsgProtocol stream over an
// open connection, and waits until id closes the stream. It never dials.
func (ps *presenceSender) send(id libp2ppeer.ID) error {
	if ps.p.local == nil {
		return nil
	}
	f, dropped := ps.p.presenceFor(id, ps.p.local.liveAgents())
	if dropped > 0 {
		ps.p.log.Debug("presence over the frame cap, agents left out", "peer", id, "dropped", dropped)
	}
	ctx, cancel := context.WithTimeout(ps.ctx, presenceTimeout)
	defer cancel()
	ctx = network.WithNoDial(network.WithAllowLimitedConn(ctx, "presence"), "presence")
	s, err := ps.h.NewStream(ctx, id, p2p.MsgProtocol)
	if err != nil {
		return err
	}
	defer s.Close()
	deadline, _ := ctx.Deadline()
	if err := s.SetDeadline(deadline); err != nil {
		return err
	}
	if err := p2p.WriteFrame(s, f); err != nil {
		s.Reset()
		return err
	}
	if err := s.CloseWrite(); err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, io.LimitReader(s, p2p.MaxFrame))
	return err
}

// presenceFor is the presence frame for the member id: the live agents on
// the topics of the teams that id shares with this machine, at most
// presencePerTopic per topic, sorted, within the frame cap. dropped counts
// the agents left out.
func (p *P2P) presenceFor(id libp2ppeer.ID, live map[string][]string) (f p2p.PresenceFrame, dropped int) {
	f.Agents = []p2p.PresenceEntry{}
	var topics []string
	for _, name := range p.set.TeamsOf(id.String()) {
		if t, ok := p.set.Get(name); ok {
			topics = append(topics, t.Topics...)
		}
	}
	slices.Sort(topics)
	size := presenceEnvelope
	for _, t := range topics {
		agents := live[t]
		if len(agents) > presencePerTopic {
			dropped += len(agents) - presencePerTopic
			agents = agents[:presencePerTopic]
		}
		for _, a := range agents {
			e := p2p.PresenceEntry{Topic: t, Agent: a}
			b, err := json.Marshal(e)
			if err != nil || size+len(b)+1 > p2p.MaxFrame {
				dropped++
				continue
			}
			size += len(b) + 1
			f.Agents = append(f.Agents, e)
		}
	}
	return f, dropped
}
