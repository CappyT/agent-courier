package p2p

import (
	"cmp"
	"context"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/CappyT/agent-courier/internal/team"
)

// leaveTimeout bounds the sends of a leave, which run in parallel, so a
// member that is offline does not hold the leave long.
const leaveTimeout = 10 * time.Second

// MemberSync keeps the member lists of the teams of this machine in step
// with the other members. It sends every list a member shares on each new
// connection to that member, and the list of a team to its members after a
// local change or a received change, and every list again on Resend. It
// merges a received list only from a member of that team. Members frames
// have no budget: they never reach a session, and members are trusted. The
// member set it reads and changes is the daemon's team.Set.
type MemberSync struct {
	ctx     context.Context
	h       host.Host
	set     *team.Set
	changed func(name string)
	log     *slog.Logger
	notifee *network.NotifyBundle
	wg      sync.WaitGroup
	// timeout bounds one send to a member, the dial included. A field, so
	// tests can lower it.
	timeout time.Duration

	mu     sync.Mutex
	closed bool
	// busy marks a running send to a peer, and again a send that must run
	// after it, so connections and changes in a burst cost one send each.
	busy  map[peer.ID]bool
	again map[peer.ID]bool
}

// NewMemberSync starts to watch the connections of h. changed runs after a
// received frame changed the list of a team; nil means nothing. ctx bounds
// every send.
func NewMemberSync(ctx context.Context, h host.Host, set *team.Set, changed func(name string), log *slog.Logger) *MemberSync {
	if changed == nil {
		changed = func(string) {}
	}
	r := &MemberSync{ctx: ctx, h: h, set: set, changed: changed, log: cmp.Or(log, slog.Default()),
		timeout: 30 * time.Second, busy: map[peer.ID]bool{}, again: map[peer.ID]bool{}}
	r.notifee = &network.NotifyBundle{ConnectedF: func(_ network.Network, c network.Conn) {
		if id := c.RemotePeer(); r.IsMember(id) {
			r.kick(id)
		}
	}}
	h.Network().Notify(r.notifee)
	return r
}

// Close stops the sends and waits for the running ones.
func (r *MemberSync) Close() {
	r.h.Network().StopNotify(r.notifee)
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.wg.Wait()
}

// IsMember is the IsMember of MsgHandler: id is a member of a team of this
// machine.
func (r *MemberSync) IsMember(id peer.ID) bool { return r.set.IsMember(id.String()) }

// MemberOf reports whether id is a member of team name: the rule of Deliver
// for a frame that names a team.
func (r *MemberSync) MemberOf(id peer.ID, name string) bool { return r.set.MemberOf(id.String(), name) }

// Changed sends the list of team name to each member of the team, after a
// local change, in the background.
func (r *MemberSync) Changed(name string) { r.kickTeam(name, "") }

// Resend sends every list to every member, in the background, so a member
// that missed a change learns it without a new connection.
func (r *MemberSync) Resend() {
	for _, id := range r.set.Peers() {
		if p, err := peer.Decode(id); err == nil {
			r.kick(p)
		}
	}
}

// kickTeam sends the lists to each member of team name but skip.
func (r *MemberSync) kickTeam(name string, skip peer.ID) {
	t, ok := r.set.Get(name)
	if !ok {
		return
	}
	self := r.h.ID()
	for _, e := range t.Members.Active() {
		if id, err := peer.Decode(e.PeerID); err == nil && id != self && id != skip {
			r.kick(id)
		}
	}
}

// SendLeft sends t, the list of a team this machine left with its own entry
// marked left, to each other member of t, in parallel, and waits for the
// sends. No member set holds t any more, so no other send reaches them.
func (r *MemberSync) SendLeft(t team.Team) {
	frames, _, err := PackMembers(t.Name, t.Members.Entries())
	if err != nil {
		r.log.Error("the member list of a left team does not pack", "team", t.Name, "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, leaveTimeout)
	defer cancel()
	self := r.h.ID()
	var wg sync.WaitGroup
	for _, e := range t.Members.Active() {
		id, err := peer.Decode(e.PeerID)
		if err != nil || id == self {
			continue
		}
		wg.Go(func() {
			if err := r.write(ctx, id, parseAddrs(e.Addrs), frames); err != nil {
				r.log.Debug("leave send failed", "peer", id, "err", err)
			}
		})
	}
	wg.Wait()
}

// Receive merges a members frame from the peer from. team.Set.Merge is the
// one check that from is a member of the team the frame names. When the
// frame changed the list, Receive runs changed and sends the list on to the
// other members.
func (r *MemberSync) Receive(from peer.ID, f MembersFrame) {
	changed, err := r.set.Merge(f.Team, from.String(), f.Entries)
	if err != nil {
		// Debug, and without the team: the name is the peer's.
		r.log.Debug("member list refused", "peer", from, "err", err)
		return
	}
	if changed {
		r.changed(f.Team)
		r.kickTeam(f.Team, from)
	}
}

// kick sends the lists id shares to id, unless a send to id runs: then that
// send runs once more when it ends.
func (r *MemberSync) kick(id peer.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if r.busy[id] {
		r.again[id] = true
		return
	}
	r.busy[id] = true
	r.wg.Go(func() {
		for {
			if err := r.send(id); err != nil {
				r.log.Debug("member list send failed", "peer", id, "err", err)
			}
			r.mu.Lock()
			if !r.again[id] || r.closed {
				delete(r.busy, id)
				delete(r.again, id)
				r.mu.Unlock()
				return
			}
			delete(r.again, id)
			r.mu.Unlock()
		}
	})
}

// send writes, on one MsgProtocol stream, the list of each team of which id
// is a member, and waits until the member closes the stream. It dials the
// addresses of every entry of id.
func (r *MemberSync) send(id peer.ID) error {
	var frames []MembersFrame
	var addrs []ma.Multiaddr
	for _, name := range r.set.TeamsOf(id.String()) {
		t, ok := r.set.Get(name)
		if !ok {
			continue
		}
		fs, skipped, err := PackMembers(t.Name, t.Members.Entries())
		if err != nil {
			return err
		}
		if skipped > 0 {
			r.log.Debug("member list entries fail the frame check, not sent", "team", t.Name, "entries", skipped)
		}
		frames = append(frames, fs...)
		for _, a := range parseAddrs(t.Members[id.String()].Addrs) {
			if !slices.ContainsFunc(addrs, a.Equal) {
				addrs = append(addrs, a)
			}
		}
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	defer cancel()
	return r.write(ctx, id, addrs, frames)
}

// parseAddrs parses the addresses of a member list entry, and skips the ones
// that do not parse.
func parseAddrs(addrs []string) []ma.Multiaddr {
	var out []ma.Multiaddr
	for _, s := range addrs {
		if a, err := ma.NewMultiaddr(s); err == nil {
			out = append(out, a)
		}
	}
	return out
}

// write sends frames to id on one MsgProtocol stream, dialing addrs when it
// must, and waits until id closes the stream. The stream may cross a relayed
// connection.
func (r *MemberSync) write(ctx context.Context, id peer.ID, addrs []ma.Multiaddr, frames []MembersFrame) error {
	if len(frames) == 0 {
		return nil
	}
	if len(addrs) > 0 {
		r.h.Peerstore().AddAddrs(id, addrs, peerstore.TempAddrTTL)
	}
	s, err := r.h.NewStream(network.WithAllowLimitedConn(ctx, "agent-courier"), id, MsgProtocol)
	if err != nil {
		return err
	}
	defer s.Close()
	deadline, _ := ctx.Deadline()
	if err := s.SetDeadline(deadline); err != nil {
		return err
	}
	for _, f := range frames {
		if err := WriteFrame(s, f); err != nil {
			s.Reset()
			return err
		}
	}
	if err := s.CloseWrite(); err != nil {
		return err
	}
	// The handler sends nothing for a members frame, and closes the stream
	// when it has read them all; a refused stream is reset.
	_, err = io.Copy(io.Discard, io.LimitReader(s, MaxFrame))
	return err
}
