//go:build unix

package daemon

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/team"
)

const (
	// p2pResync is how often the daemon checks its teams without a change
	// notice: it also writes the addresses of this machine into its member
	// list entries, which change when a reservation comes or goes.
	p2pResync = time.Minute
	// joinHold bounds how long a join keeps the host of a machine with no
	// team.
	joinHold = 2 * time.Minute
	// hostWait bounds the wait of team.invite for the host.
	hostWait = 10 * time.Second
)

var errHostDown = errors.New("the p2p host did not start")

// P2PConfig configures the libp2p host of the daemon.
type P2PConfig struct {
	// Key creates the machine key. P2P calls it once, when the machine
	// creates or joins its first team, and keeps the key in memory until the
	// daemon exits.
	Key func() (crypto.PrivKey, error)
	// Listen are the listen multiaddrs of the host.
	Listen []string
	// DefaultRelays is the relay list of a team that sets none.
	DefaultRelays []string
}

// P2P runs one libp2p host while this machine belongs to at least one team,
// or runs a join or a leave, and closes it when none holds. A machine
// without a team opens no network socket. P2P is the Network of the server.
// It reads and changes the server's team.Set, the one member set of the
// daemon.
type P2P struct {
	store *store.Store
	set   *team.Set
	log   *slog.Logger
	cfg   P2PConfig
	kick  chan struct{}
	// holds counts the joins and leaves that keep the host running.
	holds atomic.Int32

	// Fields, so tests can change them.
	resync   time.Duration
	joinHold time.Duration

	// Remote presence, in presence.go. local is set by Server.SetPresence;
	// zero durations are the defaults.
	local           *Server
	remote          remoteTable
	presenceRefresh time.Duration
	presenceTTL     time.Duration

	keyMu sync.Mutex
	key   crypto.PrivKey

	mu   sync.Mutex
	node *node
	// up is closed while a node runs.
	up chan struct{}
}

// node is a running host and the courier services on it.
type node struct {
	host    *p2p.Host
	join    *p2p.JoinService
	members *p2p.MemberSync
	cancel  context.CancelFunc
	// presence sends this machine's presence to the members.
	presence *presenceSender
	// outbox sends the send queue to the members.
	outbox *outboxWorker
}

// NewP2P returns the host lifecycle of the daemon over set, the teams of the
// server. Run drives it.
func NewP2P(st *store.Store, set *team.Set, log *slog.Logger, cfg P2PConfig) *P2P {
	return &P2P{store: st, set: set, log: log, cfg: cfg, kick: make(chan struct{}, 1), up: make(chan struct{}),
		resync: p2pResync, joinHold: joinHold}
}

// Self is the Network method that returns the peer id of this machine. It
// creates the machine key on the first call.
func (p *P2P) Self() (string, error) {
	key, err := p.machineKey()
	if err != nil {
		return "", err
	}
	id, err := libp2ppeer.IDFromPrivateKey(key)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func (p *P2P) machineKey() (crypto.PrivKey, error) {
	p.keyMu.Lock()
	defer p.keyMu.Unlock()
	if p.key == nil {
		key, err := p.cfg.Key()
		if err != nil {
			return nil, err
		}
		p.key = key
	}
	return p.key, nil
}

// Changed tells the lifecycle that the teams or a member list changed. It
// protects the connections of the members at once, and wakes the lifecycle
// without waiting for it. The member set drops the queued items of former
// members itself.
func (p *P2P) Changed() {
	if n := p.current(); n != nil {
		n.host.ProtectMembers()
	}
	select {
	case p.kick <- struct{}{}:
	default:
	}
	p.OutboxChanged()
}

// Host is the running host, or nil.
func (p *P2P) Host() *p2p.Host {
	if n := p.current(); n != nil {
		return n.host
	}
	return nil
}

func (p *P2P) current() *node {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.node
}

// IsMember reports whether id is a member of a team this machine belongs to.
func (p *P2P) IsMember(id libp2ppeer.ID) bool { return p.set.IsMember(id.String()) }

// MemberOf reports whether id is a member of the team name of this machine.
func (p *P2P) MemberOf(id libp2ppeer.ID, name string) bool { return p.set.MemberOf(id.String(), name) }

// Run checks the teams at start, after each Changed and every resync, until
// ctx ends. Then it closes the host. Every resync also sends the member
// lists again, so a member that missed a change learns it.
func (p *P2P) Run(ctx context.Context) {
	t := time.NewTicker(p.resync)
	defer t.Stop()
	defer p.stop()
	tick := false
	for {
		p.sync(ctx)
		if n := p.current(); tick && n != nil {
			n.members.Resend()
		}
		tick = false
		select {
		case <-ctx.Done():
			return
		case <-p.kick:
		case <-t.C:
			tick = true
		}
	}
}

// sync starts, updates or stops the host to match the teams and the running
// joins and leaves.
func (p *P2P) sync(ctx context.Context) {
	teams := p.set.List()
	if len(teams) == 0 && p.holds.Load() == 0 {
		p.stop()
		return
	}
	// Every team uses the default relay list.
	lists := make(map[string][]string, len(teams))
	for _, t := range teams {
		lists[t.Name] = p.cfg.DefaultRelays
	}
	n := p.current()
	if n == nil {
		if err := p.start(ctx, lists); err != nil {
			p.log.Error("start the p2p host", "err", err)
			return
		}
		p.log.Info("p2p host started", "teams", len(teams))
		n = p.current()
	} else {
		n.host.SetRelays(lists)
	}
	n.host.ProtectMembers()
	p.advertise(n)
}

// advertise writes the addresses of this machine into its entry of each
// team, and sends a list that changed to the members.
func (p *P2P) advertise(n *node) {
	for _, t := range p.set.List() {
		var addrs []string
		for _, a := range n.host.AdvertiseAddrs(t.Name) {
			addrs = append(addrs, a.String())
		}
		if p.set.SetAddrs(t.Name, addrs) {
			n.members.Changed(t.Name)
		}
	}
}

// start starts a host with the courier services: the join protocol, the
// member list sync and the msg protocol.
func (p *P2P) start(ctx context.Context, lists map[string][]string) error {
	key, err := p.machineKey()
	if err != nil {
		return err
	}
	h, err := p2p.StartHost(ctx, p2p.HostConfig{
		Key:      key,
		Listen:   slices.Clone(p.cfg.Listen),
		Relays:   lists,
		IsMember: p.IsMember,
		MemberOf: p.MemberOf,
		Log:      p.log,
	})
	if err != nil {
		return err
	}
	nctx, cancel := context.WithCancel(ctx)
	js, err := p2p.NewJoinService(h.Host(), p2p.JoinConfig{
		Set:     p.set,
		Addrs:   func() []ma.Multiaddr { return h.AdvertiseAddrs("") },
		Changed: p.TeamChanged,
		Log:     p.log,
	})
	if err != nil {
		cancel()
		h.Close()
		return err
	}
	ms := p2p.NewMemberSync(nctx, h.Host(), p.set, func(string) { p.Changed() }, p.log)
	msg := p2p.NewMsgHandler(nctx, p.IsMember, p.deliver(ms), p.log)
	h.Host().SetStreamHandler(p2p.MsgProtocol, msg.Handle)
	ps := p.startPresence(nctx, h.Host())
	ob := p.startOutbox(nctx, h)
	p.mu.Lock()
	p.node = &node{host: h, join: js, members: ms, cancel: cancel, presence: ps, outbox: ob}
	close(p.up)
	p.mu.Unlock()
	return nil
}

func (p *P2P) stop() {
	p.mu.Lock()
	n := p.node
	if n != nil {
		p.node = nil
		p.up = make(chan struct{})
	}
	p.mu.Unlock()
	if n == nil {
		return
	}
	n.cancel()
	n.outbox.close()
	n.presence.close()
	n.members.Close()
	if err := n.host.Close(); err != nil {
		p.log.Warn("close the p2p host", "err", err)
	}
	// After the close, so a presence frame that a closing stream delivered
	// does not outlive the host.
	p.remote.clear()
	p.log.Info("p2p host stopped")
}

// deliver takes the frames of the msg protocol.
func (p *P2P) deliver(ms *p2p.MemberSync) p2p.Deliver {
	return func(ctx context.Context, from libp2ppeer.ID, f p2p.Frame) (p2p.Frame, error) {
		switch f := f.(type) {
		case p2p.PresenceFrame:
			return nil, p.receivePresence(ctx, from, f)
		case p2p.MessageFrame:
			return p.receiveMessage(ctx, from, f)
		case p2p.ReceiptFrame:
			return nil, p.receiveReceipt(from, f)
		case p2p.MembersFrame:
			ms.Receive(from, f)
		}
		return nil, nil
	}
}

// waitNode waits until a node runs, and returns it.
func (p *P2P) waitNode(ctx context.Context) (*node, error) {
	for {
		p.mu.Lock()
		n, up := p.node, p.up
		p.mu.Unlock()
		if n != nil {
			return n, nil
		}
		select {
		case <-up:
		case <-ctx.Done():
			return nil, errHostDown
		}
	}
}

// InviteCode is the Network method of team.invite: the code holds the peer
// id of this machine and its addresses for team name, its circuit addresses
// first. It waits, within limits, for the host and its first reservation.
func (p *P2P) InviteCode(name string, secret []byte, expires time.Time) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), hostWait)
	defer cancel()
	n, err := p.waitNode(ctx)
	if err != nil {
		return "", err
	}
	// A code without a circuit address still works on the LAN, so a wait
	// that ends early is no error.
	_ = n.host.WaitReserved(ctx)
	return p2p.Invite{Team: name, Inviter: n.host.ID(), Addrs: n.host.AdvertiseAddrs(name), Secret: secret, Expires: expires}.Encode()
}

// Join is the Network method of team.join. On a machine with no team, it
// creates the machine key and keeps a host for the join until the join
// ends, at most joinHold.
func (p *P2P) Join(ctx context.Context, code, label string) (team.Team, error) {
	// A code that cannot work starts no host.
	inv, err := p2p.DecodeInvite(code)
	if err != nil {
		return team.Team{}, p2p.ErrBadInviteCode
	}
	if !time.Now().Before(inv.Expires) {
		return team.Team{}, p2p.ErrInviteExpired
	}
	if err := p.set.CheckJoin(inv.Team, nil); err != nil {
		return team.Team{}, err
	}
	if _, err := p.machineKey(); err != nil {
		return team.Team{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.joinHold)
	defer cancel()
	p.holds.Add(1)
	defer func() {
		p.holds.Add(-1)
		p.Changed()
	}()
	p.Changed()
	n, err := p.waitNode(ctx)
	if err != nil {
		return team.Team{}, err
	}
	return n.join.Join(ctx, code, label)
}

// TeamChanged is the Network method that runs after a local change of team
// name: it sends a change notice, and the member list to the members.
func (p *P2P) TeamChanged(name string) {
	p.Changed()
	if n := p.current(); n != nil {
		n.members.Changed(name)
	}
}

// LeaveAll leaves every team of this machine, as team.leave does, before
// the daemon stops. The leaves run in parallel, each within the leave
// timeout of p2p.MemberSync.SendLeft.
func (p *P2P) LeaveAll() {
	// The hold keeps the host up between the last Leave and the sends.
	p.holds.Add(1)
	defer func() {
		p.holds.Add(-1)
		p.Changed()
	}()
	var wg sync.WaitGroup
	for _, t := range p.set.List() {
		left, err := p.set.Leave(t.Name)
		if err != nil {
			continue
		}
		p.log.Info("team left at the stop of the daemon", "team", left.Name)
		wg.Go(func() { p.Left(left) })
	}
	wg.Wait()
}

// Left is the Network method that runs after this machine left team t: it
// sends t, with the entry of this machine marked left, to the members it
// can reach, and then lets the host stop when t was the last team.
func (p *P2P) Left(t team.Team) {
	p.holds.Add(1)
	defer func() {
		p.holds.Add(-1)
		p.Changed()
	}()
	if n := p.current(); n != nil {
		n.members.SendLeft(t)
	}
}
