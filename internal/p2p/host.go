// Package p2p holds the libp2p side of agent-courier: the host, the wire
// protocol, the access rule, the member list sync, the join and the relay.
package p2p

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/gologshim"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/libp2p/go-libp2p/p2p/protocol/autonatv2"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/holepunch"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/CappyT/agent-courier/internal/team"
)

// ProjectRelayID is the peer id of the project relay at relay.cappyt.sh.
const ProjectRelayID = "12D3KooWEBZoLrJa7yXYJPH5SuHmwWtDCFA39ntvvp883rte7qde"

// DefaultRelays is the relay list of a team that sets none: the project
// relay over QUIC and TCP. Public relays found through the DHT come after it,
// when the DHT lookup exists.
func DefaultRelays() []string {
	return []string{
		"/dns4/relay.cappyt.sh/udp/4001/quic-v1/p2p/" + ProjectRelayID,
		"/dns4/relay.cappyt.sh/tcp/4001/p2p/" + ProjectRelayID,
	}
}

// DefaultListen are the addresses of a daemon in a team: every interface,
// QUIC and TCP, IPv4 and IPv6, on ports the system picks.
func DefaultListen() []string {
	return []string{
		"/ip4/0.0.0.0/udp/0/quic-v1",
		"/ip4/0.0.0.0/tcp/0",
		"/ip6/::/udp/0/quic-v1",
		"/ip6/::/tcp/0",
	}
}

// StrangerGrace is how long an inbound connection, direct or relayed, from a
// peer outside the member set may stay open without a join stream.
const StrangerGrace = 5 * time.Second

// Defaults of the reservation and the dial order.
const (
	// readyWait bounds the wait of Connect for the reservation and for a
	// public observed address, counted from the start of the host.
	readyWait = 10 * time.Second
	// reserveTimeout bounds one connect and reservation on a relay.
	reserveTimeout = 30 * time.Second
	// retryMin and retryMax bound the backoff of a relay list that holds
	// no reservation.
	retryMin = 5 * time.Second
	retryMax = 5 * time.Minute
)

// Tags of the connection manager.
const (
	relayTag  = "agent-courier-relay"
	memberTag = "agent-courier-member"
)

// Resource manager limits of the daemon host. They are fixed, so a stranger
// can hold the same share on every machine.
const (
	peerConnsInbound    = 4
	peerConnsOutbound   = 8
	peerStreamsInbound  = 32
	peerStreamsOutbound = 64
	msgStreamsTotal     = 256
	joinStreamsPerPeer  = 1
	joinStreamsTotal    = 16
)

// HostConfig configures StartHost.
type HostConfig struct {
	// Key is the machine key.
	Key crypto.PrivKey
	// Listen are the listen multiaddrs. Empty: the host listens on no
	// address, and peers reach it only through a relay.
	Listen []string
	// Relays holds the relay list of each team, by team name. Each entry is
	// a multiaddr that ends with /p2p/<relay id>. The host holds a
	// reservation on the first reachable relay of each list.
	Relays map[string][]string
	// IsMember is the member set of the daemon: whether a peer is a member
	// of a team this machine shares.
	IsMember IsMember
	// MemberOf says whether a peer is a member of the named team. A dial to
	// a member goes through the relays of the teams it shares.
	MemberOf func(id peer.ID, team string) bool
	// Log gets the host's log, and the libp2p log at debug level.
	Log *slog.Logger

	// Fields for tests: zero means the default.
	readyWait     time.Duration
	strangerGrace time.Duration
	retryMin      time.Duration
	retryMax      time.Duration
	// allowLoopback keeps loopback addresses that Connect gets.
	allowLoopback bool
	// beforeStore runs after a relay granted a reservation and before the
	// host stores it.
	beforeStore func(h host.Host, relay peer.ID)
	extraOpts   []libp2p.Option
}

// relayEntry is one parsed relay list entry.
type relayEntry struct {
	addr      ma.Multiaddr // with /p2p/<id>
	transport ma.Multiaddr // without it
	id        peer.ID
}

// relayList is a relay list and the goroutine that holds its reservation.
type relayList struct {
	entries []relayEntry
	kick    chan struct{}
	cancel  context.CancelFunc
}

type reservation struct {
	renew time.Time
}

// Host is the libp2p host of a daemon in a team.
type Host struct {
	h   host.Host
	cfg HostConfig
	log *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	start  time.Time

	reserved chan struct{} // closed when each relay list of the start made its first attempt
	punch    chan struct{} // closed when the host serves DCUtR

	mu sync.Mutex
	// raw are the relay lists of the last change, teams the key of each
	// team's list, and lists the lists by key: two teams with the same list
	// share it.
	raw   map[string][]string
	teams map[string]string
	lists map[string]*relayList
	rsvps map[peer.ID]reservation
	// locks hold one reservation attempt per relay at a time, so a relay
	// that two lists name holds one reservation for both.
	locks map[peer.ID]chan struct{}
	hooks []func(peer.ID)
	// renewals counts the reservations taken, renewals included, for tests.
	renewals int
}

// ParseRelay parses a relay list entry: a multiaddr with a transport that
// ends with /p2p/<relay id> and has no /p2p-circuit.
func ParseRelay(s string) (ma.Multiaddr, peer.ID, error) {
	a, err := ma.NewMultiaddr(s)
	if err != nil {
		return nil, "", fmt.Errorf("relay %q: %w", s, err)
	}
	transport, id := peer.SplitAddr(a)
	if id == "" || len(transport) == 0 {
		return nil, "", fmt.Errorf("relay %q: must be a transport address that ends with /p2p/<relay id>", s)
	}
	for _, c := range transport {
		if code := c.Protocol().Code; code == ma.P_P2P || code == ma.P_CIRCUIT || code == ma.P_UNIX {
			return nil, "", fmt.Errorf("relay %q: must not carry /p2p-circuit, /unix or a second /p2p", s)
		}
	}
	return a, id, nil
}

// parseList parses a relay list. It drops, with a warning, an entry that does
// not parse, and drops an entry that names self.
func parseList(l []string, self peer.ID, log *slog.Logger) []relayEntry {
	var entries []relayEntry
	for _, s := range l {
		a, id, err := ParseRelay(s)
		if err != nil {
			log.Warn("relay list entry dropped", "err", err)
			continue
		}
		if id == self {
			continue
		}
		transport, _ := peer.SplitAddr(a)
		entries = append(entries, relayEntry{addr: a, transport: transport, id: id})
	}
	return entries
}

// Dialable reports whether a can name another machine. It is false for a
// /unix address, a loopback, unspecified, link-local or multicast IP address,
// and the DNS name localhost or a name under .localhost: in another machine's
// member list entry or invite code they name the dialing machine, its link, or a
// cloud metadata service such as 169.254.169.254. Private ranges are
// dialable, because members on one LAN use them.
func Dialable(a ma.Multiaddr) bool {
	if len(a) == 0 || slices.ContainsFunc(a, func(c ma.Component) bool { return c.Protocol().Code == ma.P_UNIX }) {
		return false
	}
	if manet.IsIPLoopback(a) || manet.IsIPUnspecified(a) {
		return false
	}
	if ip, err := manet.ToIP(a); err == nil && (ip.IsLinkLocalUnicast() || ip.IsMulticast()) {
		return false
	}
	switch a[0].Protocol().Code {
	case ma.P_DNS, ma.P_DNS4, ma.P_DNS6, ma.P_DNSADDR:
		name := strings.TrimSuffix(strings.ToLower(a[0].Value()), ".")
		if name == "localhost" || strings.HasSuffix(name, ".localhost") {
			return false
		}
	}
	return true
}

// hostOptions are the libp2p options of StartHost: QUIC and TCP, Noise,
// yamux, the AutoNAT client, AutoNAT v2, hole punching and the circuit relay
// client, without AutoRelay, and the dial ranker directFirst.
func hostOptions(cfg HostConfig, mgr network.ResourceManager) []libp2p.Option {
	opts := []libp2p.Option{
		libp2p.Identity(cfg.Key),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.ResourceManager(mgr),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.EnableAutoNATv2(),
		libp2p.DialRanker(directFirst),
		libp2p.DisableMetrics(),
	}
	if len(cfg.Listen) == 0 {
		opts = append(opts, libp2p.NoListenAddrs)
	} else {
		opts = append(opts, libp2p.ListenAddrStrings(cfg.Listen...))
	}
	return append(opts, cfg.extraOpts...)
}

// StartHost starts the libp2p host of a daemon in a team, with the options
// of hostOptions. It keeps identify address discovery, because hole punching
// needs the addresses that peers observe, and serves no AutoNAT v2 dial
// request: it runs the client only. It holds a reservation on the first
// reachable relay of each list and renews it before it expires. ctx bounds
// only the start.
func StartHost(ctx context.Context, cfg HostConfig) (*Host, error) {
	if cfg.Key == nil {
		return nil, errors.New("no machine key")
	}
	if cfg.IsMember == nil || cfg.MemberOf == nil {
		return nil, errors.New("no member set")
	}
	cfg.Log = cmp.Or(cfg.Log, slog.Default())
	cfg.readyWait = cmp.Or(cfg.readyWait, readyWait)
	cfg.strangerGrace = cmp.Or(cfg.strangerGrace, StrangerGrace)
	cfg.retryMin = cmp.Or(cfg.retryMin, retryMin)
	cfg.retryMax = cmp.Or(cfg.retryMax, retryMax)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The libp2p loggers write peer ids: they reach the daemon log only at
	// debug level.
	gologshim.SetDefaultHandler(debugHandler{cfg.Log.Handler()})

	mgr, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(hostLimits()))
	if err != nil {
		return nil, err
	}
	h, err := libp2p.New(hostOptions(cfg, mgr)...)
	if err != nil {
		mgr.Close()
		return nil, err
	}
	// AutoNAT v2 has no client-only option: its server registers the dial
	// request handler at the start.
	h.RemoveStreamHandler(autonatv2.DialProtocol)
	sub, err := h.EventBus().Subscribe([]any{new(event.EvtPeerConnectednessChanged), new(event.EvtLocalProtocolsUpdated)})
	if err != nil {
		h.Close()
		return nil, err
	}
	hctx, cancel := context.WithCancel(context.Background())
	hh := &Host{
		h:        h,
		cfg:      cfg,
		log:      cfg.Log,
		ctx:      hctx,
		cancel:   cancel,
		start:    time.Now(),
		reserved: make(chan struct{}),
		punch:    make(chan struct{}),
		lists:    map[string]*relayList{},
		rsvps:    map[peer.ID]reservation{},
		locks:    map[peer.ID]chan struct{}{},
	}
	h.Network().Notify(&network.NotifyBundle{ConnectedF: hh.connected})
	hh.wg.Go(func() { hh.events(sub) })
	var first sync.WaitGroup
	hh.mu.Lock()
	hh.setLists(cfg.Relays, &first)
	hh.mu.Unlock()
	hh.wg.Go(func() {
		first.Wait()
		close(hh.reserved)
	})
	return hh, nil
}

// Host is the libp2p host, for the protocol handlers.
func (h *Host) Host() host.Host { return h.h }

// ID is the peer id of this machine.
func (h *Host) ID() peer.ID { return h.h.ID() }

// OnConnected adds f to the functions that run, each in its own goroutine,
// when a peer goes from no connection to a direct or a relayed one. Every
// peer counts, relays and strangers too: f checks the member lists.
func (h *Host) OnConnected(f func(peer.ID)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hooks = append(h.hooks, f)
}

// SetRelays replaces the relay lists, by team name. When a list changed, the
// host keeps the reservations that a list still names and reserves for the
// new lists at once; when none changed, it does nothing, so the backoff of a
// list without a reservation holds.
func (h *Host) SetRelays(lists map[string][]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() != nil || maps.EqualFunc(h.raw, lists, slices.Equal) {
		return
	}
	h.setLists(lists, nil)
}

// setLists applies the relay lists: it starts a goroutine for each new list
// and stops the goroutines of the lists that are gone. Each new list calls
// first.Done after its first attempt, when first is not nil. h.mu is held.
func (h *Host) setLists(lists map[string][]string, first *sync.WaitGroup) {
	h.raw = make(map[string][]string, len(lists))
	h.teams = map[string]string{}
	parsed := map[string][]relayEntry{}
	for name, l := range lists {
		h.raw[name] = slices.Clone(l)
		entries := parseList(l, h.h.ID(), h.log)
		if len(entries) == 0 {
			continue
		}
		var key strings.Builder
		for _, e := range entries {
			key.WriteString(e.addr.String() + "\n")
		}
		h.teams[name] = key.String()
		parsed[key.String()] = entries
	}
	for key, l := range h.lists {
		if _, ok := parsed[key]; !ok {
			l.cancel()
			delete(h.lists, key)
		}
	}
	named := map[peer.ID]bool{}
	for key, entries := range parsed {
		for _, e := range entries {
			named[e.id] = true
		}
		if _, ok := h.lists[key]; ok {
			continue
		}
		ctx, cancel := context.WithCancel(h.ctx)
		l := &relayList{entries: entries, kick: make(chan struct{}, 1), cancel: cancel}
		h.lists[key] = l
		done := func() {}
		if first != nil {
			first.Add(1)
			done = sync.OnceFunc(first.Done)
		}
		h.wg.Go(func() { h.holdList(ctx, l, done) })
	}
	for id := range h.rsvps {
		if !named[id] {
			h.drop(id)
		}
	}
}

// CircuitAddrs are this machine's addresses through the relays of team where
// it holds a reservation: <relay list entry>/p2p-circuit/p2p/<own peer id>,
// built from the list entry and not from the host addresses, because the
// host publishes a circuit address only for a relay that announces a public
// address.
func (h *Host) CircuitAddrs(team string) []ma.Multiaddr {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.circuits(team, ma.StringCast("/p2p-circuit/p2p/"+h.h.ID().String()))
}

// circuits are the relay list entries of team, or of every team when team is
// empty, where the host holds a reservation, each with suffix. h.mu is held.
func (h *Host) circuits(team string, suffix ma.Multiaddr) []ma.Multiaddr {
	var out []ma.Multiaddr
	for name, key := range h.teams {
		if team != "" && name != team {
			continue
		}
		for _, e := range h.lists[key].entries {
			if _, ok := h.rsvps[e.id]; ok {
				out = append(out, e.addr.Encapsulate(suffix))
			}
		}
	}
	return ma.Unique(out)
}

// AdvertiseAddrs are the addresses of this machine for an invite code or a
// member list entry of team, or of every team when team is empty, best first:
// the circuit addresses through the team's relays, then the dialable host
// addresses, public ones first. They carry no /p2p part, and there are at
// most team.MaxAddrs of them.
func (h *Host) AdvertiseAddrs(name string) []ma.Multiaddr {
	h.mu.Lock()
	out := h.circuits(name, ma.StringCast("/p2p-circuit"))
	h.mu.Unlock()
	var public, private []ma.Multiaddr
	for _, a := range h.h.Addrs() {
		if !Dialable(a) || slices.ContainsFunc(a, func(c ma.Component) bool { return c.Protocol().Code == ma.P_CIRCUIT }) {
			continue
		}
		if manet.IsPublicAddr(a) {
			public = append(public, a)
		} else {
			private = append(private, a)
		}
	}
	out = slices.DeleteFunc(append(append(out, public...), private...), func(a ma.Multiaddr) bool {
		return len(a.Bytes()) > team.MaxAddr || len(a.String()) > team.MaxAddr
	})
	return out[:min(len(out), team.MaxAddrs)]
}

// Connect connects to the member id with the addresses of its member list entry.
// It waits first, at most until readyWait after the start, for the
// reservation and for a public observed address, so that DCUtR can upgrade
// a relayed connection. One dial then tries the direct addresses first and
// the circuit addresses after them: the member's own, and one through each
// relay of the teams this machine shares with the member. A relayed
// connection stays when hole punching fails. Connect drops the addresses
// that are not Dialable.
func (h *Host) Connect(ctx context.Context, id peer.ID, addrs []string) error {
	if err := h.waitReady(ctx); err != nil {
		return err
	}
	info := peer.AddrInfo{ID: id, Addrs: h.dialAddrs(id, addrs)}
	return h.h.Connect(network.WithAllowLimitedConn(ctx, "agent-courier"), info)
}

// WaitReserved waits until each relay list of the start made its first
// attempt, at most until readyWait after the start.
func (h *Host) WaitReserved(ctx context.Context) error {
	return h.wait(ctx, h.reserved)
}

// ProtectMembers protects the connections of the members in the connection
// manager, and unprotects those of peers that left the member set. The
// daemon calls it after each change of the member set.
func (h *Host) ProtectMembers() {
	cm := h.h.ConnManager()
	for _, id := range h.h.Network().Peers() {
		if h.cfg.IsMember(id) {
			cm.Protect(id, memberTag)
		} else {
			cm.Unprotect(id, memberTag)
		}
	}
}

// Close stops the reservations and closes the host and its listeners.
func (h *Host) Close() error {
	// Under mu, so no goroutine starts after the cancel.
	h.mu.Lock()
	h.cancel()
	h.mu.Unlock()
	err := h.h.Close()
	h.wg.Wait()
	return err
}

// spawn runs f in a goroutine of the host, unless the host is closed.
func (h *Host) spawn(f func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ctx.Err() == nil {
		h.wg.Go(f)
	}
}

// wakeAll wakes the goroutine of each relay list. h.mu is held.
func (h *Host) wakeAll() {
	for _, l := range h.lists {
		select {
		case l.kick <- struct{}{}:
		default:
		}
	}
}

func (h *Host) waitReady(ctx context.Context) error {
	return h.wait(ctx, h.reserved, h.punch)
}

// wait waits until each of chans is closed, at most until readyWait after
// the start.
func (h *Host) wait(ctx context.Context, chans ...chan struct{}) error {
	t := time.NewTimer(time.Until(h.start.Add(h.cfg.readyWait)))
	defer t.Stop()
	for _, c := range chans {
		select {
		case <-c:
		case <-t.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-h.ctx.Done():
			return ErrHostClosed
		}
	}
	return nil
}

// ErrHostClosed is the error of Connect on a closed host.
var ErrHostClosed = errors.New("the host is closed")

func (h *Host) dialAddrs(id peer.ID, addrs []string) []ma.Multiaddr {
	var out []ma.Multiaddr
	for _, s := range addrs {
		a, err := ma.NewMultiaddr(s)
		if err != nil {
			continue
		}
		if transport, last := peer.SplitAddr(a); last != "" {
			if last != id {
				continue
			}
			a = transport
		}
		if h.cfg.allowLoopback {
			if len(a) == 0 || slices.ContainsFunc(a, func(c ma.Component) bool { return c.Protocol().Code == ma.P_UNIX }) {
				continue
			}
		} else if !Dialable(a) {
			continue
		}
		out = append(out, a)
	}
	circuit := ma.StringCast("/p2p-circuit")
	h.mu.Lock()
	for name, key := range h.teams {
		if !h.cfg.MemberOf(id, name) {
			continue
		}
		for _, e := range h.lists[key].entries {
			if e.id != id {
				out = append(out, e.addr.Encapsulate(circuit))
			}
		}
	}
	h.mu.Unlock()
	return ma.Unique(out)
}

// directFirst is the default dial ranker, except that it delays relay
// addresses behind every direct address, private ones included: the default
// ranker starts the circuit at once when a member has only private
// addresses, and a relayed connection to a member on the same LAN would win.
func directFirst(addrs []ma.Multiaddr) []network.AddrDelay {
	isRelay := func(a ma.Multiaddr) bool {
		_, err := a.ValueForProtocol(ma.P_CIRCUIT)
		return err == nil
	}
	var relay, direct []ma.Multiaddr
	for _, a := range addrs {
		if isRelay(a) {
			relay = append(relay, a)
		} else {
			direct = append(direct, a)
		}
	}
	res := swarm.DefaultDialRanker(direct)
	for _, d := range swarm.DefaultDialRanker(relay) {
		if len(direct) > 0 {
			d.Delay += swarm.RelayDelay
		}
		res = append(res, d)
	}
	return res
}

// events runs the OnConnected hooks, notes when the host serves DCUtR, and
// wakes the relay lists when the connection to a relay drops, because the
// relay drops the reservation with it.
func (h *Host) events(sub event.Subscription) {
	defer sub.Close()
	connected := map[peer.ID]bool{}
	punch := func() {
		select {
		case <-h.punch:
		default:
			close(h.punch)
		}
	}
	if slices.Contains(h.h.Mux().Protocols(), holepunch.Protocol) {
		punch()
	}
	for {
		var e any
		select {
		case <-h.ctx.Done():
			return
		case ev, ok := <-sub.Out():
			if !ok {
				return
			}
			e = ev
		}
		switch e := e.(type) {
		case event.EvtLocalProtocolsUpdated:
			if slices.Contains(e.Added, holepunch.Protocol) {
				punch()
			}
		case event.EvtPeerConnectednessChanged:
			switch e.Connectedness {
			case network.Connected, network.Limited:
				if connected[e.Peer] {
					continue
				}
				connected[e.Peer] = true
				h.mu.Lock()
				hooks := slices.Clone(h.hooks)
				h.mu.Unlock()
				for _, f := range hooks {
					h.spawn(func() { f(e.Peer) })
				}
			case network.NotConnected:
				delete(connected, e.Peer)
				h.mu.Lock()
				_, held := h.rsvps[e.Peer]
				if held {
					h.drop(e.Peer)
					h.wakeAll()
				}
				h.mu.Unlock()
				if held {
					h.log.Debug("connection to a relay lost, reserving again", "relay", e.Peer)
				}
			}
		}
	}
}

// connected protects the connection of a member, and watches an inbound
// connection, direct or relayed, from a peer outside the member set: when
// the peer is still outside it and has no join stream open at the end of the
// grace, the host closes the connection. A relayed one holds circuit slots
// of this machine on the relay.
func (h *Host) connected(_ network.Network, c network.Conn) {
	from := c.RemotePeer()
	if h.cfg.IsMember(from) {
		h.h.ConnManager().Protect(from, memberTag)
		return
	}
	if c.Stat().Direction != network.DirInbound {
		return
	}
	h.spawn(func() {
		t := time.NewTimer(h.cfg.strangerGrace)
		defer t.Stop()
		for {
			select {
			case <-h.ctx.Done():
				return
			case <-t.C:
			}
			if c.IsClosed() {
				return
			}
			if h.cfg.IsMember(from) {
				h.h.ConnManager().Protect(from, memberTag)
				return
			}
			if !slices.ContainsFunc(c.GetStreams(), func(s network.Stream) bool { return s.Protocol() == JoinProtocol }) {
				h.log.Debug("connection from a peer outside the member set closed", "peer", from)
				c.Close()
				return
			}
			t.Reset(h.cfg.strangerGrace)
		}
	})
}

// holdList holds a reservation for the relay list l until ctx ends. It
// renews the reservation when three quarters of its life have passed, and
// retries a list without a reservation with backoff. It calls first after
// its first attempt.
func (h *Host) holdList(ctx context.Context, l *relayList, first func()) {
	defer first()
	backoff := h.cfg.retryMin
	for {
		wait := backoff
		if h.reserveList(ctx, l.entries) {
			backoff = h.cfg.retryMin
			wait = h.renewIn(l.entries)
		} else {
			backoff = min(2*backoff, h.cfg.retryMax)
		}
		first()
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-l.kick:
		case <-t.C:
		}
		t.Stop()
	}
}

// renewIn is the time to the renewal of the reservation that entries hold.
func (h *Host) renewIn(entries []relayEntry) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	wait := h.cfg.retryMax
	for _, e := range entries {
		if r, ok := h.rsvps[e.id]; ok {
			wait = min(wait, time.Until(r.renew))
		}
	}
	return max(wait, 0)
}

// reserveList keeps a reservation on one relay of l: the held one while it
// lasts, else the first relay of l that grants one.
func (h *Host) reserveList(ctx context.Context, l []relayEntry) bool {
	now := time.Now()
	h.mu.Lock()
	var due []peer.ID
	for _, e := range l {
		if r, ok := h.rsvps[e.id]; ok {
			if now.Before(r.renew) {
				h.mu.Unlock()
				return true
			}
			due = append(due, e.id)
		}
	}
	h.mu.Unlock()
	// A held reservation is renewed first, then the relays in list order.
	order := slices.Clone(due)
	for _, e := range l {
		if !slices.Contains(order, e.id) {
			order = append(order, e.id)
		}
	}
	for _, id := range order {
		var addrs []ma.Multiaddr
		for _, e := range l {
			if e.id == id {
				addrs = append(addrs, e.transport)
			}
		}
		if h.reserve(ctx, peer.AddrInfo{ID: id, Addrs: addrs}) {
			return true
		}
	}
	return false
}

// renewAt is the time to renew a reservation that expires at exp: when three
// quarters of its life have passed.
func renewAt(now, exp time.Time) time.Time {
	return now.Add(exp.Sub(now) * 3 / 4)
}

func (h *Host) reserve(ctx context.Context, info peer.AddrInfo) bool {
	h.mu.Lock()
	lock := h.locks[info.ID]
	if lock == nil {
		lock = make(chan struct{}, 1)
		h.locks[info.ID] = lock
	}
	h.mu.Unlock()
	select {
	case lock <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	defer func() { <-lock }()
	h.mu.Lock()
	r, held := h.rsvps[info.ID]
	h.mu.Unlock()
	if held && time.Now().Before(r.renew) {
		// Another list renewed it while this one waited.
		return true
	}

	rctx, cancel := context.WithTimeout(ctx, reserveTimeout)
	defer cancel()
	var rsvp *client.Reservation
	err := h.h.Connect(rctx, info)
	if err == nil {
		rsvp, err = client.Reserve(rctx, h.h, info)
	}
	if err == nil && h.cfg.beforeStore != nil {
		h.cfg.beforeStore(h.h, info.ID)
	}
	h.mu.Lock()
	if err != nil {
		if ctx.Err() == nil {
			h.drop(info.ID)
		}
		h.mu.Unlock()
		if ctx.Err() == nil {
			h.log.Debug("reservation failed", "relay", info.ID, "err", err)
		}
		return false
	}
	h.rsvps[info.ID] = reservation{renew: renewAt(time.Now(), rsvp.Expiration)}
	h.renewals++
	h.h.ConnManager().Protect(info.ID, relayTag)
	h.mu.Unlock()
	// The connection to the relay can drop between Reserve and the store
	// above, and the event of that drop found no reservation to delete.
	if h.h.Network().Connectedness(info.ID) != network.Connected {
		h.mu.Lock()
		h.drop(info.ID)
		h.wakeAll()
		h.mu.Unlock()
		h.log.Debug("connection to a relay lost after the reservation, reserving again", "relay", info.ID)
		return false
	}
	h.log.Debug("reservation held", "relay", info.ID, "expires", rsvp.Expiration)
	return true
}

// drop forgets the reservation on relay id. h.mu is held.
func (h *Host) drop(id peer.ID) {
	if _, ok := h.rsvps[id]; ok {
		delete(h.rsvps, id)
		h.h.ConnManager().Unprotect(id, relayTag)
	}
}

// hostLimits are the libp2p default limits with fixed per-peer connection
// and stream limits, and limits on the courier protocols. A member opens at
// most MaxStreamsPerPeer msg streams, which MsgHandler enforces too, and a
// joiner one join stream.
func hostLimits() rcmgr.ConcreteLimitConfig {
	cfg := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&cfg)
	cfg.PeerBaseLimit.ConnsInbound = peerConnsInbound
	cfg.PeerBaseLimit.ConnsOutbound = peerConnsOutbound
	cfg.PeerBaseLimit.Conns = peerConnsOutbound
	cfg.PeerBaseLimit.StreamsInbound = peerStreamsInbound
	cfg.PeerBaseLimit.StreamsOutbound = peerStreamsOutbound
	cfg.PeerBaseLimit.Streams = peerStreamsOutbound
	cfg.PeerLimitIncrease.ConnsInbound = 0
	cfg.PeerLimitIncrease.ConnsOutbound = 0
	cfg.PeerLimitIncrease.Conns = 0
	cfg.PeerLimitIncrease.StreamsInbound = 0
	cfg.PeerLimitIncrease.StreamsOutbound = 0
	cfg.PeerLimitIncrease.Streams = 0
	cfg.AddProtocolLimit(MsgProtocol,
		rcmgr.BaseLimit{StreamsInbound: msgStreamsTotal, StreamsOutbound: msgStreamsTotal, Streams: 2 * msgStreamsTotal, Memory: 32 << 20},
		rcmgr.BaseLimitIncrease{})
	cfg.AddProtocolPeerLimit(MsgProtocol,
		rcmgr.BaseLimit{StreamsInbound: MaxStreamsPerPeer, StreamsOutbound: MaxStreamsPerPeer, Streams: 2 * MaxStreamsPerPeer, Memory: 4 << 20},
		rcmgr.BaseLimitIncrease{})
	cfg.AddProtocolLimit(JoinProtocol,
		rcmgr.BaseLimit{StreamsInbound: joinStreamsTotal, StreamsOutbound: joinStreamsTotal, Streams: 2 * joinStreamsTotal, Memory: 4 << 20},
		rcmgr.BaseLimitIncrease{})
	cfg.AddProtocolPeerLimit(JoinProtocol,
		rcmgr.BaseLimit{StreamsInbound: joinStreamsPerPeer, StreamsOutbound: joinStreamsPerPeer, Streams: 2 * joinStreamsPerPeer, Memory: 1 << 20},
		rcmgr.BaseLimitIncrease{})
	return cfg.AutoScale()
}

// debugHandler passes every record at debug level.
type debugHandler struct{ slog.Handler }

func (d debugHandler) Enabled(ctx context.Context, _ slog.Level) bool {
	return d.Handler.Enabled(ctx, slog.LevelDebug)
}

func (d debugHandler) Handle(ctx context.Context, r slog.Record) error {
	r.Level = slog.LevelDebug
	return d.Handler.Handle(ctx, r)
}

func (d debugHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return debugHandler{d.Handler.WithAttrs(attrs)}
}

func (d debugHandler) WithGroup(name string) slog.Handler {
	return debugHandler{d.Handler.WithGroup(name)}
}
