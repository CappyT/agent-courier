//go:build unix

package p2p

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/gologshim"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	pbv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/proto"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// RelayConfig holds the limits and addresses of the relay node.
type RelayConfig struct {
	// Listen are the multiaddrs the relay listens on.
	Listen []string
	// Announce, when set, replaces the listen addresses that the relay
	// tells its peers, for a relay behind a load balancer. Each address
	// must be public and carry no /p2p or /p2p-circuit.
	Announce []string
	// MaxReservations caps the reservations the relay holds at once.
	MaxReservations int
	// MaxReservationsPerIP caps the reservations from one IP address.
	MaxReservationsPerIP int
	// MaxCircuits caps the relayed connections of one peer.
	MaxCircuits int
	// ReservationTTL is how long a reservation lasts before the peer renews it.
	ReservationTTL time.Duration
	// MaxDuration ends a relayed connection after this time.
	MaxDuration time.Duration
	// MaxData ends a relayed connection after this many bytes in one direction.
	MaxData int64
}

// Upper bounds of the limit flags. They keep the stream and memory sums of
// limits() far from overflow, and keep the memory that all circuits can hold
// under the resource manager's system memory limit.
const (
	MaxRelayReservations   = 4096
	MaxRelayCircuits       = 64
	MaxRelayCircuitsTotal  = 32768 // MaxReservations x MaxCircuits
	MaxRelayDuration       = 24 * time.Hour
	MaxRelayReservationTTL = 24 * time.Hour
	MaxRelayData           = 1 << 30
)

// DefaultRelayConfig has the limits of the design: 10 minutes and 1 MiB per
// relayed connection, 1024 reservations, 8 per IP and 16 circuits per peer.
func DefaultRelayConfig() RelayConfig {
	d := relayv2.DefaultResources()
	return RelayConfig{
		Listen: []string{
			"/ip4/0.0.0.0/udp/4001/quic-v1",
			"/ip4/0.0.0.0/tcp/4001",
			"/ip6/::/udp/4001/quic-v1",
			"/ip6/::/tcp/4001",
		},
		MaxReservations:      1024,
		MaxReservationsPerIP: d.MaxReservationsPerIP,
		MaxCircuits:          d.MaxCircuits,
		ReservationTTL:       d.ReservationTTL,
		MaxDuration:          10 * time.Minute,
		MaxData:              1 << 20,
	}
}

func (c RelayConfig) validate() error {
	switch {
	case len(c.Listen) == 0:
		return errors.New("no listen address")
	case c.MaxReservations <= 0 || c.MaxReservations > MaxRelayReservations:
		return fmt.Errorf("max reservations %d is not in 1..%d", c.MaxReservations, MaxRelayReservations)
	case c.MaxReservationsPerIP <= 0 || c.MaxReservationsPerIP > c.MaxReservations:
		return fmt.Errorf("max reservations per IP %d is not in 1..%d (max reservations)", c.MaxReservationsPerIP, c.MaxReservations)
	case c.MaxCircuits <= 0 || c.MaxCircuits > MaxRelayCircuits:
		return fmt.Errorf("max circuits %d is not in 1..%d", c.MaxCircuits, MaxRelayCircuits)
	case c.MaxReservations*c.MaxCircuits > MaxRelayCircuitsTotal:
		return fmt.Errorf("max reservations x max circuits is %d, over %d", c.MaxReservations*c.MaxCircuits, MaxRelayCircuitsTotal)
	case c.ReservationTTL <= 0 || c.ReservationTTL > MaxRelayReservationTTL:
		return fmt.Errorf("reservation TTL %v is not in (0, %v]", c.ReservationTTL, MaxRelayReservationTTL)
	case c.MaxDuration <= 0 || c.MaxDuration > MaxRelayDuration:
		return fmt.Errorf("max duration %v is not in (0, %v]", c.MaxDuration, MaxRelayDuration)
	case c.MaxData <= 0 || c.MaxData > MaxRelayData:
		return fmt.Errorf("max data %d is not in 1..%d", c.MaxData, MaxRelayData)
	}
	_, err := c.announceAddrs()
	return err
}

// announceAddrs parses Announce. A private address would send members to an
// address they cannot reach, and a /p2p or /p2p-circuit part would make the
// relay advertise another peer's identity or a circuit through itself.
func (c RelayConfig) announceAddrs() ([]ma.Multiaddr, error) {
	var out []ma.Multiaddr
	for _, s := range c.Announce {
		a, err := ma.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("announce address %q: %w", s, err)
		}
		for _, comp := range a {
			if code := comp.Protocol().Code; code == ma.P_P2P || code == ma.P_CIRCUIT {
				return nil, fmt.Errorf("announce address %q: must not carry /p2p or /p2p-circuit", s)
			}
		}
		if !manet.IsPublicAddr(a) {
			return nil, fmt.Errorf("announce address %q: must be a public address", s)
		}
		out = append(out, a)
	}
	return out, nil
}

func (c RelayConfig) resources() relayv2.Resources {
	r := relayv2.DefaultResources()
	r.Limit = &relayv2.RelayLimit{Duration: c.MaxDuration, Data: c.MaxData}
	r.ReservationTTL = c.ReservationTTL
	r.MaxReservations = c.MaxReservations
	r.MaxReservationsPerIP = c.MaxReservationsPerIP
	r.MaxReservationsPerASN = max(r.MaxReservationsPerASN, c.MaxReservationsPerIP)
	r.MaxCircuits = c.MaxCircuits
	return r
}

// Fixed resource manager budget, under the 1 GiB memory limit of the relay
// pod: the limits must not scale with the memory of the node.
const (
	relaySystemMemory = 768 << 20
	relayFDs          = 4096
)

// limits raises the libp2p defaults so that the relay limits, not the
// resource manager, decide when the relay refuses a peer. Each relayed
// connection holds a hop stream from its source and a stop stream to its
// destination, and a peer counts each circuit it is part of. validate bounds
// the inputs, so the sums cannot overflow.
func (c RelayConfig) limits() rcmgr.ConcreteLimitConfig {
	res := c.resources()
	circuits := c.MaxReservations * c.MaxCircuits
	perCircuit := int64(2*res.BufferSize + 2*4096)
	streams := 2*circuits + c.MaxReservations
	peerStreams := c.MaxCircuits + 4
	conns := 2*c.MaxReservations + 64
	circuitMemory := int64(circuits)*perCircuit + 16<<20
	peerMemory := int64(c.MaxCircuits)*perCircuit + 1<<20

	cfg := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&cfg)
	cfg.SystemBaseLimit.ConnsInbound = max(cfg.SystemBaseLimit.ConnsInbound, conns)
	cfg.SystemBaseLimit.Conns = max(cfg.SystemBaseLimit.Conns, conns+cfg.SystemBaseLimit.ConnsOutbound)
	cfg.SystemBaseLimit.StreamsInbound = max(cfg.SystemBaseLimit.StreamsInbound, 2*streams)
	cfg.SystemBaseLimit.StreamsOutbound = max(cfg.SystemBaseLimit.StreamsOutbound, 2*streams)
	cfg.SystemBaseLimit.Streams = max(cfg.SystemBaseLimit.Streams, 2*streams)
	cfg.SystemBaseLimit.FD = max(cfg.SystemBaseLimit.FD, cfg.SystemBaseLimit.Conns+64)
	// An inbound connection holds a transient slot from accept until its Noise
	// handshake ends, up to the 15 s accept timeout. With the default of about
	// 42 slots, a few dozen idle sockets would block every new connection.
	transient := conns / 2
	cfg.TransientBaseLimit.ConnsInbound = max(cfg.TransientBaseLimit.ConnsInbound, transient)
	cfg.TransientBaseLimit.Conns = max(cfg.TransientBaseLimit.Conns, transient+cfg.TransientBaseLimit.ConnsOutbound)
	cfg.TransientBaseLimit.FD = max(cfg.TransientBaseLimit.FD, transient+cfg.TransientBaseLimit.ConnsOutbound)
	cfg.TransientBaseLimit.StreamsInbound = max(cfg.TransientBaseLimit.StreamsInbound, conns)
	cfg.TransientBaseLimit.Streams = max(cfg.TransientBaseLimit.Streams, conns+cfg.TransientBaseLimit.StreamsOutbound)
	cfg.AddServiceLimit(relayv2.ServiceName,
		rcmgr.BaseLimit{Streams: streams, StreamsInbound: streams, StreamsOutbound: streams, Memory: circuitMemory},
		rcmgr.BaseLimitIncrease{})
	cfg.AddServicePeerLimit(relayv2.ServiceName,
		rcmgr.BaseLimit{Streams: peerStreams, StreamsInbound: peerStreams, StreamsOutbound: peerStreams, Memory: peerMemory},
		rcmgr.BaseLimitIncrease{})
	for _, p := range []protocol.ID{proto.ProtoIDv2Hop, proto.ProtoIDv2Stop} {
		cfg.AddProtocolLimit(p,
			rcmgr.BaseLimit{Streams: streams, StreamsInbound: streams, StreamsOutbound: streams, Memory: circuitMemory},
			rcmgr.BaseLimitIncrease{})
		cfg.AddProtocolPeerLimit(p,
			rcmgr.BaseLimit{Streams: peerStreams, StreamsInbound: peerStreams, StreamsOutbound: peerStreams, Memory: peerMemory},
			rcmgr.BaseLimitIncrease{})
	}
	// Scale adds the memory argument to the system base memory, so the
	// system memory limit comes out as relaySystemMemory.
	return cfg.Scale(relaySystemMemory-cfg.SystemBaseLimit.Memory, relayFDs)
}

// RelayStats are the counts the relay logs. They hold no peer ids. The
// refusal maps count requests by circuit relay status name.
type RelayStats struct {
	Peers               int
	Reservations        int64
	Circuits            int64
	ReservationsRefused map[string]int64
	CircuitsRefused     map[string]int64
}

// relayCounter counts reservations and circuits through the relay's metrics
// hook, because the relay service exposes no counts.
type relayCounter struct {
	reservations atomic.Int64
	circuits     atomic.Int64

	mu                  sync.Mutex
	reservationsRefused map[pbv2.Status]int64
	circuitsRefused     map[pbv2.Status]int64
}

func newRelayCounter() *relayCounter {
	return &relayCounter{reservationsRefused: map[pbv2.Status]int64{}, circuitsRefused: map[pbv2.Status]int64{}}
}

func (*relayCounter) RelayStatus(bool)                 {}
func (c *relayCounter) ConnectionOpened()              { c.circuits.Add(1) }
func (c *relayCounter) ConnectionClosed(time.Duration) { c.circuits.Add(-1) }
func (c *relayCounter) ReservationClosed(n int)        { c.reservations.Add(-int64(n)) }
func (*relayCounter) BytesTransferred(int)             {}

func (c *relayCounter) ReservationAllowed(renewal bool) {
	if !renewal {
		c.reservations.Add(1)
	}
}

func (c *relayCounter) ReservationRequestHandled(s pbv2.Status) {
	c.refuse(c.reservationsRefused, s)
}

func (c *relayCounter) ConnectionRequestHandled(s pbv2.Status) {
	c.refuse(c.circuitsRefused, s)
}

func (c *relayCounter) refuse(m map[pbv2.Status]int64, s pbv2.Status) {
	if s == pbv2.Status_OK {
		return
	}
	c.mu.Lock()
	m[s]++
	c.mu.Unlock()
}

func (c *relayCounter) refused(m map[pbv2.Status]int64) map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(m))
	for s, n := range m {
		out[s.String()] = n
	}
	return out
}

// Relay is a running circuit relay v2 node.
type Relay struct {
	host    host.Host
	service *relayv2.Relay
	counter *relayCounter
}

// StartRelay starts a libp2p host that serves circuit relay v2 and identify
// only: QUIC and TCP, Noise, no ping, no relay client, no DHT, no AutoNAT.
func StartRelay(key crypto.PrivKey, cfg RelayConfig) (*Relay, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	announce, err := cfg.announceAddrs()
	if err != nil {
		return nil, err
	}
	// The libp2p loggers write peer ids, and the relay logs must hold none.
	gologshim.SetDefaultHandler(slog.DiscardHandler)

	mgr, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(cfg.limits()))
	if err != nil {
		return nil, err
	}
	conns := 2*cfg.MaxReservations + 64
	cm, err := connmgr.NewConnManager(conns*3/4, conns)
	if err != nil {
		mgr.Close()
		return nil, err
	}
	opts := []libp2p.Option{
		libp2p.Identity(key),
		libp2p.ListenAddrStrings(cfg.Listen...),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.ResourceManager(mgr),
		libp2p.ConnectionManager(cm),
		libp2p.DisableRelay(),
		libp2p.DisableIdentifyAddressDiscovery(),
		libp2p.Ping(false),
		libp2p.ForceReachabilityPublic(),
		libp2p.DisableMetrics(),
	}
	if len(announce) > 0 {
		opts = append(opts, libp2p.AddrsFactory(func([]ma.Multiaddr) []ma.Multiaddr { return slices.Clone(announce) }))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		mgr.Close()
		return nil, err
	}
	counter := newRelayCounter()
	svc, err := relayv2.New(h, relayv2.WithResources(cfg.resources()), relayv2.WithMetricsTracer(counter))
	if err != nil {
		h.Close()
		return nil, err
	}
	return &Relay{host: h, service: svc, counter: counter}, nil
}

// ID is the relay's peer id.
func (r *Relay) ID() peer.ID { return r.host.ID() }

// Addrs are the relay's listen addresses, without its peer id.
func (r *Relay) Addrs() []ma.Multiaddr { return r.host.Network().ListenAddresses() }

// Advertised are the addresses the relay tells its peers, without its peer
// id: the announce addresses when set, else the interface addresses.
func (r *Relay) Advertised() []ma.Multiaddr { return r.host.Addrs() }

// HasPublicAddr reports whether an advertised address is public. A relay
// without one gives members reservations with no address they can reach.
func (r *Relay) HasPublicAddr() bool {
	return slices.ContainsFunc(r.Advertised(), manet.IsPublicAddr)
}

// Stats are the current counts of the relay.
func (r *Relay) Stats() RelayStats {
	return RelayStats{
		Peers:               len(r.host.Network().Peers()),
		Reservations:        r.counter.reservations.Load(),
		Circuits:            r.counter.circuits.Load(),
		ReservationsRefused: r.counter.refused(r.counter.reservationsRefused),
		CircuitsRefused:     r.counter.refused(r.counter.circuitsRefused),
	}
}

// Close stops the relay service, then the host.
func (r *Relay) Close() error {
	return errors.Join(r.service.Close(), r.host.Close())
}

// maxRelayKey caps the read of the relay key file.
const maxRelayKey = 4096

// LoadRelayKey reads a libp2p protobuf private key. The path may be a symlink,
// as a Kubernetes Secret volume mounts it. The file must not be readable by
// others or writable by the group: a Secret mounted at 0440 with an fsGroup
// passes.
func LoadRelayKey(path string) (crypto.PrivKey, error) {
	// O_NONBLOCK keeps a FIFO from blocking the open; the mode check refuses it.
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	switch {
	case !fi.Mode().IsRegular():
		return nil, fmt.Errorf("%s is not a regular file", path)
	case fi.Mode().Perm()&0o027 != 0:
		return nil, fmt.Errorf("%s has mode %v, the key must not be readable by others or writable by the group (use 0400, 0440 or 0600)", path, fi.Mode().Perm())
	}
	b, err := io.ReadAll(io.LimitReader(f, maxRelayKey+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxRelayKey {
		return nil, fmt.Errorf("%s is larger than %d bytes, not a key", path, maxRelayKey)
	}
	key, err := crypto.UnmarshalPrivateKey(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return key, nil
}

// LoadRelayID is the peer id of the key in path, for the relay list.
func LoadRelayID(path string) (peer.ID, error) {
	key, err := LoadRelayKey(path)
	if err != nil {
		return "", err
	}
	return peer.IDFromPrivateKey(key)
}

// GenerateRelayKey writes a new ed25519 key to path with mode 0600. It fails
// when path exists, so it never replaces the identity of a running relay.
func GenerateRelayKey(path string) (peer.ID, error) {
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return "", err
	}
	b, err := crypto.MarshalPrivateKey(key)
	if err != nil {
		return "", err
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		return "", err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(path)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", err
	}
	return id, nil
}
