//go:build unix

package p2p

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	pbv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/pb"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/proto"
	relayv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

const echoProto = "/agent-courier/test/echo"

func testRelayConfig() RelayConfig {
	c := DefaultRelayConfig()
	c.Listen = []string{"/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/udp/0/quic-v1"}
	// All test peers share 127.0.0.1.
	c.MaxReservationsPerIP = 64
	return c
}

func startTestRelay(t *testing.T, cfg RelayConfig) *Relay {
	t.Helper()
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := StartRelay(key, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// newPeer is a member host on loopback with the relay client enabled, which
// libp2p does by default. transport is "tcp" or "quic".
func newPeer(t *testing.T, transport string) host.Host {
	t.Helper()
	opts := []libp2p.Option{libp2p.Security(noise.ID, noise.New), libp2p.DisableMetrics()}
	switch transport {
	case "tcp":
		opts = append(opts, libp2p.Transport(tcp.NewTCPTransport), libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	case "quic":
		opts = append(opts, libp2p.Transport(libp2pquic.NewTransport), libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/quic-v1"))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	h.SetStreamHandler(echoProto, func(s network.Stream) {
		defer s.Close()
		io.Copy(s, s)
	})
	return h
}

// relayInfo is the relay's listen address for the given transport.
func relayInfo(t *testing.T, r *Relay, transport string) peer.AddrInfo {
	t.Helper()
	code := ma.P_TCP
	if transport == "quic" {
		code = ma.P_QUIC_V1
	}
	for _, a := range r.Addrs() {
		if _, err := a.ValueForProtocol(code); err == nil {
			return peer.AddrInfo{ID: r.ID(), Addrs: []ma.Multiaddr{a}}
		}
	}
	t.Fatalf("the relay has no %s address in %v", transport, r.Addrs())
	return peer.AddrInfo{}
}

func reserve(t *testing.T, h host.Host, relay peer.AddrInfo) (*client.Reservation, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.Connect(ctx, relay); err != nil {
		t.Fatalf("connect to the relay: %v", err)
	}
	return client.Reserve(ctx, h, relay)
}

func wantRefused(t *testing.T, err error, what string) {
	t.Helper()
	var rerr client.ReservationError
	if !errors.As(err, &rerr) || rerr.Status != pbv2.Status_RESERVATION_REFUSED {
		t.Fatalf("%s: err = %v, want RESERVATION_REFUSED", what, err)
	}
}

func circuitAddr(t *testing.T, relay peer.AddrInfo) ma.Multiaddr {
	t.Helper()
	a, err := ma.NewMultiaddr(relay.Addrs[0].String() + "/p2p/" + relay.ID.String() + "/p2p-circuit")
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// dialCircuit opens an echo stream from src to dst through the relay.
func dialCircuit(t *testing.T, src host.Host, relay peer.AddrInfo, dst peer.ID) network.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = network.WithAllowLimitedConn(ctx, "test")
	if err := src.Connect(ctx, peer.AddrInfo{ID: dst, Addrs: []ma.Multiaddr{circuitAddr(t, relay)}}); err != nil {
		t.Fatalf("connect through the relay: %v", err)
	}
	for _, c := range src.Network().ConnsToPeer(dst) {
		if _, err := c.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT); err != nil {
			t.Fatalf("connection to the destination is not relayed: %v", c.RemoteMultiaddr())
		}
	}
	s, err := src.NewStream(ctx, dst, echoProto)
	if err != nil {
		t.Fatalf("open a stream through the relay: %v", err)
	}
	return s
}

func echo(t *testing.T, s network.Stream, msg []byte) {
	t.Helper()
	s.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := s.Write(msg); err != nil {
		t.Fatalf("write through the relay: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(s, got); err != nil {
		t.Fatalf("read through the relay: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo = %q, want %q", got, msg)
	}
}

// relayEventually polls cond for up to 10 seconds.
func relayEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRelayReserveAndCircuit(t *testing.T) {
	cfg := testRelayConfig()
	cfg.ReservationTTL = 5 * time.Minute
	r := startTestRelay(t, cfg)
	a := newPeer(t, "tcp")
	b := newPeer(t, "quic")
	rsvp, err := reserve(t, a, relayInfo(t, r, "tcp"))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if d := time.Until(rsvp.Expiration); d < cfg.ReservationTTL-time.Minute || d > cfg.ReservationTTL+time.Minute {
		t.Fatalf("reservation expires in %v, want about %v", d, cfg.ReservationTTL)
	}
	if rsvp.LimitDuration != cfg.MaxDuration || rsvp.LimitData != uint64(cfg.MaxData) {
		t.Fatalf("reservation limits %v and %d bytes, want %v and %d", rsvp.LimitDuration, rsvp.LimitData, cfg.MaxDuration, cfg.MaxData)
	}
	if got := r.Stats().Reservations; got != 1 {
		t.Fatalf("reservations = %d, want 1", got)
	}
	s := dialCircuit(t, b, relayInfo(t, r, "quic"), a.ID())
	defer s.Close()
	echo(t, s, []byte("hello through the relay"))
	if got := r.Stats().Circuits; got != 1 {
		t.Fatalf("circuits = %d, want 1", got)
	}
}

func TestRelayReservationCap(t *testing.T) {
	cfg := testRelayConfig()
	cfg.MaxReservations, cfg.MaxReservationsPerIP = 2, 2
	r := startTestRelay(t, cfg)
	info := relayInfo(t, r, "tcp")
	held := []host.Host{newPeer(t, "tcp"), newPeer(t, "tcp")}
	for i, h := range held {
		if _, err := reserve(t, h, info); err != nil {
			t.Fatalf("reservation %d: %v", i+1, err)
		}
	}
	late := newPeer(t, "tcp")
	_, err := reserve(t, late, info)
	wantRefused(t, err, "reservation over the cap")
	st := r.Stats()
	if st.Reservations != 2 || st.ReservationsRefused["RESERVATION_REFUSED"] != 1 {
		t.Fatalf("stats = %+v, want 2 reservations and 1 refused", st)
	}
	// The relay keeps the existing reservations: the refused peer still
	// reaches a peer that holds one.
	s := dialCircuit(t, late, info, held[0].ID())
	defer s.Close()
	echo(t, s, []byte("still reserved"))
}

func TestRelayReservationCapPerIP(t *testing.T) {
	cfg := testRelayConfig()
	cfg.MaxReservations, cfg.MaxReservationsPerIP = 4, 1
	r := startTestRelay(t, cfg)
	info := relayInfo(t, r, "tcp")
	if _, err := reserve(t, newPeer(t, "tcp"), info); err != nil {
		t.Fatalf("first reservation from 127.0.0.1: %v", err)
	}
	_, err := reserve(t, newPeer(t, "tcp"), info)
	wantRefused(t, err, "second reservation from 127.0.0.1")
}

func TestRelayRenewalAndClose(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	info := relayInfo(t, r, "tcp")
	a, b, c := newPeer(t, "tcp"), newPeer(t, "tcp"), newPeer(t, "tcp")
	for i := range 2 {
		if _, err := reserve(t, a, info); err != nil {
			t.Fatalf("reservation %d: %v", i+1, err)
		}
	}
	if got := r.Stats().Reservations; got != 1 {
		t.Fatalf("reservations after a renewal = %d, want 1", got)
	}

	// A circuit to a peer without a reservation is refused and counted.
	ctx, cancel := context.WithTimeout(network.WithAllowLimitedConn(context.Background(), "test"), 10*time.Second)
	defer cancel()
	if err := b.Connect(ctx, peer.AddrInfo{ID: c.ID(), Addrs: []ma.Multiaddr{circuitAddr(t, info)}}); err == nil {
		t.Fatal("connected through the relay to a peer without a reservation")
	}
	if got := r.Stats().CircuitsRefused["NO_RESERVATION"]; got != 1 {
		t.Fatalf("circuits refused = %v, want NO_RESERVATION 1", r.Stats().CircuitsRefused)
	}

	s := dialCircuit(t, b, info, a.ID())
	echo(t, s, []byte("open"))
	if got := r.Stats().Circuits; got != 1 {
		t.Fatalf("circuits = %d, want 1", got)
	}
	s.Close()
	b.Network().ClosePeer(a.ID())
	relayEventually(t, "the circuit count to drop to 0", func() bool { return r.Stats().Circuits == 0 })

	a.Close()
	relayEventually(t, "the reservation count to drop to 0", func() bool { return r.Stats().Reservations == 0 })
}

// TestRelayHalfOpenFlood opens more idle TCP sockets than the libp2p
// default transient limit (about 42) allows, none of which sends a byte, then
// checks that a peer still reserves a slot.
func TestRelayHalfOpenFlood(t *testing.T) {
	cfg := testRelayConfig()
	cfg.MaxReservations = 128
	r := startTestRelay(t, cfg)
	info := relayInfo(t, r, "tcp")
	tcpAddr, err := manet.ToNetAddr(info.Addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	const idle = 100
	def := rcmgr.DefaultLimits
	old := int(def.Scale(relaySystemMemory-def.SystemBaseLimit.Memory, relayFDs).ToPartialLimitConfig().Transient.ConnsInbound)
	if transient := int(cfg.limits().ToPartialLimitConfig().Transient.ConnsInbound); old >= idle || transient <= idle {
		t.Fatalf("libp2p default transient limit %d and relay limit %d, want %d idle sockets between them", old, transient, idle)
	}
	for range idle {
		c, err := net.Dial("tcp", tcpAddr.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
	}
	// Wait until the idle sockets hold at least as many transient slots as
	// the libp2p default allows.
	relayEventually(t, "the relay to accept the idle sockets", func() bool {
		var n int
		r.host.Network().ResourceManager().ViewTransient(func(s network.ResourceScope) error {
			n = s.Stat().NumConnsInbound
			return nil
		})
		return n >= old
	})
	if _, err := reserve(t, newPeer(t, "tcp"), info); err != nil {
		t.Fatalf("reserve during %d idle connections: %v", idle, err)
	}
}

func TestRelayDataLimit(t *testing.T) {
	cfg := testRelayConfig()
	// The limit counts the whole relayed connection: Noise, yamux and
	// identify use part of it.
	cfg.MaxData = 16 << 10
	r := startTestRelay(t, cfg)
	info := relayInfo(t, r, "tcp")
	a, b := newPeer(t, "tcp"), newPeer(t, "tcp")
	if _, err := reserve(t, a, info); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	s := dialCircuit(t, b, info, a.ID())
	defer s.Close()
	echo(t, s, []byte("small"))

	s.SetDeadline(time.Now().Add(10 * time.Second))
	go func() {
		s.Write(make([]byte, 256<<10))
		s.CloseWrite()
	}()
	n, err := io.Copy(io.Discard, s)
	if err == nil {
		t.Fatalf("read %d bytes and a clean end, want a reset after about %d bytes", n, cfg.MaxData)
	}
	if n >= cfg.MaxData {
		t.Fatalf("read %d bytes through the relay, want fewer than the %d byte limit", n, cfg.MaxData)
	}
}

func TestRelayDurationLimit(t *testing.T) {
	cfg := testRelayConfig()
	cfg.MaxDuration = time.Second
	r := startTestRelay(t, cfg)
	info := relayInfo(t, r, "tcp")
	a, b := newPeer(t, "tcp"), newPeer(t, "tcp")
	if _, err := reserve(t, a, info); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	start := time.Now()
	s := dialCircuit(t, b, info, a.ID())
	defer s.Close()
	echo(t, s, []byte("before the limit"))

	s.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err := s.Read(make([]byte, 1))
	elapsed := time.Since(start)
	var nerr net.Error
	if err == nil || errors.As(err, &nerr) && nerr.Timeout() {
		t.Fatalf("read err = %v after %v, want the relay to reset the stream after %v", err, elapsed, cfg.MaxDuration)
	}
	if elapsed < cfg.MaxDuration/2 {
		t.Fatalf("the relayed connection ended after %v, before the %v limit", elapsed, cfg.MaxDuration)
	}
}

func TestRelayAnnounce(t *testing.T) {
	cfg := testRelayConfig()
	cfg.Announce = []string{"/ip4/1.2.3.4/tcp/4001", "/dns4/relay.example.com/udp/4001/quic-v1"}
	r := startTestRelay(t, cfg)
	if !r.HasPublicAddr() {
		t.Fatalf("advertised %v, want a public address", r.Advertised())
	}
	rsvp, err := reserve(t, newPeer(t, "tcp"), relayInfo(t, r, "tcp"))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	want := "/ip4/1.2.3.4/tcp/4001/p2p/" + r.ID().String()
	if !slices.ContainsFunc(rsvp.Addrs, func(a ma.Multiaddr) bool { return a.String() == want }) {
		t.Fatalf("reservation addresses %v lack %s", rsvp.Addrs, want)
	}
	for _, a := range rsvp.Addrs {
		if strings.Contains(a.String(), "/127.0.0.1/") {
			t.Fatalf("reservation addresses %v hold a listen address", rsvp.Addrs)
		}
	}

	// Without an announce address, a loopback relay has no public address.
	if startTestRelay(t, testRelayConfig()).HasPublicAddr() {
		t.Fatal("a loopback relay reports a public address")
	}
}

func TestRelayAnnounceRefused(t *testing.T) {
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	other, err := peer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	for addr, rule := range map[string]string{
		"/ip4/10.0.0.1/tcp/4001":                                       "public",
		"/ip4/127.0.0.1/udp/4001/quic-v1":                              "public",
		"/dns4/localhost/tcp/4001":                                     "public",
		"/ip4/1.2.3.4/tcp/4001/p2p/" + other.String():                  "/p2p",
		"/ip4/1.2.3.4/tcp/4001/p2p/" + other.String() + "/p2p-circuit": "/p2p",
		"relay.example.com":                                            "announce address",
	} {
		cfg := testRelayConfig()
		cfg.Announce = []string{addr}
		r, err := StartRelay(key, cfg)
		if err == nil {
			r.Close()
			t.Errorf("announce %s: StartRelay succeeded", addr)
			continue
		}
		if !strings.Contains(err.Error(), rule) {
			t.Errorf("announce %s: err = %v, want it to name %q", addr, err, rule)
		}
	}
}

// TestRelayProtocols checks that the relay serves hop and identify only: no
// relay client (stop) and no ping.
func TestRelayProtocols(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	got := r.host.Mux().Protocols()
	for _, p := range []protocol.ID{proto.ProtoIDv2Hop, identify.ID} {
		if !slices.Contains(got, p) {
			t.Errorf("protocols %v lack %s", got, p)
		}
	}
	for _, p := range []protocol.ID{proto.ProtoIDv2Stop, ping.ID} {
		if slices.Contains(got, p) {
			t.Errorf("protocols %v hold %s", got, p)
		}
	}
}

func TestRelayLimits(t *testing.T) {
	cfg := DefaultRelayConfig()
	if cfg.MaxReservations != 1024 || cfg.MaxReservationsPerIP != 8 || cfg.MaxCircuits != 16 {
		t.Fatalf("defaults = %+v, want 1024 reservations, 8 per IP, 16 circuits", cfg)
	}
	res := cfg.resources()
	if res.Limit.Duration != 10*time.Minute || res.Limit.Data != 1<<20 {
		t.Fatalf("relayed connection limit = %+v, want 10m and 1 MiB", res.Limit)
	}
	// At the bounds, the relay limits stay below the resource manager limits.
	for _, c := range []struct{ rsv, circ int }{{1024, 16}, {MaxRelayReservations, MaxRelayCircuitsTotal / MaxRelayReservations}, {MaxRelayCircuitsTotal / MaxRelayCircuits, MaxRelayCircuits}} {
		cfg.MaxReservations, cfg.MaxCircuits = c.rsv, c.circ
		if err := cfg.validate(); err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		l := cfg.limits().ToPartialLimitConfig()
		circuits := c.rsv * c.circ
		if got := int64(l.System.Memory); got != relaySystemMemory {
			t.Errorf("%+v: system memory = %d, want %d", c, got, relaySystemMemory)
		}
		svc := l.Service[relayv2.ServiceName]
		if int(svc.Streams) < 2*circuits || int(svc.StreamsInbound) < circuits || int(svc.StreamsOutbound) < circuits {
			t.Errorf("%+v: relay service streams = %+v, want room for %d circuits", c, svc, circuits)
		}
		if int64(svc.Memory) > relaySystemMemory*3/4 {
			t.Errorf("%+v: relay service memory %d leaves too little of the system memory %d", c, svc.Memory, relaySystemMemory)
		}
		if int(l.System.Streams) < int(svc.Streams) || int(l.System.ConnsInbound) < 2*c.rsv {
			t.Errorf("%+v: system limits %+v below the relay limits", c, l.System)
		}
		tr := l.Transient
		if int(tr.ConnsInbound) < c.rsv || int(tr.StreamsInbound) < 2*c.rsv {
			t.Errorf("%+v: transient limits %+v leave no room for half-open connections", c, tr)
		}
		if tr.ConnsInbound > l.System.ConnsInbound || tr.Conns > l.System.Conns || tr.FD > l.System.FD || tr.StreamsInbound > l.System.StreamsInbound || tr.Streams > l.System.Streams {
			t.Errorf("%+v: transient limits %+v above the system limits %+v", c, tr, l.System)
		}
		sp := l.ServicePeer[relayv2.ServiceName]
		if int(sp.Streams) != c.circ+4 || int(sp.StreamsInbound) != c.circ+4 || int(sp.StreamsOutbound) != c.circ+4 {
			t.Errorf("%+v: relay streams per peer = %+v, want %d", c, sp, c.circ+4)
		}
		for _, p := range []protocol.ID{proto.ProtoIDv2Hop, proto.ProtoIDv2Stop} {
			if int(l.Protocol[p].Streams) < int(svc.Streams) || int(l.ProtocolPeer[p].Streams) < c.circ {
				t.Errorf("%+v: %s limits below the relay limits", c, p)
			}
		}
	}
}

// TestRelayAppliesLimits reads the limits back from the running host's
// resource manager.
func TestRelayAppliesLimits(t *testing.T) {
	cfg := testRelayConfig()
	cfg.MaxReservations, cfg.MaxCircuits = 300, 20
	r := startTestRelay(t, cfg)
	want := cfg.limits().ToPartialLimitConfig()
	limitOf := func(s network.ResourceScope) rcmgr.Limit {
		l, ok := s.(rcmgr.ResourceScopeLimiter)
		if !ok {
			t.Fatalf("scope %T exposes no limit", s)
		}
		return l.Limit()
	}
	mgr := r.host.Network().ResourceManager()
	mgr.ViewSystem(func(s network.ResourceScope) error {
		l := limitOf(s)
		if l.GetMemoryLimit() != relaySystemMemory || l.GetConnLimit(network.DirInbound) != int(want.System.ConnsInbound) {
			t.Errorf("system limit memory %d inbound conns %d, want %d and %d", l.GetMemoryLimit(), l.GetConnLimit(network.DirInbound), relaySystemMemory, want.System.ConnsInbound)
		}
		return nil
	})
	mgr.ViewTransient(func(s network.ResourceScope) error {
		l := limitOf(s)
		if l.GetConnLimit(network.DirInbound) != int(want.Transient.ConnsInbound) || l.GetConnLimit(network.DirInbound) < cfg.MaxReservations {
			t.Errorf("transient inbound conns %d, want %d", l.GetConnLimit(network.DirInbound), want.Transient.ConnsInbound)
		}
		if l.GetStreamLimit(network.DirInbound) != int(want.Transient.StreamsInbound) || l.GetFDLimit() != int(want.Transient.FD) {
			t.Errorf("transient inbound streams %d fd %d, want %d and %d", l.GetStreamLimit(network.DirInbound), l.GetFDLimit(), want.Transient.StreamsInbound, want.Transient.FD)
		}
		return nil
	})
	mgr.ViewService(relayv2.ServiceName, func(s network.ServiceScope) error {
		l := limitOf(s)
		if l.GetStreamTotalLimit() != int(want.Service[relayv2.ServiceName].Streams) || l.GetStreamTotalLimit() < 2*300*20 {
			t.Errorf("relay service streams %d, want %d", l.GetStreamTotalLimit(), want.Service[relayv2.ServiceName].Streams)
		}
		return nil
	})
	mgr.ViewProtocol(proto.ProtoIDv2Hop, func(s network.ProtocolScope) error {
		if got := limitOf(s).GetStreamTotalLimit(); got != int(want.Protocol[proto.ProtoIDv2Hop].Streams) {
			t.Errorf("hop protocol streams %d, want %d", got, want.Protocol[proto.ProtoIDv2Hop].Streams)
		}
		return nil
	})
}

func TestRelayConfigInvalid(t *testing.T) {
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*RelayConfig){
		"no listen":           func(c *RelayConfig) { c.Listen = nil },
		"no reservations":     func(c *RelayConfig) { c.MaxReservations = 0 },
		"too many":            func(c *RelayConfig) { c.MaxReservations = MaxRelayReservations + 1 },
		"per IP over total":   func(c *RelayConfig) { c.MaxReservations, c.MaxReservationsPerIP = 4, 5 },
		"too many circuits":   func(c *RelayConfig) { c.MaxCircuits = MaxRelayCircuits + 1 },
		"circuit total":       func(c *RelayConfig) { c.MaxReservations, c.MaxCircuits = MaxRelayReservations, MaxRelayCircuits },
		"no duration":         func(c *RelayConfig) { c.MaxDuration = 0 },
		"long duration":       func(c *RelayConfig) { c.MaxDuration = MaxRelayDuration + time.Second },
		"long TTL":            func(c *RelayConfig) { c.ReservationTTL = MaxRelayReservationTTL + time.Second },
		"no data":             func(c *RelayConfig) { c.MaxData = -1 },
		"too much data":       func(c *RelayConfig) { c.MaxData = MaxRelayData + 1 },
		"unparsable announce": func(c *RelayConfig) { c.Announce = []string{"relay.example"} },
	} {
		cfg := testRelayConfig()
		edit(&cfg)
		if r, err := StartRelay(key, cfg); err == nil {
			r.Close()
			t.Errorf("%s: StartRelay succeeded", name)
		}
	}
}

func TestRelayKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "relay.key")
	id, err := GenerateRelayKey(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := GenerateRelayKey(path); err == nil {
		t.Fatal("GenerateRelayKey replaced an existing key")
	}
	if got, err := LoadRelayID(path); err != nil || got != id {
		t.Fatalf("LoadRelayID = %s, %v, want %s", got, err, id)
	}

	// A Kubernetes Secret volume mounts the key as a symlink.
	link := filepath.Join(dir, "link.key")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRelayKey(link); err != nil {
		t.Fatalf("load through a symlink: %v", err)
	}
	// 0440 is a Secret with defaultMode 0440 and an fsGroup.
	for _, mode := range []os.FileMode{0o400, 0o440, 0o600, 0o640} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadRelayKey(path); err != nil {
			t.Errorf("mode %v: %v", mode, err)
		}
	}
	for _, mode := range []os.FileMode{0o604, 0o660, 0o644, 0o620, 0o602} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadRelayKey(path); err == nil || !strings.Contains(err.Error(), "readable by others or writable by the group") {
			t.Errorf("mode %v: err = %v, want a refusal", mode, err)
		}
	}

	bad := filepath.Join(dir, "bad.key")
	if err := os.WriteFile(bad, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRelayKey(bad); err == nil {
		t.Fatal("loaded a file that holds no key")
	}
	big := filepath.Join(dir, "big.key")
	if err := os.WriteFile(big, make([]byte, maxRelayKey+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRelayKey(big); err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Fatalf("load a %d byte file: err = %v, want the size refusal", maxRelayKey+1, err)
	}
	fifo := filepath.Join(dir, "fifo.key")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{dir, fifo} {
		if _, err := LoadRelayKey(p); err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Errorf("load %s: err = %v, want the regular file refusal", p, err)
		}
	}
}

// TestRelayStableIdentity restarts the relay with the same key.
func TestRelayStableIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.key")
	id, err := GenerateRelayKey(path)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		key, err := LoadRelayKey(path)
		if err != nil {
			t.Fatal(err)
		}
		r, err := StartRelay(key, testRelayConfig())
		if err != nil {
			t.Fatal(err)
		}
		got := r.ID()
		r.Close()
		if got != id {
			t.Fatalf("relay id %s, want %s", got, id)
		}
	}
}

const relayLogHelperEnv = "AGENT_COURIER_RELAY_LOG_HELPER"

// TestRelayLogsNoPeerID runs a relay in a child process with libp2p debug
// logging asked for, relays a connection through it, and looks for the
// peers' ids on the child's stderr.
func TestRelayLogsNoPeerID(t *testing.T) {
	if os.Getenv(relayLogHelperEnv) == "1" {
		relayLogHelper()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRelayLogsNoPeerID$", "-test.v=false")
	cmd.Env = append(os.Environ(), relayLogHelperEnv+"=1", "GOLOG_LOG_LEVEL=debug")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		stdin.Close()
		cmd.Wait()
		t.Fatalf("read the relay address: %v; stderr:\n%s", err, stderr.String())
	}
	info, err := peer.AddrInfoFromString(strings.TrimSpace(line))
	if err != nil {
		t.Fatal(err)
	}
	a, b := newPeer(t, "tcp"), newPeer(t, "tcp")
	if _, err := reserve(t, a, *info); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	s := dialCircuit(t, b, *info, a.ID())
	echo(t, s, []byte("hello"))
	s.Close()
	stdin.Close()
	go io.Copy(io.Discard, stdout)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("relay process: %v; stderr:\n%s", err, stderr.String())
	}
	for _, id := range []peer.ID{a.ID(), b.ID()} {
		if strings.Contains(stderr.String(), id.String()) {
			t.Errorf("relay stderr holds peer id %s:\n%s", id, stderr.String())
		}
	}
}

// relayLogHelper is the child of TestRelayLogsNoPeerID: it prints the relay
// address and serves until stdin closes.
func relayLogHelper() {
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		panic(err)
	}
	cfg := testRelayConfig()
	cfg.Listen = []string{"/ip4/127.0.0.1/tcp/0"}
	r, err := StartRelay(key, cfg)
	if err != nil {
		panic(err)
	}
	os.Stdout.WriteString(r.Addrs()[0].String() + "/p2p/" + r.ID().String() + "\n")
	io.Copy(io.Discard, os.Stdin)
	r.Close()
}
