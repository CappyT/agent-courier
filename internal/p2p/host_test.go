//go:build unix

package p2p

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/gologshim"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"github.com/libp2p/go-libp2p/p2p/protocol/autonatv2"
	ma "github.com/multiformats/go-multiaddr"
)

// hostTeam is the team of the relay list of startTestHost.
const hostTeam = "t"

// members is a concurrent member set for tests.
type members struct {
	mu  sync.Mutex
	ids map[peer.ID]map[string]bool
}

func (m *members) isMember(id peer.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.ids[id]) > 0
}

func (m *members) memberOf(id peer.ID, team string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ids[id][team]
}

// add makes id a member of hostTeam.
func (m *members) add(id peer.ID) { m.addTo(id, hostTeam) }

func (m *members) addTo(id peer.ID, team string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ids == nil {
		m.ids = map[peer.ID]map[string]bool{}
	}
	if m.ids[id] == nil {
		m.ids[id] = map[string]bool{}
	}
	m.ids[id][team] = true
}

// relayEntryOf is the relay list entry of r on transport.
func relayEntryOf(t *testing.T, r *Relay, transport string) string {
	t.Helper()
	info := relayInfo(t, r, transport)
	return info.Addrs[0].String() + "/p2p/" + info.ID.String()
}

// startTestHost starts a host that listens on listen, with the relay list of
// hostTeam and short waits. tune changes it before the start.
func startTestHost(t *testing.T, listen []string, relays []string, m *members, tune func(*HostConfig)) *Host {
	t.Helper()
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := HostConfig{
		Key:           key,
		Listen:        listen,
		IsMember:      m.isMember,
		MemberOf:      m.memberOf,
		Log:           slog.New(slog.DiscardHandler),
		readyWait:     300 * time.Millisecond,
		retryMin:      100 * time.Millisecond,
		retryMax:      time.Second,
		allowLoopback: true,
	}
	if relays != nil {
		cfg.Relays = map[string][]string{hostTeam: relays}
	}
	if tune != nil {
		tune(&cfg)
	}
	h, err := StartHost(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func waitCircuit(t *testing.T, h *Host) []string {
	t.Helper()
	relayEventually(t, "a reservation", func() bool { return len(h.CircuitAddrs(hostTeam)) > 0 })
	var out []string
	for _, a := range h.CircuitAddrs(hostTeam) {
		out = append(out, a.String())
	}
	return out
}

func strs(addrs []ma.Multiaddr) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.String())
	}
	return out
}

// sendFrame opens a msg stream from src to dst, which may be relayed, sends
// a message and returns the reply.
func sendFrame(t *testing.T, src *Host, dst peer.ID, f Frame) Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(network.WithAllowLimitedConn(t.Context(), "test"), waitFor)
	defer cancel()
	s, err := src.Host().NewStream(ctx, dst, MsgProtocol)
	if err != nil {
		t.Fatalf("open a msg stream: %v", err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(waitFor))
	if err := WriteFrame(s, f); err != nil {
		t.Fatalf("write: %v", err)
	}
	s.CloseWrite()
	reply, err := ReadFrame(s)
	if err != nil {
		t.Fatalf("read the reply: %v", err)
	}
	return reply
}

func serveMsg(h *Host, isMember IsMember, got chan<- delivery) {
	m := NewMsgHandler(context.Background(), isMember, func(_ context.Context, from peer.ID, f Frame) (Frame, error) {
		got <- delivery{from, f}
		if msg, ok := f.(MessageFrame); ok {
			return ReceiptFrame{ID: msg.ID}, nil
		}
		return nil, nil
	}, slog.New(slog.DiscardHandler))
	h.Host().SetStreamHandler(MsgProtocol, m.Handle)
}

func relayed(h *Host, to peer.ID) bool {
	conns := h.Host().Network().ConnsToPeer(to)
	if len(conns) == 0 {
		return false
	}
	for _, c := range conns {
		if !c.Stat().Limited {
			return false
		}
	}
	return true
}

// Two hosts with no listen address cannot dial each other. They hold a
// reservation on the relay and exchange a frame through MsgHandler over the
// relayed connection.
func TestHostRelayedFrame(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	entry := relayEntryOf(t, r, "tcp")
	var m members
	a := startTestHost(t, nil, []string{entry}, &m, nil)
	b := startTestHost(t, nil, []string{entry}, &m, nil)
	m.add(a.ID())
	m.add(b.ID())
	if len(a.Host().Network().ListenAddresses()) > 1 || len(b.Host().Network().ListenAddresses()) > 1 {
		t.Fatalf("listen addresses %v and %v, want only the circuit", a.Host().Network().ListenAddresses(), b.Host().Network().ListenAddresses())
	}
	got := make(chan delivery, 4)
	serveMsg(a, m.isMember, got)
	connected := make(chan peer.ID, 4)
	a.OnConnected(func(id peer.ID) { connected <- id })

	circuit := waitCircuit(t, a)
	want := entry + "/p2p-circuit/p2p/" + a.ID().String()
	if !slices.Equal(circuit, []string{want}) {
		t.Fatalf("circuit addresses %v, want [%s]", circuit, want)
	}
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	if err := b.Connect(ctx, a.ID(), circuit); err != nil {
		t.Fatalf("connect through the relay: %v", err)
	}
	if !relayed(b, a.ID()) {
		t.Fatal("the connection is not relayed")
	}
	msg := testMessage("through the relay")
	if reply := sendFrame(t, b, a.ID(), msg); !reflect.DeepEqual(reply, ReceiptFrame{ID: msg.ID}) {
		t.Fatalf("reply %v, want the receipt", reply)
	}
	select {
	case d := <-got:
		if d.from != b.ID() || !reflect.DeepEqual(d.frame, msg) {
			t.Fatalf("delivered %v from %s", d.frame, d.from)
		}
	case <-time.After(waitFor):
		t.Fatal("no delivery")
	}
	for deadline := time.After(waitFor); ; {
		select {
		case id := <-connected:
			if id != b.ID() {
				continue
			}
		case <-deadline:
			t.Fatal("OnConnected did not run for the member")
		}
		break
	}
}

// The member list names only a direct address that refuses the dial:
// Connect falls back to a circuit through the relay of its own list.
func TestHostFallsBackToRelay(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	entry := relayEntryOf(t, r, "tcp")
	var m members
	a := startTestHost(t, nil, []string{entry}, &m, nil)
	b := startTestHost(t, nil, []string{entry}, &m, nil)
	m.add(a.ID())
	m.add(b.ID())
	got := make(chan delivery, 4)
	serveMsg(a, m.isMember, got)
	waitCircuit(t, a)
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	// Port 1 on loopback refuses the connection at once.
	if err := b.Connect(ctx, a.ID(), []string{"/ip4/127.0.0.1/tcp/1/p2p/" + a.ID().String()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if !relayed(b, a.ID()) {
		t.Fatal("the connection is not relayed")
	}
	msg := testMessage("fallback")
	if reply := sendFrame(t, b, a.ID(), msg); !reflect.DeepEqual(reply, ReceiptFrame{ID: msg.ID}) {
		t.Fatalf("reply %v, want the receipt", reply)
	}
}

// With a direct address and a circuit address, the direct one wins.
func TestHostDirectFirst(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	entry := relayEntryOf(t, r, "tcp")
	var m members
	loopback := []string{"/ip4/127.0.0.1/tcp/0"}
	a := startTestHost(t, loopback, []string{entry}, &m, nil)
	b := startTestHost(t, loopback, []string{entry}, &m, nil)
	circuit := waitCircuit(t, a)
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	addrs := append(circuit, strs(a.Host().Network().ListenAddresses())...)
	if err := b.Connect(ctx, a.ID(), addrs); err != nil {
		t.Fatalf("connect: %v", err)
	}
	for _, c := range b.Host().Network().ConnsToPeer(a.ID()) {
		if c.Stat().Limited {
			t.Fatalf("connection %v is relayed, want direct", c.RemoteMultiaddr())
		}
	}
}

func TestHostRenewsReservation(t *testing.T) {
	cfg := testRelayConfig()
	cfg.ReservationTTL = 2 * time.Second
	r := startTestRelay(t, cfg)
	var m members
	a := startTestHost(t, nil, []string{relayEntryOf(t, r, "tcp")}, &m, nil)
	renewals := func() int {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.renewals
	}
	relayEventually(t, "three renewals of a 2 s reservation", func() bool { return renewals() >= 4 })
	if len(a.CircuitAddrs(hostTeam)) != 1 {
		t.Fatalf("circuit addresses %v after renewals", a.CircuitAddrs(hostTeam))
	}
}

// The first relay of the list is down: the host reserves on the second.
func TestHostFirstReachableRelay(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	down := startTestRelay(t, testRelayConfig())
	downEntry := relayEntryOf(t, down, "tcp")
	down.Close()
	var m members
	a := startTestHost(t, nil, []string{downEntry, relayEntryOf(t, r, "tcp")}, &m, nil)
	circuit := waitCircuit(t, a)
	if want := relayEntryOf(t, r, "tcp") + "/p2p-circuit/p2p/" + a.ID().String(); !slices.Equal(circuit, []string{want}) {
		t.Fatalf("circuit addresses %v, want [%s]", circuit, want)
	}
}

// A stranger's inbound connection, relayed or direct, closes after the
// grace; a member's stays and is protected, and a joiner's stays while its
// join stream is open.
func TestHostClosesStrangers(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	info := relayInfo(t, r, "tcp")
	for _, mode := range []string{"relayed", "direct"} {
		t.Run(mode, func(t *testing.T) {
			var m members
			const grace = 300 * time.Millisecond
			a := startTestHost(t, []string{"/ip4/127.0.0.1/tcp/0"}, []string{relayEntryOf(t, r, "tcp")}, &m, func(c *HostConfig) { c.strangerGrace = grace })
			release := make(chan struct{})
			a.Host().SetStreamHandler(JoinProtocol, func(s network.Stream) {
				<-release
				s.Close()
			})
			waitCircuit(t, a)
			addr := circuitAddr(t, info)
			if mode == "direct" {
				addr = directAddr(t, a)
			}
			stranger, member, joiner := newPeer(t, "tcp"), newPeer(t, "tcp"), newPeer(t, "tcp")
			m.add(member.ID())
			ctx, cancel := context.WithTimeout(network.WithAllowLimitedConn(t.Context(), "test"), waitFor)
			defer cancel()
			for _, p := range []host.Host{stranger, member, joiner} {
				if err := p.Connect(ctx, peer.AddrInfo{ID: a.ID(), Addrs: []ma.Multiaddr{addr}}); err != nil {
					t.Fatalf("connect: %v", err)
				}
				if got := slices.ContainsFunc(p.Network().ConnsToPeer(a.ID()), func(c network.Conn) bool { return c.Stat().Limited }); got != (mode == "relayed") {
					t.Fatalf("relayed connection %v, want %v", got, mode == "relayed")
				}
			}
			js, err := joiner.NewStream(ctx, a.ID(), JoinProtocol)
			if err != nil {
				t.Fatalf("open a join stream: %v", err)
			}
			defer js.Reset()
			// The join handler runs once the protocol is negotiated, which the
			// first write forces.
			js.Write([]byte{0})

			relayEventually(t, "the stranger's connection to close", func() bool {
				return len(stranger.Network().ConnsToPeer(a.ID())) == 0
			})
			time.Sleep(3 * grace)
			if len(member.Network().ConnsToPeer(a.ID())) == 0 {
				t.Fatal("the member's connection was closed")
			}
			if cm := a.Host().ConnManager(); !cm.IsProtected(member.ID(), memberTag) || cm.IsProtected(joiner.ID(), memberTag) {
				t.Fatalf("protected: member %v, joiner %v; want only the member", cm.IsProtected(member.ID(), memberTag), cm.IsProtected(joiner.ID(), memberTag))
			}
			if len(joiner.Network().ConnsToPeer(a.ID())) == 0 {
				t.Fatal("the connection of a peer with an open join stream was closed")
			}
			close(release)
			relayEventually(t, "the joiner's connection to close after its join stream ended", func() bool {
				return len(joiner.Network().ConnsToPeer(a.ID())) == 0
			})
			if len(member.Network().ConnsToPeer(a.ID())) == 0 {
				t.Fatal("the member's connection was closed")
			}
		})
	}
}

// directAddr is the TCP listen address of h.
func directAddr(t *testing.T, h *Host) ma.Multiaddr {
	t.Helper()
	for _, a := range h.Host().Network().ListenAddresses() {
		if _, err := a.ValueForProtocol(ma.P_TCP); err == nil {
			return a
		}
	}
	t.Fatalf("no TCP listen address in %v", h.Host().Network().ListenAddresses())
	return nil
}

// ProtectMembers follows the member set: a peer that joins it gets
// protected, a peer that leaves it loses the protection.
func TestHostProtectMembers(t *testing.T) {
	var m members
	a := startTestHost(t, []string{"/ip4/127.0.0.1/tcp/0"}, nil, &m, nil)
	p := newPeer(t, "tcp")
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	if err := p.Connect(ctx, peer.AddrInfo{ID: a.ID(), Addrs: []ma.Multiaddr{directAddr(t, a)}}); err != nil {
		t.Fatal(err)
	}
	cm := a.Host().ConnManager()
	if cm.IsProtected(p.ID(), memberTag) {
		t.Fatal("a stranger is protected")
	}
	m.add(p.ID())
	a.ProtectMembers()
	if !cm.IsProtected(p.ID(), memberTag) {
		t.Fatal("a member is not protected")
	}
	m.mu.Lock()
	delete(m.ids, p.ID())
	m.mu.Unlock()
	a.ProtectMembers()
	if cm.IsProtected(p.ID(), memberTag) {
		t.Fatal("a peer that left the member set is still protected")
	}
}

func TestDialAddrs(t *testing.T) {
	var m members
	relayA := "/ip4/192.0.2.9/udp/4001/quic-v1/p2p/12D3KooWAJjbRkp8FPF5MKgMU53aUTxWkqvDrs4zc1VMbwRwfsbE"
	relayB := "/ip4/192.0.2.10/tcp/4001/p2p/12D3KooWLJtG8fd2hkQzTn96MrLvThmnNQjTUFZwGEsLRz5EmSzc"
	h := startTestHost(t, nil, nil, &m, func(c *HostConfig) {
		c.allowLoopback = false
		c.Relays = map[string][]string{"a": {relayA}, "b": {relayB}}
	})
	other := "12D3KooWLJtG8fd2hkQzTn96MrLvThmnNQjTUFZwGEsLRz5EmSzc"
	target, err := peer.Decode("12D3KooWQYhTNQdmr3ArTeUHRYzFg94BKyTkoWBDWez9kSCVe2Xo")
	if err != nil {
		t.Fatal(err)
	}
	m.addTo(target, "a")
	got := strs(h.dialAddrs(target, []string{
		"/ip4/192.0.2.1/tcp/4001/p2p/" + target.String(),
		"/ip4/192.0.2.2/tcp/4001/p2p/" + other,
		"/ip4/127.0.0.1/tcp/4001",
		"/ip6/::1/udp/4001/quic-v1",
		"/ip4/0.0.0.0/tcp/4001",
		"/dns4/localhost/tcp/4001",
		"/unix/tmp/x.sock",
		"/ip4/169.254.169.254/tcp/80",
		"/ip6/fe80::1/tcp/4001",
		"/ip4/224.0.0.251/udp/5353",
		"/ip4/198.51.100.1/udp/4001/quic-v1",
		"/ip4/192.168.1.5/tcp/4001",
		"/dns4/relay.example/tcp/4001/p2p/" + other + "/p2p-circuit/p2p/" + target.String(),
		"/ip4/127.0.0.1/tcp/4001/p2p/" + other + "/p2p-circuit/p2p/" + target.String(),
		"not a multiaddr",
	}))
	// Only the relays of team a, the one the target shares.
	want := []string{
		"/ip4/192.0.2.1/tcp/4001",
		"/ip4/198.51.100.1/udp/4001/quic-v1",
		"/ip4/192.168.1.5/tcp/4001",
		"/dns4/relay.example/tcp/4001/p2p/" + other + "/p2p-circuit",
		relayA + "/p2p-circuit",
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("dial addresses\n%v\nwant\n%v", got, want)
	}
}

func TestDialable(t *testing.T) {
	for s, want := range map[string]bool{
		"/ip4/192.0.2.1/tcp/4001":                       true,
		"/ip4/192.168.1.5/udp/4001/quic-v1":             true,
		"/ip4/10.0.0.1/tcp/4001":                        true,
		"/ip6/fd00::1/tcp/4001":                         true,
		"/dns4/relay.cappyt.sh/tcp/4001":                true,
		"/dns/notlocalhost/tcp/4001":                    true,
		"/ip4/127.0.0.1/tcp/4001":                       false,
		"/ip4/127.9.9.9/tcp/4001":                       false,
		"/ip6/::1/tcp/4001":                             false,
		"/ip6/::ffff:127.0.0.1/tcp/4001":                false,
		"/ip4/0.0.0.0/tcp/4001":                         false,
		"/ip6/::/udp/4001/quic-v1":                      false,
		"/dns4/localhost/tcp/4001":                      false,
		"/dns6/LOCALHOST./tcp/4001":                     false,
		"/dns/localhost/tcp/4001":                       false,
		"/dnsaddr/localhost":                            false,
		"/dns4/app.localhost/tcp/4001":                  false,
		"/dns4/a.b.localhost/udp/4001/quic-v1":          false,
		"/unix/tmp/x.sock":                              false,
		"/ip4/169.254.169.254/tcp/80":                   false,
		"/ip4/169.254.1.1/udp/4001/quic-v1":             false,
		"/ip6/fe80::1/tcp/4001":                         false,
		"/ip6/::ffff:169.254.169.254/tcp/80":            false,
		"/ip4/224.0.0.251/udp/5353":                     false,
		"/ip6/ff02::1/udp/4001/quic-v1":                 false,
		"/ip4/192.0.2.1/tcp/4001/p2p-circuit/unix/a.so": false,
	} {
		if got := Dialable(ma.StringCast(s)); got != want {
			t.Errorf("Dialable(%s) = %v, want %v", s, got, want)
		}
	}
	if Dialable(nil) {
		t.Error("Dialable(nil) = true")
	}
}

// CircuitAddrs of a team lists only the relays of that team.
func TestCircuitAddrsPerTeam(t *testing.T) {
	ra, rb := startTestRelay(t, testRelayConfig()), startTestRelay(t, testRelayConfig())
	ea, eb := relayEntryOf(t, ra, "tcp"), relayEntryOf(t, rb, "tcp")
	var m members
	h := startTestHost(t, nil, nil, &m, func(c *HostConfig) {
		c.Relays = map[string][]string{"a": {ea}, "b": {eb}}
	})
	suffix := "/p2p-circuit/p2p/" + h.ID().String()
	relayEventually(t, "both reservations", func() bool { return len(h.CircuitAddrs("a")) == 1 && len(h.CircuitAddrs("b")) == 1 })
	if got := strs(h.CircuitAddrs("a")); !slices.Equal(got, []string{ea + suffix}) {
		t.Errorf("circuit addresses of a %v, want [%s]", got, ea+suffix)
	}
	if got := strs(h.CircuitAddrs("b")); !slices.Equal(got, []string{eb + suffix}) {
		t.Errorf("circuit addresses of b %v, want [%s]", got, eb+suffix)
	}
	if got := h.CircuitAddrs("c"); len(got) != 0 {
		t.Errorf("circuit addresses of an unknown team %v", got)
	}
	adv := strs(h.AdvertiseAddrs("a"))
	if len(adv) == 0 || adv[0] != ea+"/p2p-circuit" || slices.Contains(adv, eb+"/p2p-circuit") {
		t.Errorf("advertised addresses of a %v, want %s first and no relay of b", adv, ea+"/p2p-circuit")
	}
}

// silentRelay is a relay list entry whose address accepts TCP connections
// and never answers, so an attempt on it lasts until its timeout.
func silentRelay(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", ln.Addr().(*net.TCPAddr).Port, id)
}

// Silent relay lists do not delay the reservation of another list.
func TestHostListsReserveInParallel(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	entry := relayEntryOf(t, r, "tcp")
	var m members
	lists := map[string][]string{"live": {entry}}
	for i := range 3 {
		lists[fmt.Sprint("silent", i)] = []string{silentRelay(t)}
	}
	start := time.Now()
	h := startTestHost(t, nil, nil, &m, func(c *HostConfig) { c.Relays = lists })
	for time.Since(start) < 3*time.Second && len(h.CircuitAddrs("live")) == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if len(h.CircuitAddrs("live")) == 0 {
		t.Fatalf("no reservation on the live relay %v after the start, behind silent lists", time.Since(start))
	}
}

// A SetRelays without a change neither wakes the lists, which keeps their
// backoff, nor logs the bad entries again.
func TestHostSetRelaysOnlyOnChange(t *testing.T) {
	var logs recorder
	var m members
	refused := "/ip4/127.0.0.1/tcp/1/p2p/12D3KooWAJjbRkp8FPF5MKgMU53aUTxWkqvDrs4zc1VMbwRwfsbE"
	lists := map[string][]string{hostTeam: {refused, "/ip4/192.0.2.1/tcp/4001"}}
	h := startTestHost(t, nil, nil, &m, func(c *HostConfig) {
		c.Relays = lists
		c.Log = slog.New(&logs)
		c.retryMin, c.retryMax = time.Hour, time.Hour
	})
	relayEventually(t, "the first attempt", func() bool { return logs.count("reservation failed") == 1 })
	for range 5 {
		h.SetRelays(map[string][]string{hostTeam: slices.Clone(lists[hostTeam])})
	}
	time.Sleep(300 * time.Millisecond)
	if n := logs.count("reservation failed"); n != 1 {
		t.Errorf("%d attempts after SetRelays without a change, want 1", n)
	}
	if n := logs.count("relay list entry dropped"); n != 1 {
		t.Errorf("the bad entry was logged %d times, want once", n)
	}
	h.SetRelays(map[string][]string{hostTeam: lists[hostTeam], "other": {refused}})
	relayEventually(t, "the bad entry logged again after a change", func() bool { return logs.count("relay list entry dropped") == 2 })
}

// When the connection to the relay drops, the host reserves again at once,
// long before the renewal.
func TestHostReservesAfterRelayLoss(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	var m members
	a := startTestHost(t, nil, []string{relayEntryOf(t, r, "tcp")}, &m, func(c *HostConfig) {
		c.retryMin, c.retryMax = time.Hour, time.Hour
	})
	waitCircuit(t, a)
	if n := a.renewalCount(); n != 1 {
		t.Fatalf("%d reservations, want 1", n)
	}
	a.Host().Network().ClosePeer(r.ID())
	relayEventually(t, "a new reservation after the relay loss", func() bool {
		return a.renewalCount() == 2 && len(a.CircuitAddrs(hostTeam)) == 1
	})
}

// The connection to the relay drops between Reserve and the store: the host
// sees it after the store and reserves again.
func TestHostStaleReservation(t *testing.T) {
	r := startTestRelay(t, testRelayConfig())
	var m members
	var once sync.Once
	a := startTestHost(t, nil, []string{relayEntryOf(t, r, "tcp")}, &m, func(c *HostConfig) {
		c.retryMin, c.retryMax = time.Hour, time.Hour
		c.beforeStore = func(h host.Host, relay peer.ID) {
			once.Do(func() {
				h.Network().ClosePeer(relay)
				for h.Network().Connectedness(relay) == network.Connected {
					time.Sleep(5 * time.Millisecond)
				}
				// Lets the host handle the disconnect event first.
				time.Sleep(200 * time.Millisecond)
			})
		}
	})
	relayEventually(t, "a reservation over a live connection", func() bool {
		return a.renewalCount() == 2 && a.Host().Network().Connectedness(r.ID()) == network.Connected && len(a.CircuitAddrs(hostTeam)) == 1
	})
}

func (h *Host) renewalCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.renewals
}

func TestRenewAt(t *testing.T) {
	now := time.Unix(1000, 0)
	if got, want := renewAt(now, now.Add(time.Hour)), now.Add(45*time.Minute); !got.Equal(want) {
		t.Fatalf("renewAt = %v, want %v", got, want)
	}
}

// Connect waits for the reservation and for DCUtR, at most until readyWait
// after the start.
func TestWaitReady(t *testing.T) {
	const ready = 300 * time.Millisecond
	newHost := func() *Host {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		return &Host{ctx: ctx, cancel: cancel, start: time.Now(), cfg: HostConfig{readyWait: ready},
			reserved: make(chan struct{}), punch: make(chan struct{})}
	}
	h := newHost()
	begin := time.Now()
	if err := h.waitReady(t.Context()); err != nil || time.Since(begin) < ready-20*time.Millisecond {
		t.Fatalf("waitReady without a reservation = %v after %v, want nil after %v", err, time.Since(begin), ready)
	}
	h = newHost()
	close(h.reserved)
	begin = time.Now()
	if err := h.waitReady(t.Context()); err != nil || time.Since(begin) < ready-20*time.Millisecond {
		t.Fatalf("waitReady without DCUtR = %v after %v, want nil after %v", err, time.Since(begin), ready)
	}
	if err := h.WaitReserved(t.Context()); err != nil {
		t.Fatal(err)
	}
	h = newHost()
	close(h.reserved)
	close(h.punch)
	begin = time.Now()
	if err := h.waitReady(t.Context()); err != nil || time.Since(begin) > ready/2 {
		t.Fatalf("waitReady when ready = %v after %v, want nil at once", err, time.Since(begin))
	}
	h = newHost()
	h.cancel()
	if err := h.waitReady(t.Context()); !errors.Is(err, ErrHostClosed) {
		t.Fatalf("waitReady on a closed host = %v", err)
	}
}

// recorder keeps the records it gets, at every level.
type recorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec.Clone())
	return nil
}

func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }

func (r *recorder) count(msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.records {
		if rec.Message == msg {
			n++
		}
	}
	return n
}

func (r *recorder) level(msg string) (slog.Level, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rec := range r.records {
		if rec.Message == msg {
			return rec.Level, true
		}
	}
	return 0, false
}

// The libp2p loggers reach the daemon log at debug level only.
func TestLibp2pLogsAtDebug(t *testing.T) {
	var logs recorder
	var m members
	startTestHost(t, nil, nil, &m, func(c *HostConfig) { c.Log = slog.New(&logs) })
	gologshim.Logger("agent-courier-test").Error("a libp2p error")
	if lvl, ok := logs.level("a libp2p error"); !ok || lvl != slog.LevelDebug {
		t.Fatalf("libp2p error record: level %v, present %v; want debug", lvl, ok)
	}
	info := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})
	if (debugHandler{info}).Enabled(t.Context(), slog.LevelError) {
		t.Fatal("a daemon log at info level takes libp2p errors")
	}
}

// The host has the options of the design: the ranker directFirst, AutoNAT
// v2, hole punching, the relay client, and no AutoRelay.
func TestHostOptions(t *testing.T) {
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	var cfg libp2p.Config
	if err := cfg.Apply(hostOptions(HostConfig{Key: key}, nil)...); err != nil {
		t.Fatal(err)
	}
	if cfg.DialRanker == nil || reflect.ValueOf(cfg.DialRanker).Pointer() != reflect.ValueOf(directFirst).Pointer() {
		t.Error("the host does not rank its dials with directFirst")
	}
	if !cfg.EnableAutoNATv2 || !cfg.EnableHolePunching || !cfg.Relay || cfg.EnableAutoRelay {
		t.Errorf("AutoNAT v2 %v, hole punching %v, relay client %v, AutoRelay %v; want all but AutoRelay",
			cfg.EnableAutoNATv2, cfg.EnableHolePunching, cfg.Relay, cfg.EnableAutoRelay)
	}
}

// The host runs the AutoNAT v2 client and serves no dial request.
func TestHostServesNoAutoNATv2(t *testing.T) {
	var m members
	h := startTestHost(t, []string{"/ip4/127.0.0.1/tcp/0"}, nil, &m, nil)
	protos := h.Host().Mux().Protocols()
	if slices.Contains(protos, autonatv2.DialProtocol) {
		t.Errorf("the host serves %s", autonatv2.DialProtocol)
	}
	if !slices.Contains(protos, autonatv2.DialBackProtocol) {
		t.Errorf("the host lacks the AutoNAT v2 client %s", autonatv2.DialBackProtocol)
	}
}

func TestParseRelay(t *testing.T) {
	id := "12D3KooWAJjbRkp8FPF5MKgMU53aUTxWkqvDrs4zc1VMbwRwfsbE"
	for _, ok := range []string{"/dns4/relay.cappyt.sh/udp/4001/quic-v1/p2p/" + id, "/ip4/192.0.2.1/tcp/4001/p2p/" + id} {
		if _, got, err := ParseRelay(ok); err != nil || got.String() != id {
			t.Errorf("ParseRelay(%s) = %s, %v", ok, got, err)
		}
	}
	for _, bad := range []string{
		"/ip4/192.0.2.1/tcp/4001",
		"/p2p/" + id,
		"/ip4/192.0.2.1/tcp/4001/p2p/" + id + "/p2p-circuit/p2p/" + id,
		"/unix/tmp/x/p2p/" + id,
		"relay.cappyt.sh",
	} {
		if _, _, err := ParseRelay(bad); err == nil {
			t.Errorf("ParseRelay(%s) passed", bad)
		}
	}
	if len(DefaultRelays()) != 2 {
		t.Errorf("default relays %v, want the project relay over QUIC and TCP", DefaultRelays())
	}
	for _, s := range DefaultRelays() {
		if _, got, err := ParseRelay(s); err != nil || got.String() != ProjectRelayID {
			t.Errorf("default relay %s: %s, %v", s, got, err)
		}
	}
}

// The relay waits behind private direct addresses, which the default ranker
// dials at the same time as the relay.
func TestDirectFirstRanker(t *testing.T) {
	direct := ma.StringCast("/ip4/192.168.1.2/udp/4001/quic-v1")
	circuit := ma.StringCast("/ip4/192.0.2.9/udp/4001/quic-v1/p2p/12D3KooWAJjbRkp8FPF5MKgMU53aUTxWkqvDrs4zc1VMbwRwfsbE/p2p-circuit")
	for _, d := range directFirst([]ma.Multiaddr{circuit, direct}) {
		if d.Addr.Equal(circuit) && d.Delay < swarm.RelayDelay {
			t.Fatalf("relay delay %v, want at least %v", d.Delay, swarm.RelayDelay)
		}
		if d.Addr.Equal(direct) && d.Delay != 0 {
			t.Fatalf("direct delay %v, want 0", d.Delay)
		}
	}
	if got := directFirst([]ma.Multiaddr{circuit}); len(got) != 1 || got[0].Delay != 0 {
		t.Fatalf("a relay alone = %v, want no delay", got)
	}
}

// The resource manager of a running host holds the limits of hostLimits.
func TestHostLimits(t *testing.T) {
	cfg := hostLimits().ToPartialLimitConfig()
	msg := cfg.ProtocolPeer[MsgProtocol]
	if msg.StreamsInbound != MaxStreamsPerPeer || msg.StreamsOutbound != MaxStreamsPerPeer {
		t.Errorf("msg protocol peer limits %+v", msg)
	}
	if join := cfg.ProtocolPeer[JoinProtocol]; join.StreamsInbound != joinStreamsPerPeer || join.StreamsOutbound != joinStreamsPerPeer {
		t.Errorf("join protocol peer limits %+v", join)
	}

	var m members
	h := startTestHost(t, nil, nil, &m, nil)
	rm := h.Host().Network().ResourceManager()
	limit := func(s any) rcmgr.Limit {
		l, ok := s.(rcmgr.ResourceScopeLimiter)
		if !ok {
			t.Fatalf("scope %T has no limit", s)
		}
		return l.Limit()
	}
	someone := newPeer(t, "tcp").ID()
	err := rm.ViewPeer(someone, func(s network.PeerScope) error {
		l := limit(s)
		if l.GetConnLimit(network.DirInbound) != peerConnsInbound || l.GetConnLimit(network.DirOutbound) != peerConnsOutbound ||
			l.GetStreamLimit(network.DirInbound) != peerStreamsInbound || l.GetStreamLimit(network.DirOutbound) != peerStreamsOutbound {
			t.Errorf("peer limits: conns %d in, %d out; streams %d in, %d out",
				l.GetConnLimit(network.DirInbound), l.GetConnLimit(network.DirOutbound),
				l.GetStreamLimit(network.DirInbound), l.GetStreamLimit(network.DirOutbound))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for proto, want := range map[protocol.ID]int{MsgProtocol: msgStreamsTotal, JoinProtocol: joinStreamsTotal} {
		err := rm.ViewProtocol(proto, func(s network.ProtocolScope) error {
			if l := limit(s); l.GetStreamLimit(network.DirInbound) != want || l.GetStreamLimit(network.DirOutbound) != want {
				t.Errorf("%s limits: streams %d in, %d out, want %d", proto, l.GetStreamLimit(network.DirInbound), l.GetStreamLimit(network.DirOutbound), want)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
