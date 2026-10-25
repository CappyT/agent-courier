//go:build linux

package daemon

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/team"
)

// inetSockets are the inodes of the TCP and UDP sockets, IPv4 and IPv6, that
// this process holds open.
func inetSockets(t *testing.T) map[string]bool {
	t.Helper()
	own := map[string]bool{}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		link, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if ino, ok := strings.CutPrefix(link, "socket:["); err == nil && ok {
			own[strings.TrimSuffix(ino, "]")] = true
		}
	}
	out := map[string]bool{}
	for _, table := range []string{"tcp", "tcp6", "udp", "udp6"} {
		f, err := os.Open(filepath.Join("/proc/self/net", table))
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Scan() // header
		for sc.Scan() {
			if fields := strings.Fields(sc.Text()); len(fields) > 9 && own[fields[9]] {
				out[fields[9]] = true
			}
		}
		f.Close()
	}
	return out
}

func newSockets(before, now map[string]bool) []string {
	var out []string
	for ino := range now {
		if !before[ino] {
			out = append(out, ino)
		}
	}
	return out
}

func testP2P(t *testing.T) (*P2P, *team.Set, crypto.PrivKey, *atomic.Int32) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "courier.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	var loads atomic.Int32
	set := team.NewSet(nil)
	p := NewP2P(st, set, slog.New(slog.DiscardHandler), P2PConfig{
		Key: func() (crypto.PrivKey, error) {
			loads.Add(1)
			return key, nil
		},
		Listen: []string{"/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.1/udp/0/quic-v1"},
	})
	t.Cleanup(p.stop)
	return p, set, key, &loads
}

// createTeam creates team name with topics on p, with this machine as alpha.
func createTeam(t *testing.T, p *P2P, name string, topics ...string) {
	t.Helper()
	self, err := p.Self()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.set.Create(self, name, topics, "alpha"); err != nil {
		t.Fatal(err)
	}
}

func waitSockets(t *testing.T, what string, cond func(map[string]bool) bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if cond(inetSockets(t)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// A machine with no team starts no host, opens no network socket and has no
// machine key. The same check sees the sockets of the host once the machine
// is in a team, and sees them close when the last team ends. The key is
// made once and lives in memory: a second team and a second host reuse it.
func TestP2PNoTeamNoSocket(t *testing.T) {
	before := inetSockets(t)
	p, set, key, loads := testP2P(t)
	ctx := t.Context()

	p.sync(ctx)
	if p.Host() != nil {
		t.Fatal("a machine with no team started the p2p host")
	}
	if n := loads.Load(); n != 0 {
		t.Fatalf("a machine with no team made the machine key %d times", n)
	}
	if got := newSockets(before, inetSockets(t)); len(got) != 0 {
		t.Fatalf("a machine with no team opened network sockets %v", got)
	}

	createTeam(t, p, "courier", "t")
	p.sync(ctx)
	h := p.Host()
	if h == nil {
		t.Fatal("no p2p host for a machine in a team")
	}
	if id, _ := libp2ppeer.IDFromPrivateKey(key); h.ID() != id || p.IsMember(id) {
		t.Fatalf("host %s, self in the member set %v, want the machine key %s outside it", h.ID(), p.IsMember(id), id)
	}
	waitSockets(t, "the sockets of the host", func(now map[string]bool) bool { return len(newSockets(before, now)) >= 2 })

	left, err := set.Leave("courier")
	if err != nil {
		t.Fatal(err)
	}
	p.Left(left)
	p.sync(ctx)
	if p.Host() != nil {
		t.Fatal("the host runs after the last team ended")
	}
	waitSockets(t, "the sockets of the host to close", func(now map[string]bool) bool { return len(newSockets(before, now)) == 0 })

	createTeam(t, p, "second", "u")
	p.sync(ctx)
	if p.Host() == nil || loads.Load() != 1 {
		t.Fatalf("host %v, the key made %d times, want one key for the life of the daemon", p.Host(), loads.Load())
	}
}

// runP2P runs p until the test ends.
func runP2P(t *testing.T, p *P2P) {
	t.Helper()
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		stop()
		<-done
	})
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !cond(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// Changed starts the host at once: the resync ticker is an hour away.
func TestP2PRunFollowsChanges(t *testing.T) {
	p, _, _, _ := testP2P(t)
	p.resync = time.Hour
	ctx := t.Context()
	done := make(chan struct{})
	runCtx, stop := context.WithCancel(ctx)
	go func() {
		p.Run(runCtx)
		close(done)
	}()
	createTeam(t, p, "courier", "t")
	p.Changed()
	for deadline := time.Now().Add(5 * time.Second); p.Host() == nil; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("Changed did not start the host")
		}
	}
	stop()
	<-done
	if p.Host() != nil {
		t.Fatal("the host runs after Run returned")
	}
}

func newKey(t *testing.T) (crypto.PrivKey, libp2ppeer.ID) {
	t.Helper()
	key, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := libp2ppeer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return key, id
}

// The member set holds the members of the teams of this machine, per team,
// without self and without a team it left. A members frame from a member
// changes it at once.
func TestP2PMemberSet(t *testing.T) {
	p, set, _, _ := testP2P(t)
	ctx := t.Context()
	for _, name := range []string{"one", "two", "gone"} {
		createTeam(t, p, name, "t_"+name)
	}
	_, b := newKey(t)
	_, c := newKey(t)
	addMember(t, set, "one", b.String(), "bravo")
	addMember(t, set, "two", b.String(), "bravo")
	addMember(t, set, "two", c.String(), "charlie")
	addMember(t, set, "gone", c.String(), "charlie")
	p.sync(ctx)
	n := p.current()
	if n == nil {
		t.Fatal("no host")
	}
	selfID := n.host.ID()
	check := func(id libp2ppeer.ID, name string, want bool) {
		t.Helper()
		if got := p.MemberOf(id, name); got != want {
			t.Errorf("MemberOf(%s, %s) = %v, want %v", id, name, got, want)
		}
	}
	check(b, "one", true)
	check(b, "two", true)
	check(c, "two", true)
	check(c, "gone", true)
	check(c, "one", false)
	check(selfID, "one", false)
	if p.IsMember(selfID) {
		t.Error("self is in the member set")
	}
	left, err := set.Leave("gone")
	if err != nil {
		t.Fatal(err)
	}
	p.Left(left)
	// Bravo tells this machine that charlie left team two.
	two, _ := set.Get("two")
	cLeft := two.Members[c.String()]
	cLeft.Left = true
	if _, err := p.deliver(n.members)(ctx, b, p2p.MembersFrame{Team: "two", Entries: []team.Entry{cLeft}}); err != nil {
		t.Fatal(err)
	}
	check(c, "gone", false)
	check(c, "two", false)
	if p.IsMember(c) {
		t.Error("a member that left one team, while this machine left the other, is still in the member set")
	}
	if !p.IsMember(b) {
		t.Error("a member left the member set")
	}

	// A members frame from a peer outside the team changes nothing.
	_, d := newKey(t)
	add := team.Entry{PeerID: d.String(), Label: "delta", Join: 1}
	if _, err := p.deliver(n.members)(ctx, c, p2p.MembersFrame{Team: "two", Entries: []team.Entry{add}}); err != nil {
		t.Fatal(err)
	}
	if p.IsMember(d) {
		t.Fatal("a members frame from a peer outside the team added a member")
	}
}

// dialer is a libp2p peer with key that dials over TCP.
func dialer(t *testing.T, key crypto.PrivKey) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.Identity(key), libp2p.Transport(tcp.NewTCPTransport), libp2p.Security(noise.ID, noise.New),
		libp2p.NoListenAddrs, libp2p.DisableMetrics())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// The host's stranger rule reads the member set: a member's direct
// connection stays, a stranger's closes after the grace.
func TestP2PHostReadsMemberSet(t *testing.T) {
	p, set, _, _ := testP2P(t)
	ctx := t.Context()
	createTeam(t, p, "courier", "t")
	mKey, m := newKey(t)
	addMember(t, set, "courier", m.String(), "bravo")
	p.sync(ctx)
	h := p.Host()
	var addrs []ma.Multiaddr
	for _, a := range h.Host().Network().ListenAddresses() {
		if _, err := a.ValueForProtocol(ma.P_TCP); err == nil {
			addrs = append(addrs, a)
		}
	}
	sKey, _ := newKey(t)
	member, stranger := dialer(t, mKey), dialer(t, sKey)
	for _, d := range []host.Host{member, stranger} {
		if err := d.Connect(ctx, libp2ppeer.AddrInfo{ID: h.ID(), Addrs: addrs}); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "the stranger's connection to close", func() bool { return len(stranger.Network().ConnsToPeer(h.ID())) == 0 })
	if len(member.Network().ConnsToPeer(h.ID())) == 0 {
		t.Fatal("the member's connection was closed")
	}
}

// A daemon that stops leaves every team first: LeaveAll forgets the teams
// and sends each member the list with this machine's entry marked left.
func TestP2PLeaveAll(t *testing.T) {
	p, set, _, _ := testP2P(t)
	ctx := t.Context()
	createTeam(t, p, "one", "t_one")
	createTeam(t, p, "two", "t_two")
	mKey, m := newKey(t)
	addMember(t, set, "one", m.String(), "bravo")
	addMember(t, set, "two", m.String(), "bravo")
	p.sync(ctx)
	h := p.Host()
	self := h.ID().String()

	member := dialer(t, mKey)
	var mu sync.Mutex
	left := map[string]bool{}
	handler := p2p.NewMsgHandler(ctx, func(libp2ppeer.ID) bool { return true },
		func(_ context.Context, _ libp2ppeer.ID, f p2p.Frame) (p2p.Frame, error) {
			if mf, ok := f.(p2p.MembersFrame); ok {
				for _, e := range mf.Entries {
					if e.PeerID == self && e.Left {
						mu.Lock()
						left[mf.Team] = true
						mu.Unlock()
					}
				}
			}
			return nil, nil
		}, slog.New(slog.DiscardHandler))
	member.SetStreamHandler(p2p.MsgProtocol, handler.Handle)
	if err := member.Connect(ctx, libp2ppeer.AddrInfo{ID: h.ID(), Addrs: h.Host().Network().ListenAddresses()}); err != nil {
		t.Fatal(err)
	}

	p.LeaveAll()
	if set.Len() != 0 {
		t.Fatalf("%d teams after LeaveAll", set.Len())
	}
	mu.Lock()
	defer mu.Unlock()
	if !left["one"] || !left["two"] {
		t.Fatalf("the member got the leave of %v, want one and two", left)
	}
}

// startRelay starts a relay on loopback and returns its relay list entry.
func startRelay(t *testing.T) string {
	t.Helper()
	rKey, _ := newKey(t)
	rcfg := p2p.DefaultRelayConfig()
	rcfg.Listen = []string{"/ip4/127.0.0.1/tcp/0"}
	r, err := p2p.StartRelay(rKey, rcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r.Addrs()[0].String() + "/p2p/" + r.ID().String()
}

// The default relay list reaches the host for every team: the host reserves
// on it and lists its circuit address for each team.
func TestP2PDefaultRelayList(t *testing.T) {
	p, _, _, _ := testP2P(t)
	p.resync = time.Hour
	entry := startRelay(t)
	p.cfg.DefaultRelays = []string{entry}
	runP2P(t, p)
	createTeam(t, p, "courier", "t")
	createTeam(t, p, "other", "o")
	p.Changed()
	self, _ := p.Self()
	want := entry + "/p2p-circuit/p2p/" + self
	eventually(t, "a reservation on the default relay for each team", func() bool {
		h := p.Host()
		return h != nil && slices.ContainsFunc(h.CircuitAddrs("courier"), func(a ma.Multiaddr) bool { return a.String() == want }) &&
			slices.ContainsFunc(h.CircuitAddrs("other"), func(a ma.Multiaddr) bool { return a.String() == want })
	})
}

// inviteCode is a valid invite code of an inviter with key at addrs.
func inviteCode(t *testing.T, key crypto.PrivKey, addrs ...string) string {
	t.Helper()
	id, err := libp2ppeer.IDFromPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	inv := p2p.Invite{Team: "courier", Inviter: id, Secret: make([]byte, team.SecretSize), Expires: time.Now().Add(time.Hour)}
	for _, a := range addrs {
		inv.Addrs = append(inv.Addrs, ma.StringCast(a))
	}
	code, err := inv.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return code
}

// lanIP is an IPv4 address of this machine outside loopback: invite codes
// drop loopback addresses. The test skips without one.
func lanIP(t *testing.T) string {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && n.IP.To4() != nil && !n.IP.IsLoopback() && !n.IP.IsLinkLocalUnicast() {
			return n.IP.String()
		}
	}
	t.Skip("this machine has no IPv4 address outside loopback")
	return ""
}

// silentAddr is a TCP address of this machine, outside loopback, that
// accepts connections and never answers. stop closes the listener and its
// connections.
func silentAddr(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ip := lanIP(t)
	ln, err := net.Listen("tcp", net.JoinHostPort(ip, "0"))
	if err != nil {
		t.Fatal(err)
	}
	conns := make(chan net.Conn, 16)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				close(conns)
				return
			}
			conns <- c
		}
	}()
	stop = sync.OnceFunc(func() {
		ln.Close()
		for c := range conns {
			c.Close()
		}
	})
	t.Cleanup(stop)
	return fmt.Sprintf("/ip4/%s/tcp/%d", ip, ln.Addr().(*net.TCPAddr).Port), stop
}

// A join on a machine with no team starts the host for the join and stops
// it when the join ends: after a refused dial, and after the hold when the
// inviter never answers.
func TestP2PJoinWithoutTeam(t *testing.T) {
	before := inetSockets(t)
	p, _, _, loads := testP2P(t)
	p.resync = time.Hour
	runP2P(t, p)
	if _, err := p.Join(t.Context(), "not a code", "bravo"); !errors.Is(err, p2p.ErrBadInviteCode) {
		t.Fatalf("join with a bad code = %v", err)
	}
	if loads.Load() != 0 || p.Host() != nil {
		t.Fatal("a join with a bad code started the host")
	}

	inviter, _ := newKey(t)
	type result struct{ err error }
	join := func(code string) <-chan result {
		out := make(chan result, 1)
		go func() {
			_, err := p.Join(t.Context(), code, "bravo")
			out <- result{err}
		}()
		return out
	}
	p.joinHold = 500 * time.Millisecond
	silent, stopSilent := silentAddr(t)
	done := join(inviteCode(t, inviter, silent))
	eventually(t, "the host of the join", func() bool { return p.Host() != nil })
	begin := time.Now()
	select {
	case r := <-done:
		if !errors.Is(r.err, p2p.ErrInviterUnreachable) {
			t.Fatalf("join with a silent inviter = %v", r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the join outlived its hold")
	}
	if d := time.Since(begin); d > 2*time.Second {
		t.Errorf("the join ended %v after its hold of %v", d, p.joinHold)
	}
	eventually(t, "the host to stop after the join", func() bool { return p.Host() == nil })
	stopSilent()
	waitSockets(t, "the sockets of the join to close", func(now map[string]bool) bool { return len(newSockets(before, now)) == 0 })

	p.joinHold = joinHold
	if r := <-join(inviteCode(t, inviter, "/ip4/"+lanIP(t)+"/tcp/1")); !errors.Is(r.err, p2p.ErrInviterUnreachable) {
		t.Fatalf("join with a refused inviter = %v", r.err)
	}
	eventually(t, "the host to stop after the join", func() bool { return p.Host() == nil })
}

// InviteCode carries this machine's peer id, and waits for the reservation
// on the default relay, whose circuit address comes first. The resync writes
// that address into this machine's member list entry.
func TestP2PInviteCode(t *testing.T) {
	p, set, _, _ := testP2P(t)
	entry := startRelay(t)
	p.cfg.DefaultRelays = []string{entry}
	createTeam(t, p, "courier", "t")
	// The host starts with the relay list, as after team.create.
	p.resync = 50 * time.Millisecond
	runP2P(t, p)
	code, err := p.InviteCode("courier", make([]byte, team.SecretSize), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	inv, err := p2p.DecodeInvite(code)
	if err != nil {
		t.Fatal(err)
	}
	self, _ := p.Self()
	if inv.Inviter.String() != self || inv.Team != "courier" {
		t.Fatalf("invite of %s for %s", inv.Inviter, inv.Team)
	}
	// DecodeInvite drops the loopback address of the test relay, so the
	// reservation shows on the host: InviteCode waited for it.
	if got := p.Host().AdvertiseAddrs("courier"); len(got) == 0 || got[0].String() != entry+"/p2p-circuit" {
		t.Fatalf("addresses %v after InviteCode, want %s first", got, entry+"/p2p-circuit")
	}
	eventually(t, "the circuit address in this machine's entry", func() bool {
		addrs := set.Addrs("courier", self)
		return len(addrs) > 0 && addrs[0] == entry+"/p2p-circuit"
	})
}
