//go:build unix

package daemon

import (
	"context"
	"encoding/json/v2"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/team"
)

// joinOverLoopback joins b to team name of a through b's daemon, as team
// join does, with b connected to a over TCP on loopback: invite codes carry
// no loopback address, so the join uses that connection.
func joinOverLoopback(t *testing.T, a, b *machine, name, label string) {
	t.Helper()
	b.p.holds.Add(1)
	b.p.Changed()
	eventually(t, "the host of the joiner", func() bool { return b.p.Host() != nil })
	connect(t, a, b)
	secret, expires, err := a.p.set.CreateInvite(name)
	if err != nil {
		t.Fatal(err)
	}
	code, err := p2p.Invite{Team: name, Inviter: a.id, Secret: secret, Expires: expires}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if r := b.dial(t).call("team.join", map[string]any{"code": code, "label": label}); r.Error != nil {
		t.Fatalf("team.join: %+v", r.Error)
	}
	b.p.holds.Add(-1)
	b.p.Changed()
	eventually(t, "the inviter lists the joiner", func() bool { return a.p.set.MemberOf(b.self, name) })
}

// leftOn waits until m holds the entry of id in team name marked left.
func leftOn(t *testing.T, m *machine, name, id string) {
	t.Helper()
	eventually(t, "the leave on the inviter", func() bool {
		tm, ok := m.p.set.Get(name)
		return ok && tm.Members[id].Left
	})
}

// Scenario "Member leaves": B joins A's team over a direct connection and
// leaves it through team.leave of its daemon. A marks B left. When it was
// B's only team, the host of B stops after the leave went out.
func TestTeamLeaveReachesInviter(t *testing.T) {
	var aLog, bLog lockedBuffer
	aKey, _ := newKey(t)
	bKey, _ := newKey(t)
	a := startTeamMachine(t, nil, aKey, logTo(&aLog), func(set *team.Set, self string) {
		newTeam(t, set, self, "courier", "alpha", []string{"t"})
	})
	b := startTeamMachine(t, nil, bKey, logTo(&bLog), func(*team.Set, string) {})
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("log of A:\n%s\nlog of B:\n%s", aLog.String(), bLog.String())
		}
	})
	runP2P(t, a.p)
	runP2P(t, b.p)
	joinOverLoopback(t, a, b, "courier", "bravo")

	if got := leaveText(t, b, "courier"); got != "Left team courier." {
		t.Fatalf("team.leave = %q", got)
	}
	leftOn(t, a, "courier", b.self)
	eventually(t, "the host of B to stop", func() bool { return b.p.Host() == nil })
}

// leaveText runs team.leave of team name on m's daemon and returns its text.
func leaveText(t *testing.T, m *machine, name string) string {
	t.Helper()
	r := m.dial(t).call("team.leave", map[string]any{"team": name})
	if r.Error != nil {
		t.Fatalf("team.leave: %+v", r.Error)
	}
	var res textResult
	if err := json.Unmarshal(r.Result, &res); err != nil {
		t.Fatal(err)
	}
	return res.Text
}

// A leave that reaches no member says so: A's host is gone when B leaves.
func TestTeamLeaveUnreached(t *testing.T) {
	aKey, _ := newKey(t)
	bKey, _ := newKey(t)
	a := startTeamMachine(t, nil, aKey, nil, func(set *team.Set, self string) {
		newTeam(t, set, self, "courier", "alpha", []string{"t"})
	})
	b := startTeamMachine(t, nil, bKey, nil, func(*team.Set, string) {})
	runP2P(t, b.p)
	joinOverLoopback(t, a, b, "courier", "bravo")
	a.p.stop()
	if got := leaveText(t, b, "courier"); got != "Left team courier. The leave did not reach 1 of 1 members." {
		t.Fatalf("team.leave = %q", got)
	}
	if b.p.set.Len() != 0 {
		t.Fatal("B still holds the team after a leave that reached no member")
	}
}

// B in two teams of A leaves one, then the other, its last.
func TestTeamLeaveLastOfTwo(t *testing.T) {
	var aLog, bLog lockedBuffer
	aKey, _ := newKey(t)
	bKey, _ := newKey(t)
	a := startTeamMachine(t, nil, aKey, logTo(&aLog), func(set *team.Set, self string) {
		newTeam(t, set, self, "courier", "alpha", []string{"t"})
		newTeam(t, set, self, "second", "alpha", []string{"s"})
	})
	b := startTeamMachine(t, nil, bKey, logTo(&bLog), func(*team.Set, string) {})
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("log of A:\n%s\nlog of B:\n%s", aLog.String(), bLog.String())
		}
	})
	runP2P(t, a.p)
	runP2P(t, b.p)
	joinOverLoopback(t, a, b, "courier", "bravo")
	joinOverLoopback(t, a, b, "second", "bravo")

	c := b.dial(t)
	for _, name := range []string{"second", "courier"} {
		if r := c.call("team.leave", map[string]any{"team": name}); r.Error != nil {
			t.Fatalf("team.leave %s: %+v", name, r.Error)
		}
		leftOn(t, a, name, b.self)
	}
}

// lanRelay starts a relay on a LAN address of this machine, since invite
// codes carry no loopback address, and returns its relay list entry.
func lanRelay(t *testing.T) string {
	t.Helper()
	ip := lanIP(t)
	rKey, _ := newKey(t)
	rcfg := p2p.DefaultRelayConfig()
	rcfg.Listen = []string{"/ip4/" + ip + "/tcp/0"}
	r, err := p2p.StartRelay(rKey, rcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r.Addrs()[0].String() + "/p2p/" + r.ID().String()
}

// B joins A through team.invite and team.join of their daemons, over a
// direct connection. Then A loses its direct address, as a machine behind a
// NAT whose port mapping ends, and B listens on none, so the leave of B's
// only team reaches A only through A's circuit address on the relay.
func TestTeamLeaveThroughRelay(t *testing.T) {
	entry := lanRelay(t)
	ip := lanIP(t)
	var aLog, bLog lockedBuffer
	aKey, _ := newKey(t)
	bKey, _ := newKey(t)
	a := startTeamMachine(t, nil, aKey, logTo(&aLog), func(set *team.Set, self string) {
		newTeam(t, set, self, "courier", "alpha", []string{"t"})
	}, func(c *P2PConfig) {
		c.Listen = []string{"/ip4/" + ip + "/tcp/0"}
		c.DefaultRelays = []string{entry}
	})
	b := startTeamMachine(t, nil, bKey, logTo(&bLog), func(*team.Set, string) {}, func(c *P2PConfig) {
		c.Listen = nil
		c.DefaultRelays = []string{entry}
	})
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("log of A:\n%s\nlog of B:\n%s", aLog.String(), bLog.String())
		}
	})
	// The resync writes A's circuit address into its entry.
	a.p.resync = 50 * time.Millisecond
	runP2P(t, a.p)
	runP2P(t, b.p)
	eventually(t, "A's circuit address in its entry", func() bool {
		tm, _ := a.p.set.Get("courier")
		return slices.ContainsFunc(tm.Members[a.self].Addrs, func(s string) bool { return strings.Contains(s, "/p2p-circuit") })
	})

	r := a.dial(t).call("team.invite", map[string]any{"team": "courier"})
	if r.Error != nil {
		t.Fatalf("team.invite: %+v", r.Error)
	}
	var inv teamInviteResult
	if err := json.Unmarshal(r.Result, &inv); err != nil {
		t.Fatal(err)
	}
	c := b.dial(t)
	if r := c.call("team.join", map[string]any{"code": inv.Invite.Code, "label": "bravo"}); r.Error != nil {
		t.Fatalf("team.join: %+v", r.Error)
	}
	eventually(t, "the inviter lists the joiner", func() bool { return a.p.set.MemberOf(b.self, "courier") })
	if relayedTo(b, a) {
		t.Fatal("the join used the relay, want a direct connection")
	}

	ah := a.p.Host().Host()
	// The circuit listener stays: it takes the circuits through the relay.
	direct := slices.DeleteFunc(ah.Network().ListenAddresses(), func(a ma.Multiaddr) bool {
		_, err := a.ValueForProtocol(ma.P_CIRCUIT)
		return err == nil
	})
	ah.Network().(*swarm.Swarm).ListenClose(direct...)
	if err := ah.Network().ClosePeer(b.id); err != nil {
		t.Fatal(err)
	}

	if r := c.call("team.leave", map[string]any{"team": "courier"}); r.Error != nil {
		t.Fatalf("team.leave: %+v", r.Error)
	}
	leftOn(t, a, "courier", b.self)
}

// onRecord is a log handler that runs f, once, on the first record with
// message msg, before it passes the record on.
type onRecord struct {
	slog.Handler
	msg  string
	once *sync.Once
	f    func()
}

func (h onRecord) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == h.msg {
		h.once.Do(h.f)
	}
	return h.Handler.Handle(ctx, r)
}

func (h onRecord) WithAttrs(attrs []slog.Attr) slog.Handler {
	h.Handler = h.Handler.WithAttrs(attrs)
	return h
}

func (h onRecord) WithGroup(name string) slog.Handler {
	h.Handler = h.Handler.WithGroup(name)
	return h
}

// A sync of the host lifecycle can run at any moment: on a member list
// change, or on a resync. Here one runs as soon as the daemon logs that B
// left, and the leave of B's only team still reaches A.
func TestTeamLeaveSyncAfterLeft(t *testing.T) {
	aKey, _ := newKey(t)
	bKey, _ := newKey(t)
	a := startTeamMachine(t, nil, aKey, nil, func(set *team.Set, self string) {
		newTeam(t, set, self, "courier", "alpha", []string{"t"})
	})
	var bLog lockedBuffer
	var b *machine
	h := onRecord{Handler: logTo(&bLog).Handler(), msg: "team left", once: new(sync.Once), f: func() {
		b.p.Changed()
		eventually(t, "the sync after the leave", func() bool { return b.p.Host() == nil || b.p.holds.Load() > 0 })
	}}
	b = startTeamMachine(t, nil, bKey, slog.New(h), func(*team.Set, string) {})
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("log of B:\n%s", bLog.String())
		}
	})
	runP2P(t, a.p)
	runP2P(t, b.p)
	joinOverLoopback(t, a, b, "courier", "bravo")
	if r := b.dial(t).call("team.leave", map[string]any{"team": "courier"}); r.Error != nil {
		t.Fatalf("team.leave: %+v", r.Error)
	}
	leftOn(t, a, "courier", b.self)
}

// relayedTo reports whether every connection of m to o is relayed.
func relayedTo(m, o *machine) bool {
	h := m.p.Host()
	if h == nil {
		return false
	}
	conns := h.Host().Network().ConnsToPeer(o.id)
	for _, c := range conns {
		if _, err := c.RemoteMultiaddr().ValueForProtocol(ma.P_CIRCUIT); err != nil {
			return false
		}
	}
	return len(conns) > 0
}
