package team

import (
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestMergeHigherCounterWins(t *testing.T) {
	old := Entry{PeerID: "p1", Label: "old", Join: 1}
	newer := Entry{PeerID: "p1", Label: "new", Join: 2}
	m := Members{}
	if !m.Merge(newer) || m.Merge(old) {
		t.Fatal("an entry with a lower counter replaced one with a higher counter")
	}
	if m["p1"].Label != "new" {
		t.Errorf("label = %q, want new", m["p1"].Label)
	}
	if m.Merge(newer) {
		t.Error("merging the same entry twice changed the list")
	}
}

// TestLeaveBeatsItsJoin shows that a leave wins over the join it follows,
// in both orders, and that only a new join brings the peer back.
func TestLeaveBeatsItsJoin(t *testing.T) {
	join := Entry{PeerID: "p1", Label: "b", Join: 3}
	leave := join
	leave.Left = true
	rejoin := Entry{PeerID: "p1", Label: "b", Join: 4}
	for _, order := range [][]Entry{{join, leave}, {leave, join}} {
		m := Members{}
		for _, e := range order {
			m.Merge(e)
		}
		if !m["p1"].Left || m.Has("p1") {
			t.Errorf("order %v: the join undid the leave", order)
		}
		if !m.Merge(rejoin) || !m.Has("p1") {
			t.Errorf("order %v: a new join did not bring the peer back", order)
		}
		if m.Merge(leave) {
			t.Errorf("order %v: the old leave undid the new join", order)
		}
	}
}

// TestCompareTotal shows that entries that differ in any field still have
// one order, so a merge picks the same one whatever the order.
func TestCompareTotal(t *testing.T) {
	base := Entry{PeerID: "p1", Label: "a", Join: 1, Addrs: []string{"/ip4/10.0.0.1/tcp/1"}}
	for name, edit := range map[string]func(*Entry){
		"label":   func(e *Entry) { e.Label = "b" },
		"addrs":   func(e *Entry) { e.Addrs = []string{"/ip4/10.0.0.2/tcp/1"} },
		"no addr": func(e *Entry) { e.Addrs = nil },
	} {
		other := base
		edit(&other)
		if base.Compare(other) == 0 || base.Compare(other) != -other.Compare(base) {
			t.Errorf("%s: Compare = %d and %d", name, base.Compare(other), other.Compare(base))
		}
		ab, ba := mergeAll([]Entry{base, other}), mergeAll([]Entry{other, base})
		if !reflect.DeepEqual(ab, ba) {
			t.Errorf("%s: the merge depends on the order", name)
		}
	}
}

var peers = []string{"pa", "pb", "pc", "pd"}

// randomEntry draws an entry over few peer ids, counters, labels and
// addresses, so conflicting entries of one peer id with one counter are
// frequent.
func randomEntry(rng *rand.Rand) Entry {
	e := Entry{
		PeerID: peers[rng.IntN(len(peers))],
		Label:  "l" + string(rune('a'+rng.IntN(3))),
		Join:   uint64(1 + rng.IntN(3)),
		Left:   rng.IntN(3) == 0,
	}
	if rng.IntN(2) == 0 {
		e.Addrs = []string{"/ip4/10.0.0." + string(rune('1'+rng.IntN(3))) + "/udp/4001/quic-v1"}
	}
	return e
}

func mergeAll(entries []Entry) Members {
	m := Members{}
	for _, e := range entries {
		m.Merge(e)
	}
	return m
}

// TestMergeOrderProperty shows that any order of merges gives the same
// member list: entries merged one by one in a random order, and lists of
// random subsets merged into each other in a random order, as members that
// gossip partial lists do. It also shows that a leave beats the join it
// follows: no merge order keeps a member whose leave at its counter was
// heard.
func TestMergeOrderProperty(t *testing.T) {
	rng := rand.New(rand.NewPCG(37, 10))
	for iter := range 5000 {
		entries := make([]Entry, 1+rng.IntN(24))
		for i := range entries {
			entries[i] = randomEntry(rng)
		}
		want := mergeAll(entries)
		for _, e := range entries {
			if !e.Left {
				continue
			}
			if w := want[e.PeerID]; w.Join == e.Join && !w.Left {
				t.Fatalf("iteration %d: a join kept %s in the list after its leave at counter %d", iter, e.PeerID, e.Join)
			}
		}

		shuffled := slices.Clone(entries)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		if got := mergeAll(shuffled); !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: shuffled merge\n got %v\nwant %v\nentries %v", iter, got.Entries(), want.Entries(), entries)
		}

		parts := make([]Members, 1+rng.IntN(4))
		for i := range parts {
			parts[i] = Members{}
		}
		for _, e := range shuffled {
			parts[rng.IntN(len(parts))].Merge(e)
			if rng.IntN(3) == 0 {
				parts[rng.IntN(len(parts))].Merge(e)
			}
		}
		rng.Shuffle(len(parts), func(i, j int) { parts[i], parts[j] = parts[j], parts[i] })
		got := Members{}
		for _, p := range parts {
			for _, e := range p.Entries() {
				got.Merge(e)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: merge of partial lists\n got %v\nwant %v\nentries %v", iter, got.Entries(), want.Entries(), entries)
		}
	}
}

func TestEntryValidate(t *testing.T) {
	ok := Entry{PeerID: "12D3KooWabc", Label: "alice", Addrs: []string{"/ip4/192.0.2.1/udp/4001/quic-v1"}, Join: 1}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid entry: %v", err)
	}
	full := ok
	full.Addrs = slices.Repeat([]string{"/dns4/" + strings.Repeat("a", MaxAddr-6)}, MaxAddrs)
	if err := full.Validate(); err != nil {
		t.Fatalf("%d addresses of %d bytes: %v", MaxAddrs, MaxAddr, err)
	}
	for _, tc := range []struct {
		name string
		edit func(*Entry)
		rule string
	}{
		{"empty peer id", func(e *Entry) { e.PeerID = "" }, "peer id"},
		{"peer id with a newline", func(e *Entry) { e.PeerID = "abc\nIGNORE" }, "peer id"},
		{"label in upper case", func(e *Entry) { e.Label = "Alice" }, "label"},
		{"label too long", func(e *Entry) { e.Label = strings.Repeat("a", 33) }, "label"},
		{"address without slash", func(e *Entry) { e.Addrs = []string{"ip4/1.2.3.4"} }, "multiaddr"},
		{"address with a space", func(e *Entry) { e.Addrs = []string{"/ip4/1.2.3.4 x"} }, "multiaddr"},
		{"too many addresses", func(e *Entry) { e.Addrs = slices.Repeat([]string{"/ip4/1.2.3.4"}, MaxAddrs+1) }, "multiaddr"},
		{"address over 256 bytes", func(e *Entry) { e.Addrs = []string{"/dns4/" + strings.Repeat("a", MaxAddr-5)} }, "multiaddr"},
		{"zero counter", func(e *Entry) { e.Join = 0 }, "join counter"},
	} {
		e := ok
		e.Addrs = slices.Clone(ok.Addrs)
		tc.edit(&e)
		err := e.Validate()
		if err == nil || !strings.Contains(err.Error(), tc.rule) {
			t.Errorf("%s: err = %v, want the rule of %s", tc.name, err, tc.rule)
		}
		if err != nil && strings.Contains(err.Error(), "IGNORE") {
			t.Errorf("%s: the error quotes the input", tc.name)
		}
	}
}

func TestCheckName(t *testing.T) {
	for name, want := range map[string]bool{"courier": true, "team_1-x": true, "": false, "-x": false, "Courier": false,
		strings.Repeat("a", 64): true, strings.Repeat("a", 65): false, "a b": false} {
		if got := CheckName(name) == nil; got != want {
			t.Errorf("CheckName(%q) ok = %v, want %v", name, got, want)
		}
	}
}

func TestMemberRendering(t *testing.T) {
	m := Members{
		"12D3KooWaaaaaa111111": {PeerID: "12D3KooWaaaaaa111111", Label: "bob", Join: 1},
		"12D3KooWbbbbbb222222": {PeerID: "12D3KooWbbbbbb222222", Label: "bob", Join: 1},
		"12D3KooWcccccc333333": {PeerID: "12D3KooWcccccc333333", Label: "carol", Join: 1},
		"12D3KooWdddddd444444": {PeerID: "12D3KooWdddddd444444", Label: "carol", Join: 2, Left: true},
	}
	if got := m.Sender("12D3KooWaaaaaa111111", "claude"); got != "bob~111111/claude" {
		t.Errorf("shared label: %q", got)
	}
	if got := m.Sender("12D3KooWcccccc333333", "claude"); got != "carol/claude" {
		t.Errorf("a member that left still shares its label: %q", got)
	}
}
