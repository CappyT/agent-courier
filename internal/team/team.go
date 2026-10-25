// Package team holds the teams of this machine in memory: their topics,
// relay lists, member lists and invites. Members are trusted, so a member
// list entry carries no signature. A peer id is an opaque string here: the
// p2p package maps it to a libp2p peer id.
package team

import (
	"cmp"
	"errors"
	"regexp"
	"slices"
	"strings"
)

const (
	// MaxTopics is the most topics a team has.
	MaxTopics = 16
	// MaxMembers is the most members a team holds. Entries of members that
	// left do not count.
	MaxMembers = 64
	// MaxTeams is the most teams a machine follows.
	MaxTeams = 16
	// MaxAddrs and MaxAddr cap the addresses of one member list entry.
	MaxAddrs = 4
	MaxAddr  = 256
)

var (
	nameRule   = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	labelRule  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	peerIDRule = regexp.MustCompile(`^[A-Za-z0-9]{1,128}$`)
	addrRule   = regexp.MustCompile(`^/[!-~]*$`)
)

var (
	errName   = errors.New("a team name is 1 to 64 characters of a-z, 0-9, _ and -, and starts with a letter or a digit")
	errLabel  = errors.New("a member label is 1 to 32 characters of a-z, 0-9 and -, and starts with a letter or a digit")
	errPeerID = errors.New("a peer id is 1 to 128 characters of A-Z, a-z and 0-9")
	errAddrs  = errors.New("a member list entry has at most 4 addresses, each a multiaddr of at most 256 printable ASCII characters that starts with /")
	errJoin   = errors.New("the join counter of a member list entry is at least 1")
)

func CheckName(name string) error {
	if !nameRule.MatchString(name) {
		return errName
	}
	return nil
}

func CheckLabel(label string) error {
	if !labelRule.MatchString(label) {
		return errLabel
	}
	return nil
}

func CheckPeerID(id string) error {
	if !peerIDRule.MatchString(id) {
		return errPeerID
	}
	return nil
}

// Entry is what a member list knows of one member. Join counts the joins of
// the peer id: an inviter sets it to one more than the counter it holds for
// the peer id, and a member raises its own when its addresses change. Left
// marks a member that left.
type Entry struct {
	PeerID string   `json:"peer_id"`
	Label  string   `json:"label"`
	Addrs  []string `json:"addrs,omitempty"`
	Join   uint64   `json:"join"`
	Left   bool     `json:"left,omitempty"`
}

// Validate checks every field against its rule. The error names the rule and
// never quotes the entry.
func (e Entry) Validate() error {
	if err := CheckPeerID(e.PeerID); err != nil {
		return err
	}
	if err := CheckLabel(e.Label); err != nil {
		return err
	}
	if len(e.Addrs) > MaxAddrs {
		return errAddrs
	}
	for _, a := range e.Addrs {
		if len(a) > MaxAddr || !addrRule.MatchString(a) {
			return errAddrs
		}
	}
	if e.Join == 0 {
		return errJoin
	}
	return nil
}

// Compare orders two entries of one peer id: the higher join counter, then
// on equal counters the left entry. The other fields only break the tie of
// two different entries with one counter, so the order is total and every
// member picks the same winner whatever the order it hears the entries in.
func (e Entry) Compare(o Entry) int {
	left := func(e Entry) int {
		if e.Left {
			return 1
		}
		return 0
	}
	return cmp.Or(cmp.Compare(e.Join, o.Join),
		cmp.Compare(left(e), left(o)),
		strings.Compare(e.Label, o.Label),
		slices.Compare(e.Addrs, o.Addrs),
		strings.Compare(e.PeerID, o.PeerID))
}

// Members is a member list by peer id, the entries of members that left
// included.
type Members map[string]Entry

// Merge keeps e when it is later than the entry of its peer id, and reports
// whether the list changed. Merge is commutative, associative and
// idempotent, so members that heard the same entries hold the same list.
func (m Members) Merge(e Entry) bool {
	if old, ok := m[e.PeerID]; ok && old.Compare(e) >= 0 {
		return false
	}
	m[e.PeerID] = e
	return true
}

// Entries are the entries of m, sorted by label and then by peer id.
func (m Members) Entries() []Entry {
	out := make([]Entry, 0, len(m))
	for _, e := range m {
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b Entry) int {
		return cmp.Or(strings.Compare(a.Label, b.Label), strings.Compare(a.PeerID, b.PeerID))
	})
	return out
}

// Active are the entries of the members that did not leave, sorted as
// Entries.
func (m Members) Active() []Entry {
	return slices.DeleteFunc(m.Entries(), func(e Entry) bool { return e.Left })
}

// Has reports whether peerID is a member that did not leave.
func (m Members) Has(peerID string) bool {
	e, ok := m[peerID]
	return ok && !e.Left
}

// count is the number of members that did not leave.
func (m Members) count() int {
	n := 0
	for _, e := range m {
		if !e.Left {
			n++
		}
	}
	return n
}

// clone copies m, so a caller can keep it outside the lock of a Set.
func (m Members) clone() Members {
	out := make(Members, len(m))
	for id, e := range m {
		e.Addrs = slices.Clone(e.Addrs)
		out[id] = e
	}
	return out
}

// suffixLen is how many characters of the peer id tell apart two members with
// one label. The last ones, because every ed25519 peer id starts with the
// same characters.
const suffixLen = 6

// Sender renders the member peerID of m and its agent as label/agent. When
// another member holds the same label, it renders label~<last 6 characters
// of the peer id>/agent.
func (m Members) Sender(peerID, agent string) string {
	return m.Member(peerID) + "/" + agent
}

// Member renders the member peerID of m as its label, or as label~<last 6
// characters of the peer id> when another member holds the same label.
func (m Members) Member(peerID string) string {
	e := m[peerID]
	for _, o := range m {
		if !o.Left && o.Label == e.Label && o.PeerID != peerID {
			return e.Label + "~" + peerID[max(0, len(peerID)-suffixLen):]
		}
	}
	return e.Label
}
