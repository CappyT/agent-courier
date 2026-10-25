package p2p

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/team"
)

// specCap is the frame cap of the design. Tests use it, not MaxFrame, so a
// change of MaxFrame fails them.
const specCap = 64 << 10

func testPeer(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// testEntry returns a valid member list entry of id.
func testEntry(id peer.ID, label string) team.Entry {
	return team.Entry{PeerID: id.String(), Label: label, Join: 1}
}

// maxEntries returns n entries with the longest labels, the most addresses of
// the longest form, and the widest numbers, every other one left.
func maxEntries(t *testing.T, n int) []team.Entry {
	t.Helper()
	entries := make([]team.Entry, n)
	for i := range entries {
		e := testEntry(testPeer(t), fmt.Sprintf("m%031d", i))
		e.Join, e.Left = math.MaxUint64, i%2 == 1
		for j := range team.MaxAddrs {
			head, tail := fmt.Sprintf("/dns4/h%d-", i), fmt.Sprintf("/tcp/%d", 10000+j)
			e.Addrs = append(e.Addrs, head+strings.Repeat("a", team.MaxAddr-len(head)-len(tail))+tail)
		}
		entries[i] = e
	}
	return entries
}

func testMessage(summary string) MessageFrame {
	return MessageFrame{
		ID:     message.NewID(time.Now()),
		Sender: "claude",
		Fields: message.Fields{Topic: "infra", Kind: message.KindNote, Summary: summary},
	}
}

// encode returns f as it goes on the wire.
func encode(t *testing.T, f Frame) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := WriteFrame(&buf, f); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	return buf.Bytes()
}

// raw returns body behind a length of n.
func raw(n uint32, body string) []byte {
	return append(binary.BigEndian.AppendUint32(nil, n), body...)
}

func frameOf(body string) []byte { return raw(uint32(len(body)), body) }

func TestFrameRoundTrip(t *testing.T) {
	p1, p2 := testPeer(t), testPeer(t)
	ask := testMessage("which relay?")
	ask.Kind, ask.To, ask.Issue, ask.Link = message.KindAsk, "codex", 29, "https://github.com/CappyT/agent-courier/issues/29"
	reply := testMessage("the project relay")
	reply.Kind, reply.Re = message.KindReply, ask.ID
	alice := testEntry(p1, "alice-laptop")
	alice.Join = 3
	alice.Addrs = []string{"/ip4/127.0.0.1/tcp/4001", "/ip4/127.0.0.1/udp/4001/quic-v1"}
	bob := testEntry(p2, "bob")
	bob.Left = true
	cases := []struct {
		typ   string
		frame Frame
	}{
		{"message", ask},
		{"message", reply},
		{"receipt", ReceiptFrame{ID: ask.ID}},
		{"members", MembersFrame{Team: "core", Entries: []team.Entry{alice, bob}}},
		{"presence", PresenceFrame{Agents: []PresenceEntry{{Topic: "infra", Agent: "claude"}, {Topic: "web", Agent: "codex"}}}},
		{"presence", PresenceFrame{Agents: []PresenceEntry{}}},
	}
	for _, c := range cases {
		b := encode(t, c.frame)
		if n := binary.BigEndian.Uint32(b); int(n) != len(b)-4 {
			t.Fatalf("%s: length %d, body %d bytes", c.typ, n, len(b)-4)
		}
		var obj map[string]any
		if err := json.Unmarshal(b[4:], &obj); err != nil || obj["type"] != c.typ {
			t.Fatalf("%s: body %s is not an object of its type: %v", c.typ, b[4:], err)
		}
		r := bytes.NewReader(b)
		got, err := ReadFrame(r)
		if err != nil {
			t.Fatalf("%s: ReadFrame: %v", c.typ, err)
		}
		if !reflect.DeepEqual(got, c.frame) {
			t.Fatalf("%s: got %#v, want %#v", c.typ, got, c.frame)
		}
		if _, err := ReadFrame(r); err != io.EOF {
			t.Fatalf("%s: after the last frame: %v, want io.EOF", c.typ, err)
		}
	}
	// The member names of a message are flat, next to "type".
	var obj map[string]any
	if err := json.Unmarshal(encode(t, ask)[4:], &obj); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"type", "id", "sender", "topic", "kind", "summary", "to", "issue", "link"} {
		if _, ok := obj[k]; !ok {
			t.Errorf("message frame has no member %q: %v", k, obj)
		}
	}
}

func TestReadFrameRefuses(t *testing.T) {
	id := message.NewID(time.Now()).String()
	pid := testPeer(t)
	p := pid.String()
	msg := func(members string) string {
		return `{"type":"message","id":"` + id + `",` + members + `}`
	}
	list := func(entries ...string) string {
		return `{"type":"members","team":"core","entries":[` + strings.Join(entries, ",") + `]}`
	}
	entry := func(members string) string {
		return list(`{"peer_id":"` + p + `",` + members + `}`)
	}
	okList, err := MembersFrame{Team: "core", Entries: []team.Entry{testEntry(pid, "bob")}}.marshal()
	if err != nil {
		t.Fatal(err)
	}
	okEntry := `"label":"bob","join":1`
	if _, err := ReadFrame(bytes.NewReader(frameOf(entry(okEntry)))); err != nil {
		t.Fatalf("the base entry of the cases is refused: %v", err)
	}
	addrs := strings.TrimSuffix(strings.Repeat(`"/ip4/127.0.0.1/tcp/1",`, 17), ",")
	// 257 valid entries pass the frame cap, so these are empty. The entry cap
	// is checked before the entries.
	many := slices.Repeat([]string{`{}`}, team.MaxMembers+1)
	cases := []struct {
		name string
		in   []byte
		want error
	}{
		{"zero length", raw(0, ""), errEmptyFrame},
		{"length over the cap", raw(specCap+1, ""), ErrFrameTooLarge},
		{"largest length", raw(1<<32-1, ""), ErrFrameTooLarge},
		{"truncated length", []byte{0, 0}, errTruncated},
		{"no body", raw(10, ""), errTruncated},
		{"truncated body", raw(100, `{"type":"receipt"`), errTruncated},
		{"bad json", frameOf(`{"type":"receipt","id":`), errFrameJSON},
		{"array", frameOf(`[1]`), errFrameJSON},
		{"string", frameOf(`"EVIL"`), errFrameJSON},
		{"null", frameOf(`null`), errFrameJSON},
		{"number", frameOf(`1`), errFrameJSON},
		{"whitespace", frameOf(`   `), errFrameJSON},
		{"unknown type", frameOf(`{"type":"EVIL"}`), errFrameJSON},
		{"no type", frameOf(`{"id":"` + id + `"}`), errFrameJSON},
		{"type not a string", frameOf(`{"type":1}`), errFrameJSON},
		{"type in upper case", frameOf(`{"Type":"receipt","id":"` + id + `"}`), errFrameJSON},
		{"member in upper case", frameOf(`{"type":"receipt","ID":"` + id + `"}`), errFrameJSON},
		{"unknown member", frameOf(`{"type":"receipt","id":"` + id + `","EVIL":1}`), errFrameJSON},
		{"unknown nested member", frameOf(entry(okEntry + `,"EVIL":1`)), errFrameJSON},
		{"repeated member", frameOf(`{"type":"receipt","id":"` + id + `","id":"` + id + `"}`), errFrameJSON},
		{"repeated type", frameOf(`{"type":"receipt","type":"receipt","id":"` + id + `"}`), errFrameJSON},
		{"trailing data", frameOf(`{"type":"receipt","id":"` + id + `"} {}`), errFrameJSON},
		{"invalid utf-8", frameOf("{\"type\":\"presence\",\"agents\":[{\"topic\":\"infra\",\"agent\":\"\xff\"}]}"), errFrameJSON},
		{"receipt without id", frameOf(`{"type":"receipt"}`), ErrBadFrame},
		{"receipt with bad id", frameOf(`{"type":"receipt","id":"EVIL"}`), errFrameJSON},
		{"message kind online", frameOf(msg(`"sender":"claude","topic":"infra","kind":"online","summary":"x"`)), ErrBadFrame},
		{"message bad sender", frameOf(msg(`"sender":"EVIL SENDER","topic":"infra","kind":"note","summary":"x"`)), ErrBadFrame},
		{"message sender everyone", frameOf(msg(`"sender":"everyone","topic":"infra","kind":"note","summary":"x"`)), ErrBadFrame},
		{"message without sender", frameOf(msg(`"topic":"infra","kind":"note","summary":"x"`)), ErrBadFrame},
		{"message bad topic", frameOf(msg(`"sender":"claude","topic":"EVIL","kind":"note","summary":"x"`)), ErrBadFrame},
		{"message bad recipient", frameOf(msg(`"sender":"claude","topic":"infra","kind":"note","summary":"x","to":"alice/EVIL"`)), ErrBadFrame},
		{"message recipient on a third machine", frameOf(msg(`"sender":"claude","topic":"infra","kind":"note","summary":"x","to":"evil/claude"`)), ErrBadFrame},
		{"message empty summary", frameOf(msg(`"sender":"claude","topic":"infra","kind":"note","summary":" ​ "`)), ErrBadFrame},
		{"message http link", frameOf(msg(`"sender":"claude","topic":"infra","kind":"note","summary":"x","link":"http://EVIL"`)), ErrBadFrame},
		{"message issue string", frameOf(msg(`"sender":"claude","topic":"infra","kind":"note","summary":"x","issue":"EVIL"`)), errFrameJSON},
		{"message re on a note", frameOf(msg(`"sender":"claude","topic":"infra","kind":"note","summary":"x","re":"` + id + `"`)), ErrBadFrame},
		{"message reply without re", frameOf(msg(`"sender":"claude","topic":"infra","kind":"reply","summary":"x"`)), ErrBadFrame},
		{"message without id", frameOf(`{"type":"message","sender":"claude","topic":"infra","kind":"note","summary":"x"}`), ErrBadFrame},
		{"members bad team", frameOf(strings.Replace(string(okList), `"team":"core"`, `"team":"EVIL"`, 1)), ErrBadFrame},
		{"members without entries", frameOf(list()), ErrBadFrame},
		{"members null entries", frameOf(`{"type":"members","team":"core","entries":null}`), ErrBadFrame},
		{"members over the entry cap", frameOf(list(many...)), ErrBadFrame},
		{"members repeated peer", frameOf(list(`{"peer_id":"`+p+`",`+okEntry+`}`, `{"peer_id":"`+p+`",`+okEntry+`}`)), ErrBadFrame},
		{"members bad label", frameOf(entry(`"label":"EVIL LABEL","join":1`)), ErrBadFrame},
		{"members zero join", frameOf(entry(`"label":"bob","join":0`)), ErrBadFrame},
		{"members without join", frameOf(entry(`"label":"bob"`)), ErrBadFrame},
		{"members negative join", frameOf(entry(`"label":"bob","join":-1`)), errFrameJSON},
		{"members left a string", frameOf(entry(okEntry + `,"left":"EVIL"`)), errFrameJSON},
		{"members bad addr", frameOf(entry(okEntry + `,"addrs":["/EVIL/1"]`)), ErrBadFrame},
		{"members empty addr", frameOf(entry(okEntry + `,"addrs":[""]`)), ErrBadFrame},
		{"members over the addr cap", frameOf(entry(okEntry + `,"addrs":[` + addrs + `]`)), ErrBadFrame},
		{"members bad peer", frameOf(list(`{"peer_id":"EVIL",` + okEntry + `}`)), ErrBadFrame},
		{"members peer not canonical", frameOf(list(`{"peer_id":"` + peer.ToCid(pid).String() + `",` + okEntry + `}`)), ErrBadFrame},
		{"members empty peer", frameOf(list(`{"peer_id":"",` + okEntry + `}`)), ErrBadFrame},
		{"members no peer", frameOf(list(`{` + okEntry + `}`)), ErrBadFrame},
		{"members peer a number", frameOf(list(`{"peer_id":1,` + okEntry + `}`)), errFrameJSON},
		{"old roster type", frameOf(strings.Replace(string(okList), `"type":"members"`, `"type":"roster"`, 1)), errFrameJSON},
		{"presence bad topic", frameOf(`{"type":"presence","agents":[{"topic":"EVIL","agent":"claude"}]}`), ErrBadFrame},
		{"presence bad agent", frameOf(`{"type":"presence","agents":[{"topic":"infra","agent":"EVIL"}]}`), ErrBadFrame},
		{"presence agents not a list", frameOf(`{"type":"presence","agents":{}}`), errFrameJSON},
	}
	for _, c := range cases {
		f, err := ReadFrame(bytes.NewReader(c.in))
		if !errors.Is(err, c.want) || !errors.Is(err, ErrBadFrame) {
			t.Errorf("%s: got %v, %v; want %v", c.name, f, err, c.want)
			continue
		}
		if strings.Contains(err.Error(), "EVIL") {
			t.Errorf("%s: the error quotes the frame: %v", c.name, err)
		}
	}
}

// padded returns a receipt frame whose JSON has exactly n bytes.
func padded(t *testing.T, n int) []byte {
	t.Helper()
	head := `{"type":"receipt","id":"` + message.NewID(time.Now()).String() + `"`
	body := head + strings.Repeat(" ", n-len(head)-1) + "}"
	if len(body) != n {
		t.Fatalf("padded body has %d bytes, want %d", len(body), n)
	}
	return frameOf(body)
}

func TestFrameCap(t *testing.T) {
	if _, err := ReadFrame(bytes.NewReader(padded(t, specCap))); err != nil {
		t.Fatalf("a frame of %d bytes: %v", specCap, err)
	}
	if _, err := ReadFrame(bytes.NewReader(padded(t, specCap+1))); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("a frame of %d bytes: %v, want ErrFrameTooLarge", specCap+1, err)
	}

	var buf bytes.Buffer
	if err := writePayload(&buf, padded(t, specCap)[4:]); err != nil || buf.Len() != specCap+4 {
		t.Fatalf("writing %d bytes: %v, wrote %d", specCap, err, buf.Len())
	}
	buf.Reset()
	if err := writePayload(&buf, padded(t, specCap+1)[4:]); !errors.Is(err, ErrFrameTooLarge) || buf.Len() != 0 {
		t.Fatalf("writing %d bytes: %v, wrote %d", specCap+1, err, buf.Len())
	}
}

func TestWriteFrameRefuses(t *testing.T) {
	big := MembersFrame{Team: "core", Entries: maxEntries(t, team.MaxMembers)}
	bad := testMessage("x")
	bad.Sender = "Not A Name"
	cases := []struct {
		name  string
		frame Frame
		want  error
	}{
		{"over the cap", big, ErrFrameTooLarge},
		{"invalid", bad, ErrBadFrame},
		{"empty member list", MembersFrame{Team: "core"}, ErrBadFrame},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		if err := WriteFrame(&buf, c.frame); !errors.Is(err, c.want) || buf.Len() != 0 {
			t.Errorf("%s: %v, wrote %d bytes; want %v and nothing written", c.name, err, buf.Len(), c.want)
		}
	}
}

func TestWriteFrameRefusesNonValues(t *testing.T) {
	msg := testMessage("x")
	receipt := ReceiptFrame{ID: msg.ID}
	for _, f := range []Frame{nil, &msg, (*MessageFrame)(nil), &receipt, (*PresenceFrame)(nil)} {
		var buf bytes.Buffer
		if err := WriteFrame(&buf, f); !errors.Is(err, ErrBadFrame) || buf.Len() != 0 {
			t.Errorf("%T: %v, wrote %d bytes; want ErrBadFrame and nothing written", f, err, buf.Len())
		}
	}
}

// fullPresence lists 16 topics with 100 agents each: the most a sender has.
func fullPresence() PresenceFrame {
	var f PresenceFrame
	for i := range 16 {
		for j := range 100 {
			f.Agents = append(f.Agents, PresenceEntry{Topic: fmt.Sprintf("t%d", i), Agent: fmt.Sprintf("a%d", j)})
		}
	}
	return f
}

func TestPresenceCap(t *testing.T) {
	full := fullPresence()
	got, err := ReadFrame(bytes.NewReader(encode(t, full)))
	if err != nil || !reflect.DeepEqual(got, full) {
		t.Fatalf("a presence of %d entries: %v", len(full.Agents), err)
	}

	over := PresenceFrame{Agents: append(slices.Clone(full.Agents), PresenceEntry{Topic: "t0", Agent: "a100"})}
	dup := PresenceFrame{Agents: []PresenceEntry{{Topic: "infra", Agent: "claude"}, {Topic: "web", Agent: "claude"}, {Topic: "infra", Agent: "claude"}}}
	for name, f := range map[string]PresenceFrame{"over the cap": over, "repeated entry": dup} {
		var buf bytes.Buffer
		if err := WriteFrame(&buf, f); !errors.Is(err, ErrBadFrame) || buf.Len() != 0 {
			t.Errorf("%s: writer: %v, wrote %d bytes", name, err, buf.Len())
		}
		b, err := f.marshal()
		if err != nil || len(b) > specCap {
			t.Fatalf("%s: %d bytes, %v: the test needs a frame within the cap", name, len(b), err)
		}
		if _, err := ReadFrame(bytes.NewReader(frameOf(string(b)))); !errors.Is(err, ErrBadFrame) {
			t.Errorf("%s: reader: %v, want ErrBadFrame", name, err)
		}
	}
}

// writeAll writes frames to one stream of bytes and reads the entries back.
func writeAll(t *testing.T, frames []MembersFrame) []team.Entry {
	t.Helper()
	var buf bytes.Buffer
	for i, f := range frames {
		if err := WriteFrame(&buf, f); err != nil {
			t.Fatalf("frame %d of %d entries: %v", i, len(f.Entries), err)
		}
	}
	var got []team.Entry
	for {
		f, err := ReadFrame(&buf)
		if err == io.EOF {
			return got
		}
		if err != nil {
			t.Fatalf("reading back: %v", err)
		}
		r, ok := f.(MembersFrame)
		if !ok || r.Team != "core" {
			t.Fatalf("read back %#v, want a members frame of team core", f)
		}
		got = append(got, r.Entries...)
	}
}

// A list of the largest entries, members that left included, packs into
// frames that each pass both caps and read back whole.
func TestPackMembers(t *testing.T) {
	for _, n := range []int{team.MaxMembers, 3 * team.MaxMembers} {
		entries := maxEntries(t, n)
		frames, skipped, err := PackMembers("core", entries)
		if err != nil || skipped != 0 {
			t.Fatalf("%d entries: %v, skipped %d", n, err, skipped)
		}
		if got := writeAll(t, frames); !reflect.DeepEqual(got, entries) {
			t.Fatalf("%d entries: read back %d entries", n, len(got))
		}
		// Each frame but the last is full: the first entry of the next frame
		// would pass a cap.
		for i := range len(frames) - 1 {
			next := frames[i]
			next.Entries = append(slices.Clone(next.Entries), frames[i+1].Entries[0])
			if b, err := next.marshal(); err != nil || len(b) <= specCap && len(next.Entries) <= team.MaxMembers {
				t.Fatalf("%d entries: frame %d of %d has room for the next entry", n, i, len(frames))
			}
		}
	}
}

func TestMembersEntryCap(t *testing.T) {
	// The smallest valid entries. One frame does not hold more than
	// MaxMembers of them.
	small := make([]team.Entry, team.MaxMembers+1)
	for i := range small {
		small[i] = testEntry(testPeer(t), fmt.Sprintf("m%d", i))
	}
	frames, _, err := PackMembers("core", small)
	if err != nil || len(frames) != 2 {
		t.Fatalf("%d small entries: %d frames, %v", len(small), len(frames), err)
	}
	if got := writeAll(t, frames); !reflect.DeepEqual(got, small) {
		t.Fatalf("a list of %d entries did not pass the reader", len(small))
	}

	// The writer checks the entry cap before the size.
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MembersFrame{Team: "core", Entries: small}); !errors.Is(err, ErrBadFrame) || errors.Is(err, ErrFrameTooLarge) || buf.Len() != 0 {
		t.Fatalf("writer: %v for %d entries in one frame, want the entry cap", err, len(small))
	}
}

// PackMembers skips an entry that fails the frame check and sends the rest
// of the list.
func TestPackMembersSkipsBadEntries(t *testing.T) {
	noJoin := testEntry(testPeer(t), "u")
	noJoin.Join = 0
	badAddr := testEntry(testPeer(t), "v")
	badAddr.Addrs = []string{"/EVIL/1"}
	entries := []team.Entry{testEntry(testPeer(t), "a"), noJoin, testEntry(testPeer(t), "b"), badAddr}
	frames, skipped, err := PackMembers("core", entries)
	if err != nil || len(frames) != 1 || !reflect.DeepEqual(frames[0].Entries, []team.Entry{entries[0], entries[2]}) || skipped != 2 {
		t.Fatalf("PackMembers = %v, skipped %d, %v", frames, skipped, err)
	}
	if got := writeAll(t, frames); !reflect.DeepEqual(got, frames[0].Entries) {
		t.Fatalf("the packed frames do not pass the writer and the reader: %v", got)
	}
	// A list whose entries all fail the check sends no frame.
	if frames, skipped, err := PackMembers("core", entries[3:]); err != nil || len(frames) != 0 || skipped != 1 {
		t.Fatalf("PackMembers of one bad entry = %v, %d, %v", frames, skipped, err)
	}
	if _, _, err := PackMembers("core", nil); !errors.Is(err, ErrBadFrame) {
		t.Errorf("no entries: %v, want ErrBadFrame", err)
	}
}
