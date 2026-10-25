package p2p

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"

	"github.com/CappyT/agent-courier/internal/team"
)

// waitFor bounds every wait of a test on the other host.
const waitFor = 5 * time.Second

// newHost returns a host that listens on loopback TCP only.
func newHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.DisableRelay(),
		libp2p.DisableMetrics(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

// countingStream counts the bytes the handler reads.
type countingStream struct {
	network.Stream
	read atomic.Int64
}

func (c *countingStream) Read(b []byte) (int, error) {
	n, err := c.Stream.Read(b)
	c.read.Add(int64(n))
	return n, err
}

type delivery struct {
	from  peer.ID
	frame Frame
}

// handled is a stream that Handle returned from, with the bytes it read.
type handled struct {
	from peer.ID
	read int64
}

type receiver struct {
	h       host.Host
	m       *MsgHandler
	done    chan handled
	mu      sync.Mutex
	members map[peer.ID]bool
	got     []delivery
}

func newReceiver(t *testing.T, tune func(*MsgHandler)) *receiver {
	t.Helper()
	r := &receiver{h: newHost(t), done: make(chan handled, 64), members: make(map[peer.ID]bool)}
	r.m = NewMsgHandler(t.Context(), r.isMember, r.deliver, slog.New(slog.DiscardHandler))
	if tune != nil {
		tune(r.m)
	}
	r.h.SetStreamHandler(MsgProtocol, func(s network.Stream) {
		cs := &countingStream{Stream: s}
		r.m.Handle(cs)
		r.done <- handled{s.Conn().RemotePeer(), cs.read.Load()}
	})
	return r
}

func (r *receiver) isMember(id peer.ID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.members[id]
}

func (r *receiver) setMember(id peer.ID, member bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members[id] = member
}

// deliver records f and answers a message with its receipt.
func (r *receiver) deliver(_ context.Context, from peer.ID, f Frame) (Frame, error) {
	r.mu.Lock()
	r.got = append(r.got, delivery{from, f})
	r.mu.Unlock()
	if m, ok := f.(MessageFrame); ok {
		return ReceiptFrame{ID: m.ID}, nil
	}
	return nil, nil
}

func (r *receiver) delivered() []delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.got)
}

// wait returns the next stream that Handle returned from.
func (r *receiver) wait(t *testing.T) handled {
	t.Helper()
	select {
	case h := <-r.done:
		return h
	case <-time.After(waitFor):
		t.Fatal("the handler did not return")
		return handled{}
	}
}

// open connects from to r and opens a msg stream with a deadline.
func (r *receiver) open(t *testing.T, from host.Host) network.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitFor)
	defer cancel()
	if err := from.Connect(ctx, peer.AddrInfo{ID: r.h.ID(), Addrs: r.h.Addrs()}); err != nil {
		t.Fatal(err)
	}
	s, err := from.NewStream(ctx, r.h.ID(), MsgProtocol)
	if err != nil {
		t.Fatal(err)
	}
	s.SetDeadline(time.Now().Add(waitFor))
	t.Cleanup(func() { s.Reset() })
	return s
}

// exchange writes b, closes the write side and reads to the end.
func exchange(s network.Stream, b []byte) ([]byte, error) {
	if _, err := s.Write(b); err != nil {
		return nil, err
	}
	if err := s.CloseWrite(); err != nil {
		return nil, err
	}
	return io.ReadAll(s)
}

func wantReset(t *testing.T, err error, code network.StreamErrorCode) {
	t.Helper()
	var se *network.StreamError
	if !errors.As(err, &se) || !se.Remote || se.ErrorCode != code {
		t.Fatalf("got %v, want a reset from the receiver with code %#x", err, code)
	}
}

func TestStrangerResetBeforeRead(t *testing.T) {
	r := newReceiver(t, nil)
	member, stranger := newHost(t), newHost(t)
	r.setMember(member.ID(), true)

	_, err := exchange(r.open(t, stranger), encode(t, testMessage("hello")))
	wantReset(t, err, network.StreamGated)
	if h := r.wait(t); h.from != stranger.ID() || h.read != 0 {
		t.Fatalf("handler read %d bytes from %s, want 0 from the stranger", h.read, h.from)
	}
	if got := r.delivered(); len(got) != 0 {
		t.Fatalf("delivered %d frames from a stranger", len(got))
	}

	// The same receiver takes the frames of a member.
	if _, err := exchange(r.open(t, member), encode(t, testMessage("hello"))); err != nil {
		t.Fatalf("member: %v", err)
	}
	r.wait(t)
	if got := r.delivered(); len(got) != 1 || got[0].from != member.ID() {
		t.Fatalf("deliveries %v, want one from the member", got)
	}
}

func TestMemberFramesArrive(t *testing.T) {
	r := newReceiver(t, nil)
	b := newHost(t)
	r.setMember(b.ID(), true)

	msg := testMessage("deploy done")
	frames := []Frame{
		msg,
		ReceiptFrame{ID: msg.ID},
		MembersFrame{Team: "core", Entries: []team.Entry{testEntry(b.ID(), "bob")}},
		PresenceFrame{Agents: []PresenceEntry{{Topic: "infra", Agent: "claude"}}},
	}
	var in []byte
	for _, f := range frames {
		in = append(in, encode(t, f)...)
	}
	out, err := exchange(r.open(t, b), in)
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if !bytes.Equal(out, encode(t, ReceiptFrame{ID: msg.ID})) {
		t.Fatalf("reply %q, want the receipt of the message", out)
	}
	if h := r.wait(t); h.read != int64(len(in)) {
		t.Fatalf("handler read %d bytes, want %d", h.read, len(in))
	}
	got := r.delivered()
	if len(got) != len(frames) {
		t.Fatalf("delivered %d frames, want %d", len(got), len(frames))
	}
	for i, d := range got {
		if d.from != b.ID() || !reflect.DeepEqual(d.frame, frames[i]) {
			t.Fatalf("delivery %d: %v from %s, want %v from %s", i, d.frame, d.from, frames[i], b.ID())
		}
	}
}

func TestRemovedMemberRefused(t *testing.T) {
	r := newReceiver(t, nil)
	b := newHost(t)
	r.setMember(b.ID(), true)

	if _, err := exchange(r.open(t, b), encode(t, testMessage("one"))); err != nil {
		t.Fatalf("before the removal: %v", err)
	}
	r.wait(t)

	// Removed while a stream is open: the next frame is not delivered.
	s := r.open(t, b)
	if err := WriteFrame(s, testMessage("two")); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(s); err != nil {
		t.Fatalf("receipt: %v", err)
	}
	r.setMember(b.ID(), false)
	_, err := exchange(s, encode(t, testMessage("three")))
	wantReset(t, err, network.StreamGated)
	r.wait(t)

	// Removed: the next stream is refused like a stranger's.
	_, err = exchange(r.open(t, b), encode(t, testMessage("four")))
	wantReset(t, err, network.StreamGated)
	if h := r.wait(t); h.read != 0 {
		t.Fatalf("handler read %d bytes from a removed member, want 0", h.read)
	}
	got := r.delivered()
	if len(got) != 2 {
		t.Fatalf("delivered %d frames, want the 2 sent before the removal", len(got))
	}
	for i, want := range []string{"one", "two"} {
		if m, ok := got[i].frame.(MessageFrame); !ok || m.Summary != want {
			t.Fatalf("delivery %d: %v, want the message %q", i, got[i].frame, want)
		}
	}
}

func TestBadStreamReset(t *testing.T) {
	r := newReceiver(t, nil)
	b := newHost(t)
	r.setMember(b.ID(), true)

	big := padded(t, specCap+1)
	cases := []struct {
		name string
		in   []byte
		read int64 // the bytes the handler may read, or -1 for any
	}{
		{"length over the cap", raw(specCap+1, ""), 4},
		{"frame over the cap", big, 4},
		{"zero length", raw(0, ""), 4},
		{"truncated length", []byte{0, 0}, 2},
		{"truncated frame", raw(100, `{"type":"receipt"`), -1},
		{"bad json", frameOf(`{"type":"receipt","id":`), -1},
		{"not an object", frameOf(`[1]`), -1},
		{"unknown member", frameOf(`{"type":"receipt","id":"` + testMessage("x").ID.String() + `","extra":1}`), -1},
		{"invalid field", frameOf(`{"type":"presence","agents":[{"topic":"infra","agent":"Not A Name"}]}`), -1},
		{"bad frame after a good one", append(encode(t, testMessage("x")), frameOf(`{}`)...), -1},
	}
	for _, c := range cases {
		before := len(r.delivered())
		_, err := exchange(r.open(t, b), c.in)
		var se *network.StreamError
		if !errors.As(err, &se) || !se.Remote || se.ErrorCode != network.StreamProtocolViolation {
			t.Errorf("%s: got %v, want a reset with StreamProtocolViolation", c.name, err)
		}
		if h := r.wait(t); c.read >= 0 && h.read != c.read {
			t.Errorf("%s: handler read %d bytes, want %d", c.name, h.read, c.read)
		}
		want := before
		if c.name == "bad frame after a good one" {
			want++
		}
		if got := len(r.delivered()); got != want {
			t.Errorf("%s: %d deliveries, want %d", c.name, got, want)
		}
	}
}

func TestStreamCapPerPeer(t *testing.T) {
	r := newReceiver(t, nil)
	b, c := newHost(t), newHost(t)
	r.setMember(b.ID(), true)
	r.setMember(c.ID(), true)

	// Each held stream waits in the handler for its next frame.
	held := make([]network.Stream, MaxStreamsPerPeer)
	for i := range held {
		held[i] = r.open(t, b)
		if err := WriteFrame(held[i], testMessage("held")); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadFrame(held[i]); err != nil {
			t.Fatalf("stream %d: receipt: %v", i, err)
		}
	}

	_, err := exchange(r.open(t, b), encode(t, testMessage("one too many")))
	wantReset(t, err, network.StreamResourceLimitExceeded)
	if h := r.wait(t); h.read != 0 {
		t.Fatalf("handler read %d bytes from a stream over the cap, want 0", h.read)
	}
	if got := len(r.delivered()); got != MaxStreamsPerPeer {
		t.Fatalf("delivered %d frames, want %d", got, MaxStreamsPerPeer)
	}

	// The cap is per peer.
	if _, err := exchange(r.open(t, c), encode(t, testMessage("other peer"))); err != nil {
		t.Fatalf("other member: %v", err)
	}
	r.wait(t)

	// A closed stream frees its slot.
	if _, err := exchange(held[0], nil); err != nil {
		t.Fatalf("closing a held stream: %v", err)
	}
	r.wait(t)
	if _, err := exchange(r.open(t, b), encode(t, testMessage("after a close"))); err != nil {
		t.Fatalf("after a close: %v", err)
	}
	r.wait(t)
}

func TestSlowFrameReset(t *testing.T) {
	r := newReceiver(t, func(m *MsgHandler) { m.frameTimeout = 200 * time.Millisecond })
	b := newHost(t)
	r.setMember(b.ID(), true)

	s := r.open(t, b)
	// Half a length, then nothing.
	if _, err := s.Write([]byte{0, 0}); err != nil {
		t.Fatal(err)
	}
	_, err := io.ReadAll(s)
	wantReset(t, err, network.StreamProtocolViolation)
	r.wait(t)
}

func TestStreamLifetime(t *testing.T) {
	r := newReceiver(t, func(m *MsgHandler) { m.streamTimeout = 500 * time.Millisecond })
	b := newHost(t)
	r.setMember(b.ID(), true)

	s := r.open(t, b)
	s.SetDeadline(time.Now().Add(4 * waitFor))
	start := time.Now()
	var err error
	for time.Since(start) < 3*waitFor && err == nil {
		if err = WriteFrame(s, testMessage("trickle")); err == nil {
			_, err = ReadFrame(s)
		}
		time.Sleep(100 * time.Millisecond)
	}
	wantReset(t, err, network.StreamProtocolViolation)
	if d := time.Since(start); d > waitFor {
		t.Fatalf("the stream lived %v, want about 500ms", d)
	}
	r.wait(t)
}

// slots returns the streams the handler holds for each peer.
func (r *receiver) slots() int {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	return len(r.m.open)
}

func TestSlowDeliver(t *testing.T) {
	const life = 500 * time.Millisecond
	type call struct{ at, deadline time.Time }
	calls := make(chan call, 1)
	r := newReceiver(t, func(m *MsgHandler) {
		m.streamTimeout = life
		// Returns its receipt only when the stream's life ends.
		m.deliver = func(ctx context.Context, _ peer.ID, f Frame) (Frame, error) {
			d, _ := ctx.Deadline()
			calls <- call{time.Now(), d}
			<-ctx.Done()
			return ReceiptFrame{ID: f.(MessageFrame).ID}, nil
		}
	})
	b := newHost(t)
	r.setMember(b.ID(), true)

	start := time.Now()
	s := r.open(t, b)
	msg := testMessage("slow")
	if err := WriteFrame(s, msg); err != nil {
		t.Fatal(err)
	}
	// Closed now, so the read after the receipt ends the stream cleanly.
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-calls:
		if c.deadline.IsZero() || c.deadline.Sub(c.at) > life {
			t.Fatalf("deliver got the deadline %v at %v, want at most %v later", c.deadline, c.at, life)
		}
	case <-time.After(waitFor):
		t.Fatal("deliver did not run")
	}
	// The receipt write has its own frameTimeout, after the stream's life.
	out, err := io.ReadAll(s)
	if err != nil || !bytes.Equal(out, encode(t, ReceiptFrame{ID: msg.ID})) {
		t.Fatalf("reply %q, %v; want the receipt", out, err)
	}
	r.wait(t)
	if d := time.Since(start); d > life+waitFor/2 {
		t.Fatalf("the handler held the stream %v, want about %v", d, life)
	}
	if n := r.slots(); n != 0 {
		t.Fatalf("%d slots held after the stream ended", n)
	}
}

func TestUnreadRepliesReset(t *testing.T) {
	big := fullPresence()
	r := newReceiver(t, func(m *MsgHandler) {
		m.frameTimeout = 300 * time.Millisecond
		m.deliver = func(context.Context, peer.ID, Frame) (Frame, error) { return big, nil }
	})
	b := newHost(t)
	r.setMember(b.ID(), true)

	// 16 replies of about 48 KiB fill the receive window of the stream, and
	// the member never reads them.
	s := r.open(t, b)
	var err error
	for i := 0; i < 16 && err == nil; i++ {
		err = WriteFrame(s, testMessage("never read"))
	}
	r.wait(t)
	if err == nil {
		_, err = io.ReadAll(s)
	}
	wantReset(t, err, network.StreamProtocolViolation)
	if n := r.slots(); n != 0 {
		t.Fatalf("%d slots held after the stream ended", n)
	}
}

// syncBuffer is a log destination for the handler's goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestDeliverPanic(t *testing.T) {
	var logs syncBuffer
	var panics atomic.Bool
	panics.Store(true)
	r := newReceiver(t, func(m *MsgHandler) {
		m.log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		m.deliver = func(_ context.Context, _ peer.ID, f Frame) (Frame, error) {
			if panics.Load() {
				panic(errors.New("EVIL " + f.(MessageFrame).Summary))
			}
			return nil, nil
		}
	})
	b := newHost(t)
	r.setMember(b.ID(), true)

	_, err := exchange(r.open(t, b), encode(t, testMessage("PEERTEXT")))
	wantReset(t, err, network.StreamNoError)
	r.wait(t)
	if n := r.slots(); n != 0 {
		t.Fatalf("%d slots held after the panic", n)
	}
	out := logs.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "panic") {
		t.Fatalf("no error log for the panic: %s", out)
	}
	if strings.Contains(out, "PEERTEXT") || strings.Contains(out, "EVIL") {
		t.Fatalf("the log quotes the panic value: %s", out)
	}

	// The handler serves the next stream.
	panics.Store(false)
	if _, err := exchange(r.open(t, b), encode(t, testMessage("after"))); err != nil {
		t.Fatalf("after the panic: %v", err)
	}
	r.wait(t)
}
