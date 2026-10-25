package p2p

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// MsgProtocol carries frames between the daemons of members.
const MsgProtocol protocol.ID = "/agent-courier/msg/1.0.0"

// JoinProtocol carries the join of a new machine. It is the only courier
// protocol without the member check, because a joining peer is in no member
// list yet: its handler accepts only a valid invite secret, and reads with
// readPayload so the frame cap holds there too.
const JoinProtocol protocol.ID = "/agent-courier/join/1.0.0"

// MaxStreamsPerPeer is the most MsgProtocol streams that one peer can have
// open at once. A sender opens short streams, so a few are enough.
const MaxStreamsPerPeer = 4

// IsMember reports whether id is a member of a team this daemon belongs to.
// It runs concurrently, for every stream and every frame. It must answer
// from memory, so a stranger's stream costs no database read.
type IsMember func(id peer.ID) bool

// Deliver takes one valid frame from a member. from is the peer id of the
// connection, which Noise authenticates, never a field of the frame.
//
// IsMember only says that from is in some member list this daemon shares.
// Deliver must accept a frame only when from is a member of the team the
// frame names: the team of a members frame, or the team that owns the topic
// of a message or of each presence entry.
//
// ctx ends with the life of the stream, and Deliver must return when it ends.
// A non-nil reply goes back on the same stream. An error resets the stream.
type Deliver func(ctx context.Context, from peer.ID, f Frame) (reply Frame, err error)

var errNotMember = errors.New("the peer is not in a member list this daemon shares")

// MsgHandler serves MsgProtocol. A connection gater lets any peer connect,
// because DHT and relay traffic need it, so the member check is here, before
// the handler reads a byte. The per-peer stream cap is a counter keyed by the
// authenticated peer id, so it holds whatever resource manager the host has.
type MsgHandler struct {
	ctx      context.Context
	isMember IsMember
	deliver  Deliver
	log      *slog.Logger

	// frameTimeout bounds the read of one frame and the write of one reply,
	// streamTimeout the life of a stream and of its deliver calls. Fields, so
	// tests can lower them.
	frameTimeout  time.Duration
	streamTimeout time.Duration

	mu   sync.Mutex
	open map[peer.ID]int
}

// NewMsgHandler returns a handler that passes the frames of members to
// deliver. ctx bounds every deliver call.
func NewMsgHandler(ctx context.Context, isMember IsMember, deliver Deliver, log *slog.Logger) *MsgHandler {
	return &MsgHandler{
		ctx:           ctx,
		isMember:      isMember,
		deliver:       deliver,
		log:           cmp.Or(log, slog.Default()),
		frameTimeout:  10 * time.Second,
		streamTimeout: time.Minute,
		open:          make(map[peer.ID]int),
	}
}

// Handle is the network.StreamHandler of MsgProtocol. Before it reads a byte,
// it resets the stream of a peer outside the member lists with StreamGated, and
// the stream of a peer with MaxStreamsPerPeer streams open with
// StreamResourceLimitExceeded. It resets with StreamProtocolViolation a
// stream that breaks the wire protocol or a deadline, and with StreamGated a
// stream whose peer leaves the member lists: its next frame is not delivered.
func (m *MsgHandler) Handle(s network.Stream) {
	from := s.Conn().RemotePeer()
	// Registered first, so it runs after release.
	defer func() {
		if v := recover(); v != nil {
			// The panic value can quote a frame: only its type is logged.
			m.log.Error("panic in the msg handler, stream reset", "peer", from,
				"panic", fmt.Sprintf("%T", v), "stack", string(debug.Stack()))
			s.ResetWithError(network.StreamNoError)
		}
	}()
	if !m.isMember(from) {
		// Debug, so a stranger cannot flood the log.
		m.log.Debug("stream from a peer outside the member lists, reset", "peer", from)
		s.ResetWithError(network.StreamGated)
		return
	}
	if !m.acquire(from) {
		m.log.Warn("stream over the per-peer cap, reset", "peer", from, "cap", MaxStreamsPerPeer)
		s.ResetWithError(network.StreamResourceLimitExceeded)
		return
	}
	defer m.release(from)
	if code, err := m.serve(s, from); err != nil {
		m.log.Warn("stream reset", "peer", from, "err", err)
		s.ResetWithError(code)
		return
	}
	s.Close()
}

// serve reads frames until the sender closes its side of the stream. When it
// returns an error, it also returns the reset code for the sender.
func (m *MsgHandler) serve(s network.Stream, from peer.ID) (network.StreamErrorCode, error) {
	end := time.Now().Add(m.streamTimeout)
	ctx, cancel := context.WithDeadline(m.ctx, end)
	defer cancel()
	for {
		if err := s.SetReadDeadline(m.deadline(end)); err != nil {
			return network.StreamNoError, err
		}
		f, err := ReadFrame(s)
		switch {
		case errors.Is(err, io.EOF):
			return network.StreamNoError, nil
		case errors.Is(err, ErrBadFrame) || isTimeout(err):
			return network.StreamProtocolViolation, err
		case err != nil:
			return network.StreamNoError, err
		}
		// Checked again after the read, so a peer removed while the read
		// waited delivers nothing more.
		if !m.isMember(from) {
			return network.StreamGated, errNotMember
		}
		reply, err := m.deliver(ctx, from, f)
		if err != nil {
			return network.StreamNoError, err
		}
		if reply == nil {
			continue
		}
		// A full frameTimeout also at the end of the stream's life: the
		// frame is delivered, and its receipt saves the sender a retry.
		if err := s.SetWriteDeadline(time.Now().Add(m.frameTimeout)); err != nil {
			return network.StreamNoError, err
		}
		if err := WriteFrame(s, reply); err != nil {
			if isTimeout(err) {
				return network.StreamProtocolViolation, err
			}
			return network.StreamNoError, err
		}
	}
}

func (m *MsgHandler) deadline(end time.Time) time.Time {
	if d := time.Now().Add(m.frameTimeout); d.Before(end) {
		return d
	}
	return end
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.Is(err, os.ErrDeadlineExceeded) || errors.As(err, &ne) && ne.Timeout()
}

func (m *MsgHandler) acquire(id peer.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.open[id] >= MaxStreamsPerPeer {
		return false
	}
	m.open[id]++
	return true
}

func (m *MsgHandler) release(id peer.ID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.open[id]--; m.open[id] == 0 {
		delete(m.open, id)
	}
}
