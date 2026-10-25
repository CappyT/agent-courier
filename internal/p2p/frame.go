package p2p

import (
	"encoding/binary"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/team"
)

// MaxFrame is the most bytes the JSON object of a frame can have. The 4-byte
// length in front of it does not count.
const MaxFrame = 64 << 10

// ErrBadFrame is in the chain of every error for a frame that breaks the wire
// protocol. No such error quotes the frame.
var ErrBadFrame = errors.New("bad frame")

var (
	// ErrFrameTooLarge comes from the reader and from the writer.
	ErrFrameTooLarge = fmt.Errorf("%w: a frame has at most %d bytes", ErrBadFrame, MaxFrame)
	errEmptyFrame    = fmt.Errorf("%w: a frame has at least 1 byte", ErrBadFrame)
	errTruncated     = fmt.Errorf("%w: the stream ends inside a frame", ErrBadFrame)
	errFrameJSON     = fmt.Errorf(`%w: a frame is one JSON object with a "type" of message, receipt, members or presence, and only the members of that type`, ErrBadFrame)
	errFrameValue    = fmt.Errorf("%w: a frame is a MessageFrame, ReceiptFrame, MembersFrame or PresenceFrame value", ErrBadFrame)
)

const (
	typeMessage  = "message"
	typeReceipt  = "receipt"
	typeMembers  = "members"
	typePresence = "presence"
)

// Frame is one of MessageFrame, ReceiptFrame, MembersFrame and PresenceFrame.
type Frame interface {
	// Validate checks every field against the rules of the wire protocol.
	Validate() error
	marshal() ([]byte, error)
}

// wire is the JSON form of a frame: its type and its members in one object.
type wire[F Frame] struct {
	Type  string `json:"type"`
	Frame F      `json:",embed"`
}

func marshalAs[F Frame](typ string, f F) ([]byte, error) {
	return json.Marshal(wire[F]{Type: typ, Frame: f})
}

// MessageFrame carries one message. The receiver takes the member label of
// the sender from the connection: no field of the frame names it.
type MessageFrame struct {
	ID message.ID `json:"id"`
	// Sender is the agent name on the sending machine.
	Sender         string `json:"sender"`
	message.Fields `json:",embed"`
}

// Validate refuses kind online, because onlines travel in presence frames,
// and a recipient on a third machine: To names an agent of the receiver.
func (f MessageFrame) Validate() error {
	if f.ID == (message.ID{}) {
		return errors.New("id is missing")
	}
	if err := message.CheckAgent(f.Sender); err != nil {
		return fmt.Errorf("sender: %w", err)
	}
	if f.To != "" {
		if err := message.CheckAgent(f.To); err != nil {
			return fmt.Errorf("to: %w", err)
		}
	}
	return f.Fields.Validate()
}

func (f MessageFrame) marshal() ([]byte, error) { return marshalAs(typeMessage, f) }

// ReceiptFrame acknowledges the message with the id ID.
type ReceiptFrame struct {
	ID message.ID `json:"id"`
}

func (f ReceiptFrame) Validate() error {
	if f.ID == (message.ID{}) {
		return errors.New("id is missing")
	}
	return nil
}

func (f ReceiptFrame) marshal() ([]byte, error) { return marshalAs(typeReceipt, f) }

// MembersFrame carries member list entries of one team. A peer id is in at
// most one entry.
type MembersFrame struct {
	Team    string       `json:"team"`
	Entries []team.Entry `json:"entries"`
}

func (f MembersFrame) Validate() error {
	if err := team.CheckName(f.Team); err != nil {
		return err
	}
	if len(f.Entries) == 0 || len(f.Entries) > team.MaxMembers {
		return fmt.Errorf("a members frame has 1 to %d entries", team.MaxMembers)
	}
	seen := make(map[string]bool, len(f.Entries))
	for i, e := range f.Entries {
		if err := checkEntry(e); err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		if seen[e.PeerID] {
			return fmt.Errorf("entry %d: the peer id is in an earlier entry", i)
		}
		seen[e.PeerID] = true
	}
	return nil
}

// checkEntry adds the libp2p rules to the rules of the team package. A peer
// id must be in its canonical form, so one peer cannot have two entries under
// two spellings, and an address must parse as a multiaddr.
func checkEntry(e team.Entry) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if !canonicalPeer(e.PeerID) {
		return errors.New("peer_id is not a libp2p peer id in its canonical form")
	}
	for _, a := range e.Addrs {
		// Parse errors quote the input, so they stay here.
		if _, err := ma.NewMultiaddr(a); err != nil {
			return errors.New("addrs holds a value that is not a multiaddr")
		}
	}
	return nil
}

func canonicalPeer(s string) bool {
	id, err := peer.Decode(s)
	return err == nil && id.String() == s
}

func (f MembersFrame) marshal() ([]byte, error) { return marshalAs(typeMembers, f) }

// PackMembers splits the member list entries of team name into members
// frames, in order, each within MaxFrame and the entry cap of a frame. It
// skips an entry that fails the frame check, so one bad entry does not stop
// the rest of the list, and returns how many it skipped.
func PackMembers(name string, entries []team.Entry) (frames []MembersFrame, skipped int, err error) {
	if len(entries) == 0 {
		return nil, 0, fmt.Errorf("%w: a member list has at least one entry", ErrBadFrame)
	}
	empty, err := MembersFrame{Team: name}.marshal()
	if err != nil {
		return nil, 0, err
	}
	cur, size := MembersFrame{Team: name}, len(empty)
	for _, e := range entries {
		if checkEntry(e) != nil {
			skipped++
			continue
		}
		b, err := json.Marshal(e)
		if err != nil {
			return nil, 0, err
		}
		n := len(b)
		if len(cur.Entries) > 0 {
			n++ // the comma in front of it
		}
		if len(cur.Entries) == team.MaxMembers || size+n > MaxFrame {
			frames = append(frames, cur)
			cur, size, n = MembersFrame{Team: name}, len(empty), len(b)
		}
		if size+n > MaxFrame {
			return nil, 0, ErrFrameTooLarge
		}
		cur.Entries = append(cur.Entries, e)
		size += n
	}
	if len(cur.Entries) > 0 {
		frames = append(frames, cur)
	}
	return frames, skipped, nil
}

// MaxPresence is the most entries a presence frame lists: 16 topics with 100
// agents each, the most a sender has.
const MaxPresence = team.MaxTopics * 100

// PresenceFrame lists the agents online on the topics both members share. An
// empty list means none. A topic and agent pair is in at most one entry.
type PresenceFrame struct {
	Agents []PresenceEntry `json:"agents"`
}

type PresenceEntry struct {
	Topic string `json:"topic"`
	Agent string `json:"agent"`
}

func (f PresenceFrame) Validate() error {
	if len(f.Agents) > MaxPresence {
		return fmt.Errorf("a presence frame lists at most %d agents", MaxPresence)
	}
	seen := make(map[PresenceEntry]bool, len(f.Agents))
	for i, a := range f.Agents {
		if err := message.CheckTopic(a.Topic); err != nil {
			return fmt.Errorf("agent %d: %w", i, err)
		}
		if err := message.CheckAgent(a.Agent); err != nil {
			return fmt.Errorf("agent %d: %w", i, err)
		}
		if seen[a] {
			return fmt.Errorf("agent %d: the topic and agent are in an earlier entry", i)
		}
		seen[a] = true
	}
	return nil
}

func (f PresenceFrame) marshal() ([]byte, error) { return marshalAs(typePresence, f) }

// ReadFrame reads one frame from r and checks it. It returns io.EOF when r
// ends between two frames.
func ReadFrame(r io.Reader) (Frame, error) {
	b, err := readPayload(r)
	if err != nil {
		return nil, err
	}
	return decodeFrame(b)
}

// readPayload reads the length and the JSON bytes of one frame. It refuses
// the length before it allocates or reads the body.
func readPayload(r io.Reader) ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errTruncated
		}
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n == 0 {
		return nil, errEmptyFrame
	}
	if n > MaxFrame {
		return nil, ErrFrameTooLarge
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errTruncated
		}
		return nil, err
	}
	return b, nil
}

func decodeFrame(b []byte) (Frame, error) {
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(b, &head); err != nil {
		return nil, errFrameJSON
	}
	switch head.Type {
	case typeMessage:
		return unmarshalAs[MessageFrame](b)
	case typeReceipt:
		return unmarshalAs[ReceiptFrame](b)
	case typeMembers:
		return unmarshalAs[MembersFrame](b)
	case typePresence:
		return unmarshalAs[PresenceFrame](b)
	}
	return nil, errFrameJSON
}

func unmarshalAs[F Frame](b []byte) (Frame, error) {
	var w wire[F]
	// The JSON error can quote the frame, so it stays here.
	if err := json.Unmarshal(b, &w, json.RejectUnknownMembers(true)); err != nil {
		return nil, errFrameJSON
	}
	if err := w.Frame.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrBadFrame, w.Type, err)
	}
	return w.Frame, nil
}

// WriteFrame checks f and writes it to w with one Write. It writes nothing
// when f is not valid or its JSON is over MaxFrame.
func WriteFrame(w io.Writer, f Frame) error {
	switch f.(type) {
	case MessageFrame, ReceiptFrame, MembersFrame, PresenceFrame:
	default:
		// nil, or a pointer, whose nil value would panic in Validate.
		return errFrameValue
	}
	if err := f.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrBadFrame, err)
	}
	b, err := f.marshal()
	if err != nil {
		return err
	}
	return writePayload(w, b)
}

// writePayload writes the length and the JSON bytes of one frame.
func writePayload(w io.Writer, b []byte) error {
	if len(b) > MaxFrame {
		return ErrFrameTooLarge
	}
	if len(b) == 0 {
		return errEmptyFrame
	}
	buf := make([]byte, 4, 4+len(b))
	binary.BigEndian.PutUint32(buf, uint32(len(b)))
	_, err := w.Write(append(buf, b...))
	return err
}
