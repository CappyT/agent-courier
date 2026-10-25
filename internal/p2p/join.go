package p2p

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/team"
)

// The frames of JoinProtocol. ReadFrame does not decode them, so a join frame
// never reaches the Deliver of MsgProtocol.
const (
	typeJoin       = "join"
	typeJoinReply  = "join_reply"
	typeJoinAccept = "join_accept"
)

// The results of a join reply. Offer carries the topics of the team; the
// others refuse the join and carry nothing.
const (
	joinOffer      = "offer"
	joinRefused    = "refused"
	joinLabelTaken = "label_taken"
	joinTeamFull   = "team_full"
)

const (
	// MaxJoinStreams is the most join streams an inviter serves at once, from
	// all peers, and MaxJoinStreamsPerPeer the most from one peer. Any peer
	// can open a join stream, so these caps bound the work of strangers.
	MaxJoinStreams        = 8
	MaxJoinStreamsPerPeer = 1
	// maxJoinFrames caps the members frames of a join. A list keeps the
	// entries of members that left for the life of the team, so it can have
	// more than team.MaxMembers entries; a frame holds at least 25.
	maxJoinFrames = 64
)

var (
	ErrInviterUnreachable = errors.New("the inviter is not reachable: its daemon must run during the join; the invite code stays valid")
	ErrJoinFailed         = errors.New("the join failed: the inviter answered against the join protocol, or sent a member list without the inviter and this machine")
	errJoinFrameJSON      = fmt.Errorf(`%w: a join stream carries JSON objects with a "type" of join, join_reply, join_accept or members, and only the members of that type`, ErrBadFrame)
	errJoinOrder          = fmt.Errorf("%w: the frames of a join stream are join, join_reply with an offer, join_accept, then members frames", ErrBadFrame)
)

// joinFrame is the request of the joiner: the team and secret of its invite
// code, and its label. No field names the joiner: the inviter takes its peer
// id from the connection.
type joinFrame struct {
	Team   string `json:"team"`
	Secret []byte `json:"secret"`
	Label  string `json:"label"`
}

func (f joinFrame) Validate() error {
	if err := team.CheckName(f.Team); err != nil {
		return err
	}
	if len(f.Secret) != team.SecretSize {
		return fmt.Errorf("an invite secret has %d bytes", team.SecretSize)
	}
	return team.CheckLabel(f.Label)
}

func (f joinFrame) marshal() ([]byte, error) { return marshalAs(typeJoin, f) }

// joinReplyFrame answers a join frame, and refuses a join_accept frame when
// the join no longer holds.
type joinReplyFrame struct {
	Result string   `json:"result"`
	Topics []string `json:"topics,omitempty"`
}

func (f joinReplyFrame) Validate() error {
	switch f.Result {
	case joinOffer:
		if len(f.Topics) == 0 || len(f.Topics) > team.MaxTopics {
			return fmt.Errorf("an offer has 1 to %d topics", team.MaxTopics)
		}
		for i, t := range f.Topics {
			if err := message.CheckTopic(t); err != nil {
				return fmt.Errorf("topic %d: %w", i, err)
			}
			if slices.Contains(f.Topics[:i], t) {
				return fmt.Errorf("topic %d: the topic is in an earlier entry", i)
			}
		}
		return nil
	case joinRefused, joinLabelTaken, joinTeamFull:
		if len(f.Topics) != 0 {
			return errors.New("a refusal has no topics")
		}
		return nil
	}
	return errors.New("the result of a join reply is offer, refused, label_taken or team_full")
}

func (f joinReplyFrame) marshal() ([]byte, error) { return marshalAs(typeJoinReply, f) }

// joinAcceptFrame takes the offer: the inviter adds the joiner and sends the
// member list.
type joinAcceptFrame struct{}

func (joinAcceptFrame) Validate() error { return nil }

func (f joinAcceptFrame) marshal() ([]byte, error) { return marshalAs(typeJoinAccept, f) }

// readJoinFrame reads one frame of a join stream, with the frame cap of
// readPayload, and checks it.
func readJoinFrame(r io.Reader) (Frame, error) {
	b, err := readPayload(r)
	if err != nil {
		return nil, err
	}
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(b, &head); err != nil {
		return nil, errJoinFrameJSON
	}
	switch head.Type {
	case typeJoin:
		return unmarshalAs[joinFrame](b)
	case typeJoinReply:
		return unmarshalAs[joinReplyFrame](b)
	case typeJoinAccept:
		return unmarshalAs[joinAcceptFrame](b)
	case typeMembers:
		return unmarshalAs[MembersFrame](b)
	}
	return nil, errJoinFrameJSON
}

// writeJoinFrame checks f and writes it with one Write.
func writeJoinFrame(w io.Writer, f Frame) error {
	if err := f.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrBadFrame, err)
	}
	b, err := f.marshal()
	if err != nil {
		return err
	}
	return writePayload(w, b)
}

// refusal is the join reply result for an error of the inviter's checks,
// and false for any other error.
func refusal(err error) (string, bool) {
	switch {
	case errors.Is(err, team.ErrInviteRefused):
		return joinRefused, true
	case errors.Is(err, team.ErrLabelTaken):
		return joinLabelTaken, true
	case errors.Is(err, team.ErrTeamFull):
		return joinTeamFull, true
	}
	return "", false
}

// refusalError is the error of the joiner for a refusal result.
func refusalError(result string) error {
	switch result {
	case joinRefused:
		return team.ErrInviteRefused
	case joinLabelTaken:
		return team.ErrLabelTaken
	case joinTeamFull:
		return team.ErrTeamFull
	}
	return ErrJoinFailed
}

// JoinConfig is what a JoinService needs besides its host.
type JoinConfig struct {
	// Set is the member set of the daemon.
	Set *team.Set
	// Addrs is the address source of invite codes and of this machine's own
	// member list entry, best first: the daemon puts its circuit address on
	// the relay there when it has one. Nil means the addresses of the host.
	Addrs func() []ma.Multiaddr
	// Changed runs after a join changed the member list of a team, on the
	// inviter and on the joiner, so the list goes out to the members.
	Changed func(team string)
	Log     *slog.Logger
}

// JoinService serves JoinProtocol for the inviter and makes the joins of this
// machine. The protocol on one stream:
//
//  1. The joiner sends a join frame: team, secret, label.
//  2. The inviter checks the secret first, then the label and the member
//     cap, without a change, and replies with an offer of the team's topics
//     or a refusal.
//  3. The joiner checks the topics against its own teams, and sends
//     join_accept, or closes the stream and the code stays valid.
//  4. The inviter checks again, marks the invite used and adds the joiner.
//     It sends the member list in members frames, the joiner's entry and its
//     own first, and closes. A check that fails now gets a refusal.
type JoinService struct {
	h   host.Host
	cfg JoinConfig
	log *slog.Logger

	// Fields, so tests can lower them. frameTimeout bounds one read or write,
	// streamTimeout a join stream, dialTimeout the dial of the inviter.
	frameTimeout  time.Duration
	streamTimeout time.Duration
	dialTimeout   time.Duration
	// allowLoopback keeps loopback addresses in invite codes and the join
	// dial, for tests on one machine.
	allowLoopback bool

	mu    sync.Mutex
	open  map[peer.ID]int
	total int
}

// NewJoinService sets the JoinProtocol handler of h. The deadlines of a join
// stream bound the work of the handler.
func NewJoinService(h host.Host, cfg JoinConfig) (*JoinService, error) {
	if cfg.Set == nil {
		return nil, errors.New("a join service needs the member set")
	}
	if cfg.Addrs == nil {
		cfg.Addrs = h.Addrs
	}
	if cfg.Changed == nil {
		cfg.Changed = func(string) {}
	}
	j := &JoinService{h: h, cfg: cfg, log: cmp.Or(cfg.Log, slog.Default()),
		frameTimeout: 10 * time.Second, streamTimeout: time.Minute, dialTimeout: 30 * time.Second,
		open: make(map[peer.ID]int)}
	// The label of a member without a connection to this host goes to a
	// joiner: that member is gone, for example after a restart.
	cfg.Set.SetConnected(func(id string) bool {
		p, err := peer.Decode(id)
		return err == nil && h.Network().Connectedness(p) == network.Connected
	})
	h.SetStreamHandler(JoinProtocol, j.Handle)
	return j, nil
}

// Handle is the network.StreamHandler of JoinProtocol. It is the only courier
// handler without the member check, so it caps the streams before it reads a
// byte, and takes the joiner's peer id only from the connection, which Noise
// authenticates.
func (j *JoinService) Handle(s network.Stream) {
	from := s.Conn().RemotePeer()
	defer func() {
		if v := recover(); v != nil {
			j.log.Error("panic in the join handler, stream reset", "panic", fmt.Sprintf("%T", v), "stack", string(debug.Stack()))
			s.ResetWithError(network.StreamNoError)
		}
	}()
	if !j.acquire(from) {
		// Debug, so a stranger cannot flood the log.
		j.log.Debug("join stream over the cap, reset", "peer", from)
		s.ResetWithError(network.StreamResourceLimitExceeded)
		return
	}
	defer j.release(from)
	if code, err := j.serve(s, from); err != nil {
		j.log.Debug("join stream reset", "peer", from, "err", err)
		s.ResetWithError(code)
		return
	}
	s.Close()
}

func (j *JoinService) serve(s network.Stream, from peer.ID) (network.StreamErrorCode, error) {
	rw := joinIO{s: s, end: time.Now().Add(j.streamTimeout), frame: j.frameTimeout}
	f, err := rw.read()
	if err != nil {
		return violation(err)
	}
	req, ok := f.(joinFrame)
	if !ok {
		return network.StreamProtocolViolation, errJoinOrder
	}
	joiner, self := from.String(), j.h.ID().String()
	topics, err := j.cfg.Set.CheckInvite(req.Team, req.Secret, joiner, req.Label)
	if result, ok := refusal(err); ok {
		return network.StreamNoError, rw.write(joinReplyFrame{Result: result})
	} else if err != nil {
		j.log.Error("join check failed", "err", err)
		return network.StreamNoError, err
	}
	if err := rw.write(joinReplyFrame{Result: joinOffer, Topics: topics}); err != nil {
		return violation(err)
	}
	switch f, err := rw.read(); {
	case errors.Is(err, io.EOF):
		// The joiner refused the offer.
		return network.StreamNoError, nil
	case err != nil:
		return violation(err)
	default:
		if _, ok := f.(joinAcceptFrame); !ok {
			return network.StreamProtocolViolation, errJoinOrder
		}
	}
	t, err := j.cfg.Set.AcceptInvite(req.Team, req.Secret, joiner, req.Label)
	if result, ok := refusal(err); ok {
		return network.StreamNoError, rw.write(joinReplyFrame{Result: result})
	} else if err != nil {
		j.log.Error("join failed", "err", err)
		return network.StreamNoError, err
	}
	j.log.Info("a machine joined a team with an invite of this machine", "team", t.Name)
	j.log.Debug("joined", "team", t.Name, "peer", from)
	j.cfg.Changed(t.Name)
	entries := []team.Entry{t.Members[joiner], t.Members[self]}
	for _, e := range t.Members.Entries() {
		if e.PeerID != self && e.PeerID != joiner {
			entries = append(entries, e)
		}
	}
	frames, skipped, err := PackMembers(t.Name, entries)
	if err != nil {
		return network.StreamNoError, err
	}
	if skipped > 0 {
		j.log.Debug("member list entries fail the frame check, not sent", "team", t.Name, "entries", skipped)
	}
	for _, rf := range frames {
		if err := rw.write(rf); err != nil {
			return violation(err)
		}
	}
	return network.StreamNoError, nil
}

// violation is the reset code for an error of a read or a write.
func violation(err error) (network.StreamErrorCode, error) {
	if errors.Is(err, ErrBadFrame) || isTimeout(err) {
		return network.StreamProtocolViolation, err
	}
	return network.StreamNoError, err
}

// joinIO reads and writes the frames of a join stream, each within the frame
// timeout and the end of the stream.
type joinIO struct {
	s     network.Stream
	end   time.Time
	frame time.Duration
}

func (j joinIO) deadline() time.Time {
	if d := time.Now().Add(j.frame); d.Before(j.end) {
		return d
	}
	return j.end
}

func (j joinIO) read() (Frame, error) {
	if err := j.s.SetReadDeadline(j.deadline()); err != nil {
		return nil, err
	}
	return readJoinFrame(j.s)
}

func (j joinIO) write(f Frame) error {
	if err := j.s.SetWriteDeadline(j.deadline()); err != nil {
		return err
	}
	return writeJoinFrame(j.s, f)
}

func (j *JoinService) acquire(id peer.ID) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.total >= MaxJoinStreams || j.open[id] >= MaxJoinStreamsPerPeer {
		return false
	}
	j.open[id]++
	j.total++
	return true
}

func (j *JoinService) release(id peer.ID) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.total--
	if j.open[id]--; j.open[id] == 0 {
		delete(j.open, id)
	}
}

// Join joins this machine with label to the team of an invite code. It fails
// before it dials with ErrBadInviteCode, ErrInviteExpired, or an error of
// team.Set.CheckJoin, and then with ErrInviterUnreachable, a refusal of the
// inviter (team.ErrInviteRefused, team.ErrLabelTaken, team.ErrTeamFull),
// team.ErrTopicTaken, or ErrJoinFailed. None of them quotes the code or the
// inviter's frames.
func (j *JoinService) Join(ctx context.Context, code, label string) (team.Team, error) {
	if err := team.CheckLabel(label); err != nil {
		return team.Team{}, err
	}
	inv, err := decodeInvite(code, j.allowLoopback)
	if err != nil || inv.Inviter == j.h.ID() {
		return team.Team{}, ErrBadInviteCode
	}
	if !time.Now().Before(inv.Expires) {
		return team.Team{}, ErrInviteExpired
	}
	if err := j.cfg.Set.CheckJoin(inv.Team, nil); err != nil {
		return team.Team{}, err
	}
	s, err := j.dial(ctx, inv)
	if err != nil {
		j.log.Debug("inviter not reachable", "peer", inv.Inviter, "err", err)
		return team.Team{}, ErrInviterUnreachable
	}
	topics, entries, err := j.exchange(s, inv, label)
	// Close, not Reset: an inviter that waits for join_accept reads the end
	// of the stream as a refused offer.
	s.Close()
	if err != nil {
		return team.Team{}, err
	}
	t, err := j.cfg.Set.Join(j.h.ID().String(), inv.Team, topics, inv.Inviter.String(), entries, label)
	if errors.Is(err, team.ErrJoinList) {
		return team.Team{}, fmt.Errorf("%w: %w", ErrJoinFailed, err)
	} else if err != nil {
		return team.Team{}, err
	}
	var addrs []string
	for _, a := range inviteAddrs(j.cfg.Addrs(), j.allowLoopback) {
		addrs = append(addrs, a.String())
	}
	j.cfg.Set.SetAddrs(t.Name, addrs)
	j.log.Info("this machine joined a team", "team", t.Name)
	j.cfg.Changed(t.Name)
	t, _ = j.cfg.Set.Get(t.Name)
	return t, nil
}

// dial connects to the inviter, whose peer id Noise checks, and opens a join
// stream. It dials only the addresses that joinDialable keeps.
func (j *JoinService) dial(ctx context.Context, inv Invite) (network.Stream, error) {
	ctx, cancel := context.WithTimeout(ctx, j.dialTimeout)
	defer cancel()
	if err := j.h.Connect(ctx, peer.AddrInfo{ID: inv.Inviter, Addrs: dialableAddrs(inv.Addrs, j.allowLoopback)}); err != nil {
		return nil, err
	}
	return j.h.NewStream(ctx, inv.Inviter, JoinProtocol)
}

// exchange runs the joiner's side of the protocol and returns the topics and
// the member list entries the inviter sent.
func (j *JoinService) exchange(s network.Stream, inv Invite, label string) ([]string, []team.Entry, error) {
	rw := joinIO{s: s, end: time.Now().Add(j.streamTimeout), frame: j.frameTimeout}
	if err := rw.write(joinFrame{Team: inv.Team, Secret: inv.Secret, Label: label}); err != nil {
		j.log.Debug("join request failed", "err", err)
		return nil, nil, ErrInviterUnreachable
	}
	f, err := rw.read()
	if err != nil {
		j.log.Debug("join reply failed", "err", err)
		return nil, nil, ErrJoinFailed
	}
	reply, ok := f.(joinReplyFrame)
	if !ok {
		return nil, nil, ErrJoinFailed
	}
	if reply.Result != joinOffer {
		return nil, nil, refusalError(reply.Result)
	}
	if err := j.cfg.Set.CheckJoin(inv.Team, reply.Topics); err != nil {
		return nil, nil, err
	}
	if err := rw.write(joinAcceptFrame{}); err != nil {
		j.log.Debug("join accept failed", "err", err)
		return nil, nil, ErrJoinFailed
	}
	var entries []team.Entry
	for frames := 0; ; frames++ {
		f, err := rw.read()
		switch {
		case errors.Is(err, io.EOF) && len(entries) > 0:
			return reply.Topics, entries, nil
		case err != nil:
			j.log.Debug("join member list failed", "err", err)
			return nil, nil, ErrJoinFailed
		case frames == maxJoinFrames:
			return nil, nil, ErrJoinFailed
		}
		switch f := f.(type) {
		case joinReplyFrame:
			if len(entries) == 0 && f.Result != joinOffer {
				return nil, nil, refusalError(f.Result)
			}
			return nil, nil, ErrJoinFailed
		case MembersFrame:
			if f.Team != inv.Team {
				return nil, nil, ErrJoinFailed
			}
			entries = append(entries, f.Entries...)
		default:
			return nil, nil, ErrJoinFailed
		}
	}
}
