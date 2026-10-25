//go:build unix

package daemon

import (
	"context"
	"errors"
	"io"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/team"
)

const (
	// outboxParallel is the most streams the worker has open at once.
	outboxParallel = 8
	// outboxDueEvery is the least time between two connections of a member
	// that make its items due at once.
	outboxDueEvery = time.Minute
	// outboxPoll bounds the wait of the worker when nothing wakes it.
	outboxPoll = time.Minute
	// outboxTimeout bounds one send to a member: the dial, the frames and
	// the receipts.
	outboxTimeout = 30 * time.Second
)

// outboxChanged wakes the worker of the send queue after a send queued a
// message.
func (s *Server) outboxChanged() {
	if w, ok := s.net.(interface{ OutboxChanged() }); ok {
		w.OutboxChanged()
	}
}

// receiveRemote stores m, a message from the member m.Origin of team name,
// with the rules of a local send, and pushes it through the single push
// path. It reports whether the sender gets a receipt: also for a duplicate,
// for a broadcast that reaches no agent, and for a message that the rules
// refuse for good. A message that a full pending queue or the hourly cap of
// its member refuses, that comes again while its first copy is being
// stored, or that the store failed to keep, gets none, so it comes again.
// The text of m is another person's: no log line quotes it.
func (s *Server) receiveRemote(ctx context.Context, name string, m message.Message) bool {
	wire := m.ID
	dup, err := s.teams.BeginReceive(name, m.Origin, wire)
	switch {
	case err == nil && dup:
		s.log.Debug("message from a member stored before, receipt sent again", "peer", m.Origin)
		return true
	case errors.Is(err, team.ErrReceiveRate):
		s.log.Debug("message from a member refused, it sent the most messages of one hour: it comes again", "peer", m.Origin)
		return false
	case err != nil:
		s.log.Debug("message from a member refused: it comes again", "peer", m.Origin, "err", err)
		return false
	}
	sent, err := s.store.Receive(ctx, m)
	var local message.ID
	if err == nil && !sent.Dropped {
		local = sent.Message.ID
	}
	s.teams.EndReceive(name, m.Origin, wire, local, err == nil)
	switch {
	case err == nil:
	case errors.As(err, new(*store.QueueFullError)):
		s.log.Debug("message from a member refused, a pending queue is full: it comes again", "peer", m.Origin)
		return false
	case errors.Is(err, store.ErrUnknownRecipient), errors.Is(err, store.ErrNoAsk):
		s.log.Debug("message from a member refused", "peer", m.Origin, "err", err)
		return true
	default:
		if ctx.Err() == nil {
			s.log.Error("store a message from a member", "peer", m.Origin, "err", err)
		}
		return false
	}
	if sent.Dropped {
		s.log.Debug("broadcast from a member reaches no agent, not stored", "peer", m.Origin)
		return true
	}
	for _, r := range sent.Recipients {
		s.pushTo(r, sent.Message)
	}
	s.log.Debug("message from a member stored", "peer", m.Origin, "id", sent.Message.ID,
		"recipients", len(sent.Recipients), "skipped", len(sent.Skipped))
	return true
}

// receiveMessage takes a message frame from the member from. It drops the
// frame, without a receipt, when the topic is not a topic of a team that
// from shares with this machine, by the member set in memory. The sender is
// <label>/<agent> with the label of from in the member list of this
// machine, never a field of the frame.
func (p *P2P) receiveMessage(ctx context.Context, from libp2ppeer.ID, f p2p.MessageFrame) (p2p.Frame, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	name, _ := p.set.TopicTeam(f.Topic)
	sender, ok := p.set.Sender(name, from.String(), f.Sender)
	if !ok {
		p.log.Debug("message on a topic outside the teams of the peer, dropped", "peer", from)
		return nil, nil
	}
	if p.local == nil {
		return nil, nil
	}
	m := message.Message{ID: f.ID, Sender: sender, Origin: from.String(),
		Created: p.store.Now(), Fields: f.Fields}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	if !p.local.receiveRemote(ctx, name, m) {
		return nil, nil
	}
	return p2p.ReceiptFrame{ID: f.ID}, nil
}

// receiveReceipt takes a receipt that from sent on a stream it opened. It
// deletes only the queued item of from.
func (p *P2P) receiveReceipt(from libp2ppeer.ID, f p2p.ReceiptFrame) error {
	if err := f.Validate(); err != nil {
		return err
	}
	p.set.Delivered(from.String(), f.ID)
	return nil
}

// OutboxChanged wakes the worker of the send queue: a send queued a
// message, or a member list changed.
func (p *P2P) OutboxChanged() {
	if n := p.current(); n != nil && n.outbox != nil {
		n.outbox.wake()
	}
}

// outboxWorker sends the send queue of the member set to the members while
// the host runs. It has one send at most per member, and outboxParallel in
// all.
type outboxWorker struct {
	p      *P2P
	h      *p2p.Host
	ctx    context.Context
	cancel context.CancelFunc
	kick   chan struct{}
	sem    chan struct{}
	wg     sync.WaitGroup

	mu sync.Mutex
	// busy are the members with a send running.
	busy map[string]bool
	// due are the times a connection last made a member's items due.
	due map[string]time.Time
}

// startOutbox starts the worker on h. A member that connects makes its items
// due at once, at most once in outboxDueEvery.
func (p *P2P) startOutbox(ctx context.Context, h *p2p.Host) *outboxWorker {
	w := newOutboxWorker(ctx, p, h)
	h.OnConnected(func(id libp2ppeer.ID) {
		if p.IsMember(id) {
			w.connected(id.String())
		}
	})
	w.wg.Go(w.run)
	return w
}

func newOutboxWorker(ctx context.Context, p *P2P, h *p2p.Host) *outboxWorker {
	ctx, cancel := context.WithCancel(ctx)
	return &outboxWorker{p: p, h: h, ctx: ctx, cancel: cancel, kick: make(chan struct{}, 1),
		sem: make(chan struct{}, outboxParallel), busy: map[string]bool{}, due: map[string]time.Time{}}
}

// connected makes the items of the member peer due, unless a connection
// did in the last outboxDueEvery: a member that reconnects in a loop
// cannot keep its items ahead of the others. It reports whether it made
// them due.
func (w *outboxWorker) connected(peer string) bool {
	now := w.p.store.Now()
	w.mu.Lock()
	if last, ok := w.due[peer]; ok && now.Sub(last) < outboxDueEvery {
		w.mu.Unlock()
		return false
	}
	maps.DeleteFunc(w.due, func(_ string, at time.Time) bool { return now.Sub(at) >= outboxDueEvery })
	w.due[peer] = now
	w.mu.Unlock()
	w.p.set.MakeDue(peer)
	w.wake()
	return true
}

func (w *outboxWorker) wake() {
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// close stops the worker and waits for it and its sends.
func (w *outboxWorker) close() {
	w.cancel()
	w.wg.Wait()
}

func (w *outboxWorker) run() {
	for {
		wait := outboxPoll
		if next, ok := w.pass(); ok {
			wait = min(wait, max(next.Sub(w.p.store.Now()), 0))
		}
		t := time.NewTimer(wait)
		select {
		case <-w.ctx.Done():
			t.Stop()
			return
		case <-w.kick:
		case <-t.C:
		}
		t.Stop()
	}
}

// isBusy reports whether the member peer has a send running.
func (w *outboxWorker) isBusy(peer string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.busy[peer]
}

// pass starts a send for each member with due items and no send running,
// of team.QueueBatch items at most. It returns the time of the next attempt
// of a member with no send running; the end of a send wakes the worker.
func (w *outboxWorker) pass() (time.Time, bool) {
	// Only a pass adds to busy, so a member that Take returns has no send
	// running when its own starts.
	batches, next, ok := w.p.set.Take(w.isBusy)
	w.mu.Lock()
	for _, b := range batches {
		w.busy[b.PeerID] = true
		w.wg.Go(func() { w.send(b.PeerID, b.Items) })
	}
	w.mu.Unlock()
	return next, ok
}

// send writes items, all for the member peer, on one stream, then wakes
// the worker for the items that waited for the end of the send.
func (w *outboxWorker) send(peer string, items []team.Item) {
	defer func() {
		w.mu.Lock()
		delete(w.busy, peer)
		w.mu.Unlock()
		w.wake()
	}()
	select {
	case w.sem <- struct{}{}:
	case <-w.ctx.Done():
		return
	}
	defer func() { <-w.sem }()
	if err := w.write(peer, items); err != nil && w.ctx.Err() == nil {
		w.p.log.Debug("send to a member failed, it waits for the backoff", "peer", peer, "count", len(items), "err", err)
	}
}

// write dials the member peer, writes the frames of items on one
// MsgProtocol stream and deletes each item whose receipt comes back. It
// sends nothing to a peer that is not a member of the team of an item in
// memory.
func (w *outboxWorker) write(peer string, items []team.Item) error {
	id, err := libp2ppeer.Decode(peer)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(w.ctx, outboxTimeout)
	defer cancel()
	want := map[message.ID]bool{}
	var frames []p2p.MessageFrame
	for _, it := range items {
		if !w.p.MemberOf(id, it.Team) {
			continue
		}
		f := frameOf(it.Message)
		if err := f.Validate(); err != nil {
			// A stored message passed the same rules: this is a bug.
			w.p.log.Error("queued message is no valid frame, dropped", "id", it.Message.ID, "err", err)
			w.p.set.Delivered(peer, it.Message.ID)
			continue
		}
		want[f.ID] = true
		frames = append(frames, f)
	}
	if len(frames) == 0 {
		return nil
	}
	addrs := w.p.set.Addrs(items[0].Team, peer)
	// Connect waits for the first reservation after a start: an open
	// connection needs no dial.
	if c := w.h.Host().Network().Connectedness(id); c != network.Connected && c != network.Limited {
		if err := w.h.Connect(ctx, id, addrs); err != nil {
			return err
		}
	}
	s, err := w.h.Host().NewStream(network.WithAllowLimitedConn(ctx, "agent-courier"), id, p2p.MsgProtocol)
	if err != nil {
		return err
	}
	defer s.Close()
	deadline, _ := ctx.Deadline()
	if err := s.SetDeadline(deadline); err != nil {
		return err
	}
	for _, f := range frames {
		if err := p2p.WriteFrame(s, f); err != nil {
			s.Reset()
			return err
		}
	}
	if err := s.CloseWrite(); err != nil {
		return err
	}
	// One receipt per frame at most, so a member cannot keep the stream.
	for range len(frames) {
		f, err := p2p.ReadFrame(s)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		rf, ok := f.(p2p.ReceiptFrame)
		if !ok {
			s.Reset()
			return errors.New("the member answered a message with a frame that is no receipt")
		}
		if !want[rf.ID] {
			continue
		}
		delete(want, rf.ID)
		w.p.set.Delivered(peer, rf.ID)
	}
	return nil
}

// frameOf is the frame of m, a message of this machine. Its recipient is
// the agent on the machine of the member, without the member label.
func frameOf(m message.Message) p2p.MessageFrame {
	f := p2p.MessageFrame{ID: m.ID, Sender: m.Sender, Fields: m.Fields}
	if _, agent, ok := strings.Cut(m.To, "/"); ok {
		f.To = agent
	}
	return f
}
