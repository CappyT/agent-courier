package courier

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// StreamName is the JetStream stream that stores every event. The server
// admin creates it from docs/stream.json: users may not change it.
const StreamName = "COURIER"

// Budget caps the events one sender pushes into the session per
// BudgetWindow. Events over the budget wait in pending without a push.
const (
	Budget       = 10
	BudgetWindow = 10 * time.Minute
)

// OnlineEvery is the shortest gap between two online events from the same
// sender on the same topic that reach the session.
const OnlineEvery = time.Hour

// Config selects the server, the identity and the topics to follow.
type Config struct {
	URL      string
	NkeyFile string
	Login    string
	Topics   []string
	// AckWait is how long an event stays with this agent before
	// JetStream delivers it again.
	AckWait time.Duration
}

// Delivery is an event waiting for the agent to ack it.
type Delivery struct {
	Seq       uint64
	Delivered uint64
	Event     Event
	// OverBudget marks the first event of a sender over Budget in the
	// current window. The session gets a notice instead of the event.
	OverBudget bool
}

// Bus publishes events and delivers the events of the followed topics.
type Bus struct {
	cfg Config
	nc  *nats.Conn
	js  jetstream.JetStream
	// durableKey keeps durable names secret: users may consume any
	// durable whose name they know.
	durableKey []byte

	mu      sync.Mutex
	pending map[uint64]pendingMsg
	budgets map[string]*budget
	online  map[string]time.Time
}

type budget struct {
	start  time.Time
	pushed int
}

type pendingMsg struct {
	msg      jetstream.Msg
	delivery Delivery
}

// Connect opens the connection and checks that the stream exists.
func Connect(ctx context.Context, cfg Config, opts ...nats.Option) (*Bus, error) {
	if !ValidToken(cfg.Login) {
		return nil, fmt.Errorf("login %q must match %s", cfg.Login, token)
	}
	for _, t := range cfg.Topics {
		if !ValidToken(t) {
			return nil, fmt.Errorf("topic %q must match %s", t, token)
		}
	}
	// Without a seed the server has no auth, and durable names need no secret.
	durableKey := []byte(cfg.Login)
	if cfg.NkeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(cfg.NkeyFile)
		if err != nil {
			return nil, fmt.Errorf("read nkey: %w", err)
		}
		opts = append(opts, opt)
		if durableKey, err = seed(cfg.NkeyFile); err != nil {
			return nil, err
		}
	}
	opts = append(opts,
		nats.Name("agent-courier "+cfg.Login),
		nats.MaxReconnects(-1),
		// Each user may subscribe only to its own inbox prefix, so no one
		// reads the replies and deliveries of another user.
		nats.CustomInboxPrefix("_INBOX_"+cfg.Login),
	)
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", cfg.URL, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	if _, err := js.Stream(ctx, StreamName); err != nil {
		nc.Close()
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("stream %s is missing: the server admin creates it from docs/stream.json", StreamName)
		}
		return nil, fmt.Errorf("stream %s: %w", StreamName, err)
	}
	return &Bus{
		cfg:        cfg,
		nc:         nc,
		js:         js,
		durableKey: durableKey,
		pending:    map[uint64]pendingMsg{},
		budgets:    map[string]*budget{},
		online:     map[string]time.Time{},
	}, nil
}

func seed(file string) ([]byte, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read nkey: %w", err)
	}
	kp, err := nkeys.ParseDecoratedNKey(data)
	if err != nil {
		return nil, fmt.Errorf("read nkey: %w", err)
	}
	return kp.Seed()
}

// Durable is the name of the consumer for the followed topics: the login,
// then an HMAC of the topics keyed by the seed. Another user cannot guess it.
func (b *Bus) Durable() string {
	mac := hmac.New(sha256.New, b.durableKey)
	mac.Write([]byte("agent-courier durable\x00" + strings.Join(slices.Sorted(slices.Values(b.cfg.Topics)), ",")))
	return b.cfg.Login + "_" + hex.EncodeToString(mac.Sum(nil))[:16]
}

// Close drains the connection.
func (b *Bus) Close() { _ = b.nc.Drain() }

// Login is the identity this bus publishes as.
func (b *Bus) Login() string { return b.cfg.Login }

// Publish sends one event as this login and returns its stream sequence.
func (b *Bus) Publish(ctx context.Context, e Event) (uint64, error) {
	e.Sender = b.cfg.Login
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	if err := e.Validate(); err != nil {
		return 0, err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return 0, err
	}
	ack, err := b.js.Publish(ctx, e.Subject(), data)
	if err != nil {
		return 0, fmt.Errorf("publish %s: %w", e.Subject(), err)
	}
	return ack.Sequence, nil
}

// Consume delivers the events of the followed topics to deliver until ctx
// ends. The durable consumer is named after the login and the topics, so
// events published while the agent was offline arrive when it starts again,
// and sessions on different topics keep their own filters.
// A redelivery of an event this process already delivered updates pending
// without a second push, and so does an event over the sender's budget.
func (b *Bus) Consume(ctx context.Context, deliver func(Delivery)) error {
	if len(b.cfg.Topics) == 0 {
		return fmt.Errorf("no topics to follow")
	}
	filters := make([]string, 0, len(b.cfg.Topics))
	for _, t := range b.cfg.Topics {
		filters = append(filters, TopicFilter(t))
	}
	durable := b.Durable()
	cons, err := b.js.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Durable:           durable,
		FilterSubjects:    filters,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           b.cfg.AckWait,
		MaxDeliver:        5,
		DeliverPolicy:     jetstream.DeliverNewPolicy,
		InactiveThreshold: 30 * 24 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("consumer %s: %w", durable, err)
	}
	slog.Info("follow topics", "topics", b.cfg.Topics, "consumer", durable)
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		e, err := Decode(msg.Subject(), msg.Data())
		if err != nil {
			slog.Warn("drop undecodable event", "err", err)
			_ = msg.Term()
			return
		}
		if e.Sender == b.cfg.Login {
			_ = msg.Ack()
			return
		}
		md, err := msg.Metadata()
		if err != nil {
			slog.Warn("drop event without metadata", "err", err)
			_ = msg.Term()
			return
		}
		d := Delivery{Seq: md.Sequence.Stream, Delivered: md.NumDelivered, Event: e}
		if push, ok := b.admit(msg, d); ok {
			deliver(push)
		}
	})
	if err != nil {
		return err
	}
	for _, t := range b.cfg.Topics {
		e := Event{Topic: t, Kind: KindOnline, Summary: b.cfg.Login + " is listening on " + t}
		if _, err := b.Publish(ctx, e); err != nil {
			slog.Warn("announce online", "topic", t, "err", err)
		}
	}
	<-ctx.Done()
	cc.Stop()
	return nil
}

// admit records d as pending and decides what reaches the session.
func (b *Bus) admit(msg jetstream.Msg, d Delivery) (Delivery, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if d.Event.Kind == KindOnline {
		key := d.Event.Sender + "." + d.Event.Topic
		if last, ok := b.online[key]; ok && now.Sub(last) < OnlineEvery {
			_ = msg.Ack()
			return d, false
		}
		b.online[key] = now
	}
	_, seen := b.pending[d.Seq]
	b.pending[d.Seq] = pendingMsg{msg: msg, delivery: d}
	if seen {
		return d, false
	}
	bg := b.budgets[d.Event.Sender]
	if bg == nil || now.Sub(bg.start) >= BudgetWindow {
		bg = &budget{start: now}
		b.budgets[d.Event.Sender] = bg
	}
	bg.pushed++
	switch {
	case bg.pushed <= Budget:
		return d, true
	case bg.pushed == Budget+1:
		d.OverBudget = true
		return d, true
	}
	return d, false
}

// Ack confirms that the agent handled the event with stream sequence seq.
func (b *Bus) Ack(seq uint64) error {
	b.mu.Lock()
	p, ok := b.pending[seq]
	delete(b.pending, seq)
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("no pending event with seq %d", seq)
	}
	return p.msg.DoubleAck(context.Background())
}

// Pending lists the delivered events that wait for an ack, oldest first.
func (b *Bus) Pending() []Delivery {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Delivery, 0, len(b.pending))
	for _, seq := range slices.Sorted(maps.Keys(b.pending)) {
		out = append(out, b.pending[seq].delivery)
	}
	return out
}
