package courier

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// StreamName is the JetStream stream that stores every event.
const StreamName = "COURIER"

// Config selects the server, the identity and the repos to follow.
type Config struct {
	URL      string
	NkeyFile string
	Login    string
	Repos    []string
	// AckWait is how long an event stays with this orchestrator before
	// JetStream delivers it again.
	AckWait time.Duration
}

// Delivery is an event waiting for the agent to ack it.
type Delivery struct {
	Seq       uint64
	Delivered uint64
	Event     Event
}

// Bus publishes events and delivers the events of the followed repos.
type Bus struct {
	cfg Config
	nc  *nats.Conn
	js  jetstream.JetStream

	mu      sync.Mutex
	pending map[uint64]pendingMsg
}

type pendingMsg struct {
	msg      jetstream.Msg
	delivery Delivery
}

// Connect opens the connection and creates the stream when it is missing.
func Connect(ctx context.Context, cfg Config, opts ...nats.Option) (*Bus, error) {
	if !ValidToken(cfg.Login) {
		return nil, fmt.Errorf("login %q must match %s", cfg.Login, token)
	}
	for _, r := range cfg.Repos {
		if !ValidToken(r) {
			return nil, fmt.Errorf("repo %q must match %s", r, token)
		}
	}
	if cfg.NkeyFile != "" {
		opt, err := nats.NkeyOptionFromSeed(cfg.NkeyFile)
		if err != nil {
			return nil, fmt.Errorf("read nkey: %w", err)
		}
		opts = append(opts, opt)
	}
	opts = append(opts, nats.Name("agent-courier "+cfg.Login), nats.MaxReconnects(-1))
	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", cfg.URL, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     StreamName,
		Subjects: []string{SubjectPrefix + ".>"},
		Storage:  jetstream.FileStorage,
		MaxAge:   30 * 24 * time.Hour,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("stream %s: %w", StreamName, err)
	}
	return &Bus{cfg: cfg, nc: nc, js: js, pending: map[uint64]pendingMsg{}}, nil
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

// Consume delivers the events of the followed repos to deliver until ctx
// ends. The durable consumer is named after the login, so events published
// while the orchestrator was offline arrive when it starts again.
func (b *Bus) Consume(ctx context.Context, deliver func(Delivery)) error {
	if len(b.cfg.Repos) == 0 {
		return fmt.Errorf("no repos to follow")
	}
	filters := make([]string, 0, len(b.cfg.Repos))
	for _, r := range b.cfg.Repos {
		filters = append(filters, RepoFilter(r))
	}
	cons, err := b.js.CreateOrUpdateConsumer(ctx, StreamName, jetstream.ConsumerConfig{
		Durable:        b.cfg.Login,
		FilterSubjects: filters,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        b.cfg.AckWait,
		MaxDeliver:     5,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
	})
	if err != nil {
		return fmt.Errorf("consumer %s: %w", b.cfg.Login, err)
	}
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
		b.mu.Lock()
		b.pending[d.Seq] = pendingMsg{msg: msg, delivery: d}
		b.mu.Unlock()
		deliver(d)
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	cc.Stop()
	return nil
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
