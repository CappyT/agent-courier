//go:build unix

package client

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

const (
	minBackoff     = 100 * time.Millisecond
	maxBackoff     = 5 * time.Second
	connectTimeout = StartTimeout + 5*time.Second

	codeInvalidParams   = -32602
	codeVersionMismatch = -32004
)

// Config is the setup of a Session.
type Config struct {
	Params RegisterParams
	// OnDeliver receives each deliver notification. It can be nil.
	OnDeliver func(Deliver)
	// OnRegistered receives each registration, the first one included. It
	// runs after the deliver notifications of the earlier connection and
	// before those of the new one, so a client that sees a new name gives its
	// agent the new Protocol the way it gives a push. It can be nil.
	OnRegistered func(Registration)
	// Log receives each failure to connect or register, with its cause. It
	// can be nil.
	Log *slog.Logger
}

// Session keeps a client registered with the daemon. When the connection
// breaks, it connects again, starting a daemon when none runs, and registers
// with the name and topics it had.
//
// A register error from the daemon, for example a version mismatch, stops the
// reconnects, because each one would cancel the idle exit of the daemon. Then
// each call fails with that error after one new attempt to connect.
type Session struct {
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	retry  sync.Mutex

	mu     sync.Mutex
	params RegisterParams
	cur    *Conn
	last   *Conn
	reg    Registration
	ready  chan struct{}
	fatal  error
	closed bool
	// logged and loggedAt are the last failure logged and when.
	logged   string
	loggedAt time.Time
}

// Start connects and registers in the background, and keeps doing so after
// each break until Close.
func Start(cfg Config) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Session{cfg: cfg, ctx: ctx, cancel: cancel, params: cfg.Params, ready: make(chan struct{})}
	s.wg.Go(s.reconnect)
	return s
}

// Close closes the connection and stops reconnecting.
func (s *Session) Close() error {
	s.cancel()
	s.mu.Lock()
	s.closed = true
	cur := s.cur
	s.mu.Unlock()
	if cur != nil {
		cur.Close()
	}
	s.wg.Wait()
	return nil
}

// Registration waits until the session is registered and returns the
// registration. It fails with ErrUnreachable when ctx ends first, and with
// the register error of the daemon after one.
func (s *Session) Registration(ctx context.Context) (Registration, error) {
	for {
		s.mu.Lock()
		cur, reg, ready, fatal := s.cur, s.reg, s.ready, s.fatal
		s.mu.Unlock()
		switch {
		case cur != nil:
			return reg, nil
		case fatal != nil:
			return Registration{}, fatal
		}
		if err := s.wait(ctx, ready); err != nil {
			return Registration{}, err
		}
	}
}

// Call is Conn.Call on the current connection. Without a connection it waits
// for the next one until ctx ends, then fails with ErrUnreachable. A call that
// waits when the connection breaks fails and is never sent again: a send may
// have reached the store, so the agent decides. After a register error, Call
// tries one new connection and returns that error when it fails again.
func (s *Session) Call(ctx context.Context, method string, params, result any) error {
	retried := false
	for {
		s.mu.Lock()
		cur, ready, fatal := s.cur, s.ready, s.fatal
		s.mu.Unlock()
		switch {
		case cur != nil:
			sent, err := cur.call(ctx, method, params, result)
			if sent || !errors.Is(err, ErrUnreachable) {
				return err
			}
			// The connection broke before the request left: wait for the
			// next one.
			continue
		case fatal != nil:
			if retried {
				return fatal
			}
			retried = true
			s.retryOnce(fatal)
			continue
		}
		if err := s.wait(ctx, ready); err != nil {
			return err
		}
	}
}

func (s *Session) wait(ctx context.Context, ready <-chan struct{}) error {
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
	case <-s.ctx.Done():
	}
	return ErrUnreachable
}

// retryOnce tries one connection after the register error seen, unless a
// concurrent call already replaced it.
func (s *Session) retryOnce(seen error) {
	s.retry.Lock()
	defer s.retry.Unlock()
	s.mu.Lock()
	still := s.fatal == seen
	p := s.params
	s.mu.Unlock()
	if !still {
		return
	}
	cn, reg, err := s.connect(p)
	if err != nil {
		s.failed(err)
		s.mu.Lock()
		s.fatal = err
		s.mu.Unlock()
		return
	}
	if !s.install(cn, reg) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Close closes cur itself; under mu, Add cannot race its Wait.
	if s.closed {
		return
	}
	s.wg.Go(func() {
		if s.watch(cn) {
			s.reconnect()
		}
	})
}

// reconnect connects with backoff until it succeeds, the daemon refuses the
// registration, or Close. Then it watches the connection and starts again
// when it breaks.
func (s *Session) reconnect() {
	backoff := minBackoff
	for {
		s.mu.Lock()
		p := s.params
		s.mu.Unlock()
		cn, reg, err := s.connect(p)
		if err != nil {
			s.failed(err)
			if registerRefused(err) {
				s.mu.Lock()
				s.fatal = err
				closeOnce(s.ready)
				s.mu.Unlock()
				return
			}
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, maxBackoff)
			continue
		}
		backoff = minBackoff
		if !s.install(cn, reg) {
			return
		}
		if !s.watch(cn) {
			return
		}
	}
}

// failed logs a failure to connect or register. It logs a cause only when it
// differs from the last one logged and maxBackoff has passed since, so the
// backoff loop does not flood the log.
func (s *Session) failed(err error) {
	if s.cfg.Log == nil {
		return
	}
	s.mu.Lock()
	text := err.Error()
	log := text != s.logged && time.Since(s.loggedAt) >= maxBackoff
	if log {
		s.logged, s.loggedAt = text, time.Now()
	}
	s.mu.Unlock()
	if log {
		s.cfg.Log.Warn("cannot register with the courier daemon", "err", err)
	}
}

// registerRefused tells a register error that a retry does not fix.
func registerRefused(err error) bool {
	e, ok := errors.AsType[*RPCError](err)
	return ok && (e.Code == codeVersionMismatch || e.Code == codeInvalidParams)
}

// install makes cn the current connection. It hands the notifications of the
// earlier connection first, then the registration, before cn's
// notifications. It returns false after Close.
func (s *Session) install(cn *Conn, reg Registration) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cn.Close()
		cn.release()
		return false
	}
	last := s.last
	s.last, s.reg, s.fatal = cn, reg, nil
	s.logged, s.loggedAt = "", time.Time{}
	s.params.Name, s.params.Topics = reg.Name, reg.Topics
	// A connection that broke after register is not current: dropped has
	// run already or runs after this.
	if !cn.broken.Load() {
		s.cur = cn
		closeOnce(s.ready)
	} else if s.cur == nil {
		select {
		case <-s.ready:
			s.ready = make(chan struct{})
		default:
		}
	}
	s.mu.Unlock()
	if last != nil {
		<-last.dispatched
	}
	if s.cfg.OnRegistered != nil {
		s.cfg.OnRegistered(reg)
	}
	cn.release()
	return true
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// watch waits until cn breaks. It returns false after Close.
func (s *Session) watch(cn *Conn) bool {
	select {
	case <-cn.Done():
		return s.ctx.Err() == nil
	case <-s.ctx.Done():
		cn.Close()
		return false
	}
}

// dropped runs when cn breaks, before a call can see the break, so a call
// that finds cn broken finds another current connection when it looks again.
func (s *Session) dropped(cn *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == cn {
		s.cur, s.ready = nil, make(chan struct{})
	}
}

func (s *Session) connect(p RegisterParams) (*Conn, Registration, error) {
	ctx, cancel := context.WithTimeout(s.ctx, connectTimeout)
	defer cancel()
	c, err := Dial(ctx)
	if err != nil {
		return nil, Registration{}, err
	}
	cn := newConn(c, s.cfg.OnDeliver, s.dropped)
	reg, err := cn.Register(ctx, p)
	if err != nil {
		cn.Close()
		cn.release()
		return nil, Registration{}, err
	}
	return cn, reg, nil
}
