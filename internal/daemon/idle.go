//go:build unix

package daemon

import (
	"context"
	"strconv"
	"sync"
	"time"
)

const (
	// DefaultIdleGrace is how long the daemon runs with no connected client.
	DefaultIdleGrace = 60 * time.Second
	// IdleGraceEnv overrides DefaultIdleGrace with a Go duration, for example 5m.
	IdleGraceEnv = "AGENT_COURIER_IDLE_GRACE"
)

// idle ends Serve when no connection has been open for the grace period
// and nothing holds the daemon. A zero grace never ends it.
type idle struct {
	mu    sync.Mutex
	grace time.Duration
	open  int
	// held is true while the machine is in a team: its teams live only in
	// the daemon's memory.
	held   bool
	gen    int
	timer  *time.Timer
	cancel context.CancelFunc
}

// ExitWhenIdle makes Serve return when no client has been connected for
// grace, counted from the start of Serve and from each moment the last
// connection closes or the machine leaves its last team. A client that
// connects in that time cancels the exit, and so does a team that the
// machine creates or joins. Call it before Serve.
func (s *Server) ExitWhenIdle(grace time.Duration) {
	s.idle.mu.Lock()
	defer s.idle.mu.Unlock()
	s.idle.grace = grace
}

// describe is the grace for a message to an agent, or "" without idle exit.
func (i *idle) describe() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	switch {
	case i.grace <= 0:
		return ""
	case i.grace%time.Second == 0:
		return strconv.Itoa(int(i.grace/time.Second)) + " seconds"
	}
	return i.grace.String()
}

func (i *idle) start(cancel context.CancelFunc) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.cancel = cancel
	i.arm()
}

func (i *idle) opened() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.open++
	i.disarm()
}

func (i *idle) closed() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.open--
	if i.open == 0 {
		i.arm()
	}
}

// hold keeps the daemon running while held is true. When it turns false
// with no connection open, the grace period starts.
func (i *idle) hold(held bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if held == i.held {
		return
	}
	i.held = held
	if held {
		i.disarm()
	} else if i.open == 0 {
		i.arm()
	}
}

func (i *idle) disarm() {
	i.gen++
	if i.timer != nil {
		i.timer.Stop()
		i.timer = nil
	}
}

// arm starts the grace period. A timer that fires after a later opened or
// arm sees another generation and does nothing.
func (i *idle) arm() {
	i.disarm()
	if i.grace <= 0 || i.cancel == nil || i.held {
		return
	}
	gen := i.gen
	i.timer = time.AfterFunc(i.grace, func() {
		i.mu.Lock()
		defer i.mu.Unlock()
		if i.gen == gen && i.open == 0 && !i.held {
			i.cancel()
		}
	})
}
