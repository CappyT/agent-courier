//go:build unix

// Package mcpserver is the Claude Code client: an MCP server over stdio with
// the tools send, pending, ack and agents, which pushes each deliver
// notification of the daemon into the session as a channel event. design.md
// "Claude Code client" and "Client mapping" are the contract.
package mcpserver

import (
	"cmp"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/CappyT/agent-courier/internal/client"
	"github.com/CappyT/agent-courier/internal/daemon"
	"github.com/CappyT/agent-courier/internal/version"
)

const (
	// Harness is the harness name that the client registers.
	Harness = "claude"
	// ChannelMethod is the notification that pushes an event into a Claude
	// Code session.
	ChannelMethod = "notifications/claude/channel"
	// DefaultRegisterWait is how long initialize waits for the registration.
	DefaultRegisterWait = 5 * time.Second
)

// callTimeout bounds a tool call, the wait for a connection included. Tests
// shorten it.
var callTimeout = 10 * time.Second

// Config is the setup of Serve.
type Config struct {
	Params client.RegisterParams
	// Log receives the logs. It must not write to the MCP output. Nil
	// discards them.
	Log *slog.Logger
	// RegisterWait replaces DefaultRegisterWait when it is not zero.
	RegisterWait time.Duration
}

// Serve registers with the daemon and serves MCP on in and out until in ends
// or ctx ends.
func Serve(ctx context.Context, cfg Config, in io.Reader, out io.Writer) error {
	log := cmp.Or(cfg.Log, slog.New(slog.DiscardHandler))
	wait := cmp.Or(cfg.RegisterWait, DefaultRegisterWait)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	w := &lineWriter{w: out}
	p := &pusher{w: w, log: log, initialized: make(chan struct{}), wake: make(chan struct{}, 1)}
	var wg sync.WaitGroup
	wg.Go(func() { p.run(ctx.Done()) })
	defer wg.Wait()
	defer cancel()
	sess := client.Start(client.Config{Params: cfg.Params, OnDeliver: p.deliver, OnRegistered: p.registered, Log: log})
	defer sess.Close()

	hooks := &server.Hooks{}
	hooks.AddAfterInitialize(func(ctx context.Context, _ any, _ *mcp.InitializeRequest, res *mcp.InitializeResult) {
		ctx, stop := context.WithTimeout(ctx, wait)
		defer stop()
		reg, err := sess.Registration(ctx)
		if err != nil {
			log.Warn("not registered with the daemon, the instructions are the built-in protocol", "err", err)
			return
		}
		res.Instructions = reg.Protocol
		p.mu.Lock()
		p.told = reg.Name
		p.mu.Unlock()
	})
	s := server.NewMCPServer("agent-courier", version.Version,
		server.WithExperimental(map[string]any{"claude/channel": map[string]any{}}),
		server.WithToolCapabilities(false),
		server.WithInstructions(daemon.Protocol),
		server.WithHooks(hooks),
	)
	s.AddNotificationHandler(string(mcp.MethodNotificationInitialized), func(context.Context, mcp.JSONRPCNotification) {
		p.openOnce.Do(func() { close(p.initialized) })
	})
	addTools(s, sess, p.initialized, log)

	stdio := server.NewStdioServer(s)
	stdio.SetErrorLogger(slog.NewLogLogger(log.Handler(), slog.LevelError))
	// Claude Code does not register a channel server that negotiates
	// 2026-07-28 or later.
	stdio.SetContextFunc(func(ctx context.Context) context.Context {
		return server.WithSupportedProtocolVersions(ctx, mcp.LegacyProtocolVersions())
	})
	return stdio.Listen(ctx, in, w)
}

// lineWriter serializes the writes to the MCP output. mcp-go writes each
// message with one Write, and so does the pusher, so lines never interleave.
type lineWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lineWriter) Write(b []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(b)
}

// pusher writes channel events in the order of the client's callbacks. The
// callbacks only queue, so they never block the client. The queue is written
// after the client sends initialized. A registration goes out as its protocol
// when its name differs from the name the agent has, so each push follows the
// protocol of the connection it came from.
type pusher struct {
	w   io.Writer
	log *slog.Logger

	openOnce    sync.Once
	initialized chan struct{}
	wake        chan struct{}

	mu    sync.Mutex
	queue []event
	// told is the agent name whose protocol the agent has, or "" when it has
	// only the built-in protocol.
	told string
}

// event is a deliver notification, or a registration when reg is set.
type event struct {
	reg   *client.Registration
	text  string
	attrs map[string]string
}

func (p *pusher) deliver(d client.Deliver) { p.enqueue(event{text: d.Text, attrs: d.Attrs}) }

func (p *pusher) registered(reg client.Registration) { p.enqueue(event{reg: &reg}) }

func (p *pusher) enqueue(e event) {
	p.mu.Lock()
	p.queue = append(p.queue, e)
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// run writes the queue after initialized, until done.
func (p *pusher) run(done <-chan struct{}) {
	select {
	case <-p.initialized:
	case <-done:
		return
	}
	for {
		p.mu.Lock()
		q := p.queue
		p.queue = nil
		p.mu.Unlock()
		for _, e := range q {
			p.write(e)
		}
		select {
		case <-p.wake:
		case <-done:
			return
		}
	}
}

func (p *pusher) write(e event) {
	if e.reg == nil {
		p.push(e.text, e.attrs)
		return
	}
	p.mu.Lock()
	known := e.reg.Name == p.told
	p.told = e.reg.Name
	p.mu.Unlock()
	if !known {
		p.push(e.reg.Protocol, nil)
	}
}

type channelEvent struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  channelParams `json:"params"`
}

type channelParams struct {
	Content string            `json:"content"`
	Meta    map[string]string `json:"meta,omitempty"`
}

// push writes one channel event. content is text of the daemon and meta the
// attrs of the daemon: both pass unchanged.
func (p *pusher) push(content string, meta map[string]string) {
	line, err := json.Marshal(channelEvent{JSONRPC: mcp.JSONRPC_VERSION, Method: ChannelMethod, Params: channelParams{content, meta}})
	if err != nil {
		p.log.Error("encode channel event", "err", err)
		return
	}
	if _, err := p.w.Write(append(line, '\n')); err != nil {
		p.log.Warn("write channel event", "err", err)
	}
}

// errorResult is the tool result of a failed daemon call: the daemon's own
// message, else the text of a broken connection.
func errorResult(log *slog.Logger, method string, err error) *mcp.CallToolResult {
	if e, ok := errors.AsType[*client.RPCError](err); ok {
		return mcp.NewToolResultError(e.Message)
	}
	log.Warn("daemon call failed", "method", method, "err", err)
	return mcp.NewToolResultError(client.ErrUnreachable.Error())
}
