//go:build unix

// Package daemon serves the courier to the harness clients of one user over a
// local socket, with line-delimited JSON-RPC 2.0. design.md "Socket and wire
// protocol" is the protocol.
package daemon

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/team"
)

// MaxLine is the most bytes a line can have without its newline.
const MaxLine = 64 << 10

// MaxID is the most bytes the JSON text of a request id can have.
const MaxID = 128

const maintainEvery = time.Hour

// writeTimeout is a variable so tests can lower it.
var writeTimeout = 10 * time.Second

const (
	codeParse           = -32700
	codeInvalidRequest  = -32600
	codeMethodNotFound  = -32601
	codeInvalidParams   = -32602
	codeInternal        = -32603
	codeNotRegistered   = -32001
	codeAlreadyRegister = -32002
	codeVersionMismatch = -32004
)

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func invalidParams(format string, args ...any) *rpcError {
	return &rpcError{codeInvalidParams, fmt.Sprintf(format, args...)}
}

var errInternal = &rpcError{codeInternal, "internal error"}

type request struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      jsontext.Value `json:"id,omitzero"`
	Method  string         `json:"method"`
	Params  jsontext.Value `json:"params,omitzero"`
}

type response struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      jsontext.Value `json:"id"`
	Result  any            `json:"result,omitzero"`
	Error   *rpcError      `json:"error,omitzero"`
}

var nullID = jsontext.Value("null")

// Server is the daemon. One connection is one session.
type Server struct {
	store *store.Store
	log   *slog.Logger

	mu       sync.Mutex
	sessions map[*session]struct{}
	names    map[string]*session
	wg       sync.WaitGroup
	idle     idle
	// reofferTick starts the ticker of one session's writer. Tests replace it.
	reofferTick func() (<-chan time.Time, func())
	// teams is the one member set of the daemon, in memory only.
	teams *team.Set
	// net is the p2p side of the team methods, set by SetNetwork.
	net Network
	// presence is the p2p side of remote presence, set by SetPresence.
	presence *P2P
}

type session struct {
	id   string
	conn net.Conn
	wmu  sync.Mutex
	// Only the goroutine of the session writes name and topics, under
	// Server.mu, so it reads them without the lock.
	name   string
	topics []string
	// out is set under Server.mu once register stored the agent.
	out *outbox
	// failed is set when a write fails: the session handles no more lines.
	failed atomic.Bool
}

func New(st *store.Store, log *slog.Logger) *Server {
	return &Server{store: st, log: log, sessions: map[*session]struct{}{}, names: map[string]*session{}, teams: team.NewSet(st.Now),
		reofferTick: func() (<-chan time.Time, func()) {
			t := time.NewTicker(reofferEvery)
			return t.C, t.Stop
		}}
}

// Serve accepts connections on ln until ctx ends, then closes ln and every
// connection and returns nil.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	s.idle.start(cancel)
	defer func() {
		cancel()
		s.closeAll()
		s.wg.Wait()
	}()
	s.wg.Go(func() {
		<-ctx.Done()
		ln.Close()
	})
	s.wg.Go(func() { s.maintainLoop(ctx) })
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			s.log.Warn("accept failed", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if err := checkPeer(c); err != nil {
			s.log.Warn("connection refused", "err", err)
			c.Close()
			continue
		}
		ss := &session{id: message.NewID(time.Now()).String(), conn: c}
		s.mu.Lock()
		s.sessions[ss] = struct{}{}
		s.mu.Unlock()
		s.idle.opened()
		s.wg.Go(func() { s.serveConn(ctx, ss) })
	}
}

func (s *Server) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ss := range s.sessions {
		ss.conn.Close()
	}
}

// maintainLoop runs the retention cleanup at start and every hour, and keeps
// the agents of live sessions known.
func (s *Server) maintainLoop(ctx context.Context) {
	t := time.NewTicker(maintainEvery)
	defer t.Stop()
	for {
		s.maintain(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Server) maintain(ctx context.Context) {
	if err := s.store.Cleanup(ctx); err != nil && ctx.Err() == nil {
		s.log.Error("cleanup failed", "err", err)
	}
	s.mu.Lock()
	live := map[string][]string{}
	for name, ss := range s.names {
		live[name] = ss.topics
	}
	s.mu.Unlock()
	for name, topics := range live {
		s.touch(ctx, name, topics)
	}
}

func (s *Server) touch(ctx context.Context, name string, topics []string) {
	for _, t := range topics {
		if err := s.store.Touch(ctx, t, name); err != nil && ctx.Err() == nil {
			s.log.Error("touch failed", "agent", name, "topic", t, "err", err)
		}
	}
}

func (s *Server) serveConn(ctx context.Context, ss *session) {
	defer s.drop(ctx, ss)
	s.log.Debug("connected", "session", ss.id)
	// Room for a line of MaxLine bytes and its \r\n.
	r := bufio.NewReaderSize(ss.conn, MaxLine+2)
	for {
		line, err := r.ReadSlice('\n')
		line = bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r"))
		if errors.Is(err, bufio.ErrBufferFull) || len(line) > MaxLine {
			s.log.Warn("line over the cap, closing", "session", ss.id)
			s.respond(ss, nullID, nil, &rpcError{codeInvalidRequest,
				fmt.Sprintf("a line has at most %d bytes: the daemon closes the connection", MaxLine)})
			return
		}
		if len(bytes.TrimSpace(line)) > 0 {
			if werr := s.handle(ctx, ss, line); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (s *Server) drop(ctx context.Context, ss *session) {
	ss.conn.Close()
	s.mu.Lock()
	delete(s.sessions, ss)
	s.idle.closed()
	if ss.name != "" && s.names[ss.name] == ss {
		delete(s.names, ss.name)
	}
	if ss.out != nil {
		close(ss.out.done)
	}
	s.mu.Unlock()
	if ss.name != "" {
		s.presenceChanged()
	}
	s.touch(context.WithoutCancel(ctx), ss.name, ss.topics)
	s.log.Debug("disconnected", "session", ss.id, "agent", ss.name)
}

// handle runs one line. It returns an error only when the response could not
// be written; then the connection is closed and handles no more lines.
func (s *Server) handle(ctx context.Context, ss *session, line []byte) error {
	if ss.failed.Load() {
		return net.ErrClosed
	}
	var members map[string]jsontext.Value
	if err := json.Unmarshal(line, &members); err != nil {
		if _, ok := errors.AsType[*jsontext.SyntacticError](err); ok {
			return s.respond(ss, nullID, nil, &rpcError{codeParse, "the line is not valid JSON in UTF-8, or it repeats a member name"})
		}
		return s.respond(ss, nullID, nil, &rpcError{codeInvalidRequest, "a line holds one JSON-RPC 2.0 request object"})
	}
	id, ok := members["id"]
	if !ok {
		return nil
	}
	if k := id.Kind(); k != '"' && k != '0' {
		return s.respond(ss, nullID, nil, &rpcError{codeInvalidRequest, "id must be a string or a number"})
	}
	if len(id) > MaxID {
		return s.respond(ss, nullID, nil, &rpcError{codeInvalidRequest, fmt.Sprintf("id has at most %d bytes of JSON", MaxID)})
	}
	var req request
	if err := json.Unmarshal(line, &req, json.RejectUnknownMembers(true)); err != nil || req.JSONRPC != "2.0" || req.Method == "" {
		return s.respond(ss, id, nil, &rpcError{codeInvalidRequest,
			`a request has "jsonrpc": "2.0", a method string, an id and optional params, and no other member`})
	}
	result, rerr := s.call(ctx, ss, req.Method, req.Params)
	if err := s.respond(ss, id, result, rerr); err != nil {
		return err
	}
	if req.Method == "register" && rerr == nil {
		s.afterRegister(ctx, ss)
	}
	return nil
}

func (s *Server) call(ctx context.Context, ss *session, method string, params jsontext.Value) (any, *rpcError) {
	switch method {
	case "register":
		return s.register(ctx, ss, params)
	case "send", "pending", "ack", "agents":
		if ss.name == "" {
			return nil, &rpcError{codeNotRegistered, "call register first: a session registers before any other method"}
		}
		switch method {
		case "send":
			return s.send(ctx, ss, params)
		case "pending":
			return s.pending(ctx, ss, params)
		case "ack":
			return s.ack(ctx, ss, params)
		}
		return s.agents(ctx, ss, params)
	// The team methods serve the CLI, which never registers.
	case "team.create":
		return s.teamCreate(ss, params)
	case "team.list":
		return s.teamList(params)
	case "team.leave":
		return s.teamLeave(ss, params)
	case "team.invite":
		return s.teamInvite(ss, params)
	case "team.join":
		return s.teamJoin(ctx, ss, params)
	}
	return nil, &rpcError{codeMethodNotFound, "the method must be one of register, send, pending, ack, agents, team.create, team.list, team.leave, team.invite, team.join"}
}

// decodeParams decodes an absent params or a JSON object into v. rule says
// what the params of the method are.
func decodeParams(raw jsontext.Value, v any, rule string) *rpcError {
	if len(raw) == 0 {
		raw = jsontext.Value("{}")
	}
	if raw.Kind() != '{' {
		return invalidParams("params must be a JSON object")
	}
	if err := json.Unmarshal(raw, v, json.RejectUnknownMembers(true)); err != nil {
		return invalidParams("%s", rule)
	}
	return nil
}

// respond writes the response to a request. Every error message passes
// through message.Inline, so it can reach a model as it is. When the write
// fails, respond closes the connection and returns the error.
func (s *Server) respond(ss *session, id jsontext.Value, result any, rerr *rpcError) error {
	resp := response{JSONRPC: "2.0", ID: id, Result: result}
	if rerr != nil {
		resp = response{JSONRPC: "2.0", ID: id, Error: &rpcError{rerr.Code, message.Inline(rerr.Message)}}
	}
	err := ss.write(resp)
	if errors.Is(err, errLineCap) {
		s.log.Error("response over the line cap", "session", ss.id)
		err = ss.write(response{JSONRPC: "2.0", ID: nullID, Error: errInternal})
	}
	if err != nil {
		s.log.Warn("write failed, closing", "session", ss.id, "err", err)
		ss.conn.Close()
	}
	return err
}

var errLineCap = errors.New("line over the cap")

// write sends v as one line. A failed write closes the connection, also when
// the client does not take the line within writeTimeout, so a client cannot
// stall the daemon.
func (ss *session) write(v any) error {
	line, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(line) > MaxLine {
		return errLineCap
	}
	ss.wmu.Lock()
	defer ss.wmu.Unlock()
	if err = ss.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err == nil {
		_, err = ss.conn.Write(append(line, '\n'))
	}
	if err != nil {
		ss.failed.Store(true)
		ss.conn.Close()
	}
	return err
}
