//go:build unix

package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CappyT/agent-courier/internal/daemon"
	"github.com/CappyT/agent-courier/internal/version"
)

// ErrUnreachable is the error of a call when the connection to the daemon is
// broken or missing. Its text is what the agent sees.
var ErrUnreachable = errors.New("the courier daemon is not reachable")

const writeTimeout = 10 * time.Second

// RPCError is an error response of the daemon. Message is one line that the
// daemon wrote for the agent.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string { return e.Message }

// Deliver is the params of a deliver notification.
type Deliver struct {
	Text   string            `json:"text"`
	Notice string            `json:"notice"`
	Attrs  map[string]string `json:"attrs"`
}

// Conn is one connection to the daemon, which is one session.
//
// The queue of deliver notifications has no bound, so the reads never stop
// while onDeliver is slow. The daemon bounds it: the push budgets allow at
// most 30 broadcasts and 30 direct messages per session and hour, and two
// budget notices per sender plus two per session in each budget window.
// Onlines have no session budget: one per agent and topic per hour, so the
// agents and topics of one user bound them. Each line has at most
// daemon.MaxLine bytes.
type Conn struct {
	conn      net.Conn
	onDeliver func(Deliver)
	onShut    func(*Conn)

	wmu sync.Mutex

	mu      sync.Mutex
	next    int64
	waiting map[int64]chan reply
	closed  bool
	broken  atomic.Bool
	done    chan struct{}

	qmu        sync.Mutex
	queue      []Deliver
	queued     chan struct{}
	released   chan struct{}
	releaseOne sync.Once
	dispatched chan struct{}
}

type reply struct {
	result jsontext.Value
	err    error
}

type incoming struct {
	ID     jsontext.Value `json:"id"`
	Method string         `json:"method"`
	Params jsontext.Value `json:"params"`
	Result jsontext.Value `json:"result"`
	Error  *RPCError      `json:"error"`
}

// NewConn runs the JSON-RPC protocol on c. onDeliver, when not nil, receives
// each deliver notification in order, on a goroutine of its own, so a slow
// onDeliver does not stop the reads that the daemon needs.
func NewConn(c net.Conn, onDeliver func(Deliver)) *Conn {
	cn := newConn(c, onDeliver, nil)
	cn.release()
	return cn
}

// newConn holds the deliver notifications until release. onShut, when not
// nil, runs when the connection breaks, before a call can see the break.
func newConn(c net.Conn, onDeliver func(Deliver), onShut func(*Conn)) *Conn {
	cn := &Conn{
		conn:       c,
		onDeliver:  onDeliver,
		onShut:     onShut,
		waiting:    map[int64]chan reply{},
		done:       make(chan struct{}),
		queued:     make(chan struct{}, 1),
		released:   make(chan struct{}),
		dispatched: make(chan struct{}),
	}
	go cn.read()
	go cn.dispatch()
	return cn
}

// release starts handing the held deliver notifications to onDeliver.
func (c *Conn) release() { c.releaseOne.Do(func() { close(c.released) }) }

// Done is closed when the connection is broken or closed.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Close closes the connection. Calls that wait fail with ErrUnreachable.
func (c *Conn) Close() error {
	err := c.conn.Close()
	c.shut()
	return err
}

func (c *Conn) shut() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.broken.Store(true)
	if c.onShut != nil {
		c.onShut(c)
	}
	for id, ch := range c.waiting {
		ch <- reply{err: ErrUnreachable}
		delete(c.waiting, id)
	}
	c.mu.Unlock()
	close(c.done)
}

// Call sends a request and decodes the result into result, when not nil. A
// daemon error is a *RPCError. A broken connection is ErrUnreachable, also for
// a call that was waiting: the call is never sent again.
func (c *Conn) Call(ctx context.Context, method string, params, result any) error {
	_, err := c.call(ctx, method, params, result)
	return err
}

// call is Call, and tells whether the request may have reached the daemon.
func (c *Conn) call(ctx context.Context, method string, params, result any) (sent bool, err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false, ErrUnreachable
	}
	c.next++
	id := c.next
	ch := make(chan reply, 1)
	c.waiting[id] = ch
	c.mu.Unlock()

	if sent, err := c.write(id, method, params); err != nil {
		c.forget(id)
		return sent, err
	}
	select {
	case r := <-ch:
		if r.err != nil {
			return true, r.err
		}
		if result == nil {
			return true, nil
		}
		return true, json.Unmarshal(r.result, result)
	case <-ctx.Done():
		c.forget(id)
		return true, ctx.Err()
	}
}

func (c *Conn) forget(id int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.waiting, id)
}

func (c *Conn) write(id int64, method string, params any) (sent bool, err error) {
	req := struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int64  `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params,omitempty"`
	}{"2.0", id, method, params}
	line, err := json.Marshal(req)
	if err != nil {
		return false, err
	}
	if len(line) > daemon.MaxLine {
		return false, fmt.Errorf("the request has more than %d bytes", daemon.MaxLine)
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if n, err := c.conn.Write(append(line, '\n')); err != nil {
		c.Close()
		return n > 0, ErrUnreachable
	}
	return true, nil
}

// read reads every line until the connection breaks. A line over the cap or
// a line that is not JSON-RPC breaks the connection.
func (c *Conn) read() {
	defer c.Close()
	r := bufio.NewReaderSize(c.conn, daemon.MaxLine+2)
	for {
		line, err := r.ReadSlice('\n')
		line = bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r"))
		if errors.Is(err, bufio.ErrBufferFull) || len(line) > daemon.MaxLine {
			return
		}
		if len(bytes.TrimSpace(line)) > 0 && !c.handle(line) {
			return
		}
		if err != nil {
			return
		}
	}
}

func (c *Conn) handle(line []byte) bool {
	var in incoming
	if err := json.Unmarshal(line, &in); err != nil {
		return false
	}
	if in.Method != "" {
		if in.Method == "deliver" && in.Params.Kind() == '{' {
			var d Deliver
			if err := json.Unmarshal(in.Params, &d); err != nil {
				return false
			}
			c.enqueue(d)
		}
		return true
	}
	if len(in.ID) == 0 {
		return false
	}
	// An error with a null id answers no call; the daemon closes the
	// connection after it.
	var id int64
	json.Unmarshal(in.ID, &id)
	c.mu.Lock()
	ch, ok := c.waiting[id]
	delete(c.waiting, id)
	c.mu.Unlock()
	if ok {
		if in.Error != nil {
			ch <- reply{err: in.Error}
		} else {
			ch <- reply{result: in.Result}
		}
	}
	return true
}

func (c *Conn) enqueue(d Deliver) {
	if c.onDeliver == nil {
		return
	}
	c.qmu.Lock()
	c.queue = append(c.queue, d)
	c.qmu.Unlock()
	select {
	case c.queued <- struct{}{}:
	default:
	}
}

// dispatch hands the queue to onDeliver after release, until the connection
// breaks and the queue is empty. Every holder of a held connection releases
// it, also after a failed register.
func (c *Conn) dispatch() {
	defer close(c.dispatched)
	<-c.released
	for {
		select {
		case <-c.queued:
			c.drain()
		case <-c.done:
			c.drain()
			return
		}
	}
}

func (c *Conn) drain() {
	c.qmu.Lock()
	q := c.queue
	c.queue = nil
	c.qmu.Unlock()
	for _, d := range q {
		c.onDeliver(d)
	}
}

// RegisterParams is the params of register without version, which is always
// the client's own.
type RegisterParams struct {
	Harness string
	Cwd     string
	Name    string
	Topics  []string
}

// Registration is the result of register.
type Registration struct {
	Session  string   `json:"session"`
	Name     string   `json:"name"`
	Topics   []string `json:"topics"`
	Protocol string   `json:"protocol"`
	Version  string   `json:"version"`
}

// ParamsFromEnv is the register params of a session of harness in cwd, with
// AGENT_COURIER_NAME as name and AGENT_COURIER_TOPICS, split by commas, as
// topics when they are set.
func ParamsFromEnv(harness, cwd string) RegisterParams {
	p := RegisterParams{Harness: harness, Cwd: cwd, Name: strings.TrimSpace(os.Getenv("AGENT_COURIER_NAME"))}
	for t := range strings.SplitSeq(os.Getenv("AGENT_COURIER_TOPICS"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			p.Topics = append(p.Topics, t)
		}
	}
	return p
}

// Register registers the session with the version of this client.
func (c *Conn) Register(ctx context.Context, p RegisterParams) (Registration, error) {
	params := struct {
		Harness string   `json:"harness"`
		Version string   `json:"version"`
		Cwd     string   `json:"cwd"`
		Name    string   `json:"name,omitempty"`
		Topics  []string `json:"topics,omitempty"`
	}{p.Harness, version.Version, p.Cwd, p.Name, p.Topics}
	var reg Registration
	err := c.Call(ctx, "register", params, &reg)
	return reg, err
}
