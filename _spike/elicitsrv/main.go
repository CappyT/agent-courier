// Command elicitsrv is a raw stdio MCP server that logs the client's
// initialize request and sends elicitation/create twice: once unsolicited
// after notifications/initialized (the consent-before-connect case), once
// inside a tools/call. Every line in and out goes to $ELICIT_LOG.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type msg struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

var (
	out     = bufio.NewWriter(os.Stdout)
	outMu   sync.Mutex
	logger  *log.Logger
	pending = map[string]any{} // elicitation id -> tools/call id waiting on it
	pmu     sync.Mutex
)

func send(v any) {
	b, _ := json.Marshal(v)
	logger.Printf("OUT %s", b)
	outMu.Lock()
	defer outMu.Unlock()
	out.Write(b)
	out.WriteByte('\n')
	out.Flush()
}

func elicit(id, why string) {
	send(map[string]any{"jsonrpc": "2.0", "id": id, "method": "elicitation/create", "params": map[string]any{
		"message": "agent-courier: join the swarm of acme/app on wss://courier.example (" + why + ")?",
		"requestedSchema": map[string]any{
			"type":       "object",
			"properties": map[string]any{"join": map[string]any{"type": "boolean", "title": "Join"}},
			"required":   []string{"join"},
		},
	}})
}

func main() {
	f, err := os.OpenFile(os.Getenv("ELICIT_LOG"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	logger = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<24)
	for in.Scan() {
		line := in.Bytes()
		logger.Printf("IN  %s", line)
		var m msg
		if err := json.Unmarshal(line, &m); err != nil {
			continue
		}
		switch {
		case m.Method == "initialize":
			var p struct {
				ProtocolVersion string `json:"protocolVersion"`
			}
			_ = json.Unmarshal(m.Params, &p)
			proto := p.ProtocolVersion
			if v := os.Getenv("ELICIT_PROTO"); v != "" {
				proto = v
			}
			send(map[string]any{"jsonrpc": "2.0", "id": m.ID, "result": map[string]any{
				"protocolVersion": proto,
				"capabilities":    map[string]any{"tools": map[string]any{}, "experimental": map[string]any{"claude/channel": map[string]any{}}},
				"serverInfo":      map[string]any{"name": "elicitsrv", "version": "0.0.1"},
				"instructions":    "Test server. Call consent_probe when asked.",
			}})
		case m.Method == "notifications/initialized":
			go func() { time.Sleep(500 * time.Millisecond); elicit("unsolicited-1", "unsolicited after initialized") }()
		case m.Method == "tools/list":
			send(map[string]any{"jsonrpc": "2.0", "id": m.ID, "result": map[string]any{"tools": []any{map[string]any{
				"name": "consent_probe", "description": "Asks the user to consent through MCP elicitation and returns the answer.",
				"inputSchema": map[string]any{"type": "object", "properties": map[string]any{}},
			}}}})
		case m.Method == "tools/call":
			eid := fmt.Sprintf("in-call-%v", m.ID)
			pmu.Lock()
			pending[eid] = m.ID
			pmu.Unlock()
			elicit(eid, "inside tools/call")
		case m.Method == "" && m.ID != nil:
			// A response to one of our elicitation requests.
			eid := fmt.Sprint(m.ID)
			logger.Printf("ELICIT RESPONSE %s result=%s error=%s", eid, m.Result, m.Error)
			pmu.Lock()
			callID, ok := pending[eid]
			delete(pending, eid)
			pmu.Unlock()
			if ok {
				text := fmt.Sprintf("elicitation result=%s error=%s", m.Result, m.Error)
				send(map[string]any{"jsonrpc": "2.0", "id": callID, "result": map[string]any{"content": []any{map[string]any{"type": "text", "text": text}}}})
			}
		case m.ID != nil:
			send(map[string]any{"jsonrpc": "2.0", "id": m.ID, "error": map[string]any{"code": -32601, "message": "method not found"}})
		}
	}
}
