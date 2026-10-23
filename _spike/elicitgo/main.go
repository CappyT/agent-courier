// Command elicitgo is the mcp-go v1.3.0 variant of elicitsrv, built like
// the agent-courier channel. It tries an unsolicited RequestElicitation
// after notifications/initialized, and a consent_probe tool that asks
// through a multi round-trip input request. ELICIT_LEGACY=1 adds
// WithLegacyServerInitiatedRequests; ELICIT_COURIER_PROTO=1 offers only the
// legacy revisions, as internal/courier.ServeStdio does. Wire traffic goes to $ELICIT_LOG.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func main() {
	f, err := os.OpenFile(os.Getenv("ELICIT_LOG"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	logger := log.New(f, "", log.LstdFlags|log.Lmicroseconds)
	params := mcp.ElicitationParams{
		Message: "agent-courier (mcp-go): join the swarm of acme/app?",
		RequestedSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{"join": map[string]any{"type": "boolean", "title": "Join"}},
			"required":   []string{"join"},
		},
	}
	opts := []server.ServerOption{
		server.WithExperimental(map[string]any{"claude/channel": map[string]any{}}),
		server.WithToolCapabilities(false),
		server.WithElicitation(),
	}
	if os.Getenv("ELICIT_LEGACY") != "" {
		opts = append(opts, server.WithLegacyServerInitiatedRequests())
	}
	var s *server.MCPServer
	hooks := &server.Hooks{}
	hooks.AddAfterInitialize(func(_ context.Context, _ any, req *mcp.InitializeRequest, res *mcp.InitializeResult) {
		logger.Printf("INITIALIZED client proto=%s server proto=%s client elicitation=%+v", req.Params.ProtocolVersion, res.ProtocolVersion, req.Params.Capabilities.Elicitation)
	})
	opts = append(opts, server.WithHooks(hooks))
	s = server.NewMCPServer("elicitgo", "0.0.1", opts...)
	s.AddNotificationHandler(string(mcp.MethodNotificationInitialized), func(ctx context.Context, _ mcp.JSONRPCNotification) {
		go func() {
			time.Sleep(500 * time.Millisecond)
			c, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			res, err := s.RequestElicitation(c, mcp.ElicitationRequest{Params: params})
			logger.Printf("UNSOLICITED proto=%s result=%+v err=%v", server.RequestProtocolVersion(ctx), res, err)
		}()
	})
	s.AddTool(mcp.NewTool("consent_probe", mcp.WithDescription("Asks the user to consent through MCP elicitation and returns the answer.")),
		func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if r := server.ElicitationResponse(req.Params.InputResponses, "consent"); r != nil {
				logger.Printf("MRTR answer proto=%s action=%s content=%v", server.RequestProtocolVersion(ctx), r.Action, r.Content)
				return mcp.NewToolResultText(fmt.Sprintf("elicitation action=%s content=%v", r.Action, r.Content)), nil
			}
			logger.Printf("MRTR asking proto=%s", server.RequestProtocolVersion(ctx))
			return server.NewInputRequestBuilder("").Elicit("consent", params).ToolResult(), nil
		})
	in := io.TeeReader(os.Stdin, prefixed{logger, "IN  "})
	out := io.MultiWriter(os.Stdout, prefixed{logger, "OUT "})
	stdio := server.NewStdioServer(s)
	if os.Getenv("ELICIT_COURIER_PROTO") != "" {
		// What internal/courier.ServeStdio does: offer only legacy revisions.
		stdio.SetContextFunc(func(ctx context.Context) context.Context {
			return server.WithSupportedProtocolVersions(ctx, mcp.LegacyProtocolVersions())
		})
	}
	if err := stdio.Listen(context.Background(), in, out); err != nil {
		logger.Printf("listen: %v", err)
	}
}

type prefixed struct {
	l *log.Logger
	p string
}

func (w prefixed) Write(b []byte) (int, error) {
	w.l.Printf("%s%s", w.p, b)
	return len(b), nil
}
