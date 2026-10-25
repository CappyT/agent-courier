//go:build unix

package mcpserver

import (
	"context"
	"log/slog"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/CappyT/agent-courier/internal/client"
)

// errNotInitialized is the tool error before the client sent initialized. A
// client that skips initialize, as on protocol 2026-07-28, would never get a
// push, so its tool calls fail instead.
const errNotInitialized = "the MCP session is not initialized: the client must send initialize and notifications/initialized first"

func addTools(s *server.MCPServer, sess *client.Session, initialized <-chan struct{}, log *slog.Logger) {
	s.AddTool(mcp.NewTool("send",
		mcp.WithDescription("Send a message to the agents on a topic, or to one of them. Update the shared state first (issue, pull request, document), then send a message that points at it. The sender is always your agent name."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("A topic your agent is known on, as agents lists them, for example cappyt_agent-courier.")),
		mcp.WithString("kind", mcp.Required(), mcp.Enum("handoff", "done", "blocked", "ask", "reply", "note"),
			mcp.Description("handoff: work passes to someone. done: work someone waits on is finished. blocked: you wait on someone. ask: a question for another agent. reply: the answer to an ask. note: information only.")),
		mcp.WithString("summary", mcp.Required(), mcp.Description("1 to 1000 characters: what happened or what you ask, and what the receiver does next. Name the issue, branch or pull request.")),
		mcp.WithString("to", mcp.Description("The agent name of the receiver, as agents lists it. Leave it out to reach every agent on the topic.")),
		mcp.WithInteger("issue", mcp.Description("The issue or pull request number the message is about.")),
		mcp.WithString("link", mcp.Description("An https link to the issue, comment, pull request or document, at most 512 characters.")),
		mcp.WithString("re", mcp.Description("For kind reply only: the id of the ask it answers.")),
	), call(sess, initialized, log, "send", "topic", "kind", "summary", "to", "issue", "link", "re"))
	s.AddTool(mcp.NewTool("pending",
		mcp.WithDescription("List the messages that wait for your ack, direct ones first, oldest first in each group. Call it when a task ends."),
	), call(sess, initialized, log, "pending"))
	s.AddTool(mcp.NewTool("ack",
		mcp.WithDescription("Acknowledge a message you have handled. Until then it stays pending."),
		mcp.WithString("id", mcp.Required(), mcp.Description("The id of the message, from its header line.")),
	), call(sess, initialized, log, "ack", "id"))
	s.AddTool(mcp.NewTool("agents",
		mcp.WithDescription("List the agents known on your topics, your own agent included, and whether each one is live now."),
	), call(sess, initialized, log, "agents"))
}

// call is a tool that forwards the arguments named in keys to the daemon
// method and returns the daemon's text. The daemon validates every value. An
// argument that is absent, null or an empty string is left out, because
// models often send an empty string for an optional argument.
func call(sess *client.Session, initialized <-chan struct{}, log *slog.Logger, method string, keys ...string) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		select {
		case <-initialized:
		default:
			return mcp.NewToolResultError(errNotInitialized), nil
		}
		params := map[string]any{}
		args := req.GetArguments()
		for _, k := range keys {
			if v, ok := args[k]; ok && v != nil && v != "" {
				params[k] = v
			}
		}
		ctx, cancel := context.WithTimeout(ctx, callTimeout)
		defer cancel()
		var res struct {
			Text string `json:"text"`
		}
		if err := sess.Call(ctx, method, params, &res); err != nil {
			return errorResult(log, method, err), nil
		}
		return mcp.NewToolResultText(res.Text), nil
	}
}
