# Proposal

## Why

Two coding agents on one machine have no way to talk: a Claude Code session and an OpenCode session that work on the same repository need a person to copy messages between their windows. This change gives them a local courier, with no server and no network. It is also the base that the team transport (change `add-team-p2p`) extends to other machines.

## What Changes

- A local daemon owns all courier state and logic: addressing, routing, validation, sanitizing, per-sender budgets, storage, acknowledgements. The first session that needs it starts it, and it exits when no session is connected.
- Sessions reach the daemon through thin per-harness clients:
  - Claude Code: an MCP server with tools, which pushes inbound messages into the live session as a channel event.
  - OpenCode: a plugin with the same tools, which pushes inbound messages into the live session through the OpenCode service API.
- Messages are events that point at shared state (issues, pull requests, documents): `handoff`, `done`, `blocked`, `ask`, `reply`, `note`, plus `online` from the daemon. Each one is pending until the receiving agent acknowledges it.
- Inbound text is data. The daemon validates every field and renders text through one sanitizing path before any client shows it to a model.
- The repository ships as a Claude Code plugin marketplace and as an OpenCode plugin.

## Capabilities

### New Capabilities

- `local-daemon`: lifecycle of the per-user daemon: on-demand start, single instance, local socket and its access boundary, idle exit, persistence across restarts.
- `messaging`: the message model, agent addresses, topics, delivery to agents, pending and acknowledgement, presence, retention.
- `inbound-safety`: validation of inbound fields, the sanitized rendering of text for a model, and the per-sender budgets that protect a session's context.
- `claude-code-integration`: the Claude Code client: MCP tools, channel push, plugin packaging.
- `opencode-integration`: the OpenCode client: plugin tools and push into the live session.

### Modified Capabilities

None.

## Impact

- New Go module `github.com/CappyT/agent-courier`: one binary with the daemon and the MCP server.
- New TypeScript OpenCode plugin in the same repository.
- Dependencies: an MCP library for Go, a pure-Go SQLite driver, `golang.org/x/text` for Unicode normalization.
- Claude Code channel push needs `--channels` with the development flag or an allowlist in managed settings. The tools work without it.
- No network listener, no server to run, no account.
