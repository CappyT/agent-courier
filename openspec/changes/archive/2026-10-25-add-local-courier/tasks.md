# Tasks

## 1. Scaffolding (#13)

- [x] 1.1 Create the Go module `github.com/CappyT/agent-courier` with the layout of design.md and a `Version` constant, and verify `go build ./...` succeeds
- [x] 1.2 Add `.github/workflows/ci.yml` that runs `gofmt -l`, `go vet ./...` and `go test -race ./...`, and verify the workflow passes on a pushed commit

## 2. Spike: OpenCode v2 push (#14)

- [x] 2.1 On the current OpenCode v2 release, find how a plugin learns the id of its session and verify it with a plugin that logs the id
- [x] 2.2 Measure what `POST /api/session/{id}/synthetic` with `delivery: "steer"` does on an idle session and during a running tool call, and verify both with a recorded session
- [x] 2.3 Record the results and the chosen push call in design.md, and verify `openspec validate add-local-courier` passes

## 3. Message model and rendering (#15)

- [x] 3.1 Implement the message model, field validation and kinds of the `messaging` spec, and verify a table test covers every scenario of "Message kinds" and "Message fields"
- [x] 3.2 Implement ULID ids, and verify a test shows ids sort by creation time and do not collide in 1e6 generations
- [x] 3.3 Implement the rendering of the `inbound-safety` spec, and verify a test covers every scenario of "Visible characters only", "Markup cannot break out" and "Size caps"

## 4. Storage (#16)

- [x] 4.1 Implement the SQLite store with the tables of design.md, and verify a test stores, lists and acknowledges messages
- [x] 4.2 Implement retention cleanup, and verify a test with an injected clock deletes 31-day-old messages and forgets agents idle for 31 days
- [x] 4.3 Verify with a test that a pending message survives closing and reopening the store

## 5. Daemon (#17: 5.1, 5.2, 5.6; #23: 5.3, 5.4; #24: 5.5)

- [x] 5.1 Implement the socket in a 0700 directory, the `SO_PEERCRED` check and the line-delimited JSON-RPC server with a line cap, and verify a test refuses an oversized line
- [x] 5.2 Implement `register` with default names, suffixes and topics from remotes, and verify tests cover the scenarios of "Agent identity" and "Topics"
- [x] 5.3 Implement `send`, delivery scope, offline delivery, `pending`, `ack` and redelivery, and verify tests cover each scenario of those `messaging` requirements
- [x] 5.4 Implement presence and `agents`, and verify tests cover the scenarios of "Presence"
- [x] 5.5 Implement push budgets and pending caps, and verify tests cover each scenario of the budget and cap requirements in `inbound-safety`
- [x] 5.6 Write the exact params and results of every JSON-RPC method and of the `deliver` notification in design.md "Socket and wire protocol", and verify `openspec validate --all --strict` passes

## 6. Client and lifecycle (#18)

- [x] 6.1 Implement the socket client with autostart under `flock` and the detached `agent-courier daemon`, and verify a test starts three clients at once and finds exactly one daemon
- [x] 6.2 Implement idle exit after 60 seconds with a configurable grace period, and verify tests cover both scenarios of "Idle exit"
- [x] 6.3 Verify with a test that a pending message reaches a session after the daemon exits and starts again

## 7. Claude Code client (#19)

- [x] 7.1 Implement `agent-courier mcp` with the four tools on `mcp-go`, logs on stderr, and verify a test drives it over stdio the way Claude Code does
- [x] 7.2 Implement the `claude/channel` capability and push with the legacy protocol versions, and verify a test receives a channel notification with the message attributes
- [x] 7.3 Write the agent protocol text of `inbound-safety`, served by the daemon on `register`, and verify a test checks each rule appears in it
- [x] 7.4 Add `.claude-plugin/` marketplace and plugin manifests and the `courier-send` skill, and verify `claude plugin validate .` passes and the version test fails on a mismatch
- [x] 7.5 Write the Claude Code part of README.md (install, channel flag or allowlist, tools), and verify the commands run as written on a clean shell

## 8. OpenCode client (#20)

- [x] 8.1 Implement the plugin in `opencode/` with the four tools and one daemon connection per session, and verify a test with a fake daemon covers each tool
- [x] 8.2 Implement the push with the call chosen in 2.3, and verify on a live OpenCode session that an idle session starts a turn and a busy one receives the message at the next step
- [x] 8.3 Write the OpenCode part of README.md and the supported OpenCode versions, and verify the install steps on a clean OpenCode config

## 9. Integration (#21)

- [x] 9.1 On one machine, verify a round trip: Claude Code sends a `handoff` to an OpenCode agent, OpenCode replies with `done`, both acknowledge, and both pending lists are empty; record the evidence in the issue
- [x] 9.2 Write `docs/install.md` for agents: the steps an agent runs to install the binary and the plugin in Claude Code and in OpenCode, check the install and enable the push, linked from README.md; verify a fresh agent session follows it on a clean config for each harness
