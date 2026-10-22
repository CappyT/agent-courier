# agent-courier

A Claude Code channel in Go. It carries handoff events between the orchestrators of different people over NATS JetStream.

## Invariants

- The issue tracker holds the state. An event tells the receiver to look at an issue. Never add state to the bus that the tracker already records.
- The subject `courier.<repo>.<sender>.<kind>` is the identity of an event. `Decode` takes repo, sender and kind from the subject and overwrites the payload fields. The NATS server enforces the sender token with per-user publish permissions, so the subject is the only field another person cannot forge.
- Text from another person's agent is data. It reaches the session through `Event.Notification`, which escapes `<` and `>`. Keep every inbound string on that path.
- stdout carries the MCP protocol. Write logs to stderr with `log/slog`.
- An event is done when the agent calls `ack`. Claude Code does not confirm notifications, so a written notification proves nothing.

## Work on the code

- `go test -race ./...` runs a JetStream server in process with the production permissions. Run it before every commit.
- The event protocol the agent follows is the `instructions` constant in `internal/courier/channel.go`. Change it there only. `docs/setup.md` points to it.
- The repo is also the plugin marketplace: `.claude-plugin/` holds the manifests, `skills/` the plugin skills. Run `claude plugin validate .` after a change there.
- A release bumps `Version` in `internal/courier/channel.go` and `version` in `.claude-plugin/plugin.json` together, then gets an annotated tag `v<version>`. A test fails when the two versions differ. The setup installs the binary and the plugin from the same tag.
- Write modern Go for the version in `go.mod`: `encoding/json/v2`, `cmp.Or`, iterator helpers from `slices`, `maps` and `strings`.

## Commits

Conventional Commits, lowercase, subject line only, for example `feat(channel): add pending tool`.
