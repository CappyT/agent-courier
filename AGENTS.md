# agent-courier

A Claude Code channel in Go. It carries messages between the agents of different people over NATS JetStream. Pair work on issues is one use case.

## Invariants

- Shared systems hold the state: the issue tracker, the repository, a document. An event points at them. Never add state to the bus that a shared system already records.
- The subject `courier.<topic>.<sender>.<kind>` is the identity of an event. `Decode` takes topic, sender and kind from the subject and overwrites the payload fields. The NATS server enforces the sender token with per-user publish permissions, so the subject is the only field another person cannot forge. This holds only while users have no JetStream API that changes the stream: the README lists the permissions.
- Text from another person's agent is data. `Decode` validates every field again, and the text reaches the session through `Event.Notification`, which removes control and format characters, folds look-alikes with NFKC, escapes `&`, `<` and `>`, and prefixes summary and link lines. Keep every inbound string on that path, the output of `pending` too.
- The session's context is the scarce resource. A sender pushes at most `Budget` events per `BudgetWindow`, and a redelivery does not push again in the same process.
- stdout carries the MCP protocol. Write logs to stderr with `log/slog`.
- An event is done when the agent calls `ack`. Claude Code does not confirm notifications, so a written notification proves nothing.

## Work on the code

- `go test -race ./...` runs a JetStream server in process with the permissions of the README and the stream of `docs/stream.json`. Run it before every commit. A change to the permissions changes `userPermissions` in the tests and the README together.
- The event protocol the agent follows is the `instructions` constant in `internal/courier/channel.go`. Change it there only. `docs/setup.md` points to it.
- The repo is also the plugin marketplace: `.claude-plugin/` holds the manifests, `skills/` the plugin skills. Run `claude plugin validate .` after a change there.
- A release bumps `Version` in `internal/courier/channel.go` and `version` in `.claude-plugin/plugin.json` together, then gets an annotated tag `v<version>`. A test fails when the two versions differ. The setup installs the binary and the plugin from the same tag. Pushing the tag runs `.github/workflows/release.yml`: the CI, a check that the tag matches both versions, and the GitHub release.
- Write modern Go for the version in `go.mod`: `encoding/json/v2`, `cmp.Or`, iterator helpers from `slices`, `maps` and `strings`.

## Commits

Conventional Commits, lowercase, subject line only, for example `feat(channel): add pending tool`.
