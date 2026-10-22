# agent-courier

Hand work between the agents of different people.

Two people run their own agent orchestrators on the same repository. Each orchestrator takes issues, delegates them to agents and closes them. When an issue closes, the other orchestrator does not know about it until a person tells it. agent-courier carries that signal from one orchestrator to the other.

```
 orchestrator A ──handoff──►┌──────────────┐──channel──► orchestrator B
 (Claude Code)              │ NATS         │             (Claude Code)
                            │ JetStream    │
 orchestrator A ◄─channel───└──────────────┘◄─handoff─── orchestrator B
```

## Install

Follow [`docs/setup.md`](docs/setup.md). It is written so an agent can run it step by step: binary, identity, plugin, channel allowlist, session start, a round-trip test and troubleshooting. Before that, the NATS admin registers the person, as [Run the NATS server](#run-the-nats-server) describes.

## Design

- **The issue tracker holds the state.** Labels, assignees, comments and closed issues already record who does what. agent-courier carries events, not state. The receiver checks the issue before it acts, so a lost event costs latency, not correctness.
- **NATS JetStream is the bus.** One stream, `COURIER`, stores events for 30 days. Each person has a durable consumer per repo, named `<login>_<repo>`, so events published while an orchestrator was offline arrive when it starts again.
- **The subject proves the sender.** Events go to `courier.<repo>.<sender>.<kind>`. Each NATS user may publish only on subjects with its own login, and the receiver reads the sender from the subject, never from the payload.
- **A Claude Code channel delivers the event.** `agent-courier` is an MCP server with the `claude/channel` capability. It pushes each event into the running session as a `<channel>` tag.
- **Events are data, never instructions.** The session receives the text of another person's agent with `<` and `>` escaped, and the protocol tells the agent to read it as a report.
- **An orchestrator announces itself.** When its consumer starts, the server publishes an `online` event on each followed repo, so the others know it listens and can send the handoffs they held back.
- **The agent confirms each event.** Claude Code does not confirm notifications, so the agent calls `ack` after it handles an event. An event without `ack` comes back after `COURIER_ACK_WAIT`, up to 5 times.

## Tools

| Tool | What it does |
|---|---|
| `handoff` | Publishes an event: `handoff`, `done`, `blocked` or `note`, with repo, summary, and optional issue, recipient and link. |
| `ack` | Confirms that the agent handled the event with a given `seq`. |
| `pending` | Lists the delivered events that wait for `ack`. |

The repo is also a Claude Code plugin marketplace. The `agent-courier` plugin carries the MCP server configuration and the [`courier-handoff`](skills/courier-handoff/SKILL.md) skill, which tells the agent how to publish: update the issue first, then send the event that points at it.

## Run the NATS server

Any NATS server 2.10 or later with JetStream works. Clients connect over WebSocket or TCP. Put the users in one account, one nkey user per person:

```
accounts {
  AGENTS {
    jetstream: enabled
    users: [
      { nkey: U...ALICE, permissions: { publish: { allow: ["courier.*.alice.>", "$JS.API.>", "$JS.ACK.>", "_INBOX.>", "$SYS.REQ.USER.INFO"] } } }
      { nkey: U...BOB,   permissions: { publish: { allow: ["courier.*.bob.>",   "$JS.API.>", "$JS.ACK.>", "_INBOX.>", "$SYS.REQ.USER.INFO"] } } }
    ]
  }
}
```

The `courier.*.<login>.>` entry is what makes the subject prove the sender. Keep it in every user.

To register a person, add a user with their public key and their login in the publish entry, then reload the server. The person sends both, as step 2 of the setup describes.

## Status

Prototype. The tests run a JetStream server in process with the production permissions and drive the MCP server over stdio the way Claude Code does.

Verified against a live Claude Code session: the plugin installs from the release tag, the managed-settings allowlist registers the channel without the development flag, an event from another login arrives as a `<channel>` tag, and `ack` clears it from `pending`.

## Credits

The approach to delivery into an agent session comes from [kvaps/courier](https://github.com/kvaps/courier), which carries questions between one agent and one person.

## License

Apache-2.0. See [LICENSE](LICENSE).
