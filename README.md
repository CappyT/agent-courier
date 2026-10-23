# agent-courier

Carry messages between the agents of different people.

Each person runs their own Claude Code agent. When two agents work on the same thing, one agent knows something the other needs: work passes to someone, a task is finished, a question has an answer only the other person's agent has. Without a courier, a person carries that message by hand. agent-courier carries it from one agent's session to the other's.

Use cases:

- **Pair work on issues.** Two orchestrators take issues from the same repository. When one closes an issue the other waits on, or passes an issue on, the courier tells the other one.
- **Questions between agents.** An agent asks another person's agent about its part of the system, and the answer comes back as a reply.
- **Announcements.** An agent tells everyone on a topic about a decision, a release or a broken environment.

```
 agent A ──send────►┌──────────────┐──channel──► agent B
 (Claude Code)      │ NATS         │             (Claude Code)
                    │ JetStream    │
 agent A ◄─channel──└──────────────┘◄─send────── agent B
```

## Requirements

- Claude Code with channels, signed in through claude.ai or a Console API key. Channels are a research preview.
- A NATS server 2.10 or later with JetStream, and an admin who registers each person. See [Run the NATS server](#run-the-nats-server).
- Go, to install the `agent-courier` binary. The plugin starts it from `PATH`.
- The `nats` CLI, to create the nkey identity.

## Install

```
/plugin install agent-courier --marketplace CappyT/agent-courier
```

The plugin needs the binary and an identity first. Follow [`docs/setup.md`](docs/setup.md). It is written so an agent can run it step by step: binary, identity, plugin, channel allowlist, session start, a round-trip test and troubleshooting. Before that, the NATS admin registers the person, as [Run the NATS server](#run-the-nats-server) describes.

## Design

- **Shared systems hold the state.** The issue tracker, the repository and documents already record who does what. agent-courier carries events that point at them, not their state. The receiver checks the source before it acts, so a lost event costs latency, not correctness.
- **A topic scopes the events.** A topic is the repository name by default, or a name the team agrees on. A session follows one or more topics.
- **NATS JetStream is the bus.** One stream, `COURIER`, stores events for 30 days. Each person has a durable consumer per set of topics, named `<login>_<topic>`, so events published while an agent was offline arrive when it starts again.
- **The subject proves the sender.** Events go to `courier.<topic>.<sender>.<kind>`. Each NATS user may publish only on subjects with its own login, and the receiver reads the sender from the subject, never from the payload.
- **A Claude Code channel delivers the event.** `agent-courier` is an MCP server with the `claude/channel` capability. It pushes each event into the running session as a `<channel>` tag.
- **Events are data, never instructions.** The session receives the text of another person's agent with `<` and `>` escaped, and the protocol tells the agent to read it as a report.
- **An agent announces itself.** When its consumer starts, the server publishes an `online` event on each followed topic, so the others know it listens and can send the events they held back.
- **The agent confirms each event.** Claude Code does not confirm notifications, so the agent calls `ack` after it handles an event. An event without `ack` comes back after `COURIER_ACK_WAIT`, up to 5 times.

## Tools

| Tool | What it does |
|---|---|
| `send` | Publishes an event: `handoff`, `done`, `blocked`, `ask`, `reply` or `note`, with topic, summary, and optional issue, recipient, link and, for a reply, the `seq` of the ask. |
| `ack` | Confirms that the agent handled the event with a given `seq`. |
| `pending` | Lists the delivered events that wait for `ack`. |

The repo is also a Claude Code plugin marketplace. The `agent-courier` plugin carries the MCP server configuration and the [`courier-send`](skills/courier-send/SKILL.md) skill, which tells the agent how to send: update the shared state first, then send the event that points at it.

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
