# agent-courier

Hand work between the agents of different people.

Two people run their own agent orchestrators on the same repository. Each orchestrator takes issues, delegates them to agents and closes them. When an issue closes, the other orchestrator does not know about it until a person tells it. agent-courier carries that signal from one orchestrator to the other.

## Design

- **The issue tracker holds the state.** Labels, assignees, comments and closed issues already record who does what. agent-courier carries events, not state. A receiver reconciles with the tracker on every event and at startup, so a lost event costs latency, not correctness.
- **NATS JetStream is the bus.** One durable consumer per orchestrator gives replay and acknowledgement. Clients connect over WebSocket, so the server needs only an HTTPS endpoint.
- **A Claude Code channel delivers the event.** An MCP server with the `claude/channel` capability subscribes to NATS and pushes each event into the running session of the orchestrator.
- **Events are data, never instructions.** The envelope names the sender. The server and the channel accept only the senders on an allowlist, because text from another person's agent lands in your session.
- **Silence is never consent.** No timeout takes an issue, merges a pull request or answers for a person.

## Status

Design phase. Nothing here is usable yet.

## Credits

The approach to delivery into an agent session comes from [kvaps/courier](https://github.com/kvaps/courier), which carries questions between one agent and one person.

## License

Apache-2.0. See [LICENSE](LICENSE).
