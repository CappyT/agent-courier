---
name: courier-send
description: Send an agent-courier message to the agent of another person. Use when you pass work to someone, finish work someone waits on, block on someone, need an answer from another person's agent, or answer an ask.
---

# courier-send

A message points at shared state, it does not hold it. When the message is about tracked work, the shared system changes first and the message follows. The agent-courier server sends the inbound protocol (event kinds, `ack`) with its own instructions.

## Steps

1. **Pick the kind.**
   - handoff: work passes to someone.
   - done: work someone waits on is finished.
   - blocked: you wait on someone.
   - ask: you need an answer from another person's agent.
   - reply: you answer an ask. `re` is the `seq` of the ask.
   - note: information only.
2. **Shared state first.** When the message is about an issue, bring the issue to the state the message announces, with `gh` and the repo's issue rules:
   - handoff: the receiver is the assignee, and a comment names the branch, the pull request, what is done and the open points.
   - done: the issue is closed with the commit hashes.
   - blocked: a comment names the person you wait on and the exact question.

   Done when a person who reads only the issue knows the next action. For a message about something else, such as a document or a decision, link the place that records it.
3. **Pick the topic and the receiver.** `topic` is the repository name without owner, or a name the team agreed on. `to` is the receiver's courier login: take it from the `sender` of an earlier event, or ask the user. Leave `to` empty when everyone who follows the topic can act.
4. **Write the summary.** A few lines: what happened or what you ask, and what the receiver does next. Name branches and pull requests. Pass `issue` when the message is about one, and set `url` to the comment or document from step 2. A reply shares only what the user allows.
5. **Send** with the `send` tool. Done when the tool returns `published courier.<topic>.<login>.<kind>, seq <n>`.
6. **Report** to the user: kind, topic, receiver, issue when there is one, and seq.

## Delivery

- A receiver gets every message published after its first connection, also while it is offline, for 30 days. A message published before the receiver ever connected never reaches it: hold the message until an `online` event from that person arrives, and tell the user.
- A tool error that names a permission means the NATS server refused your login on that subject. Report it to the user.
- When a message you expect does not arrive, work down the Troubleshoot section of `${CLAUDE_PLUGIN_ROOT}/docs/setup.md`.
