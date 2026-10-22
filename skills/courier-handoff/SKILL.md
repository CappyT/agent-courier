---
name: courier-handoff
description: Publish an agent-courier event to another person's orchestrator. Use when you pass an issue to someone, close an issue someone waits on, or block on someone.
---

# courier-handoff

An event is a pointer: it tells the receiver to look at an issue. The issue holds the state, so the issue changes first and the event follows. The agent-courier server sends the inbound protocol (event kinds, `ack`) with its own instructions.

## Steps

1. **Tracker first.** Bring the issue to the state the event announces, with `gh` and the repo's issue rules:
   - handoff: the receiver is the assignee, and a comment names the branch, the pull request, what is done and the open points.
   - done: the issue is closed with the commit hashes.
   - blocked: a comment names the person you wait on and the exact question.

   Done when a person who reads only the issue knows the next action.
2. **Pick the receiver.** `to` is the receiver's courier login: take it from the `sender` of an earlier event, or ask the user. Leave `to` empty when everyone who follows the repo can act.
3. **Write the summary.** A few lines: what happened, what the receiver does next, the branch and pull request names. Point at the issue for the detail. Pass `issue` and set `url` to the comment from step 1.
4. **Publish** with the `handoff` tool. `repo` is the repository name without owner. Done when the tool returns `published courier.<repo>.<login>.<kind>, seq <n>`.
5. **Report** to the user: kind, receiver, issue and seq.

## Delivery

- A receiver gets every event published after its first connection, also while it is offline, for 30 days. An event published before the receiver ever connected never reaches it: hold the handoff until an `online` event from that person arrives, and tell the user.
- A tool error that names a permission means the NATS server refused your login on that subject. Report it to the user.
