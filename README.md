# agent-courier

agent-courier carries short messages between coding agents: between two harnesses on one machine, for example Claude Code and OpenCode, and between the agents of the people in a small team. A message points at shared state, such as an issue, a pull request or a document. It never holds that state.

One local daemon per user holds the messages. Each harness talks to it through a thin client: an MCP server for Claude Code, a plugin for OpenCode. The first client starts the daemon, and the daemon exits 60 seconds after its last client. Text from another agent reaches a session only as data, rendered and capped by the daemon.

To let a coding agent do the install, point it at [docs/install.md](docs/install.md).

## Claude Code

### Install the binary

The plugin starts `agent-courier mcp` from your `PATH`. Install the binary with Go:

```sh
go install github.com/CappyT/agent-courier/cmd/agent-courier@latest
```

Or download the binary of a [release](https://github.com/CappyT/agent-courier/releases) and put it in a directory on your `PATH`. Check it:

```sh
agent-courier version
```

The version of the binary must equal the version of the plugin.

### Install the plugin

This repository is a Claude Code plugin marketplace. Add it and install the plugin:

```sh
claude plugin marketplace add CappyT/agent-courier
claude plugin install agent-courier@agent-courier
```

The plugin carries the MCP server and the `courier-send` skill, which tells the agent how to send: update the shared state first, then send a message that points at it.

### Enable the channel

The client pushes each inbound message into the running session as a channel event. Claude Code accepts the push only from a channel that you opt in for the session. agent-courier is not on the Anthropic allowlist of channel plugins, so start Claude Code with the development flag:

```sh
claude --dangerously-load-development-channels plugin:agent-courier@agent-courier
```

In an organization, an admin can approve the plugin instead with the `allowedChannelPlugins` managed setting (it needs `channelsEnabled: true`):

```json
{
  "channelsEnabled": true,
  "allowedChannelPlugins": [
    { "marketplace": "agent-courier", "plugin": "agent-courier" }
  ]
}
```

Then start the session with `claude --channels plugin:agent-courier@agent-courier`.

Without the channel, the tools still work, but no push reaches the session: inbound messages wait in `pending` until the agent calls it.

### Tools

| Tool | What it does |
| --- | --- |
| `send` | Sends a `handoff`, `done`, `blocked`, `ask`, `reply` or `note` on a topic, to one agent or to every agent on the topic, with an optional issue number, `https` link and, for a reply, the id of the ask. |
| `pending` | Lists the messages that wait for an ack, direct ones first. |
| `ack` | Acknowledges a message by its id. A message is done only after its ack. |
| `agents` | Lists the agents known on the session's topics, and whether each one is live. |

The daemon names the agent `claude-<repo>` and uses the topic `<owner>_<repo>` of the repository's `upstream` or `origin` remote. Set `AGENT_COURIER_NAME` and `AGENT_COURIER_TOPICS` (comma-separated) in the environment of Claude Code to choose others.

## OpenCode

The plugin supports OpenCode 2.1.4, the version we tested. It uses the v2 plugin API (`ctx.session.synthetic`, the session `context` hook and `ctx.tool.transform`), so OpenCode 1.x cannot load it.

### Install the binary

The plugin starts `agent-courier bridge` for each session. Install the binary as for Claude Code, in a directory on the `PATH` of the OpenCode background service, or set `AGENT_COURIER_BIN` to its full path. The version of the binary must equal the version of the plugin.

The plugin runs in the background service (`opencode serve --service`), not in the TUI, so the service needs the variables. Set them with `opencode service set env`:

```sh
opencode service set env AGENT_COURIER_BIN /home/you/.local/bin/agent-courier
```

### Install the plugin

The plugin is the `opencode/` directory of this repository. It has no runtime dependencies, so it needs no `bun install`. Clone the repository at the tag of your binary:

```sh
git clone --branch "v$(agent-courier version)" https://github.com/CappyT/agent-courier.git ~/src/agent-courier
```

Add the directory to `plugins` in `~/.config/opencode/opencode.json`, with its absolute path:

```json
{
  "plugins": ["file:///home/you/src/agent-courier/opencode"]
}
```

Restart the background service: `opencode service restart`. A restart of the TUI alone does not load the plugin or a change to it.

To check the plugin, ask the service for its plugins in a project directory. The service wants Basic auth: the user is `opencode`, the password and the URL are in `~/.local/state/opencode/service.json`:

```sh
svc=~/.local/state/opencode/service.json
curl -s -u "opencode:$(jq -r .password "$svc")" \
  "$(jq -r .url "$svc")/api/plugin?location%5Bdirectory%5D=$PWD" | jq '.data[] | select(.id == "agent-courier")'
```

The entry has `"state": {"status": "active"}`. The first request after a restart can return before the service loads the directory: repeat it.

### Push

The plugin pushes each inbound message into the session with `ctx.session.synthetic` and `delivery: "steer"`. An idle session starts a turn. A busy session gets the message at its next step, for example when a tool call ends. The session text is the text that the daemon renders. Child sessions (subagents) get no connection and no push.

### Tools

The plugin adds the tools `send`, `pending`, `ack` and `agents` in the `courier` namespace. They take the same arguments as the Claude Code tools above.

The daemon names the agent `opencode-<repo>` and picks the topic as for Claude Code. To choose others, set `AGENT_COURIER_NAME` and `AGENT_COURIER_TOPICS` (topics split by commas) with `opencode service set env`, then restart the service. One service runs every OpenCode session, so `AGENT_COURIER_NAME` applies to all of them: while one session holds the name, the next one gets `<name>-2`, then `<name>-3`.

A session that runs nothing for 30 minutes closes its connection and frees its name. The next execution or tool call connects again. When the daemon is not reachable, the plugin tries again at the next execution or tool call, at most once per execution, and waits longer after each failure, up to one minute.

## Teams

A team connects the daemons of several people, so their agents can send each other messages. The machines connect directly when they can, and through a relay when they cannot.

### Commands

| Command | What it does |
| --- | --- |
| `agent-courier team create <team> --topic <topic> [--label <label>]` | Creates a team with this machine as its only member. Repeat `--topic` for more topics. A topic belongs to one team. |
| `agent-courier team invite <team>` | Prints an invite code on stdout. The notes go to stderr, so a pipe gets only the code. |
| `agent-courier team join "<code>" [--label <label>]` | Joins the team of an invite code. The code can be wrapped, spaced, or the whole output of `team invite`. |
| `agent-courier team list [<team>]` | Lists the teams of this machine and their members, or the members of one team. |
| `agent-courier team leave <team>` | Leaves the team. |

The label names the machine in the team. It defaults to the first part of the host name. A team name matches `^[a-z0-9][a-z0-9_-]{0,63}$` and a label matches `^[a-z0-9][a-z0-9-]{0,31}$`.

Example for two people. Alice creates the team and sends an invite code to Bob by any channel:

```sh
agent-courier team create courier --topic acme_widgets --label alice
agent-courier team invite courier
```

Bob joins with the code, then checks the members:

```sh
agent-courier team join "<code>" --label bob
agent-courier team list courier
```

Messages on the topic `acme_widgets` now reach the agents of both machines. Each agent shows as `<label>/<agent>` to the other side.

### Trust

A person joins a team only with an invite from a member. Once in, members are trusted: a member can invite others and can send any message that a member may send. Text from a member is still data, and the daemon still caps what it pushes into a session. To exclude a member, create a new team.

An invite works once, within 24 hours, while the daemon of the inviter runs and the inviter is in the team.

### A team lives in memory

A team exists only in the memory of the daemon. The machine key is also in memory and is never written to disk. A restart of the daemon, or `team leave`, ends the membership of that machine. To come back, join again with a new invite code.

The daemon stays running while the machine is in a team, also when no session is open. A clean stop of the daemon (a signal) leaves every team first.

### What the relay and the DHT see

Messages are encrypted from machine to machine (Noise). A relay forwards ciphertext. It cannot read or forge a message.

- The relay (`relay.cappyt.sh`, over QUIC and TCP) sees the peer ids and IP addresses of the machines that use it, which peers talk to each other, and the traffic volume.
- The public DHT stores the peer id and the IP addresses of a member. A machine that looks up a member sees the same data. The DHT carries no message.

agent-courier does not hide this metadata.

A machine that is in no team opens no network socket.

### Offline recipients

When the recipient is offline, the message waits in the queue of the sender's daemon. It arrives when both daemons are reachable. The queue is in memory and ends with a restart of the sender's daemon. The queue holds at most 1000 messages per member.

### Replies to an ask

An agent answers an `ask` with a `reply` only with text that its user approved. The plugin rule applies to remote asks as it does to local ones.
