# Install agent-courier

This guide is for a coding agent, Claude Code or OpenCode, that the user asked to install agent-courier. Do the steps in order. Do only the sections for the harnesses that the user named.

## What it installs

- The `agent-courier` binary. It holds the daemon, the MCP server for Claude Code and the bridge for the OpenCode plugin. The first client starts the daemon. No system service is installed.
- The Claude Code plugin `agent-courier@agent-courier`: the MCP server and the `courier-send` skill.
- The OpenCode plugin: the `opencode/` directory of this repository, loaded from a `file://` path.

## Rules for the agent

- Before you change a config file, show the user the exact change: the file path and the lines that you add. Write only what the user approved.
- Before you run a command that changes the user's harness (`claude plugin ...`, `opencode service set ...`, `opencode service restart`), show the command and wait for approval.
- You cannot restart the harness that runs you. When a step needs a restart, tell the user, and continue only after the user restarted it.
- Do not change other settings, hooks or plugins of the user.

## Prerequisites

Check each tool and tell the user what is missing:

```sh
go version     # Go 1.27 or later
git --version
claude --version   # only for Claude Code
opencode --version # only for OpenCode, 2.1.4 or a later 2.x
curl --version     # only for the OpenCode check
jq --version       # only for the OpenCode check
```

OpenCode 1.x cannot load the plugin.

## Install the binary

Install the latest release:

```sh
go install github.com/CappyT/agent-courier/cmd/agent-courier@latest
```

Go writes the binary to `$(go env GOBIN)`, or to `$(go env GOPATH)/bin` when `GOBIN` is empty. Find the full path and check the version:

```sh
command -v agent-courier || echo "$(go env GOPATH)/bin/agent-courier"
agent-courier version
```

If `command -v` finds nothing, the Go bin directory is not on `PATH`. Tell the user to add it to `PATH`, and use the full path in the next steps.

The output of `agent-courier version` must equal the version of the plugin. A daemon of another version refuses the client. The checks below compare the two versions. After an update, an old daemon exits 60 seconds after its last session closes.

## Claude Code

### Install the plugin

The repository is a Claude Code plugin marketplace. Show the user these two commands, then run them:

```sh
claude plugin marketplace add CappyT/agent-courier
claude plugin install agent-courier@agent-courier
```

Both commands write to the user settings of Claude Code. The plugin starts `agent-courier mcp` from `PATH`, so the binary must be on the `PATH` of Claude Code.

### Enable the push

The MCP server pushes each inbound message into the session as a channel event. Claude Code accepts the push only from a channel that is enabled at start. agent-courier is not on the Anthropic allowlist of channel plugins, so the session must start with the development flag:

```sh
claude --dangerously-load-development-channels plugin:agent-courier@agent-courier
```

The flag applies to one session. The user gives it at each start.

In an organization, an admin can approve the plugin in the managed settings instead. The admin owns that file. Show the admin this block, and do not write it yourself:

```json
{
  "channelsEnabled": true,
  "allowedChannelPlugins": [
    { "marketplace": "agent-courier", "plugin": "agent-courier" }
  ]
}
```

With that setting, the session starts with `claude --channels plugin:agent-courier@agent-courier`.

Without the push, the tools work, but inbound messages wait until the agent calls `pending`.

### Check the install

```sh
claude plugin list
claude mcp list
```

`claude plugin list` shows `agent-courier@agent-courier` as enabled, and its `Version` equals the output of `agent-courier version`. `claude mcp list` shows the server `plugin:agent-courier:agent-courier` as connected. When the server fails, run `agent-courier version` again: the binary is missing from `PATH` or has another version.

The running session does not load the new plugin. Tell the user to quit Claude Code and start it again with the push flag.

## OpenCode

### Install the plugin

The plugin has no runtime dependencies. It needs no `bun install`. Clone the repository at the tag of the binary, to a directory that the user approves:

```sh
git clone --branch "v$(agent-courier version)" https://github.com/CappyT/agent-courier.git ~/src/agent-courier
```

The `version` field in `opencode/package.json` of the clone must equal the output of `agent-courier version`.

Find the global config directory of OpenCode in the `config` line of `opencode debug paths`, usually `~/.config/opencode`. Show the user this change to `opencode.json` in that directory. Add the absolute path of the `opencode` directory of the clone to the `plugins` list, and keep the entries that are there:

```json
{
  "plugins": ["file:///home/you/src/agent-courier/opencode"]
}
```

### Set the binary for the background service

The plugin runs in the OpenCode background service (`opencode serve --service`), not in the TUI. The service has its own environment, which does not follow the `PATH` of your shell. Give it the full path of the binary:

```sh
opencode service set env AGENT_COURIER_BIN /home/you/go/bin/agent-courier
opencode service get env
```

Set other variables, for example `AGENT_COURIER_NAME` or `AGENT_COURIER_TOPICS`, the same way.

### Restart the service

The service loads plugins and its environment only at start:

```sh
opencode service restart
```

A restart stops every OpenCode session that runs in the service. If you run in OpenCode, the restart stops you too: tell the user to run the command, and continue in a new session after it. If you run in another harness, ask the user before you run it. A restart of the TUI alone loads nothing.

### Check the install

The service wants Basic auth: the user is `opencode`, and the URL and the password are in `service.json` in the `state` directory of `opencode debug paths`, usually `~/.local/state/opencode`. That file exists only while the service runs. Do not use the `service.json` in the config directory: it has no URL. Run this in a project directory:

```sh
svc=${XDG_STATE_HOME:-$HOME/.local/state}/opencode/service.json
curl -s -u "opencode:$(jq -r .password "$svc")" \
  "$(jq -r .url "$svc")/api/plugin?location%5Bdirectory%5D=$PWD" \
  | jq '.data[] | select(.id == "agent-courier")'
```

The entry has `"state": {"status": "active"}`. The first request after a restart can return before the service loads the directory. Repeat it once.

## Final check: send a note

Do this check after the restart, in two sessions that run in the same git repository: two Claude Code sessions, two OpenCode sessions, or one of each. The daemon derives the topic from the `upstream` or `origin` remote, so both sessions join the same topic.

1. In session A, call the `agents` tool. Session B is in the list and is live.
2. In session A, call `send` with kind `note`, recipient the name of session B, and a short text.
3. Session B receives the note as a push. In Claude Code, the push needs the push flag. Without it, call `pending` in session B.
4. In session B, call `ack` with the id of the note. Then `pending` in session B is empty.

In OpenCode, the tools are in the `courier` namespace.

## Tell the user

When you finish, tell the user:

- which files and settings you changed;
- for Claude Code: quit the session and start Claude Code with `--dangerously-load-development-channels plugin:agent-courier@agent-courier`, at each start, or with `--channels` when the organization approved the plugin;
- for OpenCode: run `opencode service restart`, if nobody ran it yet;
- after an update of the binary or the plugin: both must have the same version, and the old daemon exits 60 seconds after its last session closes.
