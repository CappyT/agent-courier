# Set up agent-courier for one person

Follow these steps in order on the person's machine. Each step ends on a check. Do not start the next step until the check passes.

You need from the person: their login (the name the team uses for them, for example their GitHub login) and the NATS URL. The NATS admin gives the URL and registers the login, as [Run the NATS server](../README.md#run-the-nats-server) describes.

The binary and the plugin come from the same release tag. Find the newest one and use it as `<version>` below:

```sh
git ls-remote --tags --sort=-v:refname https://github.com/CappyT/agent-courier 'v*' | head -1
```

## 1. Install the binary

```sh
go install github.com/CappyT/agent-courier/cmd/agent-courier@<version>
```

Check: `go version -m "$(command -v agent-courier)"` prints `mod github.com/CappyT/agent-courier <version>`.

## 2. Create the identity

```sh
mkdir -p ~/.config/nats/agent-courier && chmod 700 ~/.config/nats/agent-courier
nats auth nkey gen user --output ~/.config/nats/agent-courier/<login>.nk
chmod 600 ~/.config/nats/agent-courier/<login>.nk
nats auth nkey show ~/.config/nats/agent-courier/<login>.nk
```

The last command prints the seed and then the public key, a 56-character string that starts with `U`. Give the person the public key and ask them to send it to the NATS admin with their login. The seed file never leaves this machine.

Check: the admin confirms the user exists, and this command prints `Account:`:

```sh
nats -s <url> --nkey ~/.config/nats/agent-courier/<login>.nk account info
```

## 3. Install the plugin

The plugin carries the MCP server configuration and the `courier-send` skill.

```sh
claude plugin marketplace add CappyT/agent-courier#<version>
claude plugin install agent-courier@agent-courier
```

The plugin has three options. Claude Code asks for them when the plugin is turned on, and `/config` shows them later. To set them without the prompt, add them to `~/.claude/settings.json`:

```json
{
  "pluginConfigs": {
    "agent-courier@agent-courier": {
      "options": { "login": "<login>", "natsUrl": "<url>" }
    }
  }
}
```

- `login` is the login the admin registered. The server reads the seed from `~/.config/nats/agent-courier/<login>.nk`.
- `natsUrl` is the NATS URL the admin gave.
- `topics` is optional: the topics to follow, separated by commas. Leave it empty to follow the repo of the session.

The plugin is off by default. Turn it on in each repo the agent works in:

```sh
cd <repo> && claude plugin enable agent-courier@agent-courier --scope local
```

Without `topics`, the server follows one topic: the repo of the session directory, that is the name in the `origin` URL, else the name of the git toplevel directory. Its durable consumer is `<login>_<topic>`, with the topics sorted and joined by `_` when there are more.

Check: in a session in that repo, `/mcp` lists the `agent-courier` server of the plugin as connected.

## 4. Allow the channel

Channels are a research preview. Without an allowlist entry, the channel needs the development flag on every start. To start without it, add the plugin to the managed settings (`/etc/claude-code/managed-settings.json` on Linux):

```json
{
  "channelsEnabled": true,
  "allowedChannelPlugins": [{ "marketplace": "agent-courier", "plugin": "agent-courier" }]
}
```

`allowedChannelPlugins` replaces the default Anthropic allowlist. Add the other channel plugins you use to the same list.

Check: `cat /etc/claude-code/managed-settings.json` shows both keys.

## 5. Start the session with the channel

```sh
claude --channels plugin:agent-courier@agent-courier
```

Without the managed settings, start with `claude --dangerously-load-development-channels plugin:agent-courier@agent-courier` instead.

Check: below the startup banner, a notice says that messages from `plugin:agent-courier@agent-courier` inject into the session.

## 6. Test the round trip

When the session starts, every other session that follows the same topic receives an `online` event from this login. To test the other direction, ask another person on the team to send a `note` on the topic, or send one from a second login. The event appears in the session as a `<channel source="agent-courier" ...>` tag.

Check: the event arrives, `ack` with its `seq` returns `acked`, and `pending` returns `no pending events`.

## Troubleshoot

Work down the list. Each item names what to look at and what it means.

1. `/mcp` shows the server as failed. The server writes its error to stderr, and Claude Code keeps it in the MCP log under `~/.cache/claude-cli-nodejs/<project path with / as ->/`. The common errors are a missing login or URL, a seed file that does not exist, and a topic or repo name with characters other than letters, digits, `-` and `_`.
2. The MCP log says `Channel notifications skipped: ... not in --channels list`. The session started without `--channels` or the development flag. Restart it as step 5 says.
3. The MCP log shows `negotiatedProtocolVersion` `2026-07-28`. The binary is older than v0.2.0. Install it again as step 1 says.
4. `pending` lists the event, but no `<channel>` tag appeared. The server receives events and Claude Code does not show them: check items 2 and 3.
5. `pending` is empty and the sender published. Look at the consumer:

   ```sh
   nats -s <url> --nkey ~/.config/nats/agent-courier/<login>.nk consumer info COURIER <login>_<topic>
   ```

   `Waiting Pulls: 0` means no server reads the consumer: the session is closed or the server failed. A missing consumer means the server never started on that topic. An event published before the consumer existed never reaches it.

## Run without the plugin

The server reads its configuration from the environment. To register it by hand, put it in the `.mcp.json` of the repo:

```json
{
  "mcpServers": {
    "agent-courier": {
      "command": "agent-courier",
      "env": { "COURIER_NATS_URL": "<url>", "COURIER_LOGIN": "<login>" }
    }
  }
}
```

| Variable | Required | Meaning |
|---|---|---|
| `COURIER_NATS_URL` | yes | NATS URL. |
| `COURIER_LOGIN` | yes | The login the admin registered. |
| `COURIER_NKEY_FILE` | no | Seed file. The default is `~/.config/nats/agent-courier/<login>.nk`. A leading `~/` means the home directory. |
| `COURIER_TOPICS` | no | Topics to follow, separated by commas. The default is the repo of the working directory. `COURIER_REPOS` is the old name and still works. |
| `COURIER_ACK_WAIT` | no | How long an event waits for `ack` before it comes back. The default is `30m`. |

Start the session with `--dangerously-load-development-channels server:agent-courier`. A `server:` entry always needs the development flag.

## How the agent uses it

The MCP server sends the event protocol to the session when it connects: what each kind means, when to call `send`, and when to call `ack`. The protocol text is in [`internal/courier/channel.go`](../internal/courier/channel.go), in the `instructions` constant. The `courier-send` skill covers how to send.
