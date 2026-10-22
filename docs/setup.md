# Set up agent-courier for one person

Follow these steps in order on the person's machine. Each step ends on a check. Do not start the next step until the check passes.

You need from the person: their login (the name the team uses for them, for example their GitHub login), the NATS URL, and the repos to follow. The NATS admin gives the URL.

## 1. Install the binary

```sh
go install github.com/CappyT/agent-courier/cmd/agent-courier@latest
```

Check: `command -v agent-courier` prints a path.

## 2. Create the identity

```sh
mkdir -p ~/.config/nats/agent-courier && chmod 700 ~/.config/nats/agent-courier
nats auth nkey gen user --output ~/.config/nats/agent-courier/<login>.nk
chmod 600 ~/.config/nats/agent-courier/<login>.nk
nats auth nkey show ~/.config/nats/agent-courier/<login>.nk
```

The last command prints the public key, a 56-character string that starts with `U`. Give the person the public key and ask them to send it to the NATS admin with their login. The seed file never leaves this machine.

Check: the admin confirms the user exists, and this command prints `Account:`:

```sh
nats -s <url> --nkey ~/.config/nats/agent-courier/<login>.nk account info
```

## 3. Register the MCP server

Add the server to the `.mcp.json` of each repo the orchestrator works in:

```json
{
  "mcpServers": {
    "agent-courier": {
      "command": "agent-courier",
      "env": {
        "COURIER_NATS_URL": "<url>",
        "COURIER_NKEY_FILE": "/home/<user>/.config/nats/agent-courier/<login>.nk",
        "COURIER_LOGIN": "<login>",
        "COURIER_REPOS": "<repo>,<repo>"
      }
    }
  }
}
```

- `COURIER_LOGIN` must be the login the admin registered. The server refuses events published under any other login.
- `COURIER_REPOS` takes repository names without owner, separated by commas. A name may contain only letters, digits, `-` and `_`.
- `COURIER_ACK_WAIT` is optional. It sets how long an event waits for `ack` before it comes back. The default is `30m`.

Check: `claude mcp list` shows `agent-courier` as connected.

## 4. Start the session with the channel

Custom channels are a research preview, so they need the development flag:

```sh
claude --dangerously-load-development-channels server:agent-courier
```

On a Team or Enterprise plan, an admin must first enable channels for the organization.

Check: below the startup banner, a notice says that messages from `server:agent-courier` inject into the session.

## 5. Test the round trip

Ask another person on the team to publish a `note` on a followed repo, or publish one from a second login. The event appears in the session as a `<channel source="agent-courier" ...>` tag.

Check: the event arrives, `ack` with its `seq` returns `acked`, and `pending` returns `no pending events`.

## How the orchestrator uses it

The MCP server sends the event protocol to the session when it connects: what each kind means, when to call `handoff`, and when to call `ack`. The protocol text is in [`internal/courier/channel.go`](../internal/courier/channel.go), in the `instructions` constant.
