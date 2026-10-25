# Design

## Context

The repository starts empty. See `proposal.md` for the motivation and `specs/` for the behavior. Two facts shape the design:

- Harnesses differ in how an outside process reaches a live session. Claude Code accepts a push from an MCP server only through its `claude/channel` extension, a research preview. OpenCode v2 runs one background service for the TUI, desktop and web, and its HTTP API adds items to a session's inbox (`POST /api/session/{id}/synthetic` or `/prompt`, with `delivery` set to `steer` or `queue`). MCP itself has no standard way to put content into the model's context.
- The team transport (change `add-team-p2p`) puts a libp2p host in the same daemon. Everything this change stores must work when messages come from other machines too.

## Goals / Non-Goals

**Goals:**

- One process per user owns every courier rule, so the rules are written once, in Go, and tested once.
- Clients are thin: a client translates between its harness and the daemon and holds no courier logic.
- No setup beyond installing the binary and the plugin.

**Non-Goals:**

- Codex, Gemini CLI and other harnesses.
- A Claude Code push path other than channels, such as plugin monitors.
- Any network transport. That is `add-team-p2p`.
- Protection between processes of the same user. A process of the same user can already read the user's files and sessions.

## Decisions

### The daemon owns the logic, clients are thin

The daemon holds validation, rendering, budgets, routing, presence and storage. The Claude Code MCP server and the OpenCode plugin only forward tool calls and pushes.

- Alternative: each MCP server opens a shared SQLite file and runs the rules itself. Rejected: a push needs a process that notices a new message, budgets need one shared view per session, the TypeScript plugin would duplicate every rule, and the team transport needs one network host per machine.

### Socket and wire protocol

- The socket is `daemon.sock` in a directory with mode 0700 that depends only on the uid, so every process of the user finds the same daemon whatever its environment:
  - Linux: `/run/user/<uid>/agent-courier/` when `/run/user/<uid>` is a directory that the user owns, else `/tmp/agent-courier-<uid>/`. `XDG_RUNTIME_DIR` and `TMPDIR` are not read: a GUI terminal and a tmux started from ssh can differ in them, and two socket directories would mean two daemons on one store.
  - macOS: `agent-courier-<uid>` under the user's temporary directory, which is per user and stable.
  - `AGENT_COURIER_SOCKET_DIR`, an absolute path, replaces the directory. Tests set it, so they never meet the user's daemon.
- On Linux the daemon checks the peer uid with `SO_PEERCRED` and closes a connection from another uid. The client checks the same in the other direction.
- Before its first connect, a client checks the socket directory the way the daemon does: a real directory (no symlink), owned by the user, mode 0700. Otherwise it refuses to connect, because another user could have created `/tmp/agent-courier-<uid>` first and listen as a fake daemon. On Linux the client also checks the daemon's uid with `SO_PEERCRED` and closes a connection from another uid.
- The protocol is JSON-RPC 2.0, one JSON object per line. Methods: `register`, `send`, `pending`, `ack`, `agents`. The daemon sends `deliver` notifications. A line has a size cap, and the daemon closes a connection that passes it.
- One connection is one session. `register` names the harness, the working directory, an optional agent name and optional topics. The daemon replies with the agent name and topics it assigned, and with the protocol text the client gives its agent.
- Alternative: gRPC or HTTP over the socket. Rejected: line-delimited JSON is a few lines in both Go and TypeScript and needs no generated code.

The rest of this section is the exact wire protocol. The daemon, the Claude Code client and the OpenCode plugin implement this text.

#### Framing

- A line is one JSON-RPC 2.0 object in UTF-8, ended by `\n`. The daemon also accepts `\r\n` and skips empty lines.
- A line has at most 65536 bytes without its newline (`daemon.MaxLine`). When a client sends a longer line, the daemon writes one error response with `"id": null` and code -32600, then closes the connection. The daemon writes no line longer than the cap.
- A request has `"jsonrpc": "2.0"`, a `method`, an `id` and optional `params`, and no other member. The `id` is a string or a number. The daemon refuses a null `id` and an array of requests (a batch) with -32600.
- The daemon ignores a line without `id` (a notification from the client): it sends no response and does nothing.
- The daemon handles the requests of one connection one at a time, in order. It sends one response per request, with the `id` of the request, in the same order.
- After the response to `register`, the daemon can send a `deliver` notification at any time, also between a request and its response. A client tells the two apart: a line with `method` is a notification, a line with `result` or `error` is a response.
- `params` is a JSON object. A method without params accepts an absent `params` or `{}`. The daemon refuses an unknown member, a missing required member and a member of the wrong type with -32602, except the `sender` member of `send`, which it drops.
- A client reads its socket all the time. The daemon closes a connection that does not take a line within 10 seconds.

#### Session

- The first request on a connection is `register`. The daemon refuses every other method before it with -32001, and a second `register` with -32002.
- The session lives until the connection closes. Then its agent name is free again.

#### `register`

Params:

| Member | Type | Rule |
| --- | --- | --- |
| `harness` | string, required | `^[a-z0-9]{1,16}$`, not `everyone`. Claude Code sends `claude`, OpenCode sends `opencode`. |
| `version` | string, required | The release of the client, for example `0.1.0`. It equals the daemon's `Version`, else the daemon refuses the registration with -32004. |
| `cwd` | string, required | The absolute path of the session's working directory, at most 4096 bytes. |
| `name` | string, optional | The agent name the session asks for: `^[a-z0-9][a-z0-9-]{0,47}$`, not `everyone`. |
| `topics` | array of strings, optional | At most 16 different topics, each `^[a-z0-9][a-z0-9_-]{0,63}$`. A topic given twice counts once. Absent or empty: the default topic. |

The daemon derives the default topic and the repository name from `cwd`. It reads files and never runs `git`:

1. From `cwd` up to the root, the first directory that holds a `.git` entry is the top of the work tree. The walk stops at the first directory that the user does not own: that directory and its parents are not read. A `.git` directory is the git directory. A `.git` file holds `gitdir: <path>`, as in a worktree or a submodule. A relative path is relative to the top of the work tree. A `.git` file without `gitdir:` means no remote, and the walk stops there.
   - The daemon opens only regular files that the user owns, without blocking, and reads at most 1 MiB of each. A named pipe, a device or a socket counts as missing.
   - The git directory that `gitdir:` names, and the directory that `commondir` names, must be owned by the user, else the daemon uses no remote.
2. When the git directory holds a `commondir` file, as a worktree does, the daemon reads `config` in the directory that `commondir` names. Else it reads `config` in the git directory. It reads the first `url` of `[remote "upstream"]`. When that remote is missing or its URL has fewer than two segments, it reads the first `url` of `[remote "origin"]`. It does not apply `include` or `insteadOf`.
3. The last two path segments of the URL, without a final `.git`, are the owner and the repository. The daemon reads `https://host/owner/repo.git`, `ssh://git@host:22/owner/repo`, `git@host:owner/repo.git` and a local path. A URL with fewer than two segments counts as no remote.
4. The default topic is `<owner>_<repo>`. The host is not part of it, so `o/r` on two forges gives one topic. Different names can fold to one topic, for example `a_b/c` and `a/b_c`. Without a usable remote, the default topic and the repository name are the name of the top of the work tree, or of `cwd` outside a work tree.
5. A derived topic is in lower case. Each character outside `[a-z0-9_-]` becomes `-`. It is cut to 64 characters, and leading and trailing `-` and `_` go. When nothing remains, the daemon refuses the registration with -32602.

The daemon then assigns the agent name:

- The base name is `name` when the session gives it. Else it is `<harness>-<repo>`, with the repository name in lower case, each character outside `[a-z0-9-]` replaced by `-`, cut to 48 characters, and trailing `-` removed. Without a repository name, the base name is `<harness>`.
- While a live session holds the name, the daemon tries `<base>-2`, `<base>-3` and so on. It cuts the base so the name stays at 48 characters. An asked name gets a suffix the same way.
- For each topic, the daemon records the agent on the topic and sends `online` as "Presence" in `messaging` requires.

Result:

```json
{"session": "01JB8Z4Q6VJ5W3T2K9N0R7M1XC", "name": "claude-agent-courier", "topics": ["cappyt_agent-courier"], "protocol": "...", "version": "0.1.0"}
```

- `session`: the ULID of the session, for logs.
- `name`, `topics`: what the daemon assigned. `topics` is sorted.
- `protocol`: the agent protocol of `inbound-safety`, followed by the agent's name and topics. The client gives it to its agent unchanged, before the first push.
- `version`: the daemon's `Version`.
- A registration that fails leaves no state: no name held, no agent row and no `online`.

#### `send`

Params are the message fields of `messaging`:

| Member | Type | Rule |
| --- | --- | --- |
| `topic` | string, required | The topic rule. The session follows the topic. |
| `kind` | string, required | `handoff`, `done`, `blocked`, `ask`, `reply` or `note`. |
| `summary` | string, required | 1 to 1000 characters of valid UTF-8, not only invisible characters. |
| `to` | string, optional | An agent known on the topic, not the sender. Absent: every agent on the topic. |
| `issue` | number, optional | An issue number, greater than 0. Absent or 0: none. |
| `link` | string, optional | An `https` link of printable ASCII, at most 512 characters, without a user name. |
| `re` | string, required on a reply only | The id of an `ask` on the topic. |
| `sender` | any | Dropped. The sender is the session's agent name. |

Result:

```json
{"id": "01JB8Z5D1M3KX0P7Q2W9E4R6TY", "recipients": ["opencode-agent-courier"], "skipped": [], "text": "Sent 01JB8Z5D1M3KX0P7Q2W9E4R6TY to opencode-agent-courier."}
```

- `id`: the ULID of the stored message.
- `recipients`: the agents the message waits for, sorted. It is empty for a broadcast on a topic that no other agent follows. It lists at most 100 agents, and `total` gives the number of all recipients, so the response stays under the line cap. `text` names at most 10 agents and the number of the others.
- `skipped`: the recipients of a broadcast whose pending cap the message would pass, sorted. It is always present, lists at most 100 agents, and `skipped_total` gives the number of all of them.
- `text`: one line written by the daemon that the client returns to its agent as the tool result.

#### `pending`

No params. Result:

```json
{"text": "2 pending messages, direct ones first, oldest first in each group: ...", "total": 2}
```

- `text`: `message.RenderPending` of the agent's pending messages. It shows at most 20 messages and 16 KiB.
- `total`: the number of all messages that wait for the agent.
- `pending` neither acknowledges a message nor records a push.

#### `ack`

Params: `{"id": "01JB8Z5D1M3KX0P7Q2W9E4R6TY"}`. The id is a ULID in upper or lower case. Result: `{"text": "Acknowledged 01JB8Z5D1M3KX0P7Q2W9E4R6TY."}`.

#### `agents`

No params. Result:

```json
{"agents": [{"topic": "cappyt_agent-courier", "name": "claude-agent-courier", "live": true}], "total": 1, "text": "..."}
```

- `agents`: the agents known on each topic the session follows, the session's own agent included, sorted by topic and then by name. `live` is true while a session of this daemon holds the name, whatever topics that session follows. The list has at most 100 entries.
- `total`: the number of all entries.
- `text`: the same list as lines written by the daemon. It says how many entries it leaves out.

#### `deliver`

A notification from the daemon to the session of a recipient:

```json
{"jsonrpc": "2.0", "method": "deliver", "params": {"text": "...", "notice": "courier: handoff from claude-agent-courier to opencode-agent-courier", "attrs": {"id": "01JB8Z5D1M3KX0P7Q2W9E4R6TY", "topic": "cappyt_agent-courier", "sender": "claude-agent-courier", "kind": "handoff", "to": "opencode-agent-courier", "issue": "17"}}}
```

- `text`: `message.Render` of the message.
- `notice`: `message.Notice` of the message.
- `attrs`: an object of strings. `id`, `topic`, `sender`, `kind` and `to` are always present, except `id` in a `budget` notice. `to` is `everyone` for a broadcast. `issue` (decimal) and `re` are present only when set. Every value passed the rules of `messaging`, so a client passes `attrs` on unchanged: as channel meta in Claude Code, as `metadata` in OpenCode.
- A client shows `text` and `notice` unchanged and renders nothing itself. A client without a place for `notice` drops it.
- An `online` has `kind` `online`, `to` `everyone` and `sender` the agent that came online. A `budget` notice (see "Push budgets") has `kind` `budget`, `to` the recipient and `sender` the sender over budget. `send` accepts neither kind.
- The daemon records the push when it writes the line. A `deliver` is not an acknowledgement.
- An agent follows a topic while it is known on it (`messaging`, "Topics"). The topics of the current session are only the topics it registered now. So the daemon pushes a message to the live session of each recipient whatever topics that session registered, and the session can `send` on every topic where its agent is known, for example to answer an `ask` on a topic of an earlier session.
- The daemon pushes at most once per message and session.
- Each session has a queue of 256 `deliver` notifications, which its own writer drains after the `register` response. A `send` only queues, so a recipient that does not read never delays the sender's response. When the queue is full, the daemon does not queue the push and marks the session as behind. When the writer has emptied the queue of a session that is behind, the daemon pushes again, in the order of "After register", each pending message that this session has not received. So every pending message reaches a live session within the push budgets, and a full queue only delays it.

#### Errors

An error response has `{"code": <int>, "message": <string>}` and no `data`. The message is one line that passes through `message.Inline`, and a client shows it unchanged as the tool error. A message never quotes a value that the client sent, except an agent name or a topic that passed its rule.

| Code | Meaning | When |
| --- | --- | --- |
| -32700 | parse error | The line is not valid JSON in UTF-8. |
| -32600 | invalid request | The line is not one JSON-RPC 2.0 request object, or it passes the line cap. |
| -32601 | method not found | The method is not one of `register`, `send`, `pending`, `ack`, `agents`. |
| -32602 | invalid params | The params break a rule of this section or of `messaging`. The message names the rule. |
| -32603 | internal error | The daemon failed, for example the store. The message is `internal error`. The daemon logs the cause. |
| -32001 | not registered | A method other than `register` comes before `register`. |
| -32002 | already registered | A second `register` on one connection. |
| -32003 | not implemented | The daemon does not implement the method yet. A released daemon never sends it. |
| -32004 | version mismatch | The client's `version` differs from the daemon's. The message names both versions. |
| -32010 | unknown recipient | `to` is not an agent known on the topic. |
| -32011 | not on topic | The sender's agent is not known on the topic of `send`. |
| -32012 | no ask | The `re` of a reply names no `ask` on the topic. |
| -32013 | not pending | `ack` names a message that is not pending for this agent. |
| -32014 | queue full | A direct message would pass a pending cap of its recipient. The message names the recipient and the cap. |

#### Members not in the tables

- `to: "everyone"` is refused with -32602. A broadcast has no `to`.
- `re` on a kind other than `reply` is refused with -32602.
- `to` that names the sender is refused with -32602.
- A `topic` where the agent is not known is refused with -32011. A topic where the agent is known from an earlier session is accepted.

#### After `register`

- The daemon writes the `register` response first. Then it pushes, as `deliver`, each pending message of the agent that no session of this daemon has pushed yet, or that an earlier session pushed. Direct messages come first, oldest first. The push budgets apply, and a message over budget stays in `pending`.
- Session ids are ULIDs, so a session of a new daemon never matches a stored one, and a message pending since before a restart reaches the new session once.
- An `online` goes as `deliver` to the live sessions of the topic's other agents. It does not count against the push budgets: the store allows one per agent and topic per hour.

#### Push budgets

- The windows belong to one session and slide. A new session starts with empty windows. A push counts when the daemon queues it, a push after `register` too. A push that finds the queue full counts nothing.
- The daemon checks the budgets of a message in this order and names the first one it passes: 10 pushes per sender in 10 minutes, 10 direct messages per sender in one hour, then 30 direct messages or 30 broadcasts from all senders in one hour.
- A message over budget is held: it stays in `pending` and the daemon does not queue it. The session's writer offers the held messages again, oldest first, when their budgets have room: after each push it writes, after the pushes that follow a full queue, and once a minute. So a held message reaches the live session when its window slides, without a new `register`.
- A sender with held messages in a session keeps its order: a newer message of that sender is held behind them. Onlines are never held.
- Notices for the two per-sender budgets go once per sender and budget in the window of that budget. Notices for the two session budgets go once per budget in its window, whatever the sender, and their text names no sender: "messages from all senders passed the budget of 30 broadcasts in one hour". So a session gets at most two notices per sender and two more per window.
- A notice's `text` is written by the daemon: the budget, the sender for a per-sender budget, "the courier holds these messages", and "call pending". Its `topic` is the topic of the message over budget and its `sender` the sender of that message. It has no `id`, because no stored message backs it and `ack` cannot take it. It counts against no budget.
- A notice that finds the queue full is owed: the writer offers it after the pushes that follow the full queue.
- When the writer offers held messages again and one is still over a budget whose notice did not go in the current window, the writer sends that notice. So a session that does not read its held messages gets one reminder per budget window, inside the limits above.

#### Write failures

- When a write to a connection fails or passes the 10-second deadline, the daemon closes the connection and handles no more lines of it, also lines already read.

#### Reconnect

- When the connection to the daemon breaks, the client connects again (with the autostart of "On-demand start") and registers with the name and topics it had. When the daemon assigns another name, the client gives its agent the new `protocol` text before any later push: the Claude Code client pushes it as a channel event with no meta, and the OpenCode plugin puts it in the context hook, which runs before every model call, so a push of its own would only start an extra turn on an idle session.
- A tool call that was waiting when the connection broke fails. The client does not send it again: a `send` may have reached the store, so the agent decides.

#### Client mapping

- A daemon error becomes a failed tool call with the error's `message` as its text: `CallToolResult` with `isError` in MCP, a thrown error in an OpenCode tool. A broken connection gives the text "the courier daemon is not reachable".
- A client reads `AGENT_COURIER_NAME` and `AGENT_COURIER_TOPICS` (topics split by commas) from its environment and sends them as `name` and `topics` when set.
- Claude Code: `text` is the content of `notifications/claude/channel`, `attrs` is its meta, and `notice` is dropped. The client registers before it answers `initialize`, waiting at most 5 seconds for the daemon, and sends the `protocol` text as the MCP `instructions`. When the daemon does not answer in time, the instructions are the protocol text built into the client, and the tools fail until a connection works. The client holds a `deliver` that arrives before `initialized` and writes it after. Without `--channels`, Claude Code drops the notification: the message stays pending and the agent finds it with `pending`.
- OpenCode: see "OpenCode client".

### On-demand start and single instance

A client connects to the socket. On failure it tries to take an exclusive `flock` on `daemon.lock` in the same directory, without waiting, until 5 seconds pass:

- When another process holds the lock, a daemon runs, starts or exits. The client polls the socket and the lock again.
- When the client gets the lock, it connects again. Otherwise it starts `agent-courier daemon` from its own executable in a new session (`setsid`), with stdio on `/dev/null`, working directory `/`, and the open lock file as descriptor 3, named in `AGENT_COURIER_LOCK_FD`. The client then closes its copy of the descriptor and waits for the socket.
- The lock belongs to the open file, so the daemon holds it from the moment the client took it. No moment exists in which the lock is free and no daemon runs, so a second client cannot start a second daemon. The daemon checks that descriptor 3 is `daemon.lock` of the socket directory before it uses it.
- When the started daemon exits before it listens, for example because an exiting daemon still held the lock, the client waits 200 milliseconds and tries again within the 5 seconds. One `Dial` starts at most 5 daemons, then fails with the error of the last one.
- A daemon that a client started writes its log to `daemon.log` beside `courier.db`, mode 0600, and starts a new file when the old one passes 1 MiB (one old file, `daemon.log.1`, is kept). The client that started a daemon that exited before it listened returns the last line of that log in its error. A daemon started by hand from a terminal logs to its stderr.
- The client starts the daemon from its own executable. On Linux it runs `/proc/self/exe`, so a binary replaced or deleted by an update still starts the daemon of the client's own version.
- The daemon sets close-on-exec on the inherited lock descriptor.
- The daemon releases the lock last: after it closes its connections, refreshes its agents and closes the store.

A daemon started by hand takes the lock itself and exits at once when another daemon holds it. The daemon exits when no client has been connected for 60 seconds (chosen), counted from its start and from each moment its last connection closes. A connection in that time cancels the exit. `AGENT_COURIER_IDLE_GRACE` sets another grace as a Go duration, for example `5m`. A value that is not a positive duration counts as unset.

- Alternative: a systemd user unit. Rejected by the user: no service to install. The cost is that a machine with no open session receives nothing, which matters only for `add-team-p2p`.

#### Version mismatch

A client of another version than the running daemon gets -32004 from `register`. The client then stops reconnecting, because each attempt would cancel the daemon's idle exit. Every tool call fails with the -32004 message, which names both versions and says that the old daemon exits 60 seconds after its last session closes. Each later tool call tries one new connection, so the client works as soon as the old daemon has gone. -32602 from `register` is handled the same way: it is a client bug, and retrying does not fix it.

#### Order of a reconnect

A new connection holds its `deliver` notifications until the client has handled the `register` response, and has given its agent a new `protocol` text when the name changed. The old connection's notifications all reach the agent before the new connection's first one.

### Storage

SQLite through `modernc.org/sqlite`, a pure-Go driver, so the binary needs no C toolchain. The file is `courier.db` in `$XDG_STATE_HOME/agent-courier/`, in WAL mode. Tables:

- `messages`: id, topic, sender, recipient, kind, summary, issue, link, reply reference, creation time.
- `deliveries`: message id, recipient agent, id of the session that received the push, acknowledgement time.
- `agents`: topic, agent name, last registration, time of the last `online`.

The store opens the file as a `file:` URI with an escaped path and `_txlock=immediate`, so a transaction that reads before it writes waits on the busy timeout instead of failing. It sets the directory to 0700 and creates the file with 0600 before SQLite opens it. Session ids are ULIDs, unique across daemon lifetimes, because a delivery records the last session that received its push. The daemon refreshes the agents of live sessions every hour, so a long session keeps its agent known.

A cleanup runs at start and every hour and deletes what `messaging` retention says. Budget windows live in memory. A daemon restart resets them, which a process of the same user could already cause.

### Message ids

A message id is a ULID: 48 bits of milliseconds and 80 random bits, in Crockford base32. Ids sort by time and stay unique across machines, which `add-team-p2p` needs. A few lines of code generate them, so no dependency.

### Rendering

One Go package renders every inbound string, as `inbound-safety` specifies: drop invisible characters, cap combining marks, NFKC through `golang.org/x/text`, escape `&`, `<` and `>`, prefix summary and link lines with `| `, apply the size caps. The daemon sends rendered text in `deliver`, in `pending` results and in errors. Clients never see raw fields.

### Claude Code client

`agent-courier mcp` is the MCP server, built on `github.com/mark3labs/mcp-go`. It declares the experimental capability `claude/channel` and pushes a `notifications/claude/channel` notification with the rendered text as content and the message attributes as meta. It offers only MCP protocol versions older than 2026-07-28, because Claude Code's v2 client runtime does not register a channel server that negotiates that revision. The plugin manifest starts `agent-courier mcp`, and the `courier-send` skill tells the agent how to send: update the shared state first, then send the message that points at it.

- The client queues each deliver and each registration in the order the client package hands them, and writes the queue after `initialized`. A registration whose name differs from the name the agent has goes out as its `protocol` text first, so every push follows the protocol of its own connection. Tool calls before `initialized` fail, because a client that skips `initialize` never gets a push.
- Alternative: the official MCP Go SDK. Rejected for now: `mcp-go` already carried the channel notification in the earlier prototype of this project.

### OpenCode client

A TypeScript plugin in `opencode/`: a v2 plugin module whose default export is `{ id, setup(ctx) }`. It defines the four tools. The facts below are measured on OpenCode 2.1.4 (task group 2).

- The plugin runs in the OpenCode background service, not in the TUI. The service starts one plugin instance per location (working directory) at the first request for that location, and keeps it until the service stops or reloads. One instance serves every session of the location.
- Session ids: a tool's `execute(input, context)` receives `context.sessionID`, and the `tool` and `session` hooks carry `sessionID`. `ctx.event.subscribe()` streams the events of the plugin's location only, among them `session.created`, `session.execution.started`, `session.deleted` and `session.archived`.
- An OpenCode session is durable. No event tells when a person opens or closes it in a client, and it ends only when it is deleted or archived.
- Connection: the plugin does not open the socket itself. For each session it starts `agent-courier bridge` as a child process and speaks the wire protocol over the child's stdin and stdout, one JSON-RPC object per line. The bridge runs the Go client's `Dial`: the socket directory and peer uid checks, autostart under `flock`, the spawn limit and `daemon.log`. Then it copies lines in both directions until either side closes, and exits. Its logs go to stderr, which the plugin passes to the OpenCode log. The plugin finds the binary in `AGENT_COURIER_BIN`, else `agent-courier` on `PATH`. When the bridge exits, the plugin treats it as a broken connection: the session's tool calls fail with "the courier daemon is not reachable", and the plugin starts a new bridge and registers again at the next tool call or execution, with the name and topics the daemon assigned.
- Alternative: a socket client in TypeScript. Rejected: Bun has no `flock` and no `SO_PEERCRED`, so the plugin would repeat the autostart and the security checks of the Go client with weaker tools.
- The plugin keeps a map from session id to daemon connection. It registers a session at its first `session.execution.started` or tool call, not at `session.created`. It skips child sessions (subagents, `parentID` set). It closes the connection at `session.deleted`, `session.archived` and plugin cleanup, and after 30 minutes (chosen) with no execution. A durable session that nobody uses then frees its agent name, so the default name goes to the session a person works in. Messages for the name wait in `pending` and reach the next session that registers it.
- The plugin gives its agent the protocol text from `register` through the session context hook, which runs before each model call, so the protocol is in the context before the first push. Task 8.1 verifies the hook on the current release.
- Push: `ctx.session.synthetic({ sessionID, text, description, metadata, delivery: "steer" })`. The call runs in the service process, needs no URL or password, and returns in less than 5 ms.
- On an idle session, steer starts a turn. During a running tool call, the tool completes, and the text enters at the next step, after the tool result.
- `delivery: "queue"` also starts a turn on an idle session. On a busy session it waits until the agent writes its final reply, and then the agent continues. Steer is the choice, because a `blocked` or an `ask` must reach the agent before it finishes.
- The model receives a synthetic item as a plain user message with `text` as content. Nothing marks it as text from a peer, so `text` is the daemon's rendered text, whose header line names the sender and the kind.
- The TUI does not show `text`. It shows `description` as a one-line notice, and shows nothing when `description` is empty. So `deliver` carries a rendered one-line notice, for example `courier: handoff from claude-x`, and the plugin sends it as `description`.
- `deliver` carries three fields, all from the daemon: `text` (`message.Render`), `notice` (`message.Notice`: one line from the kind and the sender name only) and `attrs` (the validated id, topic, sender, kind, recipient, issue and reply id). The plugin passes `attrs` as `metadata` and nothing else.
- `resume: false` stores the item without a turn, and the next turn delivers it. Using it would need a change to the `opencode-integration` spec, which requires a turn on an idle session.
- Alternative: `POST /api/session/{id}/prompt`. Rejected: the TUI shows the item as a message that the user typed.
- Alternative: the daemon calls `POST /api/session/{id}/synthetic` on the service, with Basic auth user `opencode` and the URL and password in `~/.local/state/opencode/service.json`. It behaves the same, but without the plugin the daemon has no session id.
- Alternative: an MCP server for OpenCode too. Rejected: an MCP server does not know which OpenCode session started it, so it cannot address the push.

### Topic and agent name

The client sends the working directory. The daemon reads the `upstream` remote, else `origin`, from the git config found from that directory, and derives `<owner>_<repo>` and the default name `<harness>-<repo>`, and adds `-2`, `-3` and so on while a live session holds the name.

### Layout and versions

- `cmd/agent-courier`: subcommands `daemon`, `mcp` and `bridge`.
- `internal/message`: model, validation, rendering, ids.
- `internal/store`, `internal/daemon`, `internal/client` (socket client and autostart), `internal/mcpserver`.
- `opencode/`: the OpenCode plugin. `.claude-plugin/` and `skills/`: the Claude Code marketplace.
- `Version` in Go, `version` in `.claude-plugin/plugin.json` and in `opencode/package.json` are equal, and a Go test fails when they differ.

## Risks / Trade-offs

- [Onlines have no per-session cap] -> The store allows one online per agent and topic per hour, and a new one replaces the unacknowledged one, so the count is bounded by the agents and topics of one user. That is inside the same-user boundary of this phase. The team change caps onlines per peer.
- [Claude Code channels stay a research preview or change shape] -> The tools work without the channel, and `pending` lists what waits. A plugin monitor is a later fallback with no allowlist.
- [The OpenCode v2 service API is weeks old and may change] -> Task group 2 measured it on OpenCode 2.1.4, and the plugin states the OpenCode versions it supports.
- [A push to an idle OpenCode session starts a turn when no person watches it] -> The per-sender budgets cap the turns. If that is not enough, the plugin sends `resume: false` to an idle session.
- [A process of the same user can register as any agent] -> Accepted: the boundary of the local mode is the user account.
- [A name suffix can move between sessions, so `claude-x-2` today is another session tomorrow] -> Pending messages belong to the name, and the default name without a suffix stays stable for the common case of one session per harness and repository.

## Migration Plan

None: the repository starts from an empty tree, and no installation of an earlier version remains.
