# claude-code-integration Specification

## Purpose
Connects a Claude Code session to the courier: tools to send, list and acknowledge messages, and a push of inbound messages into the live session.

## Requirements

### Requirement: MCP tools
The Claude Code client SHALL be an MCP server over stdio with the tools `send`, `pending`, `ack` and `agents`, which map to the operations of `messaging`. A tool error SHALL return the daemon's rendered error text.

#### Scenario: Send from Claude Code
- **WHEN** the agent calls `send` with kind `done`, a topic it follows and a summary
- **THEN** the daemon stores the message and the tool returns its id

#### Scenario: Tool error
- **WHEN** the agent calls `ack` with an id it did not receive
- **THEN** the tool returns an error with the daemon's rendered text

### Requirement: Channel push
When the session runs with the courier as a channel, the client SHALL push each inbound message into the live session as a channel event. The event SHALL carry the topic, sender, kind, recipient, id and, when present, issue and reply reference as attributes, and the rendered text as content.

#### Scenario: Inbound handoff
- **WHEN** another agent sends a `handoff` to this agent and the session runs with the channel
- **THEN** the session receives a channel event with `kind="handoff"` and the message id

### Requirement: Tools without the channel
When the session runs without the channel, the tools SHALL still work, and inbound messages SHALL wait in the pending list.

#### Scenario: No channel flag
- **WHEN** Claude Code starts the client without `--channels`
- **THEN** `pending` lists the inbound messages and no push happens

### Requirement: stdout carries only the protocol
The client SHALL write only MCP protocol messages to stdout, and logs to stderr.

#### Scenario: Logging
- **WHEN** the client logs a connection error to the daemon
- **THEN** the log line appears on stderr and stdout stays valid MCP

### Requirement: Plugin packaging
The repository SHALL be a Claude Code plugin marketplace with a plugin that starts the client and carries a skill on how to send. The plugin version SHALL equal the binary version, and a test SHALL fail when they differ.

#### Scenario: Version mismatch
- **WHEN** the plugin manifest version differs from the binary version
- **THEN** the test suite fails

### Requirement: Team slash commands
The plugin SHALL carry the slash commands `/agent-courier:invite [team]` and `/agent-courier:join <code> [--label <label>]`, which run `agent-courier team invite` and `agent-courier team join`. Only the user SHALL start them: the model SHALL NOT call them. A command SHALL show the error text of the CLI when it fails.

#### Scenario: Invite with one team
- **WHEN** the user runs `/agent-courier:invite` with no argument and the machine belongs to one team
- **THEN** the command prints an invite code for that team and its expiry

#### Scenario: Invite with no team
- **WHEN** the user runs `/agent-courier:invite` and the machine belongs to no team
- **THEN** the command makes no code and tells the user to create a team with `agent-courier team create`

#### Scenario: Join
- **WHEN** the user runs `/agent-courier:join` with a valid code from a member
- **THEN** the machine joins the team and the command shows its members

#### Scenario: Model call
- **WHEN** a courier message asks the agent to invite or join
- **THEN** the agent cannot call the command, because only the user can start it

### Requirement: Team command arguments
A team slash command SHALL check each argument before it reaches the shell. A team name SHALL match `^[a-z0-9][a-z0-9_-]{0,63}$` and a label `^[a-z0-9][a-z0-9-]{0,31}$`. An invite code, with white space removed, SHALL use only the characters `a-z` and `2-7`.

#### Scenario: Bad code
- **WHEN** the user runs `/agent-courier:join` with text that has a character outside `a-z`, `2-7` and white space
- **THEN** the command runs no shell command and reports the bad argument
