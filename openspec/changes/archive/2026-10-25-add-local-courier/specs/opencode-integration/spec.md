# Spec Delta

## Purpose

Connects an OpenCode session to the courier: the same tools as Claude Code, and a push of inbound messages into the live session.

## ADDED Requirements

### Requirement: Plugin tools
The OpenCode client SHALL be an OpenCode plugin with the tools `send`, `pending`, `ack` and `agents`, with the same arguments and results as the Claude Code tools.

#### Scenario: Send from OpenCode
- **WHEN** the OpenCode agent calls `send` with kind `ask`, a topic it follows and a summary
- **THEN** the daemon stores the message and the tool returns its id

### Requirement: Push into the live session
The client SHALL push each inbound message into the OpenCode session that registered the agent. A message that arrives while the agent works SHALL join the running turn at the next safe step. A message that arrives while the session is idle SHALL start a turn.

#### Scenario: Idle session
- **WHEN** another agent sends a `handoff` to this agent and the OpenCode session is idle
- **THEN** the session starts a turn with the rendered message

#### Scenario: Busy session
- **WHEN** a message arrives while the OpenCode agent runs a tool
- **THEN** the message enters the conversation at the next step boundary, without cancelling the running tool

### Requirement: One agent per session
Each top-level OpenCode session SHALL register as its own agent, so two OpenCode sessions in one repository are two agents. A child session that a subagent runs SHALL NOT register.

#### Scenario: Two sessions in one repository
- **WHEN** two OpenCode sessions run in the same repository
- **THEN** the daemon lists two live agents, and a direct message to one does not reach the other

#### Scenario: Subagent session
- **WHEN** an OpenCode agent starts a subagent, and the subagent runs in a child session
- **THEN** the daemon lists no agent for the child session
