# Spec Delta

## Purpose

Defines how agents address each other, which messages they exchange, how a message reaches its recipients, and when a message is done.

## ADDED Requirements

### Requirement: Agent identity
Each connected session SHALL have an agent name, unique among the live sessions of the daemon. The daemon SHALL assign the sender of a message from the session's registration, never from a field the client sends. A name SHALL match `^[a-z0-9][a-z0-9-]{0,47}$`. The name `everyone` SHALL be reserved, because a rendered broadcast names it as the recipient.

#### Scenario: Reserved name
- **WHEN** a session asks for the name `everyone`
- **THEN** the daemon refuses the registration with an error that names the rule

#### Scenario: Default name
- **WHEN** a Claude Code session registers in a clone of `github.com/CappyT/agent-courier` and gives no name
- **THEN** its agent name is `claude-agent-courier`

#### Scenario: Name taken by a live session
- **WHEN** a second session asks for a name that a live session holds
- **THEN** the daemon gives it the name with the next free suffix, such as `claude-agent-courier-2`

#### Scenario: Client sends a sender field
- **WHEN** a client sends a message with a sender field that differs from its agent name
- **THEN** the stored message carries the agent name of the session

### Requirement: Topics
A topic SHALL scope messages to a shared piece of work. A session SHALL register its agent on one or more topics. An agent SHALL follow a topic while it is known on the topic, as "Retention" defines, also when its current session registered other topics. The default topic SHALL be `<owner>_<repo>` in lower case, read from the `upstream` remote, else from `origin`, else the directory name. A topic SHALL match `^[a-z0-9][a-z0-9_-]{0,63}$`.

#### Scenario: Default topic
- **WHEN** a session registers in a clone of `github.com/CappyT/agent-courier` and names no topic
- **THEN** the session follows the topic `cappyt_agent-courier`

#### Scenario: Fork
- **WHEN** a session registers in a fork whose `upstream` remote is `github.com/CappyT/agent-courier` and names no topic
- **THEN** the session follows the topic `cappyt_agent-courier`

#### Scenario: Invalid topic
- **WHEN** a client asks to follow the topic `../etc`
- **THEN** the daemon refuses the registration with an error that names the topic rule

### Requirement: Message kinds
A message SHALL have one kind: `handoff`, `done`, `blocked`, `ask`, `reply` or `note`, sent by an agent, or `online`, sent only by the daemon. A `reply` SHALL name the id of an `ask` on the same topic.

#### Scenario: Agent sends online
- **WHEN** a client sends a message of kind `online`
- **THEN** the daemon refuses it

#### Scenario: Reply without a valid ask
- **WHEN** a client sends a `reply` whose `re` names no `ask` on that topic
- **THEN** the daemon refuses it

### Requirement: Message fields
A message SHALL carry a topic, a kind and a summary of at most 1000 characters. It MAY carry a recipient agent, an issue number, an `https` link of at most 512 characters, and for a reply the id of the ask. The daemon SHALL drop a `sender` field, as "Agent identity" requires, and SHALL refuse a message with any other field or with a value outside these limits.

#### Scenario: Summary too long
- **WHEN** a client sends a summary of 1001 characters
- **THEN** the daemon refuses the message with an error that names the limit

#### Scenario: Non-https link
- **WHEN** a client sends a link that starts with `file://`
- **THEN** the daemon refuses the message

### Requirement: Delivery scope
A message without a recipient SHALL reach every agent that follows its topic, except the sender. A message with a recipient SHALL reach only that agent. A recipient SHALL be an agent known on the topic; the daemon SHALL refuse a message to an unknown agent. The sender SHALL follow the topic, and SHALL NOT be the recipient.

#### Scenario: Broadcast on a topic
- **WHEN** agent A sends a `note` without a recipient on a topic that agents A, B and C follow
- **THEN** B and C receive it and A does not

#### Scenario: Direct message
- **WHEN** agent A sends a `handoff` to B on a topic that A, B and C follow
- **THEN** only B receives it

#### Scenario: Unknown recipient
- **WHEN** agent A sends a message to an agent that never followed the topic
- **THEN** the daemon refuses it with an error

#### Scenario: Sender outside the topic
- **WHEN** agent A sends a message on a topic that A does not follow
- **THEN** the daemon refuses it

### Requirement: Delivery to an offline agent
A message for a known agent whose session is not connected SHALL wait. The daemon SHALL push it when a session with that agent name connects again.

#### Scenario: Recipient session restarts
- **WHEN** agent A sends a `done` to B while B has no session, and later a session registers as B
- **THEN** that session receives the `done`

### Requirement: Acknowledgement
A message SHALL stay pending for a recipient until that recipient acknowledges it by id. Only a recipient of a message SHALL be able to acknowledge it. A pushed notification SHALL NOT count as an acknowledgement.

#### Scenario: Acknowledge
- **WHEN** agent B acknowledges a message it received
- **THEN** the message no longer appears in B's pending list

#### Scenario: Acknowledge someone else's message
- **WHEN** agent C acknowledges a message addressed only to B
- **THEN** the daemon refuses it and the message stays pending for B

### Requirement: Pending list
An agent SHALL be able to list its unacknowledged messages. The list SHALL show direct messages before broadcasts and older messages before newer ones within each group.

#### Scenario: Order
- **WHEN** B has two broadcasts and one direct message pending, the direct one newest
- **THEN** the pending list shows the direct message first, then the two broadcasts oldest first

### Requirement: Redelivery
A message SHALL be pushed to a session at most once. A new session of the same agent SHALL receive the agent's pending messages again as pushes, within the budgets of `inbound-safety`.

#### Scenario: Same session
- **WHEN** a message was pushed to B's session and B has not acknowledged it
- **THEN** that session does not receive a second push of it

#### Scenario: New session
- **WHEN** B's session closes with a message pending and a new session registers as B
- **THEN** the new session receives a push of that message

### Requirement: Presence
When an agent registers on a topic, the daemon SHALL send an `online` message on that topic, at most once per hour per agent and topic. A new `online` SHALL replace the unacknowledged `online` of the same sender on that topic, so a recipient holds at most one `online` per sender and topic. An agent SHALL be able to list the agents known on its topics and whether each one has a live session.

#### Scenario: Agent comes online
- **WHEN** a session registers as B on a topic that A follows
- **THEN** A receives an `online` message from B

#### Scenario: Quick restart
- **WHEN** B registers again 10 minutes after its last `online` on that topic
- **THEN** the daemon sends no new `online`

#### Scenario: Online not acknowledged
- **WHEN** B registers on a topic once an hour for 25 hours and A never acknowledges
- **THEN** A holds one `online` from B on that topic

### Requirement: Retention
The daemon SHALL delete every message 30 days after it was created, acknowledged or not. An agent SHALL stay known on a topic while it has a live session, and SHALL no longer be known 30 days after its last session on that topic ended.

#### Scenario: Long session
- **WHEN** an agent's session stays connected for 31 days and another agent sends it a direct message
- **THEN** the daemon accepts the message

#### Scenario: Old message
- **WHEN** a message is 31 days old
- **THEN** it appears in no pending list and cannot be acknowledged
