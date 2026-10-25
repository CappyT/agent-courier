# Spec Delta

## Purpose

Carries messages on team topics between the daemons of team members, so local and remote agents exchange the same messages with the same guarantees while the team lives.

## ADDED Requirements

### Requirement: Remote addresses
An agent on another machine SHALL appear as `<member label>/<agent name>`. A recipient of that form SHALL reach only that agent on that member's machine. A recipient without a label SHALL mean a local agent.

#### Scenario: Address a remote agent
- **WHEN** a session on A sends to `bob/claude-agent-courier` on a team topic
- **THEN** only the agent `claude-agent-courier` on Bob's machine receives it

### Requirement: Team topic delivery
A message sent to a team topic SHALL go to the local sessions on that topic and to every other member of the team. The receiving daemon SHALL take the sender's label from the member list entry of the connection's peer id, never from the message body.

#### Scenario: Sent to a team
- **WHEN** a session on A sends a message to a team topic of A, B and C
- **THEN** B and C each deliver it to their sessions on that topic, with the sender `<A's label>/<session name>`

#### Scenario: Topic outside the team
- **WHEN** a member sends a message on a topic that is not a topic of a team both members share
- **THEN** the receiving daemon drops it

### Requirement: Same local rules
A message from a remote member SHALL go through the validation, rendering, budgets, pending caps, acknowledgement and redelivery of the local mode, exactly as a local message.

#### Scenario: Remote budget
- **WHEN** a remote member exceeds the push budget per sender
- **THEN** the daemon stores the extra messages as pending and does not push them

### Requirement: Send queue
A message for a member that is not reachable SHALL wait in a queue in the sender's memory. The daemon SHALL retry with backoff while both machines are in the team, and SHALL drop the queued messages for a member when that member leaves or the team ends.

#### Scenario: Member briefly offline
- **WHEN** a session sends to a team while B's connection is down, and B reconnects 10 minutes later
- **THEN** B receives the message

#### Scenario: Member left
- **WHEN** B leaves the team while A holds queued messages for B
- **THEN** A drops them

### Requirement: Delivery receipt
A receiving daemon SHALL send a receipt when it has stored a message. The sending daemon SHALL remove the message from its queue only on a receipt. A receiving daemon SHALL store each message once per sending member.

#### Scenario: Receipt lost
- **WHEN** B stores a message and the receipt never reaches A
- **THEN** A sends the message again and B stores it only once

### Requirement: Remote presence
When two members connect, each daemon SHALL send the other the agents online on their shared team topics. `agents` SHALL list remote agents with their member label.

#### Scenario: Member comes online
- **WHEN** Bob's daemon connects to A with an OpenCode session open on a shared topic
- **THEN** `agents` on A lists `bob/opencode-agent-courier` as online
