# peer-identity Specification

## Purpose
Gives each running daemon a cryptographic identity, so other members can authenticate its connections, and a label people use to address it.

## Requirements

### Requirement: Ephemeral key
The daemon SHALL create an ed25519 key in memory when its first team starts, and SHALL never write it to disk. The peer id SHALL stay the same while the daemon runs, and SHALL change when the daemon starts again.

#### Scenario: First team
- **WHEN** the user creates or joins the first team
- **THEN** the daemon creates a key in memory, and no key file exists on disk

#### Scenario: Restart
- **WHEN** the daemon exits and starts again
- **THEN** it has a new peer id, and it rejoins a team only with a new invite

### Requirement: Member label
Each machine in a team SHALL have a label, unique in that team, that matches `^[a-z0-9][a-z0-9-]{0,31}$`. A person with two machines SHALL use two labels.

#### Scenario: Label shown on messages
- **WHEN** a member with the label `bob` sends a message from the agent `claude`
- **THEN** the other members show the sender as `bob/claude`
