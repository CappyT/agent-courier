# relay-node Specification

## Purpose
Defines the relay that the project runs as the default for members whose daemons cannot connect directly.

## Requirements

### Requirement: Relay mode
The binary SHALL have a `relay` subcommand that runs a libp2p circuit relay v2 and nothing else. It SHALL keep no messages and no member data, and it SHALL log only connection and reservation counts.

#### Scenario: Run the relay
- **WHEN** an operator runs `agent-courier relay`
- **THEN** it serves circuit relay reservations and its logs hold no peer ids

### Requirement: Stable identity
The relay SHALL load its key from a file given at start, so its peer id and its address in the default relay list stay the same across restarts.

#### Scenario: Restart
- **WHEN** the relay pod restarts with the same key file
- **THEN** its peer id is unchanged

### Requirement: Resource limits
The relay SHALL cap reservations, relayed connections per peer, and the duration and bytes of each relayed connection, with values set by flags.

#### Scenario: Reservation cap
- **WHEN** reservations reach the cap
- **THEN** the relay refuses new reservations and keeps the existing ones

### Requirement: What the relay sees
The relay SHALL see the peer ids and IP addresses of the peers it relays and the volume of traffic, and SHALL NOT be able to read or change the courier messages, which are encrypted end to end with Noise.

#### Scenario: Relayed message
- **WHEN** two members exchange a message through the relay
- **THEN** the relay forwards only Noise ciphertext
