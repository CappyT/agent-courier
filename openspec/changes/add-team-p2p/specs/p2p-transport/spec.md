# Spec Delta

## Purpose

Connects the daemons of team members over encrypted, authenticated libp2p connections: directly when possible, through a relay otherwise, and only for peers in the member list of a shared team.

## ADDED Requirements

### Requirement: Network only for team members
The daemon SHALL start the libp2p host only when the machine belongs to at least one team. A machine with no team SHALL open no network socket.

#### Scenario: No team
- **WHEN** the daemon starts on a machine that belongs to no team
- **THEN** it listens only on the local socket

### Requirement: Authenticated connections
Daemons SHALL talk over libp2p connections secured with Noise. A daemon SHALL accept a stream on the courier protocol only from a peer id in the member list of a team it shares, except the join protocol, which accepts only a valid invite secret.

#### Scenario: Member connects
- **WHEN** member B opens a courier stream to member A
- **THEN** A accepts it

#### Scenario: Stranger connects
- **WHEN** a peer outside every member list opens a courier stream to A
- **THEN** A resets the stream before it reads a message

### Requirement: Reachability
A daemon SHALL find members through the addresses in its member list and through the libp2p DHT, try a direct connection, use hole punching behind NAT, and use a circuit relay when a direct path fails: the project relay first, public libp2p relays after it.

#### Scenario: Both behind NAT
- **WHEN** A and B are behind NATs that defeat hole punching
- **THEN** they exchange messages through a relay from the list, and the relay sees only Noise traffic

### Requirement: Stream limits
A daemon SHALL cap the size of a frame on the courier protocol and the number of open courier streams per peer, and SHALL reset a stream that passes either cap.

#### Scenario: Oversized frame
- **WHEN** a member sends a frame larger than the cap
- **THEN** the receiving daemon resets the stream and stores nothing from it
