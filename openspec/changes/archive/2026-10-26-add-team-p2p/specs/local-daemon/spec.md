# Spec Delta

## MODIFIED Requirements

### Requirement: Idle exit
The daemon SHALL exit when no client has been connected for a grace period and the machine belongs to no team. A client that connects during the grace period SHALL cancel the exit. While the machine belongs to a team, the daemon SHALL keep running without clients, because its teams live only in its memory.

#### Scenario: Last session closes
- **WHEN** the last connected client disconnects, the machine belongs to no team, and no client connects during the grace period
- **THEN** the daemon exits

#### Scenario: Session restarts within the grace period
- **WHEN** the last client disconnects and a new client connects before the grace period ends
- **THEN** the daemon keeps running and serves the new client

#### Scenario: Member of a team
- **WHEN** the last client disconnects while the machine belongs to a team
- **THEN** the daemon keeps running until the machine leaves its last team, then the grace period starts
