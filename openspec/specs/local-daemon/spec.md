# local-daemon Specification

## Purpose
Runs one courier daemon per user on a machine, which owns all courier state and serves the harness clients of that user over a local socket.

## Requirements

### Requirement: On-demand start
A client that finds no running daemon SHALL start one and connect to it. A client SHALL NOT require the user to start, install or configure a service first.

#### Scenario: First session starts the daemon
- **WHEN** a harness client starts and no daemon runs for the user
- **THEN** the client starts a daemon, connects to it, and its tools work

#### Scenario: Later sessions reuse the daemon
- **WHEN** a second harness client starts while a daemon runs
- **THEN** the client connects to the running daemon and no second daemon starts

### Requirement: Single instance
At most one daemon SHALL serve a user on a machine. When several clients start a daemon at the same time, exactly one daemon SHALL keep running and every client SHALL connect to it.

#### Scenario: Concurrent start
- **WHEN** three clients start at the same moment and no daemon runs
- **THEN** exactly one daemon runs afterwards and all three clients are connected to it

### Requirement: Local access boundary
The daemon SHALL accept local connections only from processes of the same user on the same machine. While the user belongs to no team, it SHALL open no network listener.

#### Scenario: Another user connects
- **WHEN** a process of another local user tries to connect to the daemon socket
- **THEN** the operating system refuses the connection

#### Scenario: No network port
- **WHEN** the daemon runs and the user belongs to no team
- **THEN** it listens on no TCP or UDP port

### Requirement: Idle exit
The daemon SHALL exit when no client has been connected for a grace period. A client that connects during the grace period SHALL cancel the exit.

#### Scenario: Last session closes
- **WHEN** the last connected client disconnects and no client connects during the grace period
- **THEN** the daemon exits

#### Scenario: Session restarts within the grace period
- **WHEN** the last client disconnects and a new client connects before the grace period ends
- **THEN** the daemon keeps running and serves the new client

### Requirement: Persistence across restarts
Messages that wait for an acknowledgement SHALL survive a daemon exit, a crash and a reboot. A daemon that starts again SHALL deliver them to their agents.

#### Scenario: Pending message after a restart
- **WHEN** an agent has an unacknowledged message, the daemon exits, and the agent's session connects again later
- **THEN** the new daemon delivers that message to the agent
