# Spec Delta

## Purpose

Defines who belongs to a team: an ephemeral group of trusted machines that lives in memory, grows by invite codes from any member, and ends when its last member leaves.

## ADDED Requirements

### Requirement: Ephemeral team
A team SHALL have a name, a set of topics and a member list, each member a label and a peer id. A team SHALL live only in the memory of the daemons of its members. A daemon that exits SHALL forget its teams, and its machine SHALL need a new invite to join again.

#### Scenario: Create a team
- **WHEN** a user creates the team `courier` with the topic `cappyt_agent-courier`
- **THEN** the member list holds only this machine, and the topic is a team topic

#### Scenario: Daemon restarts
- **WHEN** the daemon of a member exits and starts again
- **THEN** it belongs to no team, and the other members drop it from their member list when they hear it left or it does not come back

### Requirement: Trusted members
Every member of a team SHALL be trusted for its identity and for the member list. A daemon SHALL accept a member list update from any member of that team, and SHALL refuse it from a peer outside the team.

#### Scenario: Member announces a new member
- **WHEN** A invites B and C is a member
- **THEN** C adds B to its member list when it hears it from A or from B

#### Scenario: Outsider sends a member list
- **WHEN** a peer outside the team sends a member list
- **THEN** the member ignores it

### Requirement: Invite code
Any member SHALL be able to create an invite code for a team. A code SHALL work once, SHALL expire 24 hours after creation or when its inviter leaves the team, and SHALL carry what the joiner needs to reach the inviter and a secret of at least 128 bits.

#### Scenario: Code used twice
- **WHEN** a second machine joins with a code that a first machine already used
- **THEN** the join fails

#### Scenario: Expired code
- **WHEN** a machine joins with a code created 25 hours ago
- **THEN** the join fails with an error that says the code expired

### Requirement: Join
A machine with a valid code SHALL join while the inviter's daemon runs. After the join, the joiner SHALL hold the inviter's member list, and the inviter SHALL hold the joiner. A machine SHALL join only with a code: no other way adds a member.

#### Scenario: Successful join
- **WHEN** machine B joins with a code from A, in a team where A already knows C
- **THEN** B's member list holds A, B and C, and A's member list holds B

#### Scenario: Inviter offline
- **WHEN** machine B joins with a valid code while A's daemon is not running
- **THEN** the join fails with an error that says the inviter is not reachable

#### Scenario: Label taken
- **WHEN** a machine joins with a label that a member connected to the inviter holds
- **THEN** the join fails with an error that names the label

#### Scenario: Rejoin after a restart
- **WHEN** B's daemon restarts and B joins again with its old label, while the old peer id is not connected to the inviter
- **THEN** the inviter marks the old peer id left and admits B under its new peer id

### Requirement: Leave
A member SHALL be able to leave a team. The daemon SHALL forget the team at once and tell the members it can reach. The other members SHALL drop the leaver and SHALL NOT add it back from an older member list. A team SHALL end when its last member leaves. There SHALL be no command to remove another member: to exclude a machine, the members leave and create a new team.

#### Scenario: Member leaves
- **WHEN** B leaves a team of A, B and C
- **THEN** A and C drop B, and B gets no more messages from that team

#### Scenario: Stale list after a leave
- **WHEN** C sends A a member list that still holds B, after A heard B leave
- **THEN** A keeps B out
