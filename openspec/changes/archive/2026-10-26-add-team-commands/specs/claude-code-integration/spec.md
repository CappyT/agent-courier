# Spec Delta

## ADDED Requirements

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
