# Proposal

## Why

A person who works in Claude Code has no command to invite a machine to a team or to join one. They leave the session for a shell and type `agent-courier team invite` or `agent-courier team join`. The plugin adds two slash commands for these steps.

## What Changes

- The plugin adds the skill `invite`, called as `/agent-courier:invite [team]`. It runs `agent-courier team invite` and shows the code and its expiry. With no team argument, it picks the only team of the machine, or asks which one.
- The plugin adds the skill `join`, called as `/agent-courier:join <code> [--label <label>]`. It runs `agent-courier team join` and shows the members of the team.
- Only the user starts these commands: the model cannot call them. An invite code and a join change the trust of the machine, so a peer message must never trigger them.
- The skills check each argument against the alphabet of its field before it reaches the shell.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `claude-code-integration`: the plugin carries the team slash commands.

## Impact

- New files `skills/invite/SKILL.md` and `skills/join/SKILL.md`.
- README section on the Claude Code plugin.
- Release 0.5.1: `internal/version`, `.claude-plugin/plugin.json` and `opencode/package.json`.
- No Go code and no daemon method changes.
