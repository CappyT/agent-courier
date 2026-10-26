# Design

## Skills, not Go code

Each command is a plugin skill in `skills/<name>/SKILL.md`. Claude Code exposes a plugin skill as `/agent-courier:<name>`. The skill tells the agent to run the existing CLI with Bash. The CLI already validates, talks to the daemon and formats the result, so no Go code changes.

## Only the user starts them

The frontmatter sets `disable-model-invocation: true`. An invite code admits a machine to the team, and a join adds this machine to another team. A courier message is data and must never lead to either, so the model cannot call these skills. `allowed-tools` grants only `Bash(agent-courier team invite:*)`, `Bash(agent-courier team join:*)` and `Bash(agent-courier team list:*)` as needed by each skill.

## Arguments before the shell

`$ARGUMENTS` is text the user pasted, and an invite code often comes from another person. The skill checks each field against its alphabet, the same rules as the CLI, before it builds the command, and passes each field in single quotes. A code keeps only `a-z` and `2-7` after white space is removed. The CLI accepts the whole output of `team invite`, but the skill accepts only the code, so the shell never sees other text.

## No team argument

`/agent-courier:invite` with no argument runs `agent-courier team list`. One team: the skill invites to it. Several: the skill asks the user which one. None: the skill tells the user to create one with `agent-courier team create`, and makes no code.
