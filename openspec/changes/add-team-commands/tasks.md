# Tasks

## 1. Team slash commands (#46)

- [x] 1.1 Add the skill `skills/invite/SKILL.md`, user-only, that runs `agent-courier team invite`, and verify `claude plugin validate .` passes
- [x] 1.2 Add the skill `skills/join/SKILL.md`, user-only, that runs `agent-courier team join`, and verify `claude plugin validate .` passes
- [x] 1.3 Document both commands in README.md, and verify a manual run of each command in a Claude Code session with the plugin from this checkout
- [x] 1.4 Bump the version to 0.5.1 in the three places, and verify `go test -race ./...` passes in the development container
