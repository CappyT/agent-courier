// Command agent-courier carries messages between coding agents.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/CappyT/agent-courier/internal/version"
)

const usage = `usage: agent-courier <command>

commands:
  daemon   run the local daemon (started on demand by the clients)
  mcp      run the MCP server for Claude Code
  bridge   connect stdin and stdout to the daemon (started by the OpenCode plugin)
  team     create, list, remove from and leave teams
  relay    run a circuit relay node for team members (see relay --help)
  version  print the version
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "daemon":
		return runDaemon(stderr)
	case "mcp":
		return runMCP(os.Stdin, stdout, stderr)
	case "bridge":
		return runBridge(os.Stdin, stdout, stderr)
	case "relay":
		return runRelay(args[1:], stdout, stderr)
	case "team":
		return runTeam(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintln(stdout, version.Version)
		return 0
	case "-h", "--help", "help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "agent-courier: unknown command %q\n\n%s", args[0], usage)
		return 2
	}
}
