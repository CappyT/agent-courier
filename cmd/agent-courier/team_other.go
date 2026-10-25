//go:build !unix

package main

import (
	"fmt"
	"io"
)

func runTeam(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agent-courier: teams need the daemon, which runs only on unix systems")
	return 1
}
