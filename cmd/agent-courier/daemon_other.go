//go:build !unix

package main

import (
	"fmt"
	"io"
)

func runDaemon(stderr io.Writer) int {
	fmt.Fprintln(stderr, "agent-courier: the daemon runs only on unix systems")
	return 1
}
