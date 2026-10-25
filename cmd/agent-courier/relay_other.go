//go:build !unix

package main

import (
	"fmt"
	"io"
)

func runRelay(_ []string, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agent-courier: the relay runs only on unix systems")
	return 1
}
