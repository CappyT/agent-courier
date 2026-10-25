//go:build !unix

package main

import (
	"fmt"
	"io"
)

func runBridge(_ io.Reader, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agent-courier: the bridge runs only on unix systems")
	return 1
}
