//go:build !unix

package main

import (
	"fmt"
	"io"
)

func runMCP(_ io.Reader, _, stderr io.Writer) int {
	fmt.Fprintln(stderr, "agent-courier: the MCP server runs only on unix systems")
	return 1
}
