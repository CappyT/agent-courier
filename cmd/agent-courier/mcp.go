//go:build unix

package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/CappyT/agent-courier/internal/client"
	"github.com/CappyT/agent-courier/internal/mcpserver"
)

// runMCP serves the Claude Code client on stdin and stdout. stdout carries
// only the MCP protocol, so the logs go to stderr.
func runMCP(stdin io.Reader, stdout, stderr io.Writer) int {
	log := slog.New(slog.NewTextHandler(stderr, nil))
	cwd, err := os.Getwd()
	if err != nil {
		log.Error("no working directory", "err", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := mcpserver.Config{Params: client.ParamsFromEnv(mcpserver.Harness, cwd), Log: log}
	if err := mcpserver.Serve(ctx, cfg, stdin, stdout); err != nil && ctx.Err() == nil {
		log.Error("mcp server stopped", "err", err)
		return 1
	}
	return 0
}
