// Command agent-courier is a Claude Code channel: an MCP server over stdio
// that pushes handoff events from NATS into the session and publishes the
// session's own events.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/CappyT/agent-courier/internal/courier"
)

func main() {
	// stdout carries the MCP protocol: logs go to stderr only.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("agent-courier", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	bus, err := courier.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer bus.Close()

	return courier.ServeStdio(ctx, courier.NewServer(ctx, bus), os.Stdin, os.Stdout)
}

func configFromEnv() (courier.Config, error) {
	cfg := courier.Config{
		URL:      os.Getenv("COURIER_NATS_URL"),
		NkeyFile: os.Getenv("COURIER_NKEY_FILE"),
		Login:    os.Getenv("COURIER_LOGIN"),
		AckWait:  30 * time.Minute,
	}
	for r := range strings.FieldsFuncSeq(os.Getenv("COURIER_REPOS"), func(c rune) bool { return c == ',' || c == ' ' }) {
		cfg.Repos = append(cfg.Repos, r)
	}
	if v := os.Getenv("COURIER_ACK_WAIT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("COURIER_ACK_WAIT: %w", err)
		}
		cfg.AckWait = d
	}
	var missing []string
	for name, v := range map[string]string{
		"COURIER_NATS_URL":  cfg.URL,
		"COURIER_NKEY_FILE": cfg.NkeyFile,
		"COURIER_LOGIN":     cfg.Login,
	} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(cfg.Repos) == 0 {
		missing = append(missing, "COURIER_REPOS")
	}
	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing environment: %s", strings.Join(missing, ", "))
	}
	return cfg, nil
}
