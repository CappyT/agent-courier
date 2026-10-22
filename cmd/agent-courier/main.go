// Command agent-courier is a Claude Code channel: an MCP server over stdio
// that pushes handoff events from NATS into the session and publishes the
// session's own events.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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
		"COURIER_NATS_URL": cfg.URL,
		"COURIER_LOGIN":    cfg.Login,
	} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing environment: %s", strings.Join(missing, ", "))
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return cfg, err
	}
	if cfg.NkeyFile == "" {
		cfg.NkeyFile = filepath.Join(home, ".config", "nats", "agent-courier", cfg.Login+".nk")
	} else if rest, ok := strings.CutPrefix(cfg.NkeyFile, "~/"); ok {
		cfg.NkeyFile = filepath.Join(home, rest)
	}

	if len(cfg.Repos) == 0 {
		// Claude Code starts an MCP server in the project directory.
		dir, err := os.Getwd()
		if err != nil {
			return cfg, err
		}
		repo := detectRepo(dir)
		if !courier.ValidToken(repo) {
			return cfg, fmt.Errorf("repo %q detected in %s is not a valid name: set COURIER_REPOS", repo, dir)
		}
		cfg.Repos = []string{repo}
	}
	return cfg, nil
}

// detectRepo names the repository of dir: the name in the origin URL, else
// the git toplevel directory, else dir itself. A worktree lives in a
// directory with another name, so the origin URL comes first.
func detectRepo(dir string) string {
	if out, err := exec.Command("git", "-C", dir, "remote", "get-url", "origin").Output(); err == nil {
		if name := repoFromURL(string(out)); name != "" {
			return name
		}
	}
	if out, err := exec.Command("git", "-C", dir, "rev-parse", "--show-toplevel").Output(); err == nil {
		dir = strings.TrimSpace(string(out))
	}
	return filepath.Base(dir)
}

// repoFromURL takes the repository name from an https, ssh or scp-style
// remote URL such as git@github.com:owner/name.git.
func repoFromURL(url string) string {
	url = strings.TrimSuffix(strings.TrimRight(strings.TrimSpace(url), "/"), ".git")
	return url[strings.LastIndexAny(url, "/:")+1:]
}
