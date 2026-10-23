// Command agent-courier is a Claude Code channel: an MCP server over stdio
// that pushes messages from other people's agents into the session and
// publishes the session's own.
package main

import (
	"cmp"
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
	// COURIER_REPOS is the name before topics replaced repos.
	topics := cmp.Or(os.Getenv("COURIER_TOPICS"), os.Getenv("COURIER_REPOS"))
	for t := range strings.FieldsFuncSeq(topics, func(c rune) bool { return c == ',' || c == ' ' }) {
		cfg.Topics = append(cfg.Topics, t)
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

	if len(cfg.Topics) == 0 {
		// Claude Code starts an MCP server in the project directory.
		dir, err := os.Getwd()
		if err != nil {
			return cfg, err
		}
		topic := detectTopic(dir)
		if !courier.ValidToken(topic) {
			return cfg, fmt.Errorf("topic %q detected in %s is not valid: set COURIER_TOPICS", topic, dir)
		}
		cfg.Topics = []string{topic}
	}
	return cfg, nil
}

// detectTopic names the repository of dir in lower case: owner_repo from the
// upstream or origin URL, else the git toplevel directory, else dir itself.
// A fork has its own owner in origin, so upstream comes first. A worktree
// lives in a directory with another name, so the URLs come first.
func detectTopic(dir string) string {
	for _, remote := range []string{"upstream", "origin"} {
		if out, err := exec.Command("git", "-C", dir, "remote", "get-url", remote).Output(); err == nil {
			if topic := topicFromURL(string(out)); topic != "" {
				return topic
			}
		}
	}
	if out, err := exec.Command("git", "-C", dir, "rev-parse", "--show-toplevel").Output(); err == nil {
		dir = strings.TrimSpace(string(out))
	}
	return tokenize(filepath.Base(dir))
}

// topicFromURL takes owner_repo from an https, ssh or scp-style remote URL
// such as git@github.com:owner/name.git. An owner has no "_", so the first
// one splits the topic again. A URL without an owner gives the name alone.
func topicFromURL(url string) string {
	url = strings.TrimSuffix(strings.TrimRight(strings.TrimSpace(url), "/"), ".git")
	rest, name := split(url)
	if rest == "" {
		return tokenize(name)
	}
	// The segment before the name is the owner, unless it is the host: the
	// first segment of an scp URL, or the one after the "//" of a scheme.
	if before, owner := split(rest); before != "" && !strings.HasSuffix(before, "/") {
		name = owner + "_" + name
	}
	return tokenize(name)
}

// tokenize lowers s and replaces each character that a subject token
// cannot hold, such as the "." of a repository name, with "-".
func tokenize(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 128 && courier.ValidToken(string(r)) {
			return r
		}
		return '-'
	}, strings.ToLower(s))
}

// split cuts s at its last "/" or ":".
func split(s string) (before, last string) {
	i := strings.LastIndexAny(s, "/:")
	if i < 0 {
		return "", s
	}
	return s[:i], s[i+1:]
}
