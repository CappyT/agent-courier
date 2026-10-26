//go:build unix

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/CappyT/agent-courier/internal/client"
	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/team"
)

const teamUsage = `usage: agent-courier team <command>

commands:
  create <team> --topic <topic> [--topic <topic>...] [--label <label>]
          create a team with this machine as its only member; the label
          defaults to the host name
  list [<team>]
          list the teams of this machine and their members, or every
          member of one team
  leave <team>
          leave a team: this machine tells the members it can reach
          and forgets the team
  invite <team>
          print an invite code for another machine; it works once,
          within 24 hours, while this machine's daemon runs and is
          in the team; stdout gets only the code, stderr the notes
  join <code> [--label <label>]
          join the team of an invite code from a member; the label
          defaults to the host name
          <code> may be wrapped, spaced, or the whole invite output

A team lives only in the memory of this machine's daemon: the daemon
runs while the machine is in a team, and forgets its teams when it
stops. To come back, join again with a new invite code.
`

// teamTimeout bounds one team command, the start of the daemon included,
// and joinTimeout a join, which dials the inviter.
const (
	teamTimeout = 15 * time.Second
	joinTimeout = 90 * time.Second
)

// codeMethodNotFound is the JSON-RPC code of a daemon that lacks a method.
const codeMethodNotFound = -32601

// hostname is a variable so tests can give any host name.
var hostname = os.Hostname

type teamMember struct {
	Label  string `json:"label"`
	PeerID string `json:"peer_id"`
}

type teamInfo struct {
	Name         string       `json:"name"`
	Topics       []string     `json:"topics"`
	Members      []teamMember `json:"members"`
	MembersTotal int          `json:"members_total"`
}

type inviteInfo struct {
	Team    string `json:"team"`
	Code    string `json:"code"`
	Expires string `json:"expires"`
}

type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

func runTeam(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, teamUsage)
		return 2
	}
	fs := flag.NewFlagSet("team "+args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var topics stringList
	var label string
	wantArgs := 0
	switch args[0] {
	case "create":
		fs.Var(&topics, "topic", "")
		fs.StringVar(&label, "label", "", "")
		wantArgs = 1
	case "list":
	case "leave", "invite":
		wantArgs = 1
	case "join":
		fs.StringVar(&label, "label", "", "")
		wantArgs = 1
	case "-h", "--help", "help":
		fmt.Fprint(stdout, teamUsage)
		return 0
	default:
		fmt.Fprintf(stderr, "agent-courier: unknown team command %q\n\n%s", args[0], teamUsage)
		return 2
	}
	pos, err := parseInterspersed(fs, args[1:])
	switch {
	case err != nil:
	case args[0] == "list" && len(pos) > 1:
		err = errors.New("team list takes 0 or 1 arguments")
	case args[0] != "list" && len(pos) != wantArgs:
		err = fmt.Errorf("team %s takes %d arguments", args[0], wantArgs)
	}
	if err != nil {
		fmt.Fprintf(stderr, "agent-courier: %v\n\n%s", err, teamUsage)
		return 2
	}

	var method string
	var params any
	switch args[0] {
	case "create", "join":
		if label == "" {
			if label, err = hostLabel(); err != nil {
				fmt.Fprintf(stderr, "agent-courier: %v\n", err)
				return 2
			}
		}
	}
	timeout := teamTimeout
	switch args[0] {
	case "create":
		method, params = "team.create", map[string]any{"team": pos[0], "topics": []string(topics), "label": label}
	case "list":
		method = "team.list"
		if len(pos) == 1 {
			params = map[string]any{"team": pos[0]}
		}
	case "leave":
		method, params = "team.leave", map[string]any{"team": pos[0]}
	case "invite":
		method, params = "team.invite", map[string]any{"team": pos[0]}
	case "join":
		code, err := p2p.FindInviteCode(pos[0])
		if err != nil {
			fmt.Fprintf(stderr, "agent-courier: %v\n", err)
			return 1
		}
		method, params, timeout = "team.join", map[string]any{"code": code, "label": label}, joinTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var res struct {
		Team   *teamInfo   `json:"team"`
		Teams  []teamInfo  `json:"teams"`
		Text   string      `json:"text"`
		Invite *inviteInfo `json:"invite"`
	}
	if err := callDaemon(ctx, method, params, &res); err != nil {
		if rerr, ok := errors.AsType[*client.RPCError](err); ok && rerr.Code == codeMethodNotFound {
			err = errors.New("the running daemon is older than this command and has no team methods: stop it, and the next command starts the new one")
		}
		fmt.Fprintf(stderr, "agent-courier: %v\n", err)
		return 1
	}
	switch {
	case res.Invite != nil:
		// Only the code goes to stdout, so a pipe gets it alone.
		fmt.Fprintf(stderr, "Invite code for team %s. It works once, until %s, while this machine's daemon runs:\n", res.Invite.Team, res.Invite.Expires)
		fmt.Fprintln(stdout, res.Invite.Code)
		fmt.Fprintln(stderr, "On the new machine, run agent-courier team join with this code.")
	case res.Text != "":
		fmt.Fprintln(stdout, res.Text)
	case res.Team != nil:
		printTeam(stdout, *res.Team)
	case len(res.Teams) == 0:
		fmt.Fprintln(stdout, "This machine belongs to no team.")
	default:
		for _, t := range res.Teams {
			printTeam(stdout, t)
		}
	}
	return 0
}

// parseInterspersed parses flags before, between and after the positional
// arguments, which flag.FlagSet alone stops at.
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

// hostLabel is the first part of the host name in lower case, when it is a
// valid member label.
func hostLabel() (string, error) {
	h, err := hostname()
	if err != nil {
		return "", fmt.Errorf("pass --label: no host name: %w", err)
	}
	h, _, _ = strings.Cut(strings.ToLower(h), ".")
	if err := team.CheckLabel(h); err != nil {
		return "", fmt.Errorf("pass --label: the host name is not a valid label: %w", err)
	}
	return h, nil
}

// callDaemon makes one call on a new connection to the daemon, which Dial
// starts when none listens.
func callDaemon(ctx context.Context, method string, params, result any) error {
	nc, err := client.Dial(ctx)
	if err != nil {
		return err
	}
	cn := client.NewConn(nc, func(client.Deliver) {})
	defer cn.Close()
	if err := cn.Call(ctx, method, params, result); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return client.ErrUnreachable
		}
		return err
	}
	return nil
}

func printTeam(w io.Writer, t teamInfo) {
	fmt.Fprintf(w, "%s\n  topics: %s\n  members:\n", t.Name, strings.Join(t.Topics, ", "))
	for _, m := range t.Members {
		fmt.Fprintf(w, "    %s  %s\n", m.Label, m.PeerID)
	}
	if more := t.MembersTotal - len(t.Members); more > 0 {
		fmt.Fprintf(w, "    and %d more: agent-courier team list %s shows them all\n", more, t.Name)
	}
}
