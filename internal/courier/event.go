// Package courier carries handoff events between the agent orchestrators of
// different people over NATS JetStream.
package courier

import (
	"cmp"
	"encoding/json/v2"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// SubjectPrefix is the first token of every event subject:
// courier.<repo>.<sender>.<kind>.
const SubjectPrefix = "courier"

// MaxSummary caps the summary so one event cannot flood a session.
const MaxSummary = 2000

// Kinds are the event kinds an orchestrator can publish.
var Kinds = []string{"handoff", "done", "blocked", "note"}

var token = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Event is one handoff signal. The issue tracker stays the source of truth:
// an event tells the receiver to look, it does not carry state.
type Event struct {
	Repo    string    `json:"repo"`
	Sender  string    `json:"sender"`
	Kind    string    `json:"kind"`
	Issue   int       `json:"issue,omitzero"`
	To      string    `json:"to,omitempty"`
	Summary string    `json:"summary"`
	URL     string    `json:"url,omitempty"`
	Time    time.Time `json:"time,omitzero"`
}

// ValidToken reports whether s can be one NATS subject token.
func ValidToken(s string) bool { return token.MatchString(s) }

// Validate checks the fields a publisher sets.
func (e Event) Validate() error {
	for name, v := range map[string]string{"repo": e.Repo, "sender": e.Sender, "kind": e.Kind} {
		if !ValidToken(v) {
			return fmt.Errorf("%s %q must match %s", name, v, token)
		}
	}
	if !slices.Contains(Kinds, e.Kind) {
		return fmt.Errorf("kind %q must be one of %s", e.Kind, strings.Join(Kinds, ", "))
	}
	if e.To != "" && !ValidToken(e.To) {
		return fmt.Errorf("to %q must match %s", e.To, token)
	}
	if strings.TrimSpace(e.Summary) == "" {
		return fmt.Errorf("summary is empty")
	}
	if len(e.Summary) > MaxSummary {
		return fmt.Errorf("summary is %d bytes, the limit is %d", len(e.Summary), MaxSummary)
	}
	return nil
}

// Subject is where the event is published.
func (e Event) Subject() string {
	return strings.Join([]string{SubjectPrefix, e.Repo, e.Sender, e.Kind}, ".")
}

// RepoFilter is the subject filter that matches every event of one repo.
func RepoFilter(repo string) string { return SubjectPrefix + "." + repo + ".>" }

// Decode reads an event received on subject. Repo, sender and kind come from
// the subject, never from the payload: the server enforces the sender token
// with per-user publish permissions, so the payload cannot claim another one.
func Decode(subject string, data []byte) (Event, error) {
	parts := strings.Split(subject, ".")
	if len(parts) != 4 || parts[0] != SubjectPrefix {
		return Event{}, fmt.Errorf("subject %q is not %s.<repo>.<sender>.<kind>", subject, SubjectPrefix)
	}
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, fmt.Errorf("payload on %s: %w", subject, err)
	}
	e.Repo, e.Sender, e.Kind = parts[1], parts[2], parts[3]
	if len(e.Summary) > MaxSummary {
		e.Summary = e.Summary[:MaxSummary]
	}
	return e, nil
}

// Notification renders the event as the content and meta of a Claude Code
// channel notification. seq is the stream sequence the agent passes to ack.
func (e Event) Notification(seq uint64) (string, map[string]string) {
	to := cmp.Or(e.To, "everyone")
	ref := e.Repo
	if e.Issue != 0 {
		ref += "#" + strconv.Itoa(e.Issue)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s -> %s: %s on %s\n", e.Sender, to, e.Kind, ref)
	b.WriteString(escape(e.Summary))
	if e.URL != "" {
		b.WriteString("\n" + escape(e.URL))
	}
	meta := map[string]string{
		"repo":   e.Repo,
		"sender": e.Sender,
		"kind":   e.Kind,
		"to":     to,
		"seq":    strconv.FormatUint(seq, 10),
	}
	if e.Issue != 0 {
		meta["issue"] = strconv.Itoa(e.Issue)
	}
	return b.String(), meta
}

// escape keeps text written by another person's agent inside the <channel>
// tag that Claude Code wraps around the content.
func escape(s string) string {
	return strings.NewReplacer("<", "&lt;", ">", "&gt;").Replace(s)
}
