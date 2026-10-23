// Package courier carries messages between the agents of different people
// over NATS JetStream.
package courier

import (
	"cmp"
	"encoding/json/v2"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// SubjectPrefix is the first token of every event subject:
// courier.<topic>.<sender>.<kind>.
const SubjectPrefix = "courier"

// MaxSummary caps the summary so one event cannot flood a session.
const MaxSummary = 2000

// MaxURL caps the link of an event.
const MaxURL = 300

// Kinds are the event kinds an agent can publish.
var Kinds = []string{"handoff", "done", "blocked", "ask", "reply", "note"}

// KindReply answers the ask whose stream sequence is in Event.Re.
const KindReply = "reply"

// KindOnline announces that an agent listens on a topic. The bus publishes
// it when its consumer starts; the agent cannot.
const KindOnline = "online"

var token = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Event is one message between agents. Shared systems such as the issue
// tracker stay the source of truth: an event points at them, it does not
// carry their state.
type Event struct {
	Topic   string    `json:"topic"`
	Sender  string    `json:"sender"`
	Kind    string    `json:"kind"`
	Issue   int       `json:"issue,omitzero"`
	To      string    `json:"to,omitempty"`
	Re      uint64    `json:"re,omitzero"`
	Summary string    `json:"summary"`
	URL     string    `json:"url,omitempty"`
	Time    time.Time `json:"time,omitzero"`
}

// ValidToken reports whether s can be one NATS subject token.
func ValidToken(s string) bool { return token.MatchString(s) }

// Validate checks the fields a publisher sets.
func (e Event) Validate() error {
	for name, v := range map[string]string{"topic": e.Topic, "sender": e.Sender, "kind": e.Kind} {
		if !ValidToken(v) {
			return fmt.Errorf("%s %q must match %s", name, v, token)
		}
	}
	if !slices.Contains(Kinds, e.Kind) && e.Kind != KindOnline {
		return fmt.Errorf("kind %q must be one of %s", e.Kind, strings.Join(Kinds, ", "))
	}
	if (e.Kind == KindReply) != (e.Re != 0) {
		return fmt.Errorf("re is the seq of the ask a reply answers: set it on a reply and only there")
	}
	if e.To != "" && !ValidToken(e.To) {
		return fmt.Errorf("to %q must match %s", e.To, token)
	}
	if e.Issue < 0 {
		return fmt.Errorf("issue %d must be positive", e.Issue)
	}
	if e.URL != "" {
		if len(e.URL) > MaxURL {
			return fmt.Errorf("url is %d bytes, the limit is %d", len(e.URL), MaxURL)
		}
		if u, err := url.Parse(e.URL); err != nil || u.Scheme != "https" || u.Host == "" {
			return fmt.Errorf("url %q must be an https link", e.URL)
		}
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
	return strings.Join([]string{SubjectPrefix, e.Topic, e.Sender, e.Kind}, ".")
}

// TopicFilter is the subject filter that matches every event of one topic.
func TopicFilter(topic string) string { return SubjectPrefix + "." + topic + ".>" }

// Decode reads an event received on subject. Topic, sender and kind come from
// the subject, never from the payload: the server enforces the sender token
// with per-user publish permissions, so the payload cannot claim another one.
// The sender's client may be modified, so Decode checks every field again.
func Decode(subject string, data []byte) (Event, error) {
	parts := strings.Split(subject, ".")
	if len(parts) != 4 || parts[0] != SubjectPrefix {
		return Event{}, fmt.Errorf("subject %q is not %s.<topic>.<sender>.<kind>", subject, SubjectPrefix)
	}
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, fmt.Errorf("payload on %s: %w", subject, err)
	}
	e.Topic, e.Sender, e.Kind = parts[1], parts[2], parts[3]
	if err := e.Validate(); err != nil {
		return Event{}, fmt.Errorf("event on %s: %w", subject, err)
	}
	return e, nil
}

// Notification renders the event as the content and meta of a Claude Code
// channel notification. seq is the stream sequence the agent passes to ack.
func (e Event) Notification(seq uint64) (string, map[string]string) {
	to := cmp.Or(e.To, "everyone")
	ref := e.Topic
	if e.Issue != 0 {
		ref += "#" + strconv.Itoa(e.Issue)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s -> %s: %s on %s", e.Sender, to, e.Kind, ref)
	if e.Re != 0 {
		fmt.Fprintf(&b, " re seq %d", e.Re)
	}
	// Every summary line starts with "| ", so no line of it can pass for
	// a header line.
	for line := range strings.Lines(clean(e.Summary)) {
		b.WriteString("\n| " + escape(strings.TrimRight(line, "\n")))
	}
	if e.URL != "" {
		b.WriteString("\n" + escape(clean(e.URL)))
	}
	meta := map[string]string{
		"topic":  e.Topic,
		"sender": e.Sender,
		"kind":   e.Kind,
		"to":     to,
		"seq":    strconv.FormatUint(seq, 10),
	}
	if e.Issue != 0 {
		meta["issue"] = strconv.Itoa(e.Issue)
	}
	if e.Re != 0 {
		meta["re"] = strconv.FormatUint(e.Re, 10)
	}
	return b.String(), meta
}

// escape keeps text written by another person's agent inside the <channel>
// tag that Claude Code wraps around the content.
func escape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}

// clean drops control and format characters, such as bidi overrides and
// zero-width spaces, which can hide text from the user who reads the
// session. It keeps newlines and turns tabs into spaces.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r == '\n':
			return r
		case r == '\t':
			return ' '
		case unicode.IsControl(r), unicode.Is(unicode.Cf, r), r == unicode.ReplacementChar:
			return -1
		}
		return r
	}, s)
}
