// Package message holds the courier message model: kinds, field rules, ids,
// and the rendering of inbound text for a model.
package message

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

type Kind string

const (
	KindHandoff Kind = "handoff"
	KindDone    Kind = "done"
	KindBlocked Kind = "blocked"
	KindAsk     Kind = "ask"
	KindReply   Kind = "reply"
	KindNote    Kind = "note"
	// KindOnline comes only from the daemon, when an agent registers.
	KindOnline Kind = "online"
	// KindBudget comes only from the daemon, in the notice that a sender
	// passed a push budget of a session. No message has it.
	KindBudget Kind = "budget"
)

// Kinds are the kinds an agent can send.
var Kinds = []Kind{KindHandoff, KindDone, KindBlocked, KindAsk, KindReply, KindNote}

const (
	MaxSummary = 1000 // characters
	MaxLink    = 512
)

var (
	agentRule = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)
	topicRule = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	// memberRule is the member part of a remote address: a member label,
	// and the last 6 characters of the peer id when two members share it.
	memberRule = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}(~[A-Za-z0-9]{6})?$`)
	originRule = regexp.MustCompile(`^[A-Za-z0-9]{1,128}$`)
)

// Everyone is the recipient that a rendered broadcast names, so no agent can
// take it as a name.
const Everyone = "everyone"

// Errors never quote the inbound value, so they can reach a model as they are.
var (
	errTopic = fmt.Errorf("a topic must match %s", topicRule)
	errAgent = fmt.Errorf("an agent name must match %s and must not be %s", agentRule, Everyone)
	errKind  = errors.New("kind must be one of handoff, done, blocked, ask, reply, note")
	errAddr  = fmt.Errorf("%w; an agent on the machine of another member is <member label>/<agent name>", errAgent)
)

// CheckAgent returns an error that names the rule when name is not a valid
// agent name.
func CheckAgent(name string) error {
	if !agentRule.MatchString(name) || name == Everyone {
		return errAgent
	}
	return nil
}

// CheckTopic returns an error that names the rule when topic is not a valid
// topic.
func CheckTopic(topic string) error {
	if !topicRule.MatchString(topic) {
		return errTopic
	}
	return nil
}

// SplitAddress splits a recipient into the member part and the agent name.
// A local agent has no member part. The member part of a remote address is
// a member label, with ~<last 6 characters of the peer id> after it when two
// members share the label, as team.Members.Sender renders a sender. It fails,
// without quoting addr, when addr is neither.
func SplitAddress(addr string) (member, agent string, err error) {
	if CheckAgent(addr) == nil {
		return "", addr, nil
	}
	member, agent, ok := strings.Cut(addr, "/")
	if !ok || !memberRule.MatchString(member) || CheckAgent(agent) != nil {
		return "", "", errAddr
	}
	return member, agent, nil
}

// Fields are what the sending agent sets. To is empty for a message to every
// agent on the topic. Re is the id of the ask that a reply answers.
type Fields struct {
	Topic   string `json:"topic"`
	Kind    Kind   `json:"kind"`
	Summary string `json:"summary"`
	To      string `json:"to,omitempty"`
	Issue   int    `json:"issue,omitzero"`
	Link    string `json:"link,omitempty"`
	Re      ID     `json:"re,omitzero"`
}

// Message is a stored message. The daemon sets ID, Sender, Origin and
// Created; Sender comes from the session's registration, never from the
// client. Origin is the peer id of the member whose machine sent the
// message, empty for a message of this machine. The sender of a remote
// message is <member label>/<agent name>, with the label from the member list of
// this machine, and its recipient is an agent of this machine.
type Message struct {
	ID      ID
	Sender  string
	Origin  string
	Created time.Time
	Fields
}

// ParseFields decodes the fields a client sends and checks them. It refuses
// unknown fields. It drops a "sender" field of any JSON type, because the
// sender is the session's agent name.
func ParseFields(data []byte) (Fields, error) {
	var in struct {
		Fields `json:",embed"`
		Sender jsontext.Value `json:"sender"`
	}
	if err := json.Unmarshal(data, &in, json.RejectUnknownMembers(true)); err != nil {
		if errors.Is(err, json.ErrUnknownName) {
			return Fields{}, errors.New("a message has only the fields topic, kind, summary, to, issue, link and re")
		}
		if errors.Is(err, errID) {
			return Fields{}, fmt.Errorf("re: %w", errID)
		}
		return Fields{}, errors.New("a message is a JSON object: topic, kind, summary, to, link and re are strings, issue is a number")
	}
	return in.Fields, in.Validate()
}

// Validate checks the fields an agent sends. An agent cannot send online.
func (f Fields) Validate() error {
	if f.Kind == KindOnline {
		return errors.New("kind online comes only from the daemon")
	}
	return f.validate()
}

// Validate checks a message from any source, including online from the daemon.
func (m Message) Validate() error {
	if m.ID == (ID{}) {
		return errors.New("id is missing")
	}
	if m.Origin == "" {
		if err := CheckAgent(m.Sender); err != nil {
			return fmt.Errorf("sender: %w", err)
		}
	} else {
		if !originRule.MatchString(m.Origin) {
			return errors.New("origin is not a peer id")
		}
		if member, _, err := SplitAddress(m.Sender); err != nil || member == "" {
			return errors.New("the sender of a remote message is <member label>/<agent name>")
		}
		if m.To != "" && CheckAgent(m.To) != nil {
			return errors.New("the recipient of a remote message is an agent of this machine")
		}
	}
	if m.Created.IsZero() {
		return errors.New("creation time is missing")
	}
	if m.Kind == KindOnline && m.To != "" {
		return errors.New("online has no recipient")
	}
	if m.To == m.Sender {
		return errors.New("a message cannot go to its sender")
	}
	return m.validate()
}

func (f Fields) validate() error {
	if err := CheckTopic(f.Topic); err != nil {
		return err
	}
	if f.Kind != KindOnline && !slices.Contains(Kinds, f.Kind) {
		return errKind
	}
	if (f.Kind == KindReply) != (f.Re != ID{}) {
		return errors.New("re is the id of the ask that a reply answers: set it on a reply and only there")
	}
	if f.To != "" {
		if _, _, err := SplitAddress(f.To); err != nil {
			return fmt.Errorf("to: %w", err)
		}
	}
	if f.Issue < 0 {
		return errors.New("issue must be a positive number, or 0 for none")
	}
	if f.Link != "" {
		if len(f.Link) > MaxLink {
			return fmt.Errorf("link is %d characters, the limit is %d", len(f.Link), MaxLink)
		}
		if strings.ContainsFunc(f.Link, func(r rune) bool { return r <= ' ' || r > '~' }) {
			return errors.New("link must be printable ASCII without spaces")
		}
		u, err := url.Parse(f.Link)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return errors.New("link must be an https link")
		}
		if u.User != nil {
			return errors.New("link must not carry a user name")
		}
	}
	if !utf8.ValidString(f.Summary) {
		return errors.New("summary is not valid UTF-8")
	}
	if n := utf8.RuneCountInString(f.Summary); n > MaxSummary {
		return fmt.Errorf("summary is %d characters, the limit is %d", n, MaxSummary)
	}
	// Checked on the cleaned form, so a summary of invisible characters
	// cannot pass as text.
	if strings.TrimSpace(clean(f.Summary)) == "" {
		return errors.New("summary is empty")
	}
	return nil
}
