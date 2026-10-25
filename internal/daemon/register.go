//go:build unix

package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/version"
)

const (
	maxTopics = 16
	maxCwd    = 4096
)

var (
	harnessRule = regexp.MustCompile(`^[a-z0-9]{1,16}$`)
	versionRule = regexp.MustCompile(`^[0-9A-Za-z.+-]{1,32}$`)
)

type registerParams struct {
	Harness string   `json:"harness"`
	Version string   `json:"version"`
	Cwd     string   `json:"cwd"`
	Name    string   `json:"name,omitempty"`
	Topics  []string `json:"topics,omitempty"`
}

type registerResult struct {
	Session  string   `json:"session"`
	Name     string   `json:"name"`
	Topics   []string `json:"topics"`
	Protocol string   `json:"protocol"`
	Version  string   `json:"version"`
}

func (s *Server) register(ctx context.Context, ss *session, raw []byte) (any, *rpcError) {
	if ss.name != "" {
		return nil, &rpcError{codeAlreadyRegister, "this connection is already registered: one connection is one session"}
	}
	var p registerParams
	if e := decodeParams(raw, &p,
		"register params are harness, version and cwd, strings, and the optional name, a string, and topics, an array of strings"); e != nil {
		return nil, e
	}
	if !harnessRule.MatchString(p.Harness) || p.Harness == message.Everyone {
		return nil, invalidParams("harness must match %s and must not be %s", harnessRule, message.Everyone)
	}
	if p.Version != version.Version {
		client := "a version that is not valid"
		if versionRule.MatchString(p.Version) {
			client = p.Version
		}
		msg := fmt.Sprintf("version mismatch: the client is %s, the daemon is %s", client, version.Version)
		switch exit := s.idle.describe(); {
		case exit == "":
		case s.teams.Len() > 0:
			msg += "; the daemon is in a team, so it runs until agent-courier team leave ends its last team, " +
				"then it exits " + exit + " after its last session closes and the next client starts its own release"
		default:
			msg += "; the daemon exits " + exit + " after its last session closes, then the next client starts its own release"
		}
		return nil, &rpcError{codeVersionMismatch, msg}
	}
	if !filepath.IsAbs(p.Cwd) || len(p.Cwd) > maxCwd {
		return nil, invalidParams("cwd must be an absolute path of at most %d bytes", maxCwd)
	}
	if p.Name != "" {
		if err := message.CheckAgent(p.Name); err != nil {
			return nil, invalidParams("name: %v", err)
		}
	}
	for _, t := range p.Topics {
		if err := message.CheckTopic(t); err != nil {
			return nil, invalidParams("topics: %v", err)
		}
	}
	topics := slices.Compact(slices.Sorted(slices.Values(p.Topics)))
	if len(topics) > maxTopics {
		return nil, invalidParams("topics: a session follows at most %d different topics", maxTopics)
	}
	base := p.Name
	if len(topics) == 0 || base == "" {
		topic, repo := repoOf(p.Cwd)
		if len(topics) == 0 {
			if message.CheckTopic(topic) != nil {
				return nil, invalidParams("no topic can be derived from cwd: give topics, where %v", message.CheckTopic(""))
			}
			topics = []string{topic}
		}
		if base == "" {
			base = defaultName(p.Harness, repo)
			if err := message.CheckAgent(base); err != nil {
				return nil, invalidParams("no agent name can be derived from harness and cwd: give name, where %v", err)
			}
		}
	}
	name := s.claim(ss, base, topics)
	online, err := s.store.RegisterTopics(ctx, name, topics)
	if err != nil {
		s.log.Error("register failed", "session", ss.id, "agent", name, "err", err)
		s.release(ss)
		return nil, errInternal
	}
	s.mu.Lock()
	ss.out = newOutbox(name)
	s.mu.Unlock()
	for _, o := range online {
		s.announce(o)
	}
	s.presenceChanged()
	s.log.Info("registered", "session", ss.id, "agent", name, "topics", topics, "harness", p.Harness)
	return registerResult{Session: ss.id, Name: name, Topics: topics, Protocol: protocolFor(name, topics), Version: version.Version}, nil
}

// defaultName is <harness>-<repo> in the agent name alphabet.
func defaultName(harness, repo string) string {
	r := slug(repo, false, maxName)
	if r == "" {
		return harness
	}
	name := harness + "-" + r
	return strings.TrimRight(name[:min(len(name), maxName)], "-")
}

// claim gives ss the name base, or base with the first suffix -2, -3 and so
// on that no live session holds.
func (s *Server) claim(ss *session, base string, topics []string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := base
	for n := 2; s.names[name] != nil; n++ {
		suffix := "-" + strconv.Itoa(n)
		name = strings.TrimRight(base[:min(len(base), maxName-len(suffix))], "-") + suffix
	}
	s.names[name] = ss
	ss.name, ss.topics = name, topics
	return name
}

func (s *Server) release(ss *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.names[ss.name] == ss {
		delete(s.names, ss.name)
	}
	ss.name, ss.topics = "", nil
}
