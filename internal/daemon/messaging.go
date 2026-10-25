//go:build unix

package daemon

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/store"
	"github.com/CappyT/agent-courier/internal/team"
)

const (
	codeUnknownRecipient = -32010
	codeNotOnTopic       = -32011
	codeNoAsk            = -32012
	codeNotPending       = -32013
	codeQueueFull        = -32014
)

const (
	// maxAgents is the most entries an agents result or the recipients of a
	// send result list.
	maxAgents = 100
	// maxNamed is the most agents the text of a send result names.
	maxNamed = 10
)

type sendResult struct {
	ID         string   `json:"id"`
	Recipients []string `json:"recipients"`
	Total      int      `json:"total"`
	Skipped    []string `json:"skipped"`
	SkipTotal  int      `json:"skipped_total"`
	// Members are the members of the team of the topic whose machines get
	// the message through the outbox.
	Members []string `json:"members"`
	Text    string   `json:"text"`
}

func (s *Server) send(ctx context.Context, ss *session, raw jsontext.Value) (any, *rpcError) {
	if len(raw) == 0 {
		raw = jsontext.Value("{}")
	}
	if raw.Kind() != '{' {
		return nil, invalidParams("params must be a JSON object")
	}
	f, err := message.ParseFields(raw)
	if err != nil {
		return nil, invalidParams("%v", err)
	}
	if f.To == ss.name {
		return nil, invalidParams("to must name another agent: a message cannot go to its sender")
	}
	now := s.store.Now()
	sent, full, err := s.sendTeam(ctx, message.Message{ID: message.NewID(now), Sender: ss.name, Created: now, Fields: f})
	if err != nil {
		if qf, ok := errors.AsType[*store.QueueFullError](err); ok {
			return nil, &rpcError{codeQueueFull, qf.Error() + "; it must acknowledge messages first"}
		}
		if qf, ok := errors.AsType[*team.QueueFullError](err); ok {
			return nil, &rpcError{codeQueueFull, message.Inline(qf.Error() + "; send to it again when it received the others")}
		}
		switch {
		case errors.Is(err, store.ErrUnknownRecipient):
			return nil, &rpcError{codeUnknownRecipient, f.To + " is not an agent known on the topic: call agents for the known ones"}
		case errors.Is(err, store.ErrSenderNotOnTopic):
			return nil, &rpcError{codeNotOnTopic, "your agent is not known on topic " + f.Topic +
				": send on a topic you registered, now or in an earlier session"}
		case errors.Is(err, store.ErrNoAsk):
			return nil, &rpcError{codeNoAsk, "re names no ask on the topic: a reply answers the id of an ask on its topic"}
		case errors.Is(err, team.ErrNotTeamTopic):
			return nil, &rpcError{codeUnknownRecipient, "a recipient <member label>/<agent> is on the machine of another member: " +
				"send it on a topic of a team this machine is a member of"}
		case errors.Is(err, team.ErrUnknownRemote):
			return nil, &rpcError{codeUnknownRecipient, "the label of the recipient names no other member of the team of topic " +
				f.Topic + ": call team.list for the members"}
		case errors.Is(err, team.ErrAmbiguousRecipient):
			return nil, &rpcError{codeUnknownRecipient, "two members of the team hold the label of the recipient: " +
				"write it as <label>~<last 6 characters of the peer id>/<agent>"}
		}
		s.log.Error("send failed", "session", ss.id, "agent", ss.name, "err", err)
		return nil, errInternal
	}
	for _, r := range sent.Recipients {
		s.pushTo(r, sent.Message)
	}
	members := make([]string, len(sent.Queued))
	for i, q := range sent.Queued {
		members[i] = q.Member
	}
	slices.Sort(members)
	if len(members) > 0 {
		s.outboxChanged()
	}
	id := sent.Message.ID.String()
	skipped := append(slices.Clone(sent.Skipped), full...)
	res := sendResult{ID: id, Recipients: nonNil(sent.Recipients[:min(len(sent.Recipients), maxAgents)]),
		Total: len(sent.Recipients), Skipped: nonNil(skipped[:min(len(skipped), maxAgents)]), SkipTotal: len(skipped),
		Members: nonNil(members[:min(len(members), maxAgents)])}
	var text string
	switch {
	case len(sent.Recipients) > 0:
		text = "Sent " + id + " to " + named(sent.Recipients) + "."
	case len(members) == 0:
		text = "Stored " + id + ": no other agent follows the topic yet, so nobody receives it."
	default:
		text = "Stored " + id + "."
	}
	if len(members) > 0 {
		text += " Queued for the machines of " + named(members) + ": each gets it when it is reachable, while both machines are in the team."
	}
	if len(sent.Skipped) > 0 {
		text += " Skipped, their pending queue is full: " + named(sent.Skipped) + "."
	}
	if len(full) > 0 {
		text += " Skipped, the send queue for their machine is full: " + named(full) + "."
	}
	res.Text = message.Inline(text)
	s.log.Info("sent", "session", ss.id, "agent", ss.name, "id", id, "kind", f.Kind,
		"recipients", len(res.Recipients), "skipped", len(skipped), "members", len(members))
	return res, nil
}

// sendTeam stores m. On a topic of a team, it queues m in memory for the
// members that the member set routes it to, and returns in full the labels
// of the members it skipped for a full send queue. A message to an agent of
// one member whose queue is full fails with a *team.QueueFullError before it
// is stored.
func (s *Server) sendTeam(ctx context.Context, m message.Message) (store.Sent, []string, error) {
	r, err := s.teams.Route(m.Topic, m.To)
	if err != nil {
		return store.Sent{}, nil, err
	}
	direct := strings.Contains(m.To, "/")
	if f := s.teams.Full(r); direct && len(f) > 0 {
		return store.Sent{}, nil, &team.QueueFullError{Member: f[0].Member}
	}
	sent, err := s.store.SendTeam(ctx, m, r)
	if err != nil {
		return store.Sent{}, nil, err
	}
	queued, skipped := s.teams.Enqueue(r, sent.Message)
	sent.Queued = queued
	full := make([]string, len(skipped))
	for i, q := range skipped {
		full[i] = q.Member
	}
	slices.Sort(full)
	return sent, full, nil
}

// named lists the first maxNamed names and counts the others.
func named(names []string) string {
	if len(names) <= maxNamed {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(names[:maxNamed], ", "), len(names)-maxNamed)
}

// nonNil turns nil into an empty slice, so it encodes as [].
func nonNil(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

type pendingResult struct {
	Text  string `json:"text"`
	Total int    `json:"total"`
}

func (s *Server) pending(ctx context.Context, ss *session, raw jsontext.Value) (any, *rpcError) {
	if e := decodeParams(raw, &struct{}{}, "pending has no params"); e != nil {
		return nil, e
	}
	ps, total, err := s.store.Pending(ctx, ss.name, message.MaxPendingMessages)
	if err != nil {
		s.log.Error("pending failed", "session", ss.id, "agent", ss.name, "err", err)
		return nil, errInternal
	}
	ms := make([]message.Message, len(ps))
	for i, p := range ps {
		ms[i] = p.Message
	}
	return pendingResult{Text: message.RenderPending(ms, total), Total: total}, nil
}

type ackParams struct {
	ID string `json:"id"`
}

type textResult struct {
	Text string `json:"text"`
}

func (s *Server) ack(ctx context.Context, ss *session, raw jsontext.Value) (any, *rpcError) {
	var p ackParams
	if e := decodeParams(raw, &p, "ack params are id, a string"); e != nil {
		return nil, e
	}
	id, err := message.ParseID(p.ID)
	if err != nil {
		return nil, invalidParams("id: %v", err)
	}
	if err := s.store.Ack(ctx, ss.name, id); err != nil {
		if errors.Is(err, store.ErrNotPending) {
			return nil, &rpcError{codeNotPending, "the message is not pending for this agent: ack an id from your own pending list"}
		}
		s.log.Error("ack failed", "session", ss.id, "agent", ss.name, "err", err)
		return nil, errInternal
	}
	if ss.out != nil {
		ss.out.unhold(id)
	}
	s.log.Debug("acknowledged", "session", ss.id, "agent", ss.name, "id", id)
	return textResult{Text: "Acknowledged " + id.String() + "."}, nil
}

type agentEntry struct {
	Topic string `json:"topic"`
	Name  string `json:"name"`
	Live  bool   `json:"live"`
}

type agentsResult struct {
	Agents []agentEntry `json:"agents"`
	Total  int          `json:"total"`
	Text   string       `json:"text"`
}

func (s *Server) agents(ctx context.Context, ss *session, raw jsontext.Value) (any, *rpcError) {
	if e := decodeParams(raw, &struct{}{}, "agents has no params"); e != nil {
		return nil, e
	}
	// known are the agents of this machine, then the live agents on the
	// machines of other members as <label>/<agent>, with Live set: the cap
	// never hides a local agent behind remote ones.
	var known, remote []agentEntry
	for _, t := range ss.topics {
		as, err := s.store.Agents(ctx, t)
		if err != nil {
			s.log.Error("agents failed", "session", ss.id, "agent", ss.name, "topic", t, "err", err)
			return nil, errInternal
		}
		for _, a := range as {
			known = append(known, agentEntry{Topic: a.Topic, Name: a.Name})
		}
		for _, name := range s.remoteAgents(t) {
			remote = append(remote, agentEntry{Topic: t, Name: name, Live: true})
		}
	}
	known = append(known, remote...)
	res := agentsResult{Agents: []agentEntry{}, Total: len(known)}
	lines := []string{fmt.Sprintf("%d agents known on your topics, as topic, name and whether a session holds the name:", len(known))}
	s.mu.Lock()
	for _, e := range known[:min(len(known), maxAgents)] {
		e.Live = e.Live || s.names[e.Name] != nil
		res.Agents = append(res.Agents, e)
		state := "offline"
		if e.Live {
			state = "live"
		}
		lines = append(lines, message.Inline(e.Topic+" "+e.Name+" "+state))
	}
	s.mu.Unlock()
	if n := len(known) - len(res.Agents); n > 0 {
		lines = append(lines, fmt.Sprintf("%d more not shown.", n))
	}
	res.Text = strings.Join(lines, "\n")
	return res, nil
}
