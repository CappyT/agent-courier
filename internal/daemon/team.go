//go:build unix

package daemon

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/p2p"
	"github.com/CappyT/agent-courier/internal/team"
)

const (
	codeNoTeam     = -32020
	codeTeamExists = -32021
	codeTopicTaken = -32022
	codeManyTeams  = -32027
	// The codes of team.invite and team.join.
	codeInviteRefused = -32029
	codeInviteExpired = -32030
	codeUnreachable   = -32031
	codeLabelTaken    = -32032
	codeTeamFull      = -32033
	codeManyInvites   = -32034
	codeJoinFailed    = -32036
)

// maxListMembers is the most members each team lists in team.list without a
// team, so 16 teams with the longest names, topics, labels and peer ids stay
// under MaxLine. A result with one team lists every member.
const maxListMembers = 10

// Network is the p2p side of the team methods: the daemon's libp2p host, with
// p2p.JoinService and p2p.MemberSync.
type Network interface {
	// Self is the peer id of this machine. The first call creates the
	// machine key.
	Self() (string, error)
	// InviteCode is the invite code of team with secret and expires: the peer
	// id and addresses of this machine with them.
	InviteCode(team string, secret []byte, expires time.Time) (string, error)
	// Join joins this machine with label to the team of code, as
	// p2p.JoinService.Join does, and returns the team.
	Join(ctx context.Context, code, label string) (team.Team, error)
	// TeamChanged runs after a local change of team, so the host follows
	// and the member list goes out to the members.
	TeamChanged(team string)
	// Leave marks the entry of this machine in team name left and forgets
	// the team, as team.Set.Leave does, then sends the list to the members
	// it can reach. It returns the team and how many of its other members
	// the leave did not reach.
	Leave(name string) (team.Team, int, error)
}

// SetNetwork gives the server its p2p side. Call it before Serve. The team
// methods need it.
func (s *Server) SetNetwork(n Network) { s.net = n }

// Teams is the one member set of the daemon. The p2p side reads and changes
// it too.
func (s *Server) Teams() *team.Set { return s.teams }

type teamMember struct {
	Label  string `json:"label"`
	PeerID string `json:"peer_id"`
}

type teamJSON struct {
	Name         string       `json:"name"`
	Topics       []string     `json:"topics"`
	Members      []teamMember `json:"members"`
	MembersTotal int          `json:"members_total"`
}

// toJSON lists at most maxMembers members of t.
func toJSON(t team.Team, maxMembers int) teamJSON {
	out := teamJSON{Name: t.Name, Topics: nonNil(t.Topics), Members: []teamMember{}}
	for _, e := range t.Members.Active() {
		out.MembersTotal++
		if len(out.Members) < maxMembers {
			out.Members = append(out.Members, teamMember{Label: e.Label, PeerID: e.PeerID})
		}
	}
	return out
}

type teamResult struct {
	Team teamJSON `json:"team"`
}

// teamsChanged keeps the daemon running while the machine is in a team:
// its teams live only in its memory.
func (s *Server) teamsChanged() { s.idle.hold(s.teams.Len() > 0) }

type teamCreateParams struct {
	Team   string   `json:"team"`
	Topics []string `json:"topics"`
	Label  string   `json:"label"`
}

func (s *Server) teamCreate(ss *session, raw jsontext.Value) (any, *rpcError) {
	var p teamCreateParams
	if e := decodeParams(raw, &p, "team.create params are team, a string; topics, an array of strings; label, a string"); e != nil {
		return nil, e
	}
	if err := team.CheckName(p.Team); err != nil {
		return nil, invalidParams("team: %v", err)
	}
	if err := team.CheckLabel(p.Label); err != nil {
		return nil, invalidParams("label: %v", err)
	}
	for _, t := range p.Topics {
		if err := message.CheckTopic(t); err != nil {
			return nil, invalidParams("topics: %v", err)
		}
	}
	if e := s.needNetwork(ss); e != nil {
		return nil, e
	}
	self, err := s.net.Self()
	if err != nil {
		s.log.Error("create the machine key", "session", ss.id, "err", err)
		return nil, errInternal
	}
	t, err := s.teams.Create(self, p.Team, p.Topics, p.Label)
	if err != nil {
		return nil, s.teamError(ss, "team create", err)
	}
	s.log.Info("team created", "session", ss.id, "team", t.Name, "topics", t.Topics)
	s.teamsChanged()
	s.net.TeamChanged(t.Name)
	return teamResult{toJSON(t, team.MaxMembers)}, nil
}

type teamListResult struct {
	Teams []teamJSON `json:"teams"`
}

type teamListParams struct {
	Team string `json:"team,omitempty"`
}

// teamList lists every team, at most team.MaxTeams, or only the team that
// params name, with every member.
func (s *Server) teamList(raw jsontext.Value) (any, *rpcError) {
	var p teamListParams
	if e := decodeParams(raw, &p, "team.list params are team, an optional string"); e != nil {
		return nil, e
	}
	out := teamListResult{Teams: []teamJSON{}}
	if p.Team != "" {
		if err := team.CheckName(p.Team); err != nil {
			return nil, invalidParams("team: %v", err)
		}
		t, ok := s.teams.Get(p.Team)
		if !ok {
			return nil, &rpcError{codeNoTeam, team.ErrNoTeam.Error()}
		}
		out.Teams = append(out.Teams, toJSON(t, team.MaxMembers))
		return out, nil
	}
	for _, t := range s.teams.List() {
		out.Teams = append(out.Teams, toJSON(t, maxListMembers))
	}
	return out, nil
}

type teamLeaveParams struct {
	Team string `json:"team"`
}

func (s *Server) teamLeave(ss *session, raw jsontext.Value) (any, *rpcError) {
	var p teamLeaveParams
	if e := decodeParams(raw, &p, "team.leave params are team, a string"); e != nil {
		return nil, e
	}
	if err := team.CheckName(p.Team); err != nil {
		return nil, invalidParams("team: %v", err)
	}
	if e := s.needNetwork(ss); e != nil {
		return nil, e
	}
	t, unreached, err := s.net.Leave(p.Team)
	if err != nil {
		return nil, s.teamError(ss, "team leave", err)
	}
	s.log.Info("team left", "session", ss.id, "team", t.Name)
	s.teamsChanged()
	text := "Left team " + t.Name + "."
	if unreached > 0 {
		text += fmt.Sprintf(" The leave did not reach %d of %d members.", unreached, len(t.Members.Active()))
	}
	return textResult{Text: text}, nil
}

// needNetwork fails a team method on a server without its p2p side: a bug
// of the caller of New, since the daemon always sets it.
func (s *Server) needNetwork(ss *session) *rpcError {
	if s.net == nil {
		s.log.Error("team method on a server without its p2p side", "session", ss.id)
		return errInternal
	}
	return nil
}

// teamError maps an error of the team set or the join to its code. Their
// messages name rules and never quote input.
func (s *Server) teamError(ss *session, op string, err error) *rpcError {
	for _, m := range []struct {
		err  error
		code int
	}{
		{team.ErrNoTeam, codeNoTeam},
		{team.ErrTeamExists, codeTeamExists},
		{team.ErrTopicTaken, codeTopicTaken},
		{team.ErrNoTopics, codeInvalidParams},
		{team.ErrTooManyTeams, codeManyTeams},
		{team.ErrTooManyInvites, codeManyInvites},
		{team.ErrInviteRefused, codeInviteRefused},
		{team.ErrLabelTaken, codeLabelTaken},
		{team.ErrTeamFull, codeTeamFull},
		{p2p.ErrBadInviteCode, codeInvalidParams},
		{p2p.ErrInviteExpired, codeInviteExpired},
		{p2p.ErrInviterUnreachable, codeUnreachable},
		{p2p.ErrJoinFailed, codeJoinFailed},
	} {
		if errors.Is(err, m.err) {
			return &rpcError{m.code, err.Error()}
		}
	}
	s.log.Error(op+" failed", "session", ss.id, "err", err)
	return errInternal
}

type teamInviteParams struct {
	Team string `json:"team"`
}

type inviteJSON struct {
	Team    string `json:"team"`
	Code    string `json:"code"`
	Expires string `json:"expires"`
}

type teamInviteResult struct {
	Invite inviteJSON `json:"invite"`
}

// teamInvite makes a new invite for a team of this machine, and returns its
// code. The code holds the secret, so no log has it.
func (s *Server) teamInvite(ss *session, raw jsontext.Value) (any, *rpcError) {
	var p teamInviteParams
	if e := decodeParams(raw, &p, "team.invite params are team, a string"); e != nil {
		return nil, e
	}
	if err := team.CheckName(p.Team); err != nil {
		return nil, invalidParams("team: %v", err)
	}
	if e := s.needNetwork(ss); e != nil {
		return nil, e
	}
	secret, expires, err := s.teams.CreateInvite(p.Team)
	if err != nil {
		return nil, s.teamError(ss, "team invite", err)
	}
	code, err := s.net.InviteCode(p.Team, secret, expires)
	if err != nil {
		s.log.Error("invite code failed", "session", ss.id, "team", p.Team, "err", err)
		return nil, errInternal
	}
	s.log.Info("invite created", "session", ss.id, "team", p.Team)
	return teamInviteResult{inviteJSON{Team: p.Team, Code: code, Expires: expires.UTC().Format(time.RFC3339)}}, nil
}

type teamJoinParams struct {
	Code  string `json:"code"`
	Label string `json:"label"`
}

func (s *Server) teamJoin(ctx context.Context, ss *session, raw jsontext.Value) (any, *rpcError) {
	var p teamJoinParams
	if e := decodeParams(raw, &p, "team.join params are code and label, strings"); e != nil {
		return nil, e
	}
	if err := team.CheckLabel(p.Label); err != nil {
		return nil, invalidParams("label: %v", err)
	}
	if p.Code == "" || len(p.Code) > p2p.MaxInviteCode {
		return nil, invalidParams("code: %v", p2p.ErrBadInviteCode)
	}
	if e := s.needNetwork(ss); e != nil {
		return nil, e
	}
	t, err := s.net.Join(ctx, p.Code, p.Label)
	if err != nil {
		return nil, s.teamError(ss, "team join", err)
	}
	s.log.Info("team joined", "session", ss.id, "team", t.Name)
	s.teamsChanged()
	return teamResult{toJSON(t, team.MaxMembers)}, nil
}
