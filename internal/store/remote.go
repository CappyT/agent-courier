package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/team"
)

// SendTeam is Send with the route r that team.Set.Route resolved for m. On
// a route of a team, a recipient <member label>/<agent> is an agent on the
// machine of another member: SendTeam stores m, and it reaches no agent of
// this machine. The daemon queues m for the members of r.
func (s *Store) SendTeam(ctx context.Context, m message.Message, r team.Route) (Sent, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Sent{}, err
	}
	defer tx.Rollback()
	sent, err := s.send(ctx, tx, m, r.Team != "" && strings.Contains(m.To, "/"))
	if err != nil {
		return Sent{}, err
	}
	return sent, tx.Commit()
}

// Receive stores the message m from the member whose peer id is m.Origin,
// with the rules of Send for the agents of this machine. The id of m is that
// of the sending machine; when a message here already has it, m is stored
// under a new local id, so no member can suppress another member's message
// by reusing its id. A broadcast that reaches no agent of this machine is
// not stored, and Sent.Dropped is true. The daemon drops a message that the
// member sent before, so Receive does not look for duplicates.
func (s *Store) Receive(ctx context.Context, m message.Message) (Sent, error) {
	if m.Origin == "" {
		return Sent{}, errors.New("a received message has an origin")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Sent{}, err
	}
	defer tx.Rollback()
	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM messages WHERE id = ?`, m.ID.String()).Scan(&one)
	switch {
	case err == nil:
		m.ID = message.NewID(s.now())
	case !errors.Is(err, sql.ErrNoRows):
		return Sent{}, err
	}
	sent, err := s.send(ctx, tx, m, false)
	if err != nil {
		return Sent{}, err
	}
	if m.To == "" && len(sent.Recipients) == 0 {
		// The broadcast reaches no agent: no commit, so nothing stays.
		sent.Dropped = true
		return sent, nil
	}
	return sent, tx.Commit()
}
