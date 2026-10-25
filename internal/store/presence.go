package store

import (
	"context"
	"errors"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/team"
)

var errRemoteSender = errors.New("a remote sender is <member label>/<agent name>, with an optional ~<6 characters of the peer id> after the label")

// checkRemoteSender checks the form that team.Members.Sender renders. The
// error does not quote sender.
func checkRemoteSender(sender string) error {
	if member, _, err := message.SplitAddress(sender); err != nil || member == "" {
		return errRemoteSender
	}
	return nil
}

// RemoteOnline stores the online message on topic of the agent sender of
// the machine of the member whose peer id is origin, and makes it pending
// for the agents known on topic. It replaces every online from origin on
// topic that waits for an agent: an agent holds at most one pending online
// per member and topic, because agents lists the live set. A remote sender
// is never known on a topic, so no agent of this machine has its name.
func (s *Store) RemoteOnline(ctx context.Context, topic, sender, origin string) (Sent, error) {
	if err := message.CheckTopic(topic); err != nil {
		return Sent{}, err
	}
	if err := checkRemoteSender(sender); err != nil {
		return Sent{}, err
	}
	if err := team.CheckPeerID(origin); err != nil {
		return Sent{}, err
	}
	now := s.Now()
	m := message.Message{ID: message.NewID(now), Sender: sender, Origin: origin, Created: now,
		Fields: message.Fields{Topic: topic, Kind: message.KindOnline, Summary: sender + " is online on " + topic}}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Sent{}, err
	}
	defer tx.Rollback()
	recipients, err := names(tx.QueryContext(ctx, `SELECT name FROM agents WHERE topic = ? AND registered >= ? ORDER BY name`,
		topic, s.cutoff()))
	if err != nil {
		return Sent{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO messages (id, topic, sender, recipient, kind, summary, issue, link, re, created, origin)
		VALUES (?, ?, ?, '', ?, ?, 0, '', '', ?, ?)`,
		m.ID.String(), m.Topic, m.Sender, m.Kind, m.Summary, m.Created.UnixMilli(), m.Origin); err != nil {
		return Sent{}, err
	}
	for _, r := range recipients {
		if _, err := tx.ExecContext(ctx, `DELETE FROM deliveries WHERE agent = ? AND acked IS NULL AND message_id IN
			(SELECT id FROM messages WHERE topic = ? AND origin = ? AND kind = ?)`,
			r, topic, origin, message.KindOnline); err != nil {
			return Sent{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO deliveries (message_id, agent) VALUES (?, ?)`, m.ID.String(), r); err != nil {
			return Sent{}, err
		}
	}
	return Sent{Message: m, Recipients: recipients}, tx.Commit()
}
