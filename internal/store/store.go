// Package store keeps the courier state in SQLite: messages, their
// deliveries to agents, and the agents known on each topic.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
	"github.com/CappyT/agent-courier/internal/team"
	_ "modernc.org/sqlite"
)

const (
	// Retention is how long a message stays, and how long an agent stays
	// known after its last registration or Touch.
	Retention = 30 * 24 * time.Hour
	// OnlineEvery is the shortest time between two online messages of one
	// agent on one topic.
	OnlineEvery = time.Hour
	// MaxPendingFromSender and MaxPendingTotal cap what waits for one
	// recipient. Online messages do not count.
	MaxPendingFromSender = 20
	MaxPendingTotal      = 200
)

var (
	ErrUnknownRecipient = errors.New("the recipient is not an agent known on the topic")
	ErrSenderNotOnTopic = errors.New("the sender does not follow the topic")
	ErrNoAsk            = errors.New("re names no ask on the topic")
	ErrNotPending       = errors.New("the message is not pending for this agent")
)

// QueueFullError refuses a direct message that would pass a pending cap of
// its recipient. Total is true for the cap of all senders, false for the cap
// of Sender.
type QueueFullError struct {
	Recipient string
	Sender    string
	Total     bool
}

func (e *QueueFullError) Error() string {
	if e.Total {
		return fmt.Sprintf("the queue of %s is full: %d messages wait for its acknowledgement", e.Recipient, MaxPendingTotal)
	}
	return fmt.Sprintf("the queue of %s from %s is full: %d messages from %s wait for its acknowledgement",
		e.Recipient, e.Sender, MaxPendingFromSender, e.Sender)
}

// recipient is "" for a message to every agent on the topic. session is the
// id of the last session that received a push of the message. Open adds the
// column origin to messages: the peer id of the member that sent a remote
// message, "" for a local one.
const schema = `
CREATE TABLE IF NOT EXISTS messages (
	id        TEXT PRIMARY KEY,
	topic     TEXT NOT NULL,
	sender    TEXT NOT NULL,
	recipient TEXT NOT NULL,
	kind      TEXT NOT NULL,
	summary   TEXT NOT NULL,
	issue     INTEGER NOT NULL,
	link      TEXT NOT NULL,
	re        TEXT NOT NULL,
	created   INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS messages_created ON messages (created);
CREATE TABLE IF NOT EXISTS deliveries (
	message_id TEXT NOT NULL REFERENCES messages (id) ON DELETE CASCADE,
	agent      TEXT NOT NULL,
	session    TEXT NOT NULL DEFAULT '',
	acked      INTEGER,
	PRIMARY KEY (agent, message_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS deliveries_message ON deliveries (message_id);
CREATE TABLE IF NOT EXISTS agents (
	topic      TEXT NOT NULL,
	name       TEXT NOT NULL,
	registered INTEGER NOT NULL,
	online     INTEGER NOT NULL,
	PRIMARY KEY (topic, name)
) STRICT, WITHOUT ROWID;
`

// Store is safe for concurrent use. Times come from the clock given to Open
// and come back in UTC.
//
// Session ids given to MarkPushed must be unique across daemon lifetimes: a
// delivery keeps the last session that received its push, so a reused id
// would keep a message from the push to a new session.
type Store struct {
	db  *sql.DB
	now func() time.Time
}

// Agent is an agent known on a topic. Online is the time of its last online
// message there, zero when it had none.
type Agent struct {
	Topic      string
	Name       string
	Registered time.Time
	Online     time.Time
}

// Pending is a message that waits for the acknowledgement of an agent.
// Session is the id of the last session that received a push of it, or "".
type Pending struct {
	message.Message
	Session string
}

// Sent is a stored message and the agents it went to. Skipped are the
// recipients of a broadcast whose pending cap it would pass. Queued are the
// members of a team that the daemon queued it for.
type Sent struct {
	Message    message.Message
	Recipients []string
	Skipped    []string
	Queued     []team.Queued
	// Dropped is true when Receive did not store a broadcast that reaches
	// no agent of this machine.
	Dropped bool
}

// Open opens or creates the database at path in WAL mode. now is the clock;
// nil means time.Now.
func Open(path string, now func() time.Time) (*Store, error) {
	if now == nil {
		now = time.Now
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, err
	}
	// SQLite gives the -wal and -shm files the mode of the database file.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	f.Close()
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, err
	}
	// _txlock=immediate takes the write lock at BEGIN, where the busy timeout
	// applies. A deferred transaction that reads and then writes fails at
	// once when another connection wrote in between.
	dsn := url.URL{Scheme: "file", Path: path,
		RawQuery: "_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate"}
	db, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := dropTeamTables(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("drop team tables in %s: %w", path, err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema in %s: %w", path, err)
	}
	if err := upgrade(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("upgrade schema in %s: %w", path, err)
	}
	return &Store{db: db, now: now}, nil
}

// dropTeamTables drops the team tables that a courier.db of an earlier
// release holds: teams, the send queue and the receive dedupe live in the
// daemon's memory.
func dropTeamTables(db *sql.DB) error {
	_, err := db.Exec(`DROP TABLE IF EXISTS outbox; DROP TABLE IF EXISTS seen;
		DROP INDEX IF EXISTS messages_wire; DROP TABLE IF EXISTS invites; DROP TABLE IF EXISTS members;
		DROP TABLE IF EXISTS team_topics; DROP TABLE IF EXISTS teams;`)
	return err
}

// upgrade adds what a courier.db of an earlier release lacks: the column
// origin of messages.
func upgrade(db *sql.DB) error {
	return addColumn(db, "messages", "origin", `TEXT NOT NULL DEFAULT ''`)
}

// addColumn adds a column that a courier.db of an earlier release lacks.
// CREATE TABLE IF NOT EXISTS does not add it to a table that exists.
func addColumn(db *sql.DB, table, column, decl string) error {
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := db.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + column + ` ` + decl)
	return err
}

func (s *Store) Close() error { return s.db.Close() }

// Now is the time of the store's clock in UTC, to the millisecond. The daemon
// stamps new messages with it, so retention counts on one clock.
func (s *Store) Now() time.Time { return s.now().UTC().Truncate(time.Millisecond) }

// cutoff is the oldest time, in Unix milliseconds, that retention keeps.
func (s *Store) cutoff() int64 { return s.now().Add(-Retention).UnixMilli() }

func fromMillis(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// Register records that agent registered on topic now. When the last online
// of agent on topic is OnlineEvery or more ago, it also stores an online
// message in the same transaction and returns it; else it returns nil.
func (s *Store) Register(ctx context.Context, topic, agent string) (*Sent, error) {
	sent, err := s.RegisterTopics(ctx, agent, []string{topic})
	if err != nil || len(sent) == 0 {
		return nil, err
	}
	return &sent[0], nil
}

// RegisterTopics registers agent on each topic as Register does, in one
// transaction: on an error no topic is registered and no online is stored.
// It returns the online messages it stored.
func (s *Store) RegisterTopics(ctx context.Context, agent string, topics []string) ([]Sent, error) {
	if err := message.CheckAgent(agent); err != nil {
		return nil, err
	}
	for _, topic := range topics {
		if err := message.CheckTopic(topic); err != nil {
			return nil, err
		}
	}
	now := s.now().UTC().Truncate(time.Millisecond)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var sent []Sent
	for _, topic := range topics {
		var last int64
		err = tx.QueryRowContext(ctx, `SELECT online FROM agents WHERE topic = ? AND name = ?`, topic, agent).Scan(&last)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		due := now.UnixMilli()-last >= OnlineEvery.Milliseconds()
		if due {
			last = now.UnixMilli()
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO agents (topic, name, registered, online) VALUES (?, ?, ?, ?)
		ON CONFLICT (topic, name) DO UPDATE SET registered = excluded.registered, online = excluded.online`,
			topic, agent, now.UnixMilli(), last); err != nil {
			return nil, err
		}
		if due {
			got, err := s.send(ctx, tx, message.Message{ID: message.NewID(now), Sender: agent, Created: now,
				Fields: message.Fields{Topic: topic, Kind: message.KindOnline, Summary: agent + " is online on " + topic}}, false)
			if err != nil {
				return nil, err
			}
			sent = append(sent, got)
		}
	}
	return sent, tx.Commit()
}

// Touch keeps agent known on topic from now on, without an online message.
// The daemon calls it for each live session every hour and when a session
// ends, so retention counts from the end of the last session.
func (s *Store) Touch(ctx context.Context, topic, agent string) error {
	if err := message.CheckTopic(topic); err != nil {
		return err
	}
	if err := message.CheckAgent(agent); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO agents (topic, name, registered, online) VALUES (?, ?, ?, 0)
		ON CONFLICT (topic, name) DO UPDATE SET registered = excluded.registered`, topic, agent, s.now().UnixMilli())
	return err
}

// Agents lists the agents known on topic, by name.
func (s *Store) Agents(ctx context.Context, topic string) ([]Agent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, registered, online FROM agents
		WHERE topic = ? AND registered >= ? ORDER BY name`, topic, s.cutoff())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Agent
	for rows.Next() {
		a := Agent{Topic: topic}
		var registered, online int64
		if err := rows.Scan(&a.Name, &registered, &online); err != nil {
			return nil, err
		}
		a.Registered, a.Online = fromMillis(registered), fromMillis(online)
		out = append(out, a)
	}
	return out, rows.Err()
}

// Send stores m and makes it pending for its recipients: m.To when set, else
// every agent known on the topic except the sender. The sender must be known
// on the topic, a recipient must be known on it, and a reply must answer an
// ask on it. A direct message that would pass a pending cap fails with a
// *QueueFullError; a broadcast skips such recipients.
func (s *Store) Send(ctx context.Context, m message.Message) (Sent, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Sent{}, err
	}
	defer tx.Rollback()
	sent, err := s.send(ctx, tx, m, false)
	if err != nil {
		return Sent{}, err
	}
	return sent, tx.Commit()
}

// send stores m in tx. A local message needs a sender known on its topic. A
// remote message, with Origin set, comes from a member instead, and its
// pending cap per sender counts every message of that member, whatever
// agent name its machine gave. remoteTo is true for a message to an agent
// of another member's machine: it reaches no agent of this one.
func (s *Store) send(ctx context.Context, tx *sql.Tx, m message.Message, remoteTo bool) (Sent, error) {
	if err := m.Validate(); err != nil {
		return Sent{}, err
	}
	cutoff := s.cutoff()
	exists := func(query string, args ...any) (bool, error) {
		var one int
		err := tx.QueryRowContext(ctx, query, args...).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return err == nil, err
	}
	if m.Origin == "" {
		if ok, err := exists(`SELECT 1 FROM agents WHERE topic = ? AND name = ? AND registered >= ?`, m.Topic, m.Sender, cutoff); err != nil {
			return Sent{}, err
		} else if !ok {
			return Sent{}, ErrSenderNotOnTopic
		}
	}
	if m.Kind == message.KindReply {
		if ok, err := exists(`SELECT 1 FROM messages WHERE id = ? AND topic = ? AND kind = ? AND created >= ?`,
			m.Re.String(), m.Topic, message.KindAsk, cutoff); err != nil {
			return Sent{}, err
		} else if !ok {
			return Sent{}, ErrNoAsk
		}
	}
	var candidates []string
	if !remoteTo {
		var err error
		candidates, err = names(tx.QueryContext(ctx, `SELECT name FROM agents WHERE topic = ?1 AND registered >= ?2
			AND (name = ?3 OR (?3 = '' AND name <> ?4)) ORDER BY name`, m.Topic, cutoff, m.To, m.Sender))
		if err != nil {
			return Sent{}, err
		}
		if m.To != "" && len(candidates) == 0 {
			return Sent{}, ErrUnknownRecipient
		}
	}
	sent := Sent{Message: m}
	for _, r := range candidates {
		if m.Kind != message.KindOnline {
			var total, fromSender int
			if err := tx.QueryRowContext(ctx, `SELECT count(*), count(*) FILTER (WHERE m.origin = ?1 AND (?1 <> '' OR m.sender = ?2))
				FROM deliveries d JOIN messages m ON m.id = d.message_id
				WHERE d.agent = ?3 AND d.acked IS NULL AND m.kind <> ?4 AND m.created >= ?5`,
				m.Origin, m.Sender, r, message.KindOnline, cutoff).Scan(&total, &fromSender); err != nil {
				return Sent{}, err
			}
			if total >= MaxPendingTotal || fromSender >= MaxPendingFromSender {
				if m.To != "" {
					return Sent{}, &QueueFullError{Recipient: r, Sender: m.Sender, Total: total >= MaxPendingTotal}
				}
				sent.Skipped = append(sent.Skipped, r)
				continue
			}
		}
		sent.Recipients = append(sent.Recipients, r)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO messages (id, topic, sender, recipient, kind, summary, issue, link, re, created, origin)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID.String(), m.Topic, m.Sender, m.To, m.Kind, m.Summary, m.Issue, m.Link, reText(m.Re), m.Created.UnixMilli(), m.Origin); err != nil {
		return Sent{}, err
	}
	for _, r := range sent.Recipients {
		if m.Kind == message.KindOnline {
			if _, err := tx.ExecContext(ctx, `DELETE FROM deliveries WHERE agent = ? AND acked IS NULL AND message_id IN
				(SELECT id FROM messages WHERE topic = ? AND sender = ? AND kind = ?)`,
				r, m.Topic, m.Sender, message.KindOnline); err != nil {
				return Sent{}, err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO deliveries (message_id, agent) VALUES (?, ?)`, m.ID.String(), r); err != nil {
			return Sent{}, err
		}
	}
	return sent, nil
}

func names(rows *sql.Rows, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func reText(re message.ID) string {
	if re == (message.ID{}) {
		return ""
	}
	return re.String()
}

// Pending returns up to limit messages that wait for agent, direct messages
// first and older messages first within each group, and the number of all
// messages that wait. A limit of 0 or less returns them all.
func (s *Store) Pending(ctx context.Context, agent string, limit int) ([]Pending, int, error) {
	const where = `FROM deliveries d JOIN messages m ON m.id = d.message_id
		WHERE d.agent = ? AND d.acked IS NULL AND m.created >= ?`
	if limit <= 0 {
		limit = -1
	}
	cutoff := s.cutoff()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()
	var total int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) `+where, agent, cutoff).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT m.id, m.topic, m.sender, m.recipient, m.kind, m.summary, m.issue, m.link, m.re, m.created, m.origin, d.session `+
		where+` ORDER BY m.recipient = '', m.created, m.id LIMIT ?`, agent, cutoff, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []Pending
	for rows.Next() {
		var p Pending
		var id, re string
		var created int64
		if err := rows.Scan(&id, &p.Topic, &p.Sender, &p.To, &p.Kind, &p.Summary, &p.Issue, &p.Link, &re, &created, &p.Origin, &p.Session); err != nil {
			return nil, 0, err
		}
		if p.ID, err = message.ParseID(id); err != nil {
			return nil, 0, fmt.Errorf("message id %q: %w", id, err)
		}
		if re != "" {
			if p.Re, err = message.ParseID(re); err != nil {
				return nil, 0, fmt.Errorf("re of %s: %w", id, err)
			}
		}
		p.Created = fromMillis(created)
		out = append(out, p)
	}
	return out, total, rows.Err()
}

// MarkPushed records that session received a push of message id for agent.
func (s *Store) MarkPushed(ctx context.Context, agent, session string, id message.ID) error {
	return s.updatePending(ctx, `UPDATE deliveries SET session = ?`, session, agent, id)
}

// Ack ends the wait of agent for message id. Only a recipient can
// acknowledge a message, and only once.
func (s *Store) Ack(ctx context.Context, agent string, id message.ID) error {
	return s.updatePending(ctx, `UPDATE deliveries SET acked = ?`, s.now().UnixMilli(), agent, id)
}

func (s *Store) updatePending(ctx context.Context, update string, value any, agent string, id message.ID) error {
	res, err := s.db.ExecContext(ctx, update+` WHERE agent = ? AND message_id = ? AND acked IS NULL
		AND message_id IN (SELECT id FROM messages WHERE created >= ?)`, value, agent, id.String(), s.cutoff())
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrNotPending
	}
	return nil
}

// Cleanup deletes the messages and the agents that retention no longer
// keeps. The other methods already ignore them; Cleanup frees the space.
func (s *Store) Cleanup(ctx context.Context) error {
	cutoff := s.cutoff()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM messages WHERE created < ?`, cutoff); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM agents WHERE registered < ?`, cutoff); err != nil {
		return err
	}
	return tx.Commit()
}
