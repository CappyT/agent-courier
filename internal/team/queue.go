package team

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/CappyT/agent-courier/internal/message"
)

const (
	// QueueMinBackoff and QueueMaxBackoff bound the wait after a send to a
	// member that got no receipt: it doubles from the first to the second.
	QueueMinBackoff = 5 * time.Second
	QueueMaxBackoff = 10 * time.Minute
	// QueueBatch is the most items of one member that one Take returns.
	QueueBatch = 32
	// QueueMax is the most items that wait for one member, so a local agent
	// stuck in a loop cannot fill memory.
	QueueMax = 1000
	// DedupeTTL is how long a received message id stays a duplicate. A
	// sender retries every QueueMaxBackoff at most, so a lost receipt is
	// answered long before the record goes.
	DedupeTTL = 24 * time.Hour
	// MaxReceivedPerHour caps the messages this machine takes from one
	// member in one hour, stored or not, over all teams.
	MaxReceivedPerHour = 600
)

var (
	ErrReceiveRate = errors.New("the member sent the most messages this machine takes in one hour")
	// ErrReceiveBusy refuses a message while a receive of the same id from
	// the same member runs: the member sends it again.
	ErrReceiveBusy = errors.New("a message with this id from the member is being stored")
)

// QueueFullError refuses a message for a member that has QueueMax items
// waiting. Member is its label as Members.Member renders it.
type QueueFullError struct{ Member string }

func (e *QueueFullError) Error() string {
	return fmt.Sprintf("the send queue for the machine of %s is full: %d messages wait until it is reachable", e.Member, QueueMax)
}

// Backoff is the wait after the attempts-th send without a receipt.
func Backoff(attempts int) time.Duration {
	d := QueueMinBackoff
	for i := 1; i < attempts && d < QueueMaxBackoff; i++ {
		d *= 2
	}
	return min(d, QueueMaxBackoff)
}

// item is a message that waits in the send queue for one member.
type item struct {
	team     string
	msg      message.Message
	attempts int
	next     time.Time
}

// inboxKey names the messages that one member sent in one team.
type inboxKey struct{ team, peer string }

// received is a message id that a member sent: local is the id the store
// gave the message, zero when it stored none. done is false while the
// receive runs. at is when the receive ended.
type received struct {
	local message.ID
	done  bool
	at    time.Time
}

// Item is a message that waits for a member: its team, and the sends of it
// before this one that got no receipt.
type Item struct {
	Team     string
	Message  message.Message
	Attempts int
}

// Batch is the due items of the member PeerID, oldest first.
type Batch struct {
	PeerID string
	Items  []Item
}

// Enqueue queues m for each member of r that is still a member of r.Team
// and has fewer than QueueMax items waiting. It returns the members it
// queued m for, and full, the members it skipped for a full queue. m is a
// message of this machine. When m answers an ask that a member sent, the
// queued copy names the wire id of the ask, so the members find it.
func (s *Set) Enqueue(r Route, m message.Message) (queued, full []Queued) {
	if r.Team == "" || len(r.Peers) == 0 {
		return nil, nil
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if wire, ok := s.wire[r.Team][m.Re]; ok {
		m.Re = wire
	}
	for _, q := range r.Peers {
		switch {
		case !s.peers[q.PeerID][r.Team]:
		case len(s.queue[q.PeerID]) >= QueueMax:
			full = append(full, q)
		default:
			s.queue[q.PeerID] = append(s.queue[q.PeerID], &item{team: r.Team, msg: m, next: now})
			queued = append(queued, q)
		}
	}
	return queued, full
}

// Full returns the members of r whose queue holds QueueMax items, without a
// change, so a send to one member can fail before it stores anything.
func (s *Set) Full(r Route) []Queued {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var full []Queued
	for _, q := range r.Peers {
		if len(s.queue[q.PeerID]) >= QueueMax {
			full = append(full, q)
		}
	}
	return full
}

// Take returns, for each member that busy does not name and that has due
// items, up to QueueBatch of them, oldest first. Each item it returns gets
// its next attempt as if its send fails, so a lost receipt leaves it to the
// backoff. next is the earliest attempt among the members that busy does
// not name and that got no batch, and ok is false when there is none: the
// end of a send makes the caller take again. busy nil names no member;
// Take calls it with the set locked, so it must not call the set.
func (s *Set) Take(busy func(peer string) bool) (batches []Batch, next time.Time, ok bool) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, peer := range slices.Sorted(maps.Keys(s.queue)) {
		if busy != nil && busy(peer) {
			continue
		}
		var b Batch
		for _, it := range s.queue[peer] {
			if len(b.Items) == QueueBatch {
				break
			}
			if it.next.After(now) {
				continue
			}
			b.Items = append(b.Items, Item{Team: it.team, Message: it.msg, Attempts: it.attempts})
			it.attempts++
			it.next = now.Add(Backoff(it.attempts))
		}
		if len(b.Items) > 0 {
			b.PeerID = peer
			batches = append(batches, b)
			continue
		}
		for _, it := range s.queue[peer] {
			if !ok || it.next.Before(next) {
				next, ok = it.next, true
			}
		}
	}
	return batches, next, ok
}

// MakeDue makes the items for peer due at once, after it connected.
func (s *Set) MakeDue(peer string) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range s.queue[peer] {
		if it.next.After(now) {
			it.next = now
		}
	}
}

// Delivered deletes the item of message id for peer, the member that sent
// the receipt, and reports whether there was one. A receipt never touches
// the item of another member.
func (s *Set) Delivered(peer string, id message.ID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := s.queue[peer]
	i := slices.IndexFunc(items, func(it *item) bool { return it.msg.ID == id })
	if i < 0 {
		return false
	}
	items = slices.Delete(items, i, i+1)
	if len(items) == 0 {
		delete(s.queue, peer)
	} else {
		s.queue[peer] = items
	}
	return true
}

// QueueLen is the number of items that wait for peer.
func (s *Set) QueueLen(peer string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.queue[peer])
}

// BeginReceive starts the receive of the message with id wire from the
// member peer of team name. dup is true for an id that the member sent in
// the team in the last DedupeTTL: the caller sends the receipt again and
// stores nothing. Else BeginReceive holds the id until EndReceive, and a receive of
// the same id meanwhile fails with ErrReceiveBusy. A member that sent
// MaxReceivedPerHour messages in the last hour gets ErrReceiveRate, and a
// peer that is not a member of the team ErrNotMember.
func (s *Set) BeginReceive(name, peer string, wire message.ID) (dup bool, err error) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.peers[peer][name] {
		return false, ErrNotMember
	}
	k := inboxKey{name, peer}
	s.expire(k, now)
	r, seen := s.seen[k][wire]
	switch {
	case seen && r.done:
		return true, nil
	case seen:
		return false, ErrReceiveBusy
	}
	times := s.rate[peer]
	i := slices.IndexFunc(times, func(at time.Time) bool { return now.Sub(at) < time.Hour })
	if i < 0 {
		i = len(times)
	}
	times = times[i:]
	s.rate[peer] = times
	if len(times) >= MaxReceivedPerHour {
		return false, ErrReceiveRate
	}
	if s.seen[k] == nil {
		s.seen[k] = map[message.ID]received{}
	}
	s.seen[k][wire] = received{}
	return false, nil
}

// EndReceive ends the receive that BeginReceive started. keep records wire
// for DedupeTTL and counts it in the hourly cap of peer; local
// is the id the store gave the message, zero when it stored none. Without
// keep the id is free again, so the message can come again.
func (s *Set) EndReceive(name, peer string, wire, local message.ID, keep bool) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	k := inboxKey{name, peer}
	if r, ok := s.seen[k][wire]; !ok || r.done {
		// The member left, or the team ended, while the receive ran.
		return
	}
	if !keep {
		delete(s.seen[k], wire)
		return
	}
	s.seen[k][wire] = received{local: local, done: true, at: now}
	s.rate[peer] = append(s.rate[peer], now)
	if local != (message.ID{}) {
		if s.wire[name] == nil {
			s.wire[name] = map[message.ID]message.ID{}
		}
		s.wire[name][local] = wire
	}
}

// expire drops the records of the inbox k that ended DedupeTTL ago or more,
// with their wire ids. s.mu is held.
func (s *Set) expire(k inboxKey, now time.Time) {
	for wire, r := range s.seen[k] {
		if r.done && now.Sub(r.at) >= DedupeTTL {
			delete(s.seen[k], wire)
			delete(s.wire[k.team], r.local)
		}
	}
	if len(s.seen[k]) == 0 {
		delete(s.seen, k)
	}
}

// prune drops the queued items, the received ids and the receive times of
// each peer that is no longer a member of their team, and all of a team
// this machine left. s.mu is held, after index.
func (s *Set) prune() {
	for peer, items := range s.queue {
		items = slices.DeleteFunc(items, func(it *item) bool { return !s.peers[peer][it.team] })
		if len(items) == 0 {
			delete(s.queue, peer)
		} else {
			s.queue[peer] = items
		}
	}
	for k, ids := range s.seen {
		if s.peers[k.peer][k.team] {
			continue
		}
		for _, r := range ids {
			delete(s.wire[k.team], r.local)
		}
		delete(s.seen, k)
	}
	maps.DeleteFunc(s.wire, func(name string, _ map[message.ID]message.ID) bool { return s.teams[name] == nil })
	maps.DeleteFunc(s.rate, func(peer string, _ []time.Time) bool { return len(s.peers[peer]) == 0 })
}
