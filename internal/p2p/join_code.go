package p2p

import (
	"encoding/base32"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/CappyT/agent-courier/internal/team"
)

// MaxInviteCode is the most characters an invite code has: version 1 with the
// longest team name, 4 addresses of team.MaxAddr bytes, and room to spare.
const MaxInviteCode = 2048

// inviteVersion is the first byte of an invite code.
const inviteVersion = 1

// inviteEncoding is base32 of RFC 4648 in lower case, without padding. A code
// is pasted into a shell, so it has no character that needs quoting.
var inviteEncoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

var (
	// ErrBadInviteCode names the rule of a code and never quotes it.
	ErrBadInviteCode = errors.New("an invite code is the base32 text that agent-courier team invite prints on another member's machine")
	ErrInviteExpired = errors.New("the invite code expired: ask a member for a new one")
)

// Invite is what an invite code carries: what the joiner needs to reach the
// inviter, and the secret of the invite.
type Invite struct {
	Team    string
	Inviter peer.ID
	// Addrs are addresses of the inviter without its /p2p part, at most
	// team.MaxAddrs.
	Addrs   []ma.Multiaddr
	Secret  []byte
	Expires time.Time
}

// Encode is the code of i. Version 1 is, in order: the version byte; the team
// name and the binary peer id of the inviter, each a 1-byte length and its
// bytes; the 16-byte secret; the expiry in Unix seconds, 8 bytes big-endian;
// the address count, 1 byte; and each address as a 2-byte big-endian length
// and its binary multiaddr. Integers are unsigned.
func (i Invite) Encode() (string, error) {
	if err := i.check(); err != nil {
		return "", err
	}
	id := []byte(i.Inviter)
	b := []byte{inviteVersion, byte(len(i.Team))}
	b = append(b, i.Team...)
	b = append(b, byte(len(id)))
	b = append(b, id...)
	b = append(b, i.Secret...)
	b = binary.BigEndian.AppendUint64(b, uint64(i.Expires.Unix()))
	b = append(b, byte(len(i.Addrs)))
	for _, a := range i.Addrs {
		b = binary.BigEndian.AppendUint16(b, uint16(len(a.Bytes())))
		b = append(b, a.Bytes()...)
	}
	code := inviteEncoding.EncodeToString(b)
	if len(code) > MaxInviteCode {
		return "", ErrBadInviteCode
	}
	return code, nil
}

// check applies the rules of a code to i.
func (i Invite) check() error {
	if team.CheckName(i.Team) != nil || len(i.Secret) != team.SecretSize || len(i.Addrs) > team.MaxAddrs || i.Expires.Unix() <= 0 {
		return ErrBadInviteCode
	}
	if !canonicalPeer(i.Inviter.String()) || len(i.Inviter) > 255 {
		return ErrBadInviteCode
	}
	for _, a := range i.Addrs {
		if n := len(a.Bytes()); n == 0 || n > team.MaxAddr || len(a.String()) > team.MaxAddr {
			return ErrBadInviteCode
		}
		if _, last := ma.SplitLast(a); last != nil && last.Code() == ma.P_P2P {
			return ErrBadInviteCode
		}
	}
	return nil
}

// inviteAddrs are the first team.MaxAddrs addresses of addrs that fit an
// invite code and that joinDialable keeps, without a trailing /p2p part.
func inviteAddrs(addrs []ma.Multiaddr, allowLoopback bool) []ma.Multiaddr {
	var out []ma.Multiaddr
	for _, a := range addrs {
		if rest, last := ma.SplitLast(a); last != nil && last.Code() == ma.P_P2P {
			a = rest
		}
		if n := len(a.Bytes()); n == 0 || n > team.MaxAddr || len(a.String()) > team.MaxAddr || !joinDialable(a, allowLoopback) {
			continue
		}
		if out = append(out, a); len(out) == team.MaxAddrs {
			break
		}
	}
	return out
}

// joinDialable reports whether an invite code may carry a, and the joiner
// dial it: Dialable of the host. allowLoopback keeps loopback addresses, for
// tests on one machine.
func joinDialable(a ma.Multiaddr, allowLoopback bool) bool {
	if allowLoopback && manet.IsIPLoopback(a) {
		return !slices.ContainsFunc(a, func(c ma.Component) bool { return c.Protocol().Code == ma.P_UNIX })
	}
	return Dialable(a)
}

// dialableAddrs are the addresses of addrs that joinDialable keeps.
func dialableAddrs(addrs []ma.Multiaddr, allowLoopback bool) []ma.Multiaddr {
	return slices.DeleteFunc(slices.Clone(addrs), func(a ma.Multiaddr) bool { return !joinDialable(a, allowLoopback) })
}

// DecodeInvite parses an invite code. Case and the white space around it do
// not count. Every error is ErrBadInviteCode, which does not quote the code.
// An expired code decodes: the caller checks Expires. It drops the addresses
// that Dialable refuses: /unix, loopback, link-local and unspecified.
func DecodeInvite(code string) (Invite, error) { return decodeInvite(code, false) }

func decodeInvite(code string, allowLoopback bool) (Invite, error) {
	code = strings.ToLower(strings.TrimSpace(code))
	if code == "" || len(code) > MaxInviteCode {
		return Invite{}, ErrBadInviteCode
	}
	b, err := inviteEncoding.DecodeString(code)
	if err != nil {
		return Invite{}, ErrBadInviteCode
	}
	r := codeReader{b: b}
	if r.byte() != inviteVersion {
		return Invite{}, ErrBadInviteCode
	}
	var i Invite
	i.Team = string(r.next(int(r.byte())))
	id, err := peer.IDFromBytes(r.next(int(r.byte())))
	i.Inviter = id
	i.Secret = slices.Clone(r.next(team.SecretSize))
	i.Expires = time.Unix(int64(binary.BigEndian.Uint64(r.next(8))), 0)
	n := int(r.byte())
	for range min(n, team.MaxAddrs+1) {
		a, aerr := ma.NewMultiaddrBytes(r.next(int(binary.BigEndian.Uint16(r.next(2)))))
		if aerr != nil {
			r.bad = true
			break
		}
		i.Addrs = append(i.Addrs, a)
	}
	if r.bad || len(r.b) != 0 || err != nil || i.check() != nil {
		return Invite{}, ErrBadInviteCode
	}
	i.Addrs = dialableAddrs(i.Addrs, allowLoopback)
	return i, nil
}

// codeReader reads the fields of a code. A read past the end sets bad.
type codeReader struct {
	b   []byte
	bad bool
}

func (r *codeReader) next(n int) []byte {
	if n > len(r.b) {
		r.bad = true
		r.b = nil
		return make([]byte, n)
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *codeReader) byte() byte { return r.next(1)[0] }
