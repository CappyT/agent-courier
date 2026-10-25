package message

import (
	"crypto/rand"
	"errors"
	"sync"
	"time"
)

// ID is a ULID: 48 bits of Unix milliseconds and 80 random bits. Its text
// form is 26 characters of Crockford base32, which sort like the creation
// times.
type ID [16]byte

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var decode = func() (t [256]byte) {
	for i := range t {
		t[i] = 0xff
	}
	for i, c := range []byte(crockford) {
		t[c] = byte(i)
		t[c|0x20] = byte(i)
	}
	return t
}()

var gen struct {
	sync.Mutex
	last ID
}

// NewID returns a new ID for the time now. IDs sort by that time. Calls that
// follow each other with the same millisecond return increasing IDs. A call
// with an earlier time than the call before it starts a new random sequence,
// so its ID sorts by its own time, before the IDs made for later times.
func NewID(now time.Time) ID {
	ms := uint64(now.UnixMilli())
	var id ID
	for i := range 6 {
		id[i] = byte(ms >> (40 - 8*i))
	}
	gen.Lock()
	defer gen.Unlock()
	if [6]byte(id[:6]) == [6]byte(gen.last[:6]) && increment(&gen.last) {
		return gen.last
	}
	rand.Read(id[6:])
	gen.last = id
	return id
}

// increment adds one to the random part of id and reports false when it
// overflows.
func increment(id *ID) bool {
	for i := len(id) - 1; i >= 6; i-- {
		if id[i]++; id[i] != 0 {
			return true
		}
	}
	return false
}

// ParseID reads the text form of an ID, in upper or lower case.
func ParseID(s string) (ID, error) {
	var id ID
	return id, id.UnmarshalText([]byte(s))
}

func (id ID) String() string {
	b, _ := id.MarshalText()
	return string(b)
}

func (id ID) MarshalText() ([]byte, error) {
	b := make([]byte, 26)
	// 128 bits in 26 groups of 5 bits: the first group holds the top 3 bits.
	hi := uint64(id[0])<<56 | uint64(id[1])<<48 | uint64(id[2])<<40 | uint64(id[3])<<32 |
		uint64(id[4])<<24 | uint64(id[5])<<16 | uint64(id[6])<<8 | uint64(id[7])
	lo := uint64(id[8])<<56 | uint64(id[9])<<48 | uint64(id[10])<<40 | uint64(id[11])<<32 |
		uint64(id[12])<<24 | uint64(id[13])<<16 | uint64(id[14])<<8 | uint64(id[15])
	for i := 25; i >= 0; i-- {
		b[i] = crockford[lo&31]
		lo = lo>>5 | hi<<59
		hi >>= 5
	}
	return b, nil
}

var errID = errors.New("an id is 26 characters of Crockford base32 that start with 0 to 7")

func (id *ID) UnmarshalText(b []byte) error {
	if len(b) != 26 || decode[b[0]] > 7 {
		return errID
	}
	var hi, lo uint64
	for _, c := range b {
		v := decode[c]
		if v == 0xff {
			return errID
		}
		hi = hi<<5 | lo>>59
		lo = lo<<5 | uint64(v)
	}
	for i := range 8 {
		id[i] = byte(hi >> (56 - 8*i))
		id[8+i] = byte(lo >> (56 - 8*i))
	}
	return nil
}
