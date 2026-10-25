package message

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestIDsSortByCreationTime(t *testing.T) {
	t0 := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	var ids []string
	for i := range 2000 {
		// Two ids per millisecond, so the order inside a millisecond counts too.
		ids = append(ids, NewID(t0.Add(time.Duration(i/2)*time.Millisecond)).String())
	}
	if !slices.IsSorted(ids) {
		t.Fatal("ids do not sort by creation time")
	}
	early, late := NewID(t0).String(), NewID(t0.Add(time.Millisecond)).String()
	if early >= late {
		t.Fatalf("%s sorts after %s", early, late)
	}
}

func TestIDsDoNotCollide(t *testing.T) {
	const n = 1_000_000
	seen := make(map[ID]struct{}, n)
	now := time.Now()
	for i := range n {
		// 1000 ids share each millisecond, so most come from the monotonic
		// path and the first of each millisecond from the random one.
		id := NewID(now.Add(time.Duration(i/1000) * time.Millisecond))
		if _, dup := seen[id]; dup {
			t.Fatalf("id %s repeats after %d ids", id, i)
		}
		seen[id] = struct{}{}
	}
}

func TestIDText(t *testing.T) {
	id := NewID(time.UnixMilli(1<<48 - 1))
	s := id.String()
	if len(s) != 26 || s[:10] != "7ZZZZZZZZZ" {
		t.Fatalf("max time encodes as %s", s)
	}
	for _, in := range []string{s, strings.ToLower(s)} {
		got, err := ParseID(in)
		if err != nil || got != id {
			t.Fatalf("ParseID(%s) = %s, %v", in, got, err)
		}
	}
	if got := (ID{}).String(); got != strings.Repeat("0", 26) {
		t.Fatalf("zero id is %s", got)
	}
	ms := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC).UnixMilli()
	if got := NewID(time.UnixMilli(ms)); got[0] != byte(ms>>40) || got[5] != byte(ms) {
		t.Fatalf("time bits of %x do not hold %d", got, ms)
	}
	for _, bad := range []string{"", s[:25], s + "0", "8" + s[1:], "U" + s[1:], s[:25] + "I", s[:25] + "!", s[:25] + "\u00e9"[:1]} {
		if _, err := ParseID(bad); err == nil {
			t.Errorf("ParseID(%q) succeeds", bad)
		}
	}
}
