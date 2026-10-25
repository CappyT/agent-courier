package message

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

const (
	MaxPushLines       = 20
	MaxPushChars       = 1200
	MaxPendingMessages = 20
	MaxPendingBytes    = 16 << 10
	MaxNotice          = 80
	// MaxMarks caps the marks on one character.
	MaxMarks = 2
)

const cutMark = "| [cut]"

// Render is the text of m that a session shows to its model: a header line
// written by the daemon, then the link and the summary, each line prefixed
// with "| ". It shows at most MaxPushLines lines and MaxPushChars characters
// and ends with "| [cut]" when it drops text.
func Render(m Message) string {
	ref := Inline(m.Topic)
	if m.Issue != 0 {
		ref += "#" + strconv.Itoa(m.Issue)
	}
	header := fmt.Sprintf("%s -> %s: %s on %s (id %s", Inline(m.Sender), Inline(cmp.Or(m.To, Everyone)), Inline(string(m.Kind)), ref, m.ID)
	if m.Re != (ID{}) {
		header += ", re " + m.Re.String()
	}
	lines := append([]string{header + ")"}, quote(m.Link)...)
	return capLines(append(lines, quote(m.Summary)...))
}

// RenderPending renders the first messages of a pending list that fit in
// MaxPendingMessages and MaxPendingBytes. total is the length of the whole
// list, so the text can say how many more wait.
func RenderPending(ms []Message, total int) string {
	if total == 0 {
		return "No pending messages."
	}
	const more = "\n\n%d more wait: acknowledge the messages above to see them."
	var b strings.Builder
	fmt.Fprintf(&b, "%d pending messages, direct ones first, oldest first in each group:", total)
	// The footer counts at most total messages, so this reserves enough room.
	footer := len(fmt.Sprintf(more, total))
	shown := 0
	for _, m := range ms[:min(len(ms), MaxPendingMessages)] {
		r := Render(m)
		if b.Len()+2+len(r)+footer > MaxPendingBytes {
			break
		}
		b.WriteString("\n\n" + r)
		shown++
	}
	if total > shown {
		fmt.Fprintf(&b, more, total-shown)
	}
	return b.String()
}

// BudgetHeader is the header of a budget notice, which has no id.
const BudgetHeader = "courier -> %s: budget"

// RenderBudget is the text of the notice that the courier holds messages
// for the session of recipient because they passed budget, the text of a
// push budget. sender is the sender that passed a per-sender budget, empty
// for a budget of all senders.
func RenderBudget(sender, recipient, budget string) string {
	who := "Messages from all senders passed the budget of " + Inline(budget) + "."
	if sender != "" {
		who = Inline(sender) + " passed the push budget of " + Inline(budget) + "."
	}
	return capLines([]string{fmt.Sprintf(BudgetHeader, Inline(recipient)), who,
		"The courier holds these messages: call pending."})
}

// Notice is a one-line notice of m for a harness that shows one, at most
// MaxNotice characters. It names only the kind, the sender and, for a direct
// message, the recipient.
func Notice(m Message) string {
	s := "courier: " + Inline(string(m.Kind)) + " from " + Inline(m.Sender)
	if m.To != "" {
		s += " to " + Inline(m.To)
	}
	return cut(s, MaxNotice)
}

// Inline renders an inbound string for one line of text, such as an agent
// name or an error: visible characters only, escaped, at most MaxPushChars
// characters.
func Inline(s string) string {
	s = escape(strings.ReplaceAll(clean(s), "\n", " "))
	if utf8.RuneCountInString(s) > MaxPushChars {
		s = cut(s, MaxPushChars-len(" [cut]")) + " [cut]"
	}
	return s
}

// cut shortens escaped text to at most n characters. It drops an entity that
// the cut would split, so no "&am" or "&l" remains at the end.
func cut(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	s = string(r[:n])
	if before, after, found := strings.CutLast(s, "&"); found && !strings.Contains(after, ";") {
		return before
	}
	return s
}

// quote returns the lines of s, cleaned, escaped and prefixed with "| ", so
// no line can pass for a line of the harness.
func quote(s string) []string {
	var out []string
	for line := range strings.Lines(clean(s)) {
		out = append(out, "| "+escape(strings.TrimSuffix(line, "\n")))
	}
	return out
}

// capLines joins lines and cuts them to MaxPushLines and MaxPushChars,
// including the cut mark.
func capLines(lines []string) string {
	n := len(lines) - 1
	for _, l := range lines {
		n += utf8.RuneCountInString(l)
	}
	if len(lines) <= MaxPushLines && n <= MaxPushChars {
		return strings.Join(lines, "\n")
	}
	budget := MaxPushChars - len(cutMark) - 1
	var b strings.Builder
	used := 0
	for i, l := range lines[:min(len(lines), MaxPushLines-1)] {
		sep := min(i, 1)
		left := budget - used - sep
		if left <= len("| ") {
			break
		}
		l = cut(l, left)
		if sep == 1 {
			b.WriteByte('\n')
		}
		b.WriteString(l)
		used += sep + utf8.RuneCountInString(l)
	}
	b.WriteString("\n" + cutMark)
	return b.String()
}

var escaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// escape keeps inbound text inside the tag a harness wraps around it.
func escape(s string) string { return escaper.Replace(s) }

// clean keeps only visible characters and newlines, so text cannot hide from
// the person who reads the session. NFKC first folds look-alikes such as the
// fullwidth U+FF1C into the ASCII characters that escape replaces. Then clean
// drops control and format characters such as bidi overrides, private-use
// and unassigned code points, variation selectors and other
// default-ignorable characters, keeps at most MaxMarks marks (nonspacing,
// spacing or enclosing) on one character, turns Unicode line and paragraph separators into newlines, and
// tabs and the braille blank into spaces.
func clean(s string) string {
	var b strings.Builder
	marks := 0
	for _, r := range norm.NFKC.String(s) {
		switch {
		case r == '\n', unicode.In(r, unicode.Zl, unicode.Zp):
			r = '\n'
		case r == '\t', r == '\u2800':
			r = ' '
		case !unicode.IsGraphic(r), unicode.In(r, unicode.Variation_Selector, unicode.Other_Default_Ignorable_Code_Point):
			continue
		}
		if unicode.IsMark(r) {
			if marks++; marks > MaxMarks {
				continue
			}
		} else {
			marks = 0
		}
		b.WriteRune(r)
	}
	return b.String()
}
