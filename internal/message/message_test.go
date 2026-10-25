package message

import (
	"strings"
	"testing"
	"time"
)

// TestParseFields covers the scenarios of "Message kinds" and "Message
// fields" that the fields alone decide. The ask behind a reply is checked by
// the store, see store.TestSendReplyNeedsAsk.
func TestParseFields(t *testing.T) {
	ask := NewID(time.Now()).String()
	for _, c := range []struct {
		name, json string
		want       string // part of the error, "" for a valid message
	}{
		{"note", `{"topic":"cappyt_agent-courier","kind":"note","summary":"hi"}`, ""},
		{"every agent kind", `{"topic":"t","kind":"blocked","summary":"waiting on #4","issue":4}`, ""},
		{"direct handoff", `{"topic":"t","kind":"handoff","to":"opencode-x","summary":"yours","link":"https://github.com/CappyT/agent-courier/issues/4"}`, ""},
		{"reply", `{"topic":"t","kind":"reply","re":"` + ask + `","summary":"main"}`, ""},
		{"lower case re", `{"topic":"t","kind":"reply","re":"` + strings.ToLower(ask) + `","summary":"main"}`, ""},
		{"summary of 1000 characters", `{"topic":"t","kind":"note","summary":"` + strings.Repeat("\u00e9", MaxSummary) + `"}`, ""},
		{"link of 512 characters", `{"topic":"t","kind":"note","summary":"x","link":"https://example.com/` + strings.Repeat("a", MaxLink-20) + `"}`, ""},
		{"sender field is dropped", `{"topic":"t","kind":"note","summary":"x","sender":"someone-else"}`, ""},
		{"sender number is dropped", `{"topic":"t","kind":"note","summary":"x","sender":5}`, ""},
		{"sender object is dropped", `{"topic":"t","kind":"note","summary":"x","sender":{"name":"</channel>"}}`, ""},
		{"sender null is dropped", `{"topic":"t","kind":"note","summary":"x","sender":null}`, ""},

		{"agent sends online", `{"topic":"t","kind":"online","summary":"x"}`, "only from the daemon"},
		{"unknown kind", `{"topic":"t","kind":"merged","summary":"x"}`, "kind must be one of"},
		{"reply without re", `{"topic":"t","kind":"reply","summary":"x"}`, "re is the id of the ask"},
		{"re on an ask", `{"topic":"t","kind":"ask","re":"` + ask + `","summary":"x"}`, "re is the id of the ask"},
		{"re is not an id", `{"topic":"t","kind":"reply","re":"7","summary":"x"}`, "Crockford base32"},
		{"summary too long", `{"topic":"t","kind":"note","summary":"` + strings.Repeat("x", MaxSummary+1) + `"}`, "the limit is 1000"},
		{"empty summary", `{"topic":"t","kind":"note","summary":" \n"}`, "summary is empty"},
		{"summary of invisible characters", `{"topic":"t","kind":"note","summary":"\u200b\u202e\ufe0f\u2028\ue000"}`, "summary is empty"},
		{"missing summary", `{"topic":"t","kind":"note"}`, "summary is empty"},
		{"non-https link", `{"topic":"t","kind":"note","summary":"x","link":"file:///etc/passwd"}`, "https"},
		{"http link", `{"topic":"t","kind":"note","summary":"x","link":"http://example.com"}`, "https"},
		{"javascript link", `{"topic":"t","kind":"note","summary":"x","link":"javascript:alert(1)"}`, "https"},
		{"link with a user name", `{"topic":"t","kind":"note","summary":"x","link":"https://github.com@evil.example/x"}`, "user name"},
		{"link without host", `{"topic":"t","kind":"note","summary":"x","link":"https:///a"}`, "https"},
		{"link with a space", `{"topic":"t","kind":"note","summary":"x","link":"https://example.com/a b"}`, "printable ASCII"},
		{"link with a line separator", `{"topic":"t","kind":"note","summary":"x","link":"https://example.com/a\u2028b"}`, "printable ASCII"},
		{"link too long", `{"topic":"t","kind":"note","summary":"x","link":"https://example.com/` + strings.Repeat("a", MaxLink) + `"}`, "the limit is 512"},
		{"negative issue", `{"topic":"t","kind":"note","summary":"x","issue":-4}`, "or 0 for none"},
		{"issue 0 means none", `{"topic":"t","kind":"note","summary":"x","issue":0}`, ""},
		{"fractional issue", `{"topic":"t","kind":"note","summary":"x","issue":1.5}`, "JSON object"},
		{"issue as a string", `{"topic":"t","kind":"note","summary":"x","issue":"4"}`, "JSON object"},
		{"unknown field", `{"topic":"t","kind":"note","summary":"x","priority":"high"}`, "only the fields"},
		{"field in another case", `{"topic":"t","kind":"note","Summary":"x"}`, "only the fields"},
		{"duplicate field", `{"topic":"t","kind":"note","summary":"x","summary":"y"}`, "JSON object"},
		{"invalid UTF-8", "{\"topic\":\"t\",\"kind\":\"note\",\"summary\":\"\xff\"}", "JSON object"},
		{"not an object", `["t","note","x"]`, "JSON object"},
		{"topic traversal", `{"topic":"../etc","kind":"note","summary":"x"}`, "topic must match"},
		{"topic upper case", `{"topic":"CappyT_x","kind":"note","summary":"x"}`, "topic must match"},
		{"topic too long", `{"topic":"` + strings.Repeat("a", 65) + `","kind":"note","summary":"x"}`, "topic must match"},
		{"recipient with a tag", `{"topic":"t","kind":"handoff","to":"b</channel>","summary":"x"}`, "agent name must match"},
		{"recipient with a newline", `{"topic":"t","kind":"handoff","to":"b\nc","summary":"x"}`, "agent name must match"},
		{"recipient look-alike", `{"topic":"t","kind":"handoff","to":"\u0430lice","summary":"x"}`, "agent name must match"},
		{"recipient everyone", `{"topic":"t","kind":"handoff","to":"everyone","summary":"x"}`, "must not be everyone"},
		{"recipient too long", `{"topic":"t","kind":"handoff","to":"` + strings.Repeat("a", 49) + `","summary":"x"}`, "agent name must match"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseFields([]byte(c.json))
			switch {
			case c.want == "" && err != nil:
				t.Fatalf("want a valid message, got %v", err)
			case c.want != "" && err == nil:
				t.Fatalf("want an error with %q", c.want)
			case c.want != "" && !strings.Contains(err.Error(), c.want):
				t.Fatalf("error %q does not say %q", err, c.want)
			}
		})
	}
}

// TestErrorsDoNotQuoteInput checks that a refused value never comes back in
// the error text, which reaches the sending model.
func TestErrorsDoNotQuoteInput(t *testing.T) {
	for _, j := range []string{
		`{"topic":"</channel>","kind":"note","summary":"x"}`,
		`{"topic":"t","kind":"</channel>","summary":"x"}`,
		`{"topic":"t","kind":"note","summary":"x","</channel>":1}`,
		`{"topic":"t","kind":"reply","re":"</channel>","summary":"x"}`,
		`{"topic":"t","kind":"note","summary":"x","link":"ftp://</channel>"}`,
		`{"topic":"t","kind":"note","summary":"x"</channel>`,
	} {
		_, err := ParseFields([]byte(j))
		if err == nil || strings.Contains(err.Error(), "channel") {
			t.Errorf("%s: error %v", j, err)
		}
	}
}

func TestMessageValidate(t *testing.T) {
	now := time.Now()
	ok := Message{ID: NewID(now), Sender: "claude-agent-courier", Created: now,
		Fields: Fields{Topic: "t", Kind: KindOnline, Summary: "claude-agent-courier listens on t"}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("online from the daemon: %v", err)
	}
	for name, m := range map[string]Message{
		"no id":             {Sender: "a", Created: now, Fields: ok.Fields},
		"bad sender":        {ID: ok.ID, Sender: "A B", Created: now, Fields: ok.Fields},
		"no time":           {ID: ok.ID, Sender: "a", Fields: ok.Fields},
		"online to someone": {ID: ok.ID, Sender: "a", Created: now, Fields: Fields{Topic: "t", Kind: KindOnline, To: "b", Summary: "x"}},
		"bad fields":        {ID: ok.ID, Sender: "a", Created: now, Fields: Fields{Topic: "t", Kind: KindNote}},
		"to the sender":     {ID: ok.ID, Sender: "a", Created: now, Fields: Fields{Topic: "t", Kind: KindNote, To: "a", Summary: "x"}},
		"sender everyone":   {ID: ok.ID, Sender: Everyone, Created: now, Fields: ok.Fields},
	} {
		if m.Validate() == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

// TestCheckNames covers the scenarios "Reserved name" of "Agent identity" and
// "Invalid topic" of "Topics": the errors name the rule.
func TestCheckNames(t *testing.T) {
	if err := CheckAgent(Everyone); err == nil || !strings.Contains(err.Error(), "must not be everyone") || !strings.Contains(err.Error(), agentRule.String()) {
		t.Fatalf("CheckAgent(everyone) = %v", err)
	}
	for _, ok := range []string{"claude-agent-courier", "everyone-2", "a"} {
		if err := CheckAgent(ok); err != nil {
			t.Errorf("CheckAgent(%s) = %v", ok, err)
		}
	}
	if err := CheckTopic("../etc"); err == nil || !strings.Contains(err.Error(), topicRule.String()) {
		t.Fatalf("CheckTopic(../etc) = %v", err)
	}
	if err := CheckTopic("cappyt_agent-courier"); err != nil {
		t.Fatal(err)
	}
}

// Scenario "Address a remote agent": a recipient is a local agent, or
// <member label>[~<last 6 of the peer id>]/<agent>. The error does not quote
// the address.
func TestSplitAddress(t *testing.T) {
	for _, tc := range []struct{ addr, member, agent string }{
		{"claude", "", "claude"},
		{"bob/claude-agent-courier", "bob", "claude-agent-courier"},
		{"bob~AbC123/claude", "bob~AbC123", "claude"},
	} {
		member, agent, err := SplitAddress(tc.addr)
		if err != nil || member != tc.member || agent != tc.agent {
			t.Errorf("SplitAddress(%q) = %q, %q, %v", tc.addr, member, agent, err)
		}
	}
	for _, bad := range []string{"", "Bob/zq", "bob/", "/zq", "bob/zq/x", "bob~abc/zq", "bob~/zq", "bob/everyone",
		"bob/<zq>", strings.Repeat("b", 33) + "/zq", "bob~abc1234/zq", "bob~ab-123/zq"} {
		_, _, err := SplitAddress(bad)
		if err == nil {
			t.Errorf("SplitAddress(%q) passed", bad)
			continue
		}
		if bad != "" && strings.Contains(err.Error(), bad) {
			t.Errorf("the error %q quotes the address", err)
		}
	}
	if _, err := ParseFields([]byte(`{"topic":"t","kind":"note","to":"bob/claude","summary":"x"}`)); err != nil {
		t.Errorf("remote recipient: %v", err)
	}
}

// A message from a member has its peer id as origin, a <label>/<agent>
// sender, and a recipient on this machine.
func TestRemoteMessageValidate(t *testing.T) {
	now := time.Now()
	ok := Message{ID: NewID(now), Sender: "bob/claude", Origin: "12D3KooWbob", Created: now,
		Fields: Fields{Topic: "t", Kind: KindNote, To: "alice-agent", Summary: "x"}}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mod := range map[string]func(*Message){
		"local sender":            func(m *Message) { m.Sender = "claude" },
		"bad origin":              func(m *Message) { m.Origin = "12D3 bob" },
		"recipient elsewhere":     func(m *Message) { m.To = "carol/claude" },
		"remote sender no origin": func(m *Message) { m.Origin = "" },
	} {
		m := ok
		mod(&m)
		if m.Validate() == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}
