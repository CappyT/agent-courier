package main

import "testing"

func TestTopicFromURL(t *testing.T) {
	for url, want := range map[string]string{
		"git@github.com:CappyT/agent-courier.git\n":     "cappyt_agent-courier",
		"https://github.com/CappyT/agent-courier.git":   "cappyt_agent-courier",
		"https://github.com/CappyT/agent-courier/":      "cappyt_agent-courier",
		"ssh://git@git.example.com:2222/acme-corp/kube": "acme-corp_kube",
		"https://gitlab.com/group/sub/api":              "sub_api",
		"git@host:kube.git":                             "kube",
		"/srv/git/kube.git":                             "git_kube",
	} {
		if got := topicFromURL(url); got != want {
			t.Errorf("topicFromURL(%q) = %q, want %q", url, got, want)
		}
	}
}
