package main

import "testing"

func TestRepoFromURL(t *testing.T) {
	for url, want := range map[string]string{
		"git@github.com:CappyT/agent-courier.git\n":     "agent-courier",
		"https://github.com/CappyT/agent-courier.git":   "agent-courier",
		"https://github.com/CappyT/agent-courier/":      "agent-courier",
		"ssh://git@git.example.com:2222/acme-corp/kube": "kube",
		"git@host:kube.git":                             "kube",
	} {
		if got := repoFromURL(url); got != want {
			t.Errorf("repoFromURL(%q) = %q, want %q", url, got, want)
		}
	}
}
