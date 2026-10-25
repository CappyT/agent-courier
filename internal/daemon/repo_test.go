//go:build unix

package daemon

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CappyT/agent-courier/internal/message"
)

func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

// clone makes dir/name a work tree whose .git/config holds config.
func clone(t *testing.T, dir, name, config string) string {
	t.Helper()
	top := filepath.Join(dir, name)
	writeFile(t, filepath.Join(top, ".git", "config"), config)
	return top
}

const (
	originConfig = `[core]
	bare = false
[remote "origin"]
	url = https://github.com/CappyT/agent-courier.git
	fetch = +refs/heads/*:refs/remotes/origin/*
`
	forkConfig = `[remote "origin"]
	url = git@github.com:someone/my-fork.git
[remote "upstream"]
	url = https://github.com/CappyT/agent-courier
`
)

func TestRepoOf(t *testing.T) {
	dir := t.TempDir()

	worktree := filepath.Join(dir, "wt")
	main := clone(t, dir, "main", originConfig)
	writeFile(t, filepath.Join(main, ".git", "worktrees", "wt", "commondir"), "../..\n")
	writeFile(t, filepath.Join(worktree, ".git"), "gitdir: "+filepath.Join(main, ".git", "worktrees", "wt")+"\n")

	submodule := filepath.Join(dir, "super", "sub")
	writeFile(t, filepath.Join(dir, "super", ".git", "modules", "sub", "config"), "[remote \"origin\"]\n\turl = ssh://git@git.example.com:2222/Group/Sub.Module.git\n")
	writeFile(t, filepath.Join(submodule, ".git"), "gitdir: ../.git/modules/sub\n")

	noRemote := clone(t, dir, "My Project.v2", "[core]\n\tbare = false\n")
	nested := filepath.Join(clone(t, dir, "nested", originConfig), "internal", "daemon")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name, dir, topic, repo string
	}{
		{"https origin", clone(t, dir, "a", originConfig), "cappyt_agent-courier", "agent-courier"},
		{"fork prefers upstream", clone(t, dir, "b", forkConfig), "cappyt_agent-courier", "agent-courier"},
		{"scp origin", clone(t, dir, "c", "[remote \"origin\"]\n\turl = git@github.com:CappyT/agent-courier.git\n"), "cappyt_agent-courier", "agent-courier"},
		{"quoted url with comment", clone(t, dir, "d", "[Remote \"origin\"]\n\tURL = \"https://github.com/CappyT/agent-courier.git\" # main\n"), "cappyt_agent-courier", "agent-courier"},
		{"subdirectory of a work tree", nested, "cappyt_agent-courier", "agent-courier"},
		{"worktree", worktree, "cappyt_agent-courier", "agent-courier"},
		{"submodule with port and dots", submodule, "group_sub-module", "Sub.Module"},
		{"remote with one segment", clone(t, dir, "e", "[remote \"origin\"]\n\turl = git@host:solo.git\n"), "e", "e"},
		{"other remote only", clone(t, dir, "f", "[remote \"mine\"]\n\turl = https://github.com/x/y\n"), "f", "f"},
		{"no remote", noRemote, "my-project-v2", "My Project.v2"},
		{"no work tree", filepath.Join(dir, "plain", "Dir_Name"), "dir_name", "Dir_Name"},
		{"root", "/", "", "/"},
		{"upstream with one segment falls to origin", clone(t, dir, "g", "[remote \"upstream\"]\n\turl = git@host:solo.git\n"+originConfig), "cappyt_agent-courier", "agent-courier"},
		{"first url wins", clone(t, dir, "h", originConfig+"[remote \"origin\"]\n\turl = https://github.com/x/y\n"), "cappyt_agent-courier", "agent-courier"},
		{"long owner and repository", clone(t, dir, "i", "[remote \"origin\"]\n\turl = https://github.com/"+strings.Repeat("O", 40)+"/"+strings.Repeat("r", 40)+"\n"),
			strings.Repeat("o", 40) + "_" + strings.Repeat("r", 23), strings.Repeat("r", 40)},
		{"colon in a local path", clone(t, dir, "j", "[remote \"origin\"]\n\turl = /srv/a:b/repo.git\n"), "a-b_repo", "repo"},
		{"remote inside the read cap", clone(t, dir, "k", padConfig(maxRead-len(originConfig)-1)+originConfig), "cappyt_agent-courier", "agent-courier"},
		{"remote after the read cap", clone(t, dir, "l", padConfig(maxRead)+originConfig), "l", "l"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.dir != "/" {
				if err := os.MkdirAll(tc.dir, 0o755); err != nil {
					t.Fatal(err)
				}
			}
			topic, repo := repoOf(tc.dir)
			if topic != tc.topic || repo != tc.repo {
				t.Errorf("repoOf = %q, %q; want %q, %q", topic, repo, tc.topic, tc.repo)
			}
		})
	}
}

// padConfig is a git config comment of n bytes.
func padConfig(n int) string { return "#" + strings.Repeat("x", n-2) + "\n" }

func TestRepoOfOwnership(t *testing.T) {
	dir := t.TempDir()
	foreignFiles(t, "foreign", "foreign-gd", "foreign-common")

	outer := clone(t, dir, "outer", originConfig)
	stopped := filepath.Join(outer, "foreign", "work")

	gitdirRepo := filepath.Join(dir, "g")
	writeFile(t, filepath.Join(gitdirRepo, "foreign-gd", "config"), originConfig)
	writeFile(t, filepath.Join(gitdirRepo, ".git"), "gitdir: foreign-gd\n")

	worktree := filepath.Join(dir, "w")
	writeFile(t, filepath.Join(dir, "foreign-common", "config"), originConfig)
	writeFile(t, filepath.Join(worktree, "gd", "commondir"), "../../foreign-common\n")
	writeFile(t, filepath.Join(worktree, ".git"), "gitdir: gd\n")

	inner := filepath.Join(clone(t, dir, "outer2", originConfig), "inner")
	writeFile(t, filepath.Join(inner, ".git"), "not a gitdir line\n")

	for _, tc := range []struct{ name, dir, topic string }{
		{"walk stops at a directory of another user", stopped, "work"},
		{"cwd of another user", clone(t, dir, "foreign", originConfig), "foreign"},
		{"gitdir of another user", gitdirRepo, "g"},
		{"commondir of another user", worktree, "w"},
		{".git file without gitdir stops the walk", inner, "inner"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.MkdirAll(tc.dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if topic, _ := repoOf(tc.dir); topic != tc.topic {
				t.Errorf("topic %q, want %q", topic, tc.topic)
			}
		})
	}

	t.Run("config of another user", func(t *testing.T) {
		repo := clone(t, dir, "k", originConfig)
		foreignFiles(t, "config")
		if topic, _ := repoOf(repo); topic != "k" {
			t.Errorf("topic %q, want k", topic)
		}
	})
}

func TestOwnerRepo(t *testing.T) {
	for _, tc := range []struct {
		url, owner, repo string
		ok               bool
	}{
		{"https://github.com/CappyT/agent-courier.git", "CappyT", "agent-courier", true},
		{"https://github.com/CappyT/agent-courier/", "CappyT", "agent-courier", true},
		{"ssh://git@host:22/group/sub/repo", "sub", "repo", true},
		{"git@github.com:CappyT/agent-courier.git", "CappyT", "agent-courier", true},
		{"/srv/git/repo.git", "git", "repo", true},
		{"/srv/a:b/repo.git", "a:b", "repo", true},
		{"file:///srv/git/repo.git", "git", "repo", true},
		{"../repo", "", "", false},
		{"git@host:repo.git", "", "", false},
		{"", "", "", false},
	} {
		owner, repo, ok := ownerRepo(tc.url)
		if ok != tc.ok || ok && (owner != tc.owner || repo != tc.repo) {
			t.Errorf("ownerRepo(%q) = %q, %q, %v; want %q, %q, %v", tc.url, owner, repo, ok, tc.owner, tc.repo, tc.ok)
		}
	}
}

func TestDefaultName(t *testing.T) {
	for _, tc := range []struct{ harness, repo, want string }{
		{"claude", "agent-courier", "claude-agent-courier"},
		{"opencode", "My_Repo.js", "opencode-my-repo-js"},
		{"claude", "", "claude"},
		{"claude", "...", "claude"},
		{"claude", "a-very-long-repository-name-that-goes-past-the-limit", "claude-a-very-long-repository-name-that-goes-pas"},
		{"claude", "a-very-long-repository-name-that-goes-pa-st", "claude-a-very-long-repository-name-that-goes-pa"},
	} {
		got := defaultName(tc.harness, tc.repo)
		if got != tc.want {
			t.Errorf("defaultName(%q, %q) = %q, want %q", tc.harness, tc.repo, got, tc.want)
		}
		if err := message.CheckAgent(got); err != nil {
			t.Errorf("defaultName(%q, %q) = %q: %v", tc.harness, tc.repo, got, err)
		}
	}
}
