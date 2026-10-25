//go:build unix

package daemon

import (
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	maxTopic = 64
	maxName  = 48
)

// repoOf returns the default topic and the repository name for dir: from the
// upstream remote, else origin, else the name of the work tree or of dir.
// topic is "" when nothing usable remains.
func repoOf(dir string) (topic, repo string) {
	top, config := gitConfig(dir)
	urls := remoteURLs(config)
	for _, remote := range []string{"upstream", "origin"} {
		if owner, name, ok := ownerRepo(urls[remote]); ok {
			return slug(owner+"_"+name, true, maxTopic), name
		}
	}
	base := filepath.Base(top)
	return slug(base, true, maxTopic), base
}

// gitConfig finds the work tree that holds dir and the path of its git
// config. Without a work tree, top is dir. config is "" when there is no
// config to read. The walk stops at the first directory that the user does
// not own, and reads neither it nor its parents.
func gitConfig(dir string) (top, config string) {
	dir = filepath.Clean(dir)
	for d := dir; ; d = filepath.Dir(d) {
		if fi, err := os.Lstat(d); err != nil || !fi.IsDir() || !owned(fi) {
			return dir, ""
		}
		if gitDir, found := gitDirOf(d); found {
			if gitDir == "" {
				return d, ""
			}
			if common, err := readSmall(filepath.Join(gitDir, "commondir")); err == nil {
				gitDir = resolve(gitDir, strings.TrimSpace(common))
				if !ownedDir(gitDir) {
					return d, ""
				}
			}
			return d, filepath.Join(gitDir, "config")
		}
		if d == filepath.Dir(d) {
			return dir, ""
		}
	}
}

// gitDirOf looks for the .git entry of top. found is false when top has no
// usable .git entry, so the walk goes on. gitDir is "" when the entry ends the
// walk without a config: a .git file without a gitdir line, or a git
// directory of another user.
func gitDirOf(top string) (gitDir string, found bool) {
	p := filepath.Join(top, ".git")
	fi, err := os.Lstat(p)
	switch {
	case err != nil:
		return "", false
	case fi.IsDir():
		if !owned(fi) {
			return "", true
		}
		return p, true
	case !fi.Mode().IsRegular():
		return "", false
	}
	data, err := readSmall(p)
	if err != nil {
		return "", false
	}
	path, ok := strings.CutPrefix(strings.TrimSpace(data), "gitdir:")
	if !ok {
		return "", true
	}
	gitDir = resolve(top, strings.TrimSpace(path))
	if !ownedDir(gitDir) {
		return "", true
	}
	return gitDir, true
}

func resolve(base, path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(base, path)
}

func ownedDir(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir() && owned(fi)
}

// maxRead caps what the daemon reads of one git file.
const maxRead = 1 << 20

// readSmall reads at most maxRead bytes of a regular file of the user. It
// opens without blocking and checks the file again after the open, so a named
// pipe or a device cannot stall the daemon.
func readSmall(path string) (string, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() || !owned(fi) {
		return "", errNotRegular
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil || !fi.Mode().IsRegular() || !owned(fi) {
		return "", errNotRegular
	}
	data, err := io.ReadAll(io.LimitReader(f, maxRead))
	return string(data), err
}

var errNotRegular = errors.New("not a regular file of the user")

// remoteURLs reads the first url of each [remote "name"] section of a git
// config file.
func remoteURLs(path string) map[string]string {
	urls := map[string]string{}
	if path == "" {
		return urls
	}
	data, err := readSmall(path)
	if err != nil {
		return urls
	}
	remote := ""
	for line := range strings.Lines(data) {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "[") {
			end := strings.IndexByte(line, ']')
			if end < 0 {
				remote = ""
				continue
			}
			remote = ""
			if section, sub, ok := strings.Cut(line[1:end], " "); ok && strings.EqualFold(section, "remote") {
				remote = configValue(sub)
			}
			line = strings.TrimSpace(line[end+1:])
		}
		key, value, ok := strings.Cut(line, "=")
		if remote == "" || !ok || !strings.EqualFold(strings.TrimSpace(key), "url") {
			continue
		}
		if _, seen := urls[remote]; !seen {
			urls[remote] = configValue(value)
		}
	}
	return urls
}

// configValue reads a git config value: quotes join, a backslash escapes,
// and # or ; outside quotes starts a comment.
func configValue(s string) string {
	var b strings.Builder
	quoted := false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"':
			quoted = !quoted
		case c == '\\' && i+1 < len(s):
			i++
			b.WriteByte(s[i])
		case (c == '#' || c == ';') && !quoted:
			return strings.TrimSpace(b.String())
		default:
			b.WriteByte(c)
		}
	}
	return strings.TrimSpace(b.String())
}

// ownerRepo reads the last two path segments of a remote URL: an URL with a
// scheme, an scp-like user@host:path, or a local path.
func ownerRepo(remote string) (owner, repo string, ok bool) {
	path := remote
	if strings.Contains(remote, "://") {
		u, err := url.Parse(remote)
		if err != nil {
			return "", "", false
		}
		path = u.Path
	} else if colon := strings.IndexByte(remote, ':'); colon > 0 && !strings.Contains(remote[:colon], "/") {
		path = remote[colon+1:]
	}
	path = strings.Trim(strings.TrimSuffix(strings.Trim(path, "/"), ".git"), "/")
	segs := strings.Split(path, "/")
	if len(segs) < 2 {
		return "", "", false
	}
	owner, repo = segs[len(segs)-2], segs[len(segs)-1]
	ok = owner != "" && owner != "." && owner != ".." && repo != "" && repo != "." && repo != ".."
	return owner, repo, ok
}

// slug folds s into the topic alphabet, or the agent name alphabet when
// underscore is false, and cuts it to n characters.
func slug(s string, underscore bool, n int) string {
	b := []byte(strings.Map(func(r rune) rune {
		switch {
		case 'a' <= r && r <= 'z', '0' <= r && r <= '9', r == '-', r == '_' && underscore:
			return r
		}
		return '-'
	}, strings.ToLower(s)))
	b = b[:min(len(b), n)]
	return strings.Trim(string(b), "-_")
}
