//go:build unix

package daemon

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	// LogName is the log of a daemon that a client started, in the state
	// directory beside the store.
	LogName = "daemon.log"
	// MaxLog is the size above which a starting daemon moves its log to
	// LogName.1 and begins a new one.
	MaxLog = 1 << 20

	maxLastLine = 512
)

// StateDir is $XDG_STATE_HOME/agent-courier, with ~/.local/state as the
// default of XDG_STATE_HOME.
func StateDir() (string, error) {
	state := os.Getenv("XDG_STATE_HOME")
	if !filepath.IsAbs(state) {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		state = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(state, "agent-courier"), nil
}

// OpenLog opens the log in dir for appending, with mode 0600. A log over
// MaxLog becomes LogName.1 first, which replaces the older one.
func OpenLog(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, LogName)
	if fi, err := os.Lstat(path); err == nil && fi.Size() > MaxLog {
		if err := os.Rename(path, path+".1"); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
}

// LastLogLine is the last line of the log in dir, at most 512 bytes, or ""
// without one.
func LastLogLine(dir string) string {
	f, err := os.Open(filepath.Join(dir, LogName))
	if err != nil {
		return ""
	}
	defer f.Close()
	const tail = 4096
	if fi, err := f.Stat(); err == nil && fi.Size() > tail {
		f.Seek(-tail, io.SeekEnd)
	}
	b, _ := io.ReadAll(io.LimitReader(f, tail))
	b = bytes.TrimRight(b, "\r\n")
	if i := bytes.LastIndexByte(b, '\n'); i >= 0 {
		b = b[i+1:]
	}
	if len(b) > maxLastLine {
		b = b[len(b)-maxLastLine:]
	}
	return string(bytes.ToValidUTF8(b, []byte("?")))
}
