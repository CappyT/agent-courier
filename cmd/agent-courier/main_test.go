package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/CappyT/agent-courier/internal/version"
)

func TestRun(t *testing.T) {
	for _, tc := range []struct {
		args     []string
		code     int
		inStdout string
		inStderr string
	}{
		{nil, 2, "", "usage:"},
		{[]string{"version"}, 0, version.Version, ""},
		{[]string{"help"}, 0, "usage:", ""},
		{[]string{"bogus"}, 2, "", `unknown command "bogus"`},
	} {
		var out, errOut bytes.Buffer
		if code := run(tc.args, &out, &errOut); code != tc.code {
			t.Errorf("run(%q) = %d, want %d", tc.args, code, tc.code)
		}
		if !strings.Contains(out.String(), tc.inStdout) || !strings.Contains(errOut.String(), tc.inStderr) {
			t.Errorf("run(%q): stdout %q, stderr %q", tc.args, out.String(), errOut.String())
		}
	}
}
