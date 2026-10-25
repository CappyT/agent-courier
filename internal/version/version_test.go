package version

import (
	"encoding/json/v2"
	"os"
	"path/filepath"
	"testing"
)

// TestManifestVersions fails when a manifest's version differs from Version.
func TestManifestVersions(t *testing.T) {
	for _, path := range []string{
		filepath.Join("..", "..", ".claude-plugin", "plugin.json"),
		filepath.Join("..", "..", "opencode", "package.json"),
	} {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var m struct {
			Version string `json:"version"`
		}
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if m.Version != Version {
			t.Errorf("%s has version %q, want %q as in internal/version", path, m.Version, Version)
		}
	}
}
