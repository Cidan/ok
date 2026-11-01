// Package scripts holds what CI and people run by path. It has no code but this check.
package scripts

import (
	"os"
	"path/filepath"
	"testing"
)

// CI runs these by path, so one that loses its executable bit fails there with "Permission denied",
// and no other test runs it.
func TestScriptsAreExecutable(t *testing.T) {
	paths, err := filepath.Glob("*.sh")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no scripts found")
	}
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o111 == 0 {
			t.Errorf("%s is not executable (mode %o)", p, info.Mode().Perm())
		}
	}
}
