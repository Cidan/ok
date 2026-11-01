// Package secretstest is what tests of secrets' readers share.
package secretstest

import (
	"os"
	"path/filepath"
	"slices"
	"time"
)

// Dirs are the directories of secrets on this machine now.
func Dirs() []string {
	dirs, _ := filepath.Glob("/dev/shm/ok-secrets-*")
	return dirs
}

// Left are the directories of secrets not among before that are still there after within. Those
// directories are the machine's, and go test runs other packages beside the caller's that make theirs
// there too: one of theirs goes as their reader ends, and one a reader left never does.
func Left(before []string, within time.Duration) []string {
	left := slices.DeleteFunc(Dirs(), func(d string) bool { return slices.Contains(before, d) })
	for deadline := time.Now().Add(within); len(left) > 0 && time.Now().Before(deadline); {
		time.Sleep(50 * time.Millisecond)
		left = slices.DeleteFunc(left, func(d string) bool {
			_, err := os.Stat(d)
			return os.IsNotExist(err)
		})
	}
	return left
}
