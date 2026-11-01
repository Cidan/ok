package secretstest

import (
	"os"
	"slices"
	"testing"
	"time"
)

func dir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("/dev/shm", "ok-secrets-")
	if err != nil {
		t.Skip(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

// A directory that was there before is not counted; one that goes in time is not left; one that stays is.
func TestLeft(t *testing.T) {
	old := dir(t)
	before := Dirs()
	if !slices.Contains(before, old) {
		t.Fatalf("%s is not among %v", old, before)
	}
	going := dir(t)
	go func() {
		time.Sleep(100 * time.Millisecond)
		os.RemoveAll(going)
	}()
	if left := Left(before, time.Minute); slices.Contains(left, going) || slices.Contains(left, old) {
		t.Errorf("a directory that went, and one from before: %v", left)
	}
	stays := dir(t)
	if left := Left(before, 100*time.Millisecond); !slices.Contains(left, stays) {
		t.Errorf("a directory that stays is not left: %v", left)
	}
}
