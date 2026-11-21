package worker

import (
	"errors"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// A guard over a presentation fails once its count of failed reads of ok rises, or falls (its
// daemon restarted), or is gone; a directory with no count has no guard.
func TestTheReadsGuardFailsOnceOkfsFailsARead(t *testing.T) {
	dir := t.TempDir()
	if watchReads(dir) != nil {
		t.Fatal("a guard over a directory that keeps no count")
	}
	set := func(v string) {
		t.Helper()
		if err := unix.Setxattr(dir, okfsFailuresAttr, []byte(v), 0); err != nil {
			if errors.Is(err, syscall.ENOTSUP) {
				t.Fatalf("the test's file system holds no user attributes: %v", err)
			}
			t.Fatal(err)
		}
	}
	set("3")
	g := watchReads(dir)
	if g == nil {
		t.Fatal("no guard over a directory that keeps a count")
	}
	if err := g.failed(); err != nil {
		t.Fatalf("before any failure: %v", err)
	}
	set("4")
	if err := g.failed(); err == nil || !strings.Contains(err.Error(), "failed 1 reads") {
		t.Fatalf("after a failed read: %v", err)
	}
	set("0")
	if err := g.failed(); err == nil || !strings.Contains(err.Error(), "restarted") {
		t.Fatalf("after the daemon restarted: %v", err)
	}
	if err := unix.Removexattr(dir, okfsFailuresAttr); err != nil {
		t.Fatal(err)
	}
	if err := g.failed(); err == nil || !strings.Contains(err.Error(), "no longer reports") {
		t.Fatalf("with the count gone: %v", err)
	}
	var none *readsGuard
	if err := none.failed(); err != nil {
		t.Fatalf("no guard: %v", err)
	}
}
