package store

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func mustLock(t *testing.T, s *Store, repo int64, principal string, paths ...string) []Lock {
	t.Helper()
	ls, err := s.LockPaths(context.Background(), repo, principal, paths)
	if err != nil {
		t.Fatal(err)
	}
	return ls
}

func lockRefused(t *testing.T, err error, holder string) {
	t.Helper()
	var rej *LockRejection
	if !errors.As(err, &rej) || rej.Held.Holder != holder {
		t.Fatalf("got %v, want a refusal naming %s's lock", err, holder)
	}
}

func landRefused(t *testing.T, err error, reason LandReason, paths ...string) {
	t.Helper()
	var rej *LandRejection
	if !errors.As(err, &rej) || rej.Reason != reason || !slices.Equal(rej.Paths, paths) {
		t.Fatalf("got %v, want a landing refused with reason %d on %v", err, reason, paths)
	}
}

func lockedPathsOf(t *testing.T, s *Store, repo int64, prefix string) []string {
	t.Helper()
	ls, err := s.Locks(context.Background(), repo, prefix)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, l := range ls {
		out = append(out, l.Path)
	}
	return out
}

// ageLocks makes a repository's locks taken hours ago.
func ageLocks(t *testing.T, s *Store, repo int64, hours int) {
	t.Helper()
	dbOf(s).exec(t, `UPDATE file_lock SET taken_at = @at WHERE repository_id = @repo`,
		map[string]any{"repo": repo, "at": time.Now().Add(-time.Duration(hours) * time.Hour)})
}

// A lock is its holder's alone: another's lock of the path is refused, naming them, and so is their
// unlock; the holder takes it again, its hours starting over, and unlocks it. A lock past 18 hours
// holds nothing: another takes the path. The repository's root, and a path not clean, are no lock's.
func TestLocks(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	first := mustLock(t, e.s, e.repo, alice, "art/hero.uasset", "art/maps")
	if len(first) != 2 || first[0].Path != "art/hero.uasset" || first[0].Holder != alice || first[0].Expires() != first[0].TakenAt.Add(LockFor) {
		t.Fatalf("locks taken: %+v", first)
	}
	_, err := e.s.LockPaths(ctx, e.repo, bob, []string{"docs/a.md", "art/hero.uasset"})
	lockRefused(t, err, alice)
	if got := lockedPathsOf(t, e.s, e.repo, ""); !slices.Equal(got, []string{"art/hero.uasset", "art/maps"}) {
		t.Fatalf("a refused lock took some of its paths: %v", got)
	}
	_, err = e.s.UnlockPaths(ctx, e.repo, bob, []string{"art/maps"})
	lockRefused(t, err, alice)
	again := mustLock(t, e.s, e.repo, alice, "art/hero.uasset")
	if !again[0].TakenAt.After(first[0].TakenAt) {
		t.Fatal("taking a lock again did not start its hours over")
	}
	if got := lockedPathsOf(t, e.s, e.repo, "art/maps"); !slices.Equal(got, []string{"art/maps"}) {
		t.Fatalf("locks under art/maps: %v", got)
	}
	if got := lockedPathsOf(t, e.s, e.repo, "art/m"); got != nil {
		t.Fatalf("locks under art/m: %v", got)
	}
	unlocked, err := e.s.UnlockPaths(ctx, e.repo, alice, []string{"art/maps", "never/locked"})
	if err != nil || !slices.Equal(unlocked, []string{"art/maps"}) {
		t.Fatalf("unlocked %v, %v", unlocked, err)
	}
	ageLocks(t, e.s, e.repo, 19)
	if got := lockedPathsOf(t, e.s, e.repo, ""); got != nil {
		t.Fatalf("locks past 18 hours are listed: %v", got)
	}
	if ls := mustLock(t, e.s, e.repo, bob, "art/hero.uasset"); ls[0].Holder != bob {
		t.Fatalf("an expired lock kept its path: %+v", ls)
	}
	for _, bad := range [][]string{{""}, {"/"}, {"a/../b"}, nil} {
		if _, err := e.s.LockPaths(ctx, e.repo, alice, bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("locking %q: %v", bad, err)
		}
	}
}

// A CL that changes a path someone else holds a live lock of is refused its landing — and so its
// submission, whose dry run lands — naming them, though it uploads; so is one that changes a path
// under a locked directory. One that changes other paths lands, and so does the holder's own,
// releasing their locks of the paths it changes and no others.
func TestLandingHonorsLocks(t *testing.T) {
	e := newCLEnv(t)
	e.landChange("base", map[string]string{"a.txt": "a\n", "b.txt": "b\n", "dir/x": "x\n", "z.txt": "z\n"})
	base := e.head()
	mustLock(t, e.s, e.repo, bob, "a.txt", "dir", "z.txt")
	change := func(path string) map[string]string {
		files := e.flatAt(base)
		files[path] += "changed\n"
		return files
	}
	onA := e.open(e.commit(alice, newChange(), base, "a", change("a.txt")))
	_, _, err := e.land(e.head(), true, LandItem{Number: onA, Patchset: 1})
	landRefused(t, err, LandLocked, "a.txt")
	_, _, err = e.land(e.head(), false, LandItem{Number: onA, Patchset: 1})
	landRefused(t, err, LandLocked, "a.txt")
	underDir := e.open(e.commit(alice, newChange(), base, "x", change("dir/x")))
	_, _, err = e.land(e.head(), false, LandItem{Number: underDir, Patchset: 1})
	landRefused(t, err, LandLocked, "dir")
	_, err = e.s.PrepareCandidate(context.Background(), e.repo, onA, 1, e.head(), alice)
	landRefused(t, err, LandLocked, "a.txt")

	onB := e.open(e.commit(alice, newChange(), base, "b", change("b.txt")))
	e.mustLand(LandItem{Number: onB, Patchset: 1})

	// Bob's own CL lands, and releases his lock of a.txt, not of z.txt or dir.
	bobs := e.mustUpload(bob, Ready, e.commit(bob, newChange(), e.head(), "bob's a", change("a.txt")))[0].Number
	e.mustLand(LandItem{Number: bobs, Patchset: 1})
	if got := lockedPathsOf(t, e.s, e.repo, ""); !slices.Equal(got, []string{"dir", "z.txt"}) {
		t.Fatalf("locks after bob's landing: %v", got)
	}
}

// A lock taken between a landing's plan and its transaction refuses it, the transaction reading the
// locks again under main's head.
func TestLockTakenWhileLanding(t *testing.T) {
	e := newCLEnv(t)
	e.landChange("base", map[string]string{"a.txt": "a\n"})
	n := e.open(e.commit(alice, newChange(), e.head(), "a", map[string]string{"a.txt": "a2\n"}))
	e.s.landPlanned = func() {
		e.s.landPlanned = nil
		mustLock(t, e.s, e.repo, bob, "a.txt")
	}
	_, _, err := e.land(e.head(), false, LandItem{Number: n, Patchset: 1})
	landRefused(t, err, LandLocked, "a.txt")
}

// An administrator breaks anyone's locks, and is told those that were live; the daily collection
// deletes the rows of locks past 18 hours.
func TestBreakAndExpireLocks(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	mustLock(t, e.s, e.repo, bob, "a", "b")
	broken, err := e.s.BreakLocks(ctx, e.repo, []string{"a", "c"})
	if err != nil || len(broken) != 1 || broken[0].Path != "a" || broken[0].Holder != bob {
		t.Fatalf("broke %+v, %v", broken, err)
	}
	mustLock(t, e.s, e.repo, alice, "a")
	ageLocks(t, e.s, e.repo, 19)
	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil || r.LocksExpired != 2 {
		t.Fatalf("collection deleted %d expired locks, %v", r.LocksExpired, err)
	}
	if n := dbOf(e.s).count(t, "file_lock", "repository_id = @repo", map[string]any{"repo": e.repo}); n != 0 {
		t.Fatalf("%d lock rows left", n)
	}
}
