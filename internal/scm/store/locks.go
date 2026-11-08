package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/scm/store/backend"
)

// Locks (docs/scm.md, "Locks"): a person locks paths of a repository — any file, or a directory and
// everything under it — so that no one else's CL changing them lands while they work on them. A lock
// is theirs for LockFor from when they took it, judged from its record as it is read: no timer ends
// it. They unlock it, a landing of theirs that changes its path releases it, or an administrator
// breaks it. Uploading is never refused: the landing is, and so the submission, whose dry run lands.

// LockFor is how long a lock is held from when it was taken.
const LockFor = 18 * time.Hour

// maxLockPaths bounds the paths one call locks, unlocks, or breaks.
const maxLockPaths = 1000

// Lock is a person's lock of a path.
type Lock struct {
	Path string
	// Holder is the person's stable ID, and HolderName their name as of when they took it.
	Holder, HolderName string
	TakenAt            time.Time
}

// Expires is when the lock ends unless it is taken again.
func (l Lock) Expires() time.Time { return l.TakenAt.Add(LockFor) }

// live reports whether the lock holds at now.
func (l Lock) live(now time.Time) bool { return now.Before(l.Expires()) }

func lockOf(l backend.FileLock) Lock {
	return Lock{Path: l.Path, Holder: l.Holder, HolderName: l.HolderName, TakenAt: l.TakenAt}
}

// LockRejection refuses a lock or an unlock of a path someone else holds.
type LockRejection struct {
	Held    Lock
	Message string
}

func (r *LockRejection) Error() string { return r.Message }

// heldBy words who holds a lock, and until when.
func (l Lock) heldBy() string {
	return fmt.Sprintf("%s locked %s until %s", l.HolderName, l.Path, l.Expires().UTC().Format("2006-01-02 15:04 UTC"))
}

// lockPaths are paths as a lock names them: clean, repository-relative, none the root, each once.
func lockPaths(paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, invalid("no path to lock")
	}
	if len(paths) > maxLockPaths {
		return nil, invalid("%d paths: at most %d at once", len(paths), maxLockPaths)
	}
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		c, err := cleanPath(p)
		if err != nil {
			return nil, err
		}
		if c == "" {
			return nil, invalid("the repository's root is not a path to lock")
		}
		out = append(out, c)
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

// LockPaths locks paths for principal, every one or none: a path someone else holds a live lock of
// refuses them all. One principal holds already is taken again, its LockFor starting over. It holds
// main's head as it writes, so a landing is ordered against it: one that read the locks before is
// done before it, and one after sees it.
func (s *Store) LockPaths(ctx context.Context, repo int64, principal string, paths []string) ([]Lock, error) {
	paths, err := lockPaths(paths)
	if err != nil {
		return nil, err
	}
	name, err := s.nameOf(ctx, principal)
	if err != nil {
		return nil, err
	}
	var out []Lock
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		out = nil
		if _, _, ok, err := tx.Head(ctx, repo, true); err != nil {
			return err
		} else if !ok {
			return notFound("repository %d", repo)
		}
		held, err := tx.FileLocks(ctx, repo, paths)
		if err != nil {
			return err
		}
		now := time.Now()
		put := make([]backend.FileLock, len(paths))
		for i, p := range paths {
			if l, ok := held[p]; ok && l.Holder != principal && lockOf(l).live(now) {
				return &LockRejection{Held: lockOf(l), Message: lockOf(l).heldBy()}
			}
			put[i] = backend.FileLock{Path: p, Holder: principal, HolderName: name}
		}
		got, err := tx.PutFileLocks(ctx, repo, put)
		if err != nil {
			return err
		}
		for _, l := range got {
			out = append(out, lockOf(l))
		}
		return nil
	})
	return out, err
}

// UnlockPaths ends principal's locks of paths, and returns the paths they held. A path someone
// else holds a live lock of refuses them all — an administrator breaks it (BreakLocks) — and one no
// one holds is passed over.
func (s *Store) UnlockPaths(ctx context.Context, repo int64, principal string, paths []string) ([]string, error) {
	paths, err := lockPaths(paths)
	if err != nil {
		return nil, err
	}
	var out []string
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		held, err := tx.FileLocks(ctx, repo, paths)
		if err != nil {
			return err
		}
		now := time.Now()
		for _, p := range paths {
			if l, ok := held[p]; ok && l.Holder != principal && lockOf(l).live(now) {
				return &LockRejection{Held: lockOf(l), Message: lockOf(l).heldBy() + ": only they unlock it, or an administrator breaks it"}
			}
		}
		out, err = tx.DeleteFileLocks(ctx, repo, paths, principal)
		return err
	})
	return out, err
}

// BreakLocks ends whoever's locks of paths, and returns those it ended that were live: an
// administrator's, in an emergency (the service allows it them alone).
func (s *Store) BreakLocks(ctx context.Context, repo int64, paths []string) ([]Lock, error) {
	paths, err := lockPaths(paths)
	if err != nil {
		return nil, err
	}
	var out []Lock
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		out = nil
		held, err := tx.FileLocks(ctx, repo, paths)
		if err != nil {
			return err
		}
		gone, err := tx.DeleteFileLocks(ctx, repo, paths, "")
		if err != nil {
			return err
		}
		now := time.Now()
		for _, p := range gone {
			if l := lockOf(held[p]); l.live(now) {
				out = append(out, l)
			}
		}
		return nil
	})
	return out, err
}

// Locks are a repository's live locks of prefix and everything under it (the whole repository's,
// for ""), by path.
func (s *Store) Locks(ctx context.Context, repo int64, prefix string) ([]Lock, error) {
	prefix, err := cleanPath(prefix)
	if err != nil {
		return nil, err
	}
	all, err := s.ops(ctx).FileLocks(ctx, repo, nil)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	var out []Lock
	for _, l := range all {
		if under(l.Path, prefix) && lockOf(l).live(now) {
			out = append(out, lockOf(l))
		}
	}
	slices.SortFunc(out, func(a, b Lock) int { return strings.Compare(a.Path, b.Path) })
	return out, nil
}

// under reports whether p is prefix or a path under it ("" is every path's prefix).
func under(p, prefix string) bool {
	return prefix == "" || p == prefix || strings.HasPrefix(p, prefix+"/")
}

// lockedPaths refuses a landing step whose CL changes a path someone other than its author holds a
// live lock of, of the repository's locks: its paths are main's path history's, each changed file
// and every directory above one, so a lock of a directory holds everything under it.
func lockedPaths(locks map[string]backend.FileLock, number int64, author string, paths []string, now time.Time) error {
	if len(locks) == 0 {
		return nil
	}
	var held []Lock
	for _, p := range paths {
		if l, ok := locks[p]; ok && l.Holder != author && lockOf(l).live(now) {
			held = append(held, lockOf(l))
		}
	}
	if len(held) == 0 {
		return nil
	}
	words := make([]string, len(held))
	paths = make([]string, len(held))
	for i, l := range held {
		words[i], paths[i] = l.heldBy(), l.Path
	}
	return &LandRejection{Number: number, Reason: LandLocked, Paths: paths,
		Message: fmt.Sprintf("CL %d changes locked paths: %s (submit it once they unlock them, or the locks expire)", number, strings.Join(words, "; "))}
}

// releasedLocks are the paths of locks the author holds that a landing of theirs changing paths
// releases.
func releasedLocks(locks map[string]backend.FileLock, author string, paths []string) []string {
	var out []string
	for _, p := range paths {
		if l, ok := locks[p]; ok && l.Holder == author {
			out = append(out, p)
		}
	}
	return out
}
