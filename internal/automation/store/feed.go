package store

import (
	"context"

	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/scm/scmfeed"
)

// Cursor returns the place in a repository's log of the last of its events automation has
// applied (0: none).
func (s *Store) Cursor(ctx context.Context, repo int64) (int64, error) {
	return s.b.FollowCursor(ctx, scmfeed.Topic, repo)
}

// SetCursor records the last of a repository's events applied. It never moves the cursor back,
// so an event is never applied twice however many followers there are.
func (s *Store) SetCursor(ctx context.Context, repo, seq int64) error {
	return s.b.AdvanceFollowCursor(ctx, scmfeed.Topic, repo, seq)
}

// LegacyCursor, Seed, and LeaveLegacy are a follow.Legacy's: automation's place in the SCM's feed
// before it followed each repository's log, which it leaves once.
func (s *Store) LegacyCursor(ctx context.Context) (int64, error) { return s.b.LegacyCursor(ctx) }

func (s *Store) Seed(ctx context.Context, repo, seq int64) error {
	return s.b.SeedFollowCursor(ctx, scmfeed.Topic, repo, seq)
}

func (s *Store) LeaveLegacy(ctx context.Context) error { return s.b.ForgetLegacyCursor(ctx) }

// LeadBuildCache runs fn holding the build cache sweep's lock, with a context that ends if the
// lock is lost, and gives the lock up when fn returns. It returns lock.ErrNotAcquired when
// another replica holds the lock, and otherwise fn's error.
func (s *Store) LeadBuildCache(ctx context.Context, fn lock.LockFn) error {
	return s.b.Lead(ctx, "buildcache", fn)
}
