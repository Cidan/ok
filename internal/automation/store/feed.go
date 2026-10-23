package store

import (
	"context"

	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/scmfeed"
)

// Cursor returns the place in a repository's log of the last of its events automation has
// applied (0: none).
func (s *Store) Cursor(ctx context.Context, repo int64) (int64, error) {
	return s.b.FollowCursor(ctx, scmfeed.Topic, repo)
}

// Cursors are Cursor's of repos, at once; a repository with none is absent.
func (s *Store) Cursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return s.b.FollowCursors(ctx, scmfeed.Topic, repos)
}

// SetCursor records the last of a repository's events applied. It never moves the cursor back,
// so an event is never applied twice however many followers there are.
func (s *Store) SetCursor(ctx context.Context, repo, seq int64) error {
	return s.b.AdvanceFollowCursor(move.WithRepository(ctx, repo), scmfeed.Topic, repo, seq)
}
