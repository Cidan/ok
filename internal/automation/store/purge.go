package store

import (
	"context"
	"fmt"

	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/move"
)

// Purging (docs/hosted.md, "Deleting, renaming, transferring"): a repository the directory purged
// is forgotten whole — its runs' logs in the bucket first, then its rows — so one interrupted is
// done again from the start, what is gone already taken as done.

// purgePage bounds the runs whose logs one step deletes.
const purgePage = 500

// DirectoryCursors are the places in each repository's directory log applied.
func (s *Store) DirectoryCursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return s.b.FollowCursors(ctx, dirfeed.Topic, repos)
}

// DirectoryCursor is DirectoryCursors' of one repository.
func (s *Store) DirectoryCursor(ctx context.Context, repo int64) (int64, error) {
	return s.b.FollowCursor(ctx, dirfeed.Topic, repo)
}

// SetDirectoryCursor records the last of a repository's directory events applied; it never moves
// back.
func (s *Store) SetDirectoryCursor(ctx context.Context, repo, seq int64) error {
	// The directory's events are every repository's, wherever it is or moves: no move gates them.
	return s.b.AdvanceFollowCursor(move.Untagged(ctx), dirfeed.Topic, repo, seq)
}

// PurgeRepository forgets a repository the directory purged: every run's log — whole, and the
// segments of its leases — then every row.
func (s *Store) PurgeRepository(ctx context.Context, repo int64) error {
	ctx = move.Untagged(ctx) // the directory purged it: wherever it was, it is gone
	var after int64
	for {
		ids, err := s.b.RunIDsOf(ctx, repo, after, purgePage)
		if err != nil {
			return err
		}
		for _, id := range ids {
			keys := []string{logKey(id)}
			live, err := s.blobs.List(ctx, liveDir(id))
			if err != nil {
				return fmt.Errorf("listing run %d's log segments: %w", id, err)
			}
			for _, o := range live {
				keys = append(keys, o.Key)
			}
			if err := s.blobs.Delete(ctx, keys...); err != nil {
				return fmt.Errorf("deleting run %d's log: %w", id, err)
			}
		}
		if len(ids) < purgePage {
			break
		}
		after = ids[len(ids)-1]
	}
	return s.txn(ctx, func(tx Ops) error {
		if err := tx.DropOwner(ctx, repo); err != nil {
			return err
		}
		return tx.DeleteRepository(ctx, repo)
	})
}
