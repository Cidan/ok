package store

import (
	"context"

	"github.com/Cidan/ok/internal/lock"
)

// Cursor returns the last SCM change-feed event automation has applied (0: none).
func (s *Store) Cursor(ctx context.Context) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, `SELECT coalesce((SELECT seq FROM feed_cursor), 0)`).Scan(&seq)
	return seq, err
}

// SetCursor records the last change-feed event applied. It never moves the cursor back, so an
// event is never applied twice however many followers there are.
func (s *Store) SetCursor(ctx context.Context, seq int64) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO feed_cursor (seq) VALUES ($1)
		ON CONFLICT (id) DO UPDATE SET seq = greatest(feed_cursor.seq, EXCLUDED.seq)`, seq)
	return err
}

// leaders is the bucket of the locks (internal/lock) whose holders follow the SCM's change
// feed and run the landing queue: one replica holds each at a time.
const leaders = "leaders"

// LeadFeed runs fn holding the change feed's lock, with a context that ends if the lock is lost,
// and gives the lock up when fn returns. It returns lock.ErrNotAcquired when another replica
// holds the lock, and otherwise fn's error.
func (s *Store) LeadFeed(ctx context.Context, fn lock.LockFn) error {
	return s.locks.LockFn(ctx, leaders, "feed", 0, fn)
}

// LeadBuildCache runs fn holding the build cache sweep's lock, as LeadFeed does the feed's.
func (s *Store) LeadBuildCache(ctx context.Context, fn lock.LockFn) error {
	return s.locks.LockFn(ctx, leaders, "buildcache", 0, fn)
}

// LeadQueue runs fn holding the landing queue's lock, as LeadFeed does the feed's.
func (s *Store) LeadQueue(ctx context.Context, fn lock.LockFn) error {
	return s.locks.LockFn(ctx, leaders, "queue", 0, fn)
}
