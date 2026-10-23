package spannerlock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/lock/locktest"
	"github.com/Cidan/ok/internal/lock/spannerlock"
	"github.com/Cidan/ok/internal/testdb"
)

func client(t *testing.T) *spanner.Client {
	t.Helper()
	return testdb.SpannerClient(t, testdb.Spanner(t, spannerlock.DDL))
}

func exec(t *testing.T, c *spanner.Client, sql string, params map[string]any) {
	t.Helper()
	if _, err := c.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{SQL: sql, Params: params})
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSpanner(t *testing.T) {
	locktest.Run(t, func(t *testing.T) locktest.Env {
		c := client(t)
		return locktest.Env{
			DB: spannerlock.DB(c),
			Plant: func(t *testing.T, bucket, id, owner string, age time.Duration) {
				exec(t, c, `INSERT INTO locks (bucket, id, owner, refresh_id, last_refresh)
					VALUES (@b, @i, @o, 'r', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @age MICROSECOND))`,
					map[string]any{"b": bucket, "i": id, "o": owner, "age": age.Microseconds()})
			},
			Steal: func(t *testing.T, bucket, id string) {
				exec(t, c, `UPDATE locks SET owner = 'another locker', refresh_id = 'its own' WHERE bucket = @b AND id = @i`,
					map[string]any{"b": bucket, "i": id})
			},
			Row: func(t *testing.T, bucket, id string) (string, time.Time, bool) {
				row, err := c.Single().ReadRow(context.Background(), "locks", spanner.Key{bucket, id}, []string{"owner", "last_refresh"})
				if spanner.ErrCode(err) == 5 { // NotFound
					return "", time.Time{}, false
				}
				if err != nil {
					t.Fatal(err)
				}
				var owner string
				var at time.Time
				if err := row.Columns(&owner, &at); err != nil {
					t.Fatal(err)
				}
				return owner, at, true
			},
		}
	})
}

func inTx(ctx context.Context, c *spanner.Client, fn func(context.Context, *spanner.ReadWriteTransaction) error) error {
	_, err := c.ReadWriteTransaction(ctx, fn)
	return err
}

// A transaction's lock (InTx) holds against another's, and against a lease taken meanwhile; a lease
// held refuses a transaction; the row a transaction made stays, free to a lease.
func TestInTx(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := lock.NewOn(spannerlock.DB(c))
	defer func() { cancel(); l.WaitForRefreshers() }()
	// Two transactions at once, each holding the lock while it counts: one runs after the other.
	exec(t, c, `INSERT INTO locks (bucket, id, owner, refresh_id, last_refresh) VALUES ('n', 'count', '', '0', CURRENT_TIMESTAMP())`, nil)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			errs <- inTx(ctx, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
				if err := spannerlock.InTx(ctx, tx, "b", "x"); err != nil {
					return err
				}
				_, err := tx.Update(ctx, spanner.Statement{SQL: `UPDATE locks SET refresh_id = CAST(CAST(refresh_id AS INT64) + 1 AS STRING)
					WHERE bucket = 'n' AND id = 'count'`})
				return err
			})
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("a transaction holding the lock: %v", err)
		}
	}
	row, err := c.Single().ReadRow(ctx, "locks", spanner.Key{"n", "count"}, []string{"refresh_id"})
	if err != nil {
		t.Fatal(err)
	}
	var n string
	if err := row.Columns(&n); err != nil || n != "2" {
		t.Fatalf("two transactions counted %q, %v", n, err)
	}
	if ok, err := l.TryLock(ctx, "b", "x"); err != nil || !ok {
		t.Fatalf("a lease of the row the transactions made: %v, %v", ok, err)
	}
	if err := inTx(ctx, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return spannerlock.InTx(ctx, tx, "b", "x")
	}); !errors.Is(err, lock.ErrHeld) {
		t.Errorf("a transaction while a lease is held: %v", err)
	}
}

// A transaction checks a lease's fence: it holds while the lock is held, and fails once the lock is
// given up or another holds it.
func TestCheckFence(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	check := func(ctx context.Context, f data.Fence) error {
		return inTx(ctx, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			return spannerlock.CheckFence(ctx, tx, f)
		})
	}
	l, other := lock.NewOn(spannerlock.DB(c)), lock.NewOn(spannerlock.DB(c))
	defer func() { cancel(); l.WaitForRefreshers(); other.WaitForRefreshers() }()
	var fence data.Fence
	if err := l.LockFn(ctx, "b", "x", 0, func(ctx context.Context) error {
		fence, _ = data.FenceOf(ctx)
		return check(ctx, fence)
	}); err != nil {
		t.Fatalf("a transaction under the lock: %v", err)
	}
	if err := check(ctx, fence); !errors.Is(err, data.ErrFenced) {
		t.Errorf("once the lock was given up: %v", err)
	}
	f, ok, err := other.TryLockFenced(ctx, "b", "x")
	if err != nil || !ok {
		t.Fatalf("another taking it: %v %v", ok, err)
	}
	if err := check(ctx, fence); !errors.Is(err, data.ErrFenced) {
		t.Errorf("the first holder's, once another holds it: %v", err)
	}
	if err := check(ctx, f); err != nil {
		t.Errorf("the new holder's: %v", err)
	}
}

// A fenced transaction running past the lease keeps its lock: the fence holds off a takeover, never
// the holder's own refreshes.
func TestLongFencedTransaction(t *testing.T) {
	c := client(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := lock.NewOn(spannerlock.DB(c))
	defer func() { cancel(); l.WaitForRefreshers() }()
	err := l.LockFn(ctx, "b", "long", 0, func(ctx context.Context) error {
		fence, _ := data.FenceOf(ctx)
		if err := inTx(ctx, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			if err := spannerlock.CheckFence(ctx, tx, fence); err != nil {
				return err
			}
			for end := time.Now().Add(lock.Lease + 2*time.Second); time.Now().Before(end); time.Sleep(time.Second) {
				if err := tx.Query(ctx, spanner.Statement{SQL: `SELECT 1`}).Do(func(*spanner.Row) error { return nil }); err != nil {
					return err
				}
			}
			return spannerlock.CheckFence(ctx, tx, fence)
		}); err != nil {
			return err
		}
		return context.Cause(ctx)
	})
	if err != nil {
		t.Errorf("a fenced transaction past the lease: %v", err)
	}
}
