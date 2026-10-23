package lock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/lock/locktest"
	"github.com/Cidan/ok/internal/testdb"
)

// pool is a test's database, with its locks table.
func pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	p := testdb.Pool(t)
	if err := lock.Ensure(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPostgres(t *testing.T) {
	locktest.Run(t, func(t *testing.T) locktest.Env {
		p := pool(t)
		ctx := context.Background()
		return locktest.Env{
			DB: lock.Postgres(p),
			Plant: func(t *testing.T, bucket, id, owner string, age time.Duration) {
				if _, err := p.Exec(ctx, `INSERT INTO locks VALUES ($1, $2, $3, 'r', now() - make_interval(secs => $4))`,
					bucket, id, owner, age.Seconds()); err != nil {
					t.Fatal(err)
				}
			},
			Steal: func(t *testing.T, bucket, id string) {
				if _, err := p.Exec(ctx, `UPDATE locks SET owner = 'another locker', refresh_id = 'its own' WHERE bucket = $1 AND id = $2`,
					bucket, id); err != nil {
					t.Fatal(err)
				}
			},
			Row: func(t *testing.T, bucket, id string) (string, time.Time, bool) {
				var owner string
				var at time.Time
				err := p.QueryRow(ctx, `SELECT owner, last_refresh FROM locks WHERE bucket = $1 AND id = $2`, bucket, id).Scan(&owner, &at)
				if errors.Is(err, pgx.ErrNoRows) {
					return "", time.Time{}, false
				}
				if err != nil {
					t.Fatal(err)
				}
				return owner, at, true
			},
		}
	})
}

// A lock held by a transaction (InTx) excludes another transaction until it ends, and a lease
// until it ends; a lease held refuses a transaction; the row a transaction made stays, free.
func TestInTx(t *testing.T) {
	p := pool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := func(fn func(pgx.Tx) error) error { return pgx.BeginFunc(ctx, p, fn) }
	tx1, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.InTx(ctx, tx1, "b", "x"); err != nil {
		t.Fatal(err)
	}
	took := make(chan error, 1)
	go func() { took <- in(func(tx pgx.Tx) error { return lock.InTx(ctx, tx, "b", "x") }) }()
	leased := make(chan bool, 1)
	l := lock.New(p)
	defer func() { cancel(); l.WaitForRefreshers() }()
	go func() {
		ok, err := l.TryLock(ctx, "b", "x")
		if err != nil {
			t.Error(err)
		}
		leased <- ok
	}()
	select {
	case err := <-took:
		t.Fatalf("a second transaction took the lock while the first held it: %v", err)
	case ok := <-leased:
		t.Fatalf("a lease was taken while a transaction held the lock: %v", ok)
	case <-time.After(time.Second):
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// The second transaction or the lease goes first: a transaction after the lease is refused.
	if err := <-took; err != nil && !errors.Is(err, lock.ErrHeld) {
		t.Errorf("the second transaction, once the first ended: %v", err)
	}
	if ok := <-leased; !ok {
		t.Error("the lease, once the transactions ended: not taken")
	}
	if err := in(func(tx pgx.Tx) error { return lock.InTx(ctx, tx, "b", "x") }); !errors.Is(err, lock.ErrHeld) {
		t.Errorf("a transaction while a lease is held: %v", err)
	}
	if err := l.Unlock(ctx, "b", "x"); err != nil {
		t.Fatal(err)
	}
	if err := in(func(tx pgx.Tx) error { return lock.InTx(ctx, tx, "b", "y") }); err != nil {
		t.Fatal(err)
	}
	if ok, err := l.TryLock(ctx, "b", "y"); err != nil || !ok {
		t.Errorf("a lease of the row a transaction made: %v, %v", ok, err)
	}
}

// Ensure makes the table where there is none, and is done where it is, however many make it at
// once.
func TestEnsure(t *testing.T) {
	p := testdb.Pool(t)
	ctx := context.Background()
	errs := make(chan error, 8)
	for range 8 {
		go func() { errs <- lock.Ensure(ctx, p) }()
	}
	for range 8 {
		if err := <-errs; err != nil {
			t.Errorf("Ensure at once: %v", err)
		}
	}
	l := lock.New(p)
	defer l.WaitForRefreshers()
	tctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if ok, err := l.TryLock(tctx, "b", "x"); err != nil || !ok {
		t.Errorf("a lock once the table is made: %v, %v", ok, err)
	}
}

// A transaction checks a lease's fence: it holds while the lock is held, and fails once the lock
// is given up or another holds it.
func TestCheckFence(t *testing.T) {
	p := pool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	check := func(ctx context.Context, f data.Fence) error {
		return pgx.BeginFunc(ctx, p, func(tx pgx.Tx) error { return lock.CheckFence(ctx, tx, f) })
	}
	l, other := lock.New(p), lock.New(p)
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
	p := pool(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := lock.New(p)
	defer func() { cancel(); l.WaitForRefreshers() }()
	err := l.LockFn(ctx, "b", "long", 0, func(ctx context.Context) error {
		fence, _ := data.FenceOf(ctx)
		if err := pgx.BeginFunc(ctx, p, func(tx pgx.Tx) error {
			if err := lock.CheckFence(ctx, tx, fence); err != nil {
				return err
			}
			for end := time.Now().Add(lock.Lease + 2*time.Second); time.Now().Before(end); time.Sleep(time.Second) {
				if _, err := tx.Exec(ctx, `SELECT 1`); err != nil {
					return err
				}
			}
			return lock.CheckFence(ctx, tx, fence)
		}); err != nil {
			return err
		}
		return context.Cause(ctx)
	})
	if err != nil {
		t.Errorf("a fenced transaction past the lease: %v", err)
	}
}

// A takeover waits for a fenced transaction of the holder's: a lapsed lease is taken over only once
// the transaction ends.
func TestTakeoverWaitsForFence(t *testing.T) {
	p := pool(t)
	ctx := context.Background()
	if err := lock.Ensure(ctx, p); err != nil {
		t.Fatal(err)
	}
	db := lock.Postgres(p)
	if ok, err := db.Take(ctx, "b", "w", "first", "r1"); err != nil || !ok {
		t.Fatalf("taking it: %v, %v", ok, err)
	}
	if _, err := p.Exec(ctx, `UPDATE locks SET last_refresh = now() - interval '1 hour' WHERE bucket = 'b' AND id = 'w'`); err != nil {
		t.Fatal(err)
	}
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := lock.CheckFence(ctx, tx, data.Fence{Lock: true, Bucket: "b", ID: "w", Holder: "r1"}); err != nil {
		t.Fatalf("the lapsed lease's fence, untaken: %v", err)
	}
	took := make(chan bool, 1)
	go func() {
		ok, _ := db.Take(ctx, "b", "w", "second", "r2")
		took <- ok
	}()
	select {
	case <-took:
		t.Fatal("a takeover went ahead of a fenced transaction")
	case <-time.After(time.Second):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if ok := <-took; !ok {
		t.Error("the takeover, once the fenced transaction ended, did not take it")
	}
}
