package lock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/testdb"
)

// env is one test's database, the context its locks are held in, and its lockers. When the
// test ends, the context ends — giving up every lock still held — and the lockers' refreshers
// are waited for, before the pool closes.
type env struct {
	ctx     context.Context
	pool    *pgxpool.Pool
	lockers []*Lock
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(), Schema); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &env{ctx: ctx, pool: pool}
	t.Cleanup(func() {
		cancel()
		for _, l := range e.lockers {
			l.WaitForRefreshers()
		}
	})
	return e
}

// locker is a new locker of the test's database: another process, as far as locks go.
func (e *env) locker() *Lock {
	l := New(e.pool)
	e.lockers = append(e.lockers, l)
	return l
}

func tryLock(t *testing.T, l *Lock, ctx context.Context, bucket, id string, want bool) {
	t.Helper()
	locked, err := l.TryLock(ctx, bucket, id)
	if err != nil {
		t.Fatal(err)
	}
	if locked != want {
		t.Fatalf("TryLock(%s, %s) = %v, want %v", bucket, id, locked, want)
	}
}

func unlock(t *testing.T, l *Lock, bucket, id string) {
	t.Helper()
	if err := l.Unlock(context.Background(), bucket, id); err != nil {
		t.Fatal(err)
	}
}

func TestTryLock(t *testing.T) {
	e := newEnv(t)
	ctx, l := e.ctx, e.locker()
	tryLock(t, l, ctx, "test", "trylock", true)
	unlock(t, l, "test", "trylock")
}

// A lock is held once: a second TryLock, even by its own locker, fails until it is unlocked.
func TestTryLockTwice(t *testing.T) {
	e := newEnv(t)
	ctx, l := e.ctx, e.locker()
	tryLock(t, l, ctx, "test_twice", "simple_lock", true)
	tryLock(t, l, ctx, "test_twice", "simple_lock", false)
	unlock(t, l, "test_twice", "simple_lock")
	tryLock(t, l, ctx, "test_twice", "simple_lock", true)
	unlock(t, l, "test_twice", "simple_lock")
}

// A held lock is refreshed, and stays held for as long as it is: another locker cannot take
// it.
func TestTryLockRefresh(t *testing.T) {
	e := newEnv(t)
	ctx, pool, l, other := e.ctx, e.pool, e.locker(), e.locker()
	tryLock(t, l, ctx, "test_refresh", "simple_lock", true)
	refreshed := func() time.Time {
		var at time.Time
		if err := pool.QueryRow(ctx, `SELECT last_refresh FROM locks WHERE bucket = 'test_refresh' AND id = 'simple_lock'`).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	first := refreshed()
	time.Sleep(2*refreshEvery + refreshEvery/2)
	if !refreshed().After(first) {
		t.Fatalf("the lock was not refreshed: last at %s, first %s", refreshed(), first)
	}
	tryLock(t, other, ctx, "test_refresh", "simple_lock", false)
	unlock(t, l, "test_refresh", "simple_lock")
}

// Lock waits for a lock someone else holds, up to its wait.
func TestLock(t *testing.T) {
	e := newEnv(t)
	ctx, l, other := e.ctx, e.locker(), e.locker()
	locked, err := l.Lock(ctx, "test_lock", "simple_lock", time.Second)
	if err != nil || !locked {
		t.Fatalf("Lock = %v, %v", locked, err)
	}
	locked, err = other.Lock(ctx, "test_lock", "simple_lock", retryEvery+retryEvery/2)
	if err != nil || locked {
		t.Fatalf("Lock of a held lock = %v, %v", locked, err)
	}
	go func() {
		time.Sleep(retryEvery)
		_ = l.Unlock(context.Background(), "test_lock", "simple_lock")
	}()
	locked, err = other.Lock(ctx, "test_lock", "simple_lock", 5*retryEvery)
	if err != nil || !locked {
		t.Fatalf("Lock once the holder unlocked = %v, %v", locked, err)
	}
	unlock(t, other, "test_lock", "simple_lock")
}

// A lock whose holder stopped refreshing it — it died — is taken over once its lease has run
// out, and not before.
func TestLapsedLease(t *testing.T) {
	e := newEnv(t)
	ctx, pool, l := e.ctx, e.pool, e.locker()
	if _, err := pool.Exec(ctx, `INSERT INTO locks VALUES ('test_lapsed', 'dead', 'a dead locker', 'r', now() - make_interval(secs => $1)),
		('test_lapsed', 'alive', 'a live locker', 'r', now() - make_interval(secs => $2))`, (lease + time.Second).Seconds(), (lease - 2*time.Second).Seconds()); err != nil {
		t.Fatal(err)
	}
	tryLock(t, l, ctx, "test_lapsed", "dead", true)
	tryLock(t, l, ctx, "test_lapsed", "alive", false)
	var owner string
	if err := pool.QueryRow(ctx, `SELECT owner FROM locks WHERE bucket = 'test_lapsed' AND id = 'dead'`).Scan(&owner); err != nil || owner != l.id {
		t.Fatalf("the lapsed lock's owner is %q (%v), want the locker that took it", owner, err)
	}
}

// Only the locker that took a lock can unlock it.
func TestUnlockNotOwner(t *testing.T) {
	e := newEnv(t)
	ctx, l, other := e.ctx, e.locker(), e.locker()
	tryLock(t, l, ctx, "test_owner", "simple_lock", true)
	if err := other.Unlock(ctx, "test_owner", "simple_lock"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("another locker's Unlock = %v, want ErrNotOwner", err)
	}
	unlock(t, l, "test_owner", "simple_lock")
	if err := l.Unlock(ctx, "test_owner", "simple_lock"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("an Unlock of a lock already given up = %v, want ErrNotOwner", err)
	}
}

// A lock's context ending gives it up.
func TestContextEndUnlocks(t *testing.T) {
	e := newEnv(t)
	l, other := e.locker(), e.locker()
	held, cancel := context.WithCancel(e.ctx)
	tryLock(t, l, held, "test_context", "simple_lock", true)
	cancel()
	l.WaitForRefreshers()
	tryLock(t, other, e.ctx, "test_context", "simple_lock", true)
}

// LockFn runs its function holding the lock and gives the lock up when it returns.
func TestLockFn(t *testing.T) {
	e := newEnv(t)
	ctx, l, other := e.ctx, e.locker(), e.locker()
	ran := false
	err := l.LockFn(ctx, "test_fn", "simple_lock", 0, func(ctx context.Context) error {
		ran = true
		tryLock(t, other, ctx, "test_fn", "simple_lock", false)
		return errors.New("its own error")
	})
	if !ran || err == nil || err.Error() != "its own error" {
		t.Fatalf("LockFn = %v (ran %v), want the function's error", err, ran)
	}
	tryLock(t, other, ctx, "test_fn", "simple_lock", true)
}

// LockFn does not run its function when the lock is not taken within its wait.
func TestLockFnNotAcquired(t *testing.T) {
	e := newEnv(t)
	ctx, l, other := e.ctx, e.locker(), e.locker()
	tryLock(t, l, ctx, "test_fn_held", "simple_lock", true)
	err := other.LockFn(ctx, "test_fn_held", "simple_lock", retryEvery/2, func(context.Context) error {
		t.Error("the function ran without the lock")
		return nil
	})
	if !errors.Is(err, ErrNotAcquired) {
		t.Fatalf("LockFn of a held lock = %v, want ErrNotAcquired", err)
	}
}

// A lock lost while LockFn's function runs — someone else took it over — ends the function's
// context, with ErrLost its cause, within a refresh: the function stops acting as the holder.
func TestLockFnLost(t *testing.T) {
	e := newEnv(t)
	ctx, pool, l := e.ctx, e.pool, e.locker()
	var cause error
	started := time.Now()
	var lostAfter time.Duration
	err := l.LockFn(ctx, "test_fn_lost", "simple_lock", 0, func(ctx context.Context) error {
		if _, err := pool.Exec(context.Background(), `UPDATE locks SET owner = 'another locker', refresh_id = 'its own'
			WHERE bucket = 'test_fn_lost' AND id = 'simple_lock'`); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			cause, lostAfter = context.Cause(ctx), time.Since(started)
		case <-time.After(5 * refreshEvery):
			t.Error("the function's context did not end when its lock was taken over")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(cause, ErrLost) || lostAfter > 2*refreshEvery+refreshEvery/2 {
		t.Fatalf("the context ended %s in with cause %v, want ErrLost within a refresh", lostAfter, cause)
	}
	var owner string
	if err := pool.QueryRow(ctx, `SELECT owner FROM locks WHERE bucket = 'test_fn_lost' AND id = 'simple_lock'`).Scan(&owner); err != nil || owner != "another locker" {
		t.Fatalf("the lock's owner after LockFn is %q (%v): LockFn gave up a lock it no longer held", owner, err)
	}
}
