// Package locktest is the suite every lock.DB passes: run Run against an implementation.
package locktest

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/lock"
)

// Env is a database for one test: a DB over its locks table, and how the test reaches into the
// table's rows, as only a test does.
type Env struct {
	DB lock.DB
	// Plant writes a lock's row as another locker's, owner, refreshed age ago.
	Plant func(t *testing.T, bucket, id, owner string, age time.Duration)
	// Steal gives a lock's row to another locker, as a takeover would.
	Steal func(t *testing.T, bucket, id string)
	// Row reads a lock's row: its owner and its last refresh; ok false for none.
	Row func(t *testing.T, bucket, id string) (owner string, refreshed time.Time, ok bool)
}

var names atomic.Int64

func bucket(kind string) string { return fmt.Sprintf("locktest.%s.%d", kind, names.Add(1)) }

// env is one test's: its database, the context its locks are held in, and its lockers. When the
// test ends, the context ends — giving up every lock still held — and the lockers' refreshers
// are waited for.
type env struct {
	Env
	ctx     context.Context
	lockers []*lock.Lock
}

func newEnv(t *testing.T, open func(t *testing.T) Env) *env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	e := &env{Env: open(t), ctx: ctx}
	t.Cleanup(func() {
		cancel()
		for _, l := range e.lockers {
			l.WaitForRefreshers()
		}
	})
	return e
}

// locker is a new locker: another process, as far as locks go.
func (e *env) locker() *lock.Lock {
	l := lock.NewOn(e.DB)
	e.lockers = append(e.lockers, l)
	return l
}

func tryLock(t *testing.T, l *lock.Lock, ctx context.Context, b, id string, want bool) {
	t.Helper()
	locked, err := l.TryLock(ctx, b, id)
	if err != nil {
		t.Fatal(err)
	}
	if locked != want {
		t.Fatalf("TryLock(%s, %s) = %v, want %v", b, id, locked, want)
	}
}

func unlock(t *testing.T, l *lock.Lock, b, id string) {
	t.Helper()
	if err := l.Unlock(context.Background(), b, id); err != nil {
		t.Fatal(err)
	}
}

// Run runs the suite, each test with a database open makes.
func Run(t *testing.T, open func(t *testing.T) Env) {
	for _, tc := range []struct {
		name string
		fn   func(t *testing.T, e *env)
	}{
		{"TryLock", testTryLock},
		{"TryLockTwice", testTryLockTwice},
		{"Refresh", testRefresh},
		{"Lock", testLock},
		{"LapsedLease", testLapsedLease},
		{"UnlockNotOwner", testUnlockNotOwner},
		{"ContextEndUnlocks", testContextEndUnlocks},
		{"LockFn", testLockFn},
		{"LockFnNotAcquired", testLockFnNotAcquired},
		{"LockFnLost", testLockFnLost},
		{"Fences", testFences},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newEnv(t, open)) })
	}
}

func testTryLock(t *testing.T, e *env) {
	b, l := bucket("trylock"), e.locker()
	tryLock(t, l, e.ctx, b, "a", true)
	unlock(t, l, b, "a")
}

// A lock is held once: a second TryLock, even by its own locker, fails until it is unlocked.
func testTryLockTwice(t *testing.T, e *env) {
	b, l := bucket("twice"), e.locker()
	tryLock(t, l, e.ctx, b, "a", true)
	tryLock(t, l, e.ctx, b, "a", false)
	unlock(t, l, b, "a")
	tryLock(t, l, e.ctx, b, "a", true)
	unlock(t, l, b, "a")
}

// A held lock is refreshed, and stays held for as long as it is: another locker cannot take it.
func testRefresh(t *testing.T, e *env) {
	b, l, other := bucket("refresh"), e.locker(), e.locker()
	tryLock(t, l, e.ctx, b, "a", true)
	_, first, ok := e.Row(t, b, "a")
	if !ok {
		t.Fatal("a held lock has no row")
	}
	time.Sleep(2*lock.RefreshEvery + lock.RefreshEvery/2)
	if _, later, _ := e.Row(t, b, "a"); !later.After(first) {
		t.Fatalf("the lock was not refreshed: last at %s, first %s", later, first)
	}
	tryLock(t, other, e.ctx, b, "a", false)
	unlock(t, l, b, "a")
}

// Lock waits for a lock someone else holds, up to its wait.
func testLock(t *testing.T, e *env) {
	b, l, other := bucket("lock"), e.locker(), e.locker()
	locked, err := l.Lock(e.ctx, b, "a", time.Second)
	if err != nil || !locked {
		t.Fatalf("Lock = %v, %v", locked, err)
	}
	locked, err = other.Lock(e.ctx, b, "a", lock.RetryEvery+lock.RetryEvery/2)
	if err != nil || locked {
		t.Fatalf("Lock of a held lock = %v, %v", locked, err)
	}
	go func() {
		time.Sleep(lock.RetryEvery)
		_ = l.Unlock(context.Background(), b, "a")
	}()
	locked, err = other.Lock(e.ctx, b, "a", 5*lock.RetryEvery)
	if err != nil || !locked {
		t.Fatalf("Lock once the holder unlocked = %v, %v", locked, err)
	}
	unlock(t, other, b, "a")
}

// A lock whose holder stopped refreshing it — it died — is taken over once its lease has run
// out, and not before.
func testLapsedLease(t *testing.T, e *env) {
	b, l := bucket("lapsed"), e.locker()
	e.Plant(t, b, "dead", "a dead locker", lock.Lease+time.Second)
	e.Plant(t, b, "alive", "a live locker", lock.Lease-2*time.Second)
	tryLock(t, l, e.ctx, b, "dead", true)
	tryLock(t, l, e.ctx, b, "alive", false)
	if owner, _, _ := e.Row(t, b, "dead"); owner != l.ID() {
		t.Fatalf("the lapsed lock's owner is %q, want the locker that took it", owner)
	}
}

// Only the locker that took a lock can unlock it.
func testUnlockNotOwner(t *testing.T, e *env) {
	b, l, other := bucket("owner"), e.locker(), e.locker()
	tryLock(t, l, e.ctx, b, "a", true)
	if err := other.Unlock(e.ctx, b, "a"); !errors.Is(err, lock.ErrNotOwner) {
		t.Fatalf("another locker's Unlock = %v, want ErrNotOwner", err)
	}
	unlock(t, l, b, "a")
	if err := l.Unlock(e.ctx, b, "a"); !errors.Is(err, lock.ErrNotOwner) {
		t.Fatalf("an Unlock of a lock already given up = %v, want ErrNotOwner", err)
	}
}

// A lock's context ending gives it up.
func testContextEndUnlocks(t *testing.T, e *env) {
	b, l, other := bucket("context"), e.locker(), e.locker()
	held, cancel := context.WithCancel(e.ctx)
	tryLock(t, l, held, b, "a", true)
	cancel()
	l.WaitForRefreshers()
	tryLock(t, other, e.ctx, b, "a", true)
}

// LockFn runs its function holding the lock and gives the lock up when it returns.
func testLockFn(t *testing.T, e *env) {
	b, l, other := bucket("fn"), e.locker(), e.locker()
	ran := false
	err := l.LockFn(e.ctx, b, "a", 0, func(ctx context.Context) error {
		ran = true
		tryLock(t, other, ctx, b, "a", false)
		return errors.New("its own error")
	})
	if !ran || err == nil || err.Error() != "its own error" {
		t.Fatalf("LockFn = %v (ran %v), want the function's error", err, ran)
	}
	tryLock(t, other, e.ctx, b, "a", true)
}

// LockFn does not run its function when the lock is not taken within its wait.
func testLockFnNotAcquired(t *testing.T, e *env) {
	b, l, other := bucket("fnheld"), e.locker(), e.locker()
	tryLock(t, l, e.ctx, b, "a", true)
	err := other.LockFn(e.ctx, b, "a", lock.RetryEvery/2, func(context.Context) error {
		t.Error("the function ran without the lock")
		return nil
	})
	if !errors.Is(err, lock.ErrNotAcquired) {
		t.Fatalf("LockFn of a held lock = %v, want ErrNotAcquired", err)
	}
}

// A lock lost while LockFn's function runs — someone else took it over — ends the function's
// context, with ErrLost its cause, within two refreshes of the takeover: the function stops acting
// as the holder, and LockFn gives up nothing it no longer holds.
func testLockFnLost(t *testing.T, e *env) {
	b, l := bucket("fnlost"), e.locker()
	var cause error
	var lostAfter time.Duration
	err := l.LockFn(e.ctx, b, "a", 0, func(ctx context.Context) error {
		e.Steal(t, b, "a")
		stolen := time.Now()
		select {
		case <-ctx.Done():
			cause, lostAfter = context.Cause(ctx), time.Since(stolen)
		case <-time.After(5 * lock.RefreshEvery):
			t.Error("the function's context did not end when its lock was taken over")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(cause, lock.ErrLost) || lostAfter > 2*lock.RefreshEvery+lock.RefreshEvery/2 {
		t.Fatalf("the context ended %s after the takeover with cause %v, want ErrLost within two refreshes", lostAfter, cause)
	}
	if owner, _, _ := e.Row(t, b, "a"); owner == l.ID() || owner == "" {
		t.Fatalf("the lock's owner after LockFn is %q: LockFn gave up a lock it no longer held", owner)
	}
}

// LockFn's context carries its lease's fence, and TryLockFenced returns its own: each names the
// lock, and a lease of its own.
func testFences(t *testing.T, e *env) {
	b, l := bucket("fences"), e.locker()
	var first data.Fence
	err := l.LockFn(e.ctx, b, "a", 0, func(ctx context.Context) error {
		f, ok := data.FenceOf(ctx)
		if !ok || !f.Lock || f.Bucket != b || f.ID != "a" || f.Holder == "" {
			t.Errorf("LockFn's fence = %+v, %v", f, ok)
		}
		first = f
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	f, ok, err := l.TryLockFenced(e.ctx, b, "a")
	if err != nil || !ok || f.Bucket != b || f.ID != "a" || f.Holder == "" || f.Holder == first.Holder {
		t.Fatalf("TryLockFenced = %+v, %v, %v; want a lease of its own", f, ok, err)
	}
	unlock(t, l, b, "a")
}
