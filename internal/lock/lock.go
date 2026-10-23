// Package lock is distributed locks over a role's own database (docs/hosted.md, "Shared
// primitives"). A lock is a row of the locks table — which a role's migrations make, and which
// Ensure (or its Spanner side's) makes before them — held one of two ways: by a locker that took
// it, for as long as it keeps refreshing it, free once it goes unrefreshed for the Lease (a lease,
// for what spans transactions); or by a transaction that locked its row (InTx), until it ends (for
// what one transaction does). The two exclude each other. The database's clock judges every
// lease, so lockers on different machines never compare their clocks. A locker keeps its locks in
// a DB: PostgreSQL's (Postgres), or Spanner's (spannerlock).
package lock

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data"
)

const (
	// RefreshEvery is how often a held lock is refreshed.
	RefreshEvery = time.Second
	// Lease is how long a lock stays held unrefreshed: a holder that dies, or cannot reach the
	// database, loses its lock this long after its last refresh.
	Lease = 6 * time.Second
	// RetryEvery is how often Lock and LockFn try again for a lock someone else holds.
	RetryEvery = time.Second
)

var (
	// ErrNotAcquired is LockFn's error when the lock was not taken within its wait.
	ErrNotAcquired = errors.New("lock: unable to acquire the lock within the wait")
	// ErrLost is the cause of the end of the context LockFn runs its function with when the
	// lock is lost.
	ErrLost = errors.New("lock: the lock was lost")
	// ErrNotOwner is Unlock's error for a lock the locker does not hold.
	ErrNotOwner = errors.New("lock: attempted to unlock a lock this locker doesn't own")
	// ErrHeld is InTx's error when a locker holds the lock's lease.
	ErrHeld = errors.New("lock: a locker holds the lock's lease")

	errRefreshMismatch = errors.New("lock: refresh ID mismatch, aborting refresh")
)

// DB is where a locker keeps its locks: the locks table of a role's database, each lock a row of
// its bucket, its ID, its owner (the locker), its refresh ID (the lease), and when it was last
// refreshed, by the database's clock.
type DB interface {
	// Take takes the lock for owner's lease refreshID if no one holds it — it has no row, or its
	// row went unrefreshed for the Lease — reporting whether it did.
	Take(ctx context.Context, bucket, id, owner, refreshID string) (bool, error)
	// Refresh renews the lease refreshID while the lock's row still carries it, reporting whether
	// it did.
	Refresh(ctx context.Context, bucket, id, refreshID string) (bool, error)
	// Release gives the lock up if owner holds it — by the lease refreshID, unless that is "" —
	// reporting whether it did.
	Release(ctx context.Context, bucket, id, owner, refreshID string) (bool, error)
}

// Lock is a locker. The locks it takes are its own: only it can unlock them.
type Lock struct {
	db             DB
	id             string
	refresherCount sync.WaitGroup
}

// LockFn is what LockFn runs while it holds a lock.
type LockFn func(context.Context) error

// held is one lock a refresher keeps.
type held struct {
	bucket, id, refreshID string
	lost                  func() // told once the lock is lost; nil when no one is
}

// New returns a locker over the locks table of the PostgreSQL database pool reaches (Postgres). A
// process makes one per database and shares it, so that a lock taken in one place can be unlocked
// in another.
func New(pool *pgxpool.Pool) *Lock { return NewOn(Postgres(pool)) }

// NewOn returns a locker over db, as New does over PostgreSQL's.
func NewOn(db DB) *Lock { return &Lock{db: db, id: rand.Text()} }

// ID is the locker's: the owner its locks' rows name.
func (l *Lock) ID() string { return l.id }

// TryLock attempts to take the lock id in bucket, and returns whether it did. It does not
// wait: if someone else holds the lock, it returns false at once.
//
// The lock is held until Unlock, until ctx ends (at its deadline, say), or until this process
// dies or cannot reach the database for the lease, whichever comes first.
func (l *Lock) TryLock(ctx context.Context, bucket, id string) (bool, error) {
	lk, err := l.tryLock(ctx, bucket, id, nil)
	return lk != nil, err
}

// TryLockFenced takes the lock as TryLock does, and returns the fence of the lease it took (ok
// false, and no fence, if someone else holds it).
func (l *Lock) TryLockFenced(ctx context.Context, bucket, id string) (data.Fence, bool, error) {
	lk, err := l.tryLock(ctx, bucket, id, nil)
	if err != nil || lk == nil {
		return data.Fence{}, false, err
	}
	return lk.fence(), true, nil
}

func (lk *held) fence() data.Fence {
	return data.Fence{Lock: true, Bucket: lk.bucket, ID: lk.id, Holder: lk.refreshID}
}

// Lock takes the lock id in bucket as TryLock does, trying again until wait has passed or ctx
// ends, and returns whether it took it. It holds the lock as TryLock does.
func (l *Lock) Lock(ctx context.Context, bucket, id string, wait time.Duration) (bool, error) {
	lk, err := l.acquire(ctx, bucket, id, wait, nil)
	return lk != nil, err
}

// LockFn takes the lock id in bucket as Lock does and runs fn holding it, with a context that
// ends when ctx does or when the lock is lost — its lease could not be refreshed, or someone
// else took it over — with ErrLost its cause, so that fn stops acting once it no longer holds
// the lock, and that carries the lease's fence (data.WithFence), so that what fn writes through a
// store that checks it is never written once another holds the lock. The lock is given up when
// fn returns. LockFn returns fn's error, ctx's if it ended before the lock was taken, or
// ErrNotAcquired if the lock was not taken within wait.
func (l *Lock) LockFn(ctx context.Context, bucket, id string, wait time.Duration, fn LockFn) error {
	holding, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	lk, err := l.acquire(holding, bucket, id, wait, func() { cancel(ErrLost) })
	if err != nil {
		return err
	}
	if lk == nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		return ErrNotAcquired
	}
	err = fn(data.WithFence(holding, lk.fence()))
	unlockCtx, done := context.WithTimeout(context.WithoutCancel(ctx), Lease)
	_, _ = l.db.Release(unlockCtx, lk.bucket, lk.id, l.id, lk.refreshID)
	done()
	return err
}

// Unlock gives up a lock this locker took.
func (l *Lock) Unlock(ctx context.Context, bucket, id string) error {
	ok, err := l.db.Release(ctx, bucket, id, l.id, "")
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotOwner
	}
	return nil
}

// WaitForRefreshers waits until every refresher this locker started has stopped.
func (l *Lock) WaitForRefreshers() {
	l.refresherCount.Wait()
}

// acquire tries for the lock until wait has passed or ctx ends, and returns it, or nil.
func (l *Lock) acquire(ctx context.Context, bucket, id string, wait time.Duration, lost func()) (*held, error) {
	lk, err := l.tryLock(ctx, bucket, id, lost)
	if err != nil || lk != nil {
		return lk, err
	}
	tick := time.NewTicker(RetryEvery)
	defer tick.Stop()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return nil, nil
		case <-ctx.Done():
			return nil, nil
		case <-tick.C:
			lk, err := l.tryLock(ctx, bucket, id, lost)
			if err != nil || lk != nil {
				return lk, err
			}
		}
	}
}

// tryLock takes the lock if no one holds it, and starts refreshing it.
func (l *Lock) tryLock(ctx context.Context, bucket, id string, lost func()) (*held, error) {
	lk := &held{bucket: bucket, id: id, refreshID: rand.Text(), lost: lost}
	ok, err := l.db.Take(ctx, bucket, id, l.id, lk.refreshID)
	if err != nil || !ok {
		return nil, err
	}
	l.refresherCount.Add(1)
	go l.startLockRefresher(ctx, lk)
	return lk, nil
}

// startLockRefresher refreshes a held lock until ctx ends, when it unlocks it, or until a
// refresh fails — the lock taken over, or the database out of reach — when it stops and tells
// the holder. A holder that dies stops refreshing, so its lock is free a lease after its last
// refresh: a crash or a deployment never leaves a lock held.
func (l *Lock) startLockRefresher(ctx context.Context, lk *held) {
	tick := time.NewTicker(RefreshEvery)
	defer l.refresherCount.Done()
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			// The context is done, so the unlock needs one of its own.
			unlockCtx, cancel := context.WithTimeout(context.Background(), Lease)
			_, _ = l.db.Release(unlockCtx, lk.bucket, lk.id, l.id, lk.refreshID)
			cancel()
			return
		case <-tick.C:
			if err := l.doLockRefresh(ctx, lk); err != nil {
				if lk.lost != nil {
					lk.lost()
				}
				return
			}
		}
	}
}

// doLockRefresh renews a held lock's lease while it is still this refresher's. A refresh that
// cannot finish while the last one's lease still runs fails: after that, someone else may have
// taken the lock.
func (l *Lock) doLockRefresh(ctx context.Context, lk *held) error {
	ctx, cancel := context.WithTimeout(ctx, Lease-RefreshEvery)
	defer cancel()
	ok, err := l.db.Refresh(ctx, lk.bucket, lk.id, lk.refreshID)
	if err != nil {
		return err
	}
	if !ok {
		return errRefreshMismatch
	}
	return nil
}
