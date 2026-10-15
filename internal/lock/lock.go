// Package lock is distributed locks over PostgreSQL. A lock is a row of the locks table in a
// role's own database — which its migrations make with Schema — held by the locker that took it
// for as long as that locker keeps refreshing it, and free once it goes unrefreshed for the lease.
// The database's clock judges every lease, so lockers on different machines never compare their
// clocks.
package lock

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema makes the locks table; a role that takes locks includes it in its migrations.
const Schema = `CREATE TABLE locks (
    bucket       text NOT NULL,
    id           text NOT NULL,
    owner        text NOT NULL,
    refresh_id   text NOT NULL,
    last_refresh timestamptz NOT NULL,
    PRIMARY KEY (bucket, id)
);`

const (
	// refreshEvery is how often a held lock is refreshed.
	refreshEvery = time.Second
	// lease is how long a lock stays held unrefreshed: a holder that dies, or cannot reach the
	// database, loses its lock this long after its last refresh.
	lease = 6 * time.Second
	// retryEvery is how often Lock and LockFn try again for a lock someone else holds.
	retryEvery = time.Second
)

var (
	// ErrNotAcquired is LockFn's error when the lock was not taken within its wait.
	ErrNotAcquired = errors.New("lock: unable to acquire the lock within the wait")
	// ErrLost is the cause of the end of the context LockFn runs its function with when the
	// lock is lost.
	ErrLost = errors.New("lock: the lock was lost")
	// ErrNotOwner is Unlock's error for a lock the locker does not hold.
	ErrNotOwner = errors.New("lock: attempted to unlock a lock this locker doesn't own")

	errRefreshMismatch = errors.New("lock: refresh ID mismatch, aborting refresh")
)

// Lock is a locker. The locks it takes are its own: only it can unlock them.
type Lock struct {
	pool           *pgxpool.Pool
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

// New returns a locker over the locks table of the database pool reaches. A process makes one
// per database and shares it, so that a lock taken in one place can be unlocked in another.
func New(pool *pgxpool.Pool) *Lock {
	return &Lock{pool: pool, id: rand.Text()}
}

// TryLock attempts to take the lock id in bucket, and returns whether it did. It does not
// wait: if someone else holds the lock, it returns false at once.
//
// The lock is held until Unlock, until ctx ends (at its deadline, say), or until this process
// dies or cannot reach the database for the lease, whichever comes first.
func (l *Lock) TryLock(ctx context.Context, bucket, id string) (bool, error) {
	lk, err := l.tryLock(ctx, bucket, id, nil)
	return lk != nil, err
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
// the lock. The lock is given up when fn returns. LockFn returns fn's error, ctx's if it ended
// before the lock was taken, or ErrNotAcquired if the lock was not taken within wait.
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
	err = fn(holding)
	unlockCtx, done := context.WithTimeout(context.WithoutCancel(ctx), lease)
	_ = l.refreshUnlock(unlockCtx, lk)
	done()
	return err
}

// Unlock gives up a lock this locker took.
func (l *Lock) Unlock(ctx context.Context, bucket, id string) error {
	tag, err := l.pool.Exec(ctx, `DELETE FROM locks WHERE bucket = $1 AND id = $2 AND owner = $3`, bucket, id, l.id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
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
	tick := time.NewTicker(retryEvery)
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

// tryLock takes the lock if no one holds it — it has no row, or its row went unrefreshed for
// the lease — in one statement, and starts refreshing it.
func (l *Lock) tryLock(ctx context.Context, bucket, id string, lost func()) (*held, error) {
	lk := &held{bucket: bucket, id: id, refreshID: rand.Text(), lost: lost}
	err := l.pool.QueryRow(ctx, `INSERT INTO locks (bucket, id, owner, refresh_id, last_refresh)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (bucket, id) DO UPDATE SET owner = EXCLUDED.owner, refresh_id = EXCLUDED.refresh_id,
			last_refresh = EXCLUDED.last_refresh
		WHERE locks.last_refresh < now() - make_interval(secs => $5)
		RETURNING refresh_id`, bucket, id, l.id, lk.refreshID, lease.Seconds()).Scan(new(string))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
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
	tick := time.NewTicker(refreshEvery)
	defer l.refresherCount.Done()
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			// The context is done, so the unlock needs one of its own.
			unlockCtx, cancel := context.WithTimeout(context.Background(), lease)
			_ = l.refreshUnlock(unlockCtx, lk)
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
	ctx, cancel := context.WithTimeout(ctx, lease-refreshEvery)
	defer cancel()
	tag, err := l.pool.Exec(ctx, `UPDATE locks SET last_refresh = now() WHERE bucket = $1 AND id = $2 AND refresh_id = $3`,
		lk.bucket, lk.id, lk.refreshID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errRefreshMismatch
	}
	return nil
}

// refreshUnlock gives up a held lock if it is still this refresher's.
func (l *Lock) refreshUnlock(ctx context.Context, lk *held) error {
	_, err := l.pool.Exec(ctx, `DELETE FROM locks WHERE bucket = $1 AND id = $2 AND owner = $3 AND refresh_id = $4`,
		lk.bucket, lk.id, l.id, lk.refreshID)
	return err
}
