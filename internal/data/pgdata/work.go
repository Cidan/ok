package pgdata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/work"
)

// WorkSchema makes the table a role keeps its keyed work in (internal/work): a row per kind and
// key, marked dirty when what the work acts on changes — due now, or (due_at) at a time — and
// leased to one holder at a time. A role that does keyed work includes it in its migrations.
const WorkSchema = `CREATE TABLE work (
    kind        text        NOT NULL,
    key         bigint      NOT NULL,
    dirty       boolean     NOT NULL DEFAULT false,
    due_at      timestamptz,
    holder      text,
    lease_until timestamptz,
    PRIMARY KEY (kind, key)
);
CREATE INDEX work_due ON work (kind, key) WHERE dirty OR holder IS NOT NULL;`

// MarkWork marks key's work of kind dirty, due now, and records the mark's job in q's attempt
// (work.Marked), enqueued before it commits: q is a transaction Txn began, or the pool, when the
// mark is a transaction of its own. The row's lock, held until the transaction ends, orders every
// take and release of the work after it.
func MarkWork(ctx context.Context, q Querier, kind string, key int64) error {
	if pool, ok := poolOf(q); ok {
		return Txn(ctx, pool, func(tx pgx.Tx) error { return MarkWork(ctx, tx, kind, key) })
	}
	if _, err := q.Exec(ctx, `INSERT INTO work (kind, key, dirty) VALUES ($1, $2, true)
		ON CONFLICT (kind, key) DO UPDATE SET dirty = true, due_at = NULL`, kind, key); err != nil {
		return err
	}
	return work.Marked(attemptOf(q), kind, key, time.Time{})
}

// MarkWorkAt marks key's work of kind dirty, due at at — or sooner, if it is marked so already —
// and records the mark's job, due at at, as MarkWork does.
func MarkWorkAt(ctx context.Context, q Querier, kind string, key int64, at time.Time) error {
	if pool, ok := poolOf(q); ok {
		return Txn(ctx, pool, func(tx pgx.Tx) error { return MarkWorkAt(ctx, tx, kind, key, at) })
	}
	if _, err := q.Exec(ctx, `INSERT INTO work (kind, key, dirty, due_at) VALUES ($1, $2, true, $3)
		ON CONFLICT (kind, key) DO UPDATE SET dirty = true,
			due_at = CASE WHEN NOT work.dirty THEN EXCLUDED.due_at WHEN work.due_at IS NULL THEN NULL
				ELSE least(work.due_at, EXCLUDED.due_at) END`, kind, key, at); err != nil {
		return err
	}
	return work.Marked(attemptOf(q), kind, key, at)
}

// PendStep records a step to take once q's transaction commits (work.Pend): its job is enqueued as
// the transaction is about to commit. q is a transaction Txn began, or what wraps one.
func PendStep(q Querier, kind string, key int64, at time.Time, body []byte) error {
	if _, ok := poolOf(q); ok {
		return errors.New("pgdata: a step is pended in a transaction, whose commit it follows")
	}
	return work.Pend(attemptOf(q), kind, key, at, body)
}

// PeekWork reads who holds key's work, without a lock.
func PeekWork(ctx context.Context, q Querier, kind string, key int64) (work.Peeked, error) {
	var p work.Peeked
	var left float64
	err := q.QueryRow(ctx, `SELECT dirty, coalesce(holder, ''), holder IS NOT NULL AND lease_until >= now(),
			coalesce(greatest(extract(epoch FROM lease_until - now()), 0), 0)::float8
		FROM work WHERE kind = $1 AND key = $2`, kind, key).Scan(&p.Marked, &p.Holder, &p.Held, &left)
	if errors.Is(err, pgx.ErrNoRows) {
		return work.Peeked{}, nil
	}
	p.Exists, p.Left = true, time.Duration(left*float64(time.Second))
	return p, err
}

// MarkedWork are up to limit pieces of kind's work after key after, in key order, marked — due at
// their mark's time, zero for now — or held by a holder whose lease lapsed, due now (work.Store's
// Marked).
func MarkedWork(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, kind string, after int64, limit int) ([]work.Mark, error) {
	rows, err := q.Query(ctx, `SELECT key, CASE WHEN dirty AND due_at > now() THEN due_at END FROM work
		WHERE kind = $1 AND key > $2 AND (dirty OR holder IS NOT NULL) AND (dirty OR lease_until < now())
		ORDER BY key LIMIT $3`, kind, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (work.Mark, error) {
		m := work.Mark{Kind: kind}
		var at *time.Time
		err := r.Scan(&m.Key, &at)
		if at != nil {
			m.At = *at
		}
		return m, err
	})
}

// TakeWork leases key's work to holder for ttl, if no one holds it or its holder's lease lapsed —
// making its row if it has none — clearing its mark if it is due now; a mark due later stays. The
// work is due if it was marked due now, or its holder's lease lapsed before it let go. It waits for
// a transaction marking the work to end: the row's lock.
func TakeWork(ctx context.Context, pool *pgxpool.Pool, kind string, key int64, holder string, ttl time.Duration) (work.Taken, error) {
	var t work.Taken
	err := Txn(ctx, pool, func(tx pgx.Tx) error {
		t = work.Taken{}
		for range 2 {
			var held, lapsed, dueNow bool
			var later *time.Time
			err := tx.QueryRow(ctx, `SELECT holder IS NOT NULL AND lease_until >= now(), holder IS NOT NULL AND lease_until < now(),
					dirty AND (due_at IS NULL OR due_at <= now()), CASE WHEN dirty AND due_at > now() THEN due_at END
				FROM work WHERE kind = $1 AND key = $2 FOR UPDATE`, kind, key).Scan(&held, &lapsed, &dueNow, &later)
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				tag, err := tx.Exec(ctx, `INSERT INTO work (kind, key, dirty, holder, lease_until)
					VALUES ($1, $2, false, $3, now() + make_interval(secs => $4)) ON CONFLICT (kind, key) DO NOTHING`, kind, key, holder, ttl.Seconds())
				if err != nil || tag.RowsAffected() > 0 {
					t.Took = err == nil
					return err
				}
				continue // another transaction made the row as this one looked: read it, made
			case err != nil:
				return err
			case held:
				return nil
			}
			if _, err := tx.Exec(ctx, `UPDATE work SET holder = $3, lease_until = now() + make_interval(secs => $4), dirty = $5, due_at = $6
				WHERE kind = $1 AND key = $2`, kind, key, holder, ttl.Seconds(), later != nil, later); err != nil {
				return err
			}
			t.Took, t.Due = true, dueNow || lapsed
			if later != nil {
				t.Later = *later
			}
			return nil
		}
		return nil
	})
	return t, err
}

// RenewWork extends holder's lease on key's work by ttl from now; it reports false if the lease is
// another's.
func RenewWork(ctx context.Context, q Querier, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE work SET lease_until = now() + make_interval(secs => $4)
		WHERE kind = $1 AND key = $2 AND holder = $3`, kind, key, holder, ttl.Seconds())
	return tag.RowsAffected() > 0, err
}

// ReleaseWork ends holder's lease on key's work — unless the work was marked meanwhile, due now,
// when it clears the mark and keeps the lease, reporting again: the holder does the work again. A
// mark due later stays, for its job. Nothing happens if the lease is another's.
func ReleaseWork(ctx context.Context, q Querier, kind string, key int64, holder string) (again bool, err error) {
	err = q.QueryRow(ctx, `UPDATE work SET
			holder = CASE WHEN dirty AND (due_at IS NULL OR due_at <= now()) THEN holder END,
			lease_until = CASE WHEN dirty AND (due_at IS NULL OR due_at <= now()) THEN lease_until END,
			dirty = dirty AND coalesce(due_at > now(), false),
			due_at = CASE WHEN dirty AND due_at > now() THEN due_at END
		WHERE kind = $1 AND key = $2 AND holder = $3
		RETURNING holder IS NOT NULL`, kind, key, holder).Scan(&again)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return again, err
}

// LetWork ends holder's lease on key's work and leaves its mark as it is: work a claim
// (work.Claim) held while it was marked is done by whoever takes it next, a job brought at once for
// a mark due now. Nothing happens if the lease is another's.
func LetWork(ctx context.Context, q Querier, kind string, key int64, holder string) error {
	if pool, ok := poolOf(q); ok {
		return Txn(ctx, pool, func(tx pgx.Tx) error { return LetWork(ctx, tx, kind, key, holder) })
	}
	var due bool
	err := q.QueryRow(ctx, `UPDATE work SET holder = NULL, lease_until = NULL WHERE kind = $1 AND key = $2 AND holder = $3
		RETURNING dirty AND (due_at IS NULL OR due_at <= now())`, kind, key, holder).Scan(&due)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	case err != nil || !due:
		return err
	}
	return work.Marked(attemptOf(q), kind, key, time.Time{})
}

// CheckWorkFence fails the transaction q is with data.ErrFenced unless f's work is still leased to
// f's holder, and holds its row against a takeover until the transaction ends.
func CheckWorkFence(ctx context.Context, q Querier, f data.Fence) error {
	var held bool
	err := q.QueryRow(ctx, `SELECT holder = $3 AND lease_until >= now() FROM work WHERE kind = $1 AND key = $2 FOR SHARE`,
		f.Kind, f.Key, f.Holder).Scan(&held)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !held {
		return fmt.Errorf("%w: %s", data.ErrFenced, f)
	}
	return err
}

// Work is keyed work's store (internal/work) over the work table pool reaches, each operation a
// transaction of its own, as spannerdata.Work is on Spanner.
func Work(pool *pgxpool.Pool) work.Store { return workStore{pool} }

type workStore struct{ pool *pgxpool.Pool }

func (w workStore) Mark(ctx context.Context, kind string, key int64) error {
	return MarkWork(ctx, w.pool, kind, key)
}

func (w workStore) MarkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return MarkWorkAt(ctx, w.pool, kind, key, at)
}

func (w workStore) Peek(ctx context.Context, kind string, key int64) (work.Peeked, error) {
	return PeekWork(ctx, w.pool, kind, key)
}

func (w workStore) Take(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (work.Taken, error) {
	return TakeWork(ctx, w.pool, kind, key, holder, ttl)
}

func (w workStore) Renew(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return RenewWork(ctx, w.pool, kind, key, holder, ttl)
}

func (w workStore) Release(ctx context.Context, kind string, key int64, holder string) (bool, error) {
	return ReleaseWork(ctx, w.pool, kind, key, holder)
}

func (w workStore) Let(ctx context.Context, kind string, key int64, holder string) error {
	return LetWork(ctx, w.pool, kind, key, holder)
}

func (w workStore) Marked(ctx context.Context, kind string, after int64, limit int) ([]work.Mark, error) {
	return MarkedWork(ctx, w.pool, kind, after, limit)
}

func (w workStore) Jobs() work.Jobs { return work.JobsOf(w.pool) }

// Database is the pool the store's work is in (work.RegisterStore).
func (w workStore) Database() any { return w.pool }
