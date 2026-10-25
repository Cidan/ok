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

// MarkWork marks key's work of kind dirty, due now.
func MarkWork(ctx context.Context, q Querier, kind string, key int64) error {
	_, err := q.Exec(ctx, `INSERT INTO work (kind, key, dirty) VALUES ($1, $2, true)
		ON CONFLICT (kind, key) DO UPDATE SET dirty = true, due_at = NULL`, kind, key)
	return err
}

// MarkWorkAt marks key's work of kind dirty, due at at — or sooner, if it is marked so already.
func MarkWorkAt(ctx context.Context, q Querier, kind string, key int64, at time.Time) error {
	_, err := q.Exec(ctx, `INSERT INTO work (kind, key, dirty, due_at) VALUES ($1, $2, true, $3)
		ON CONFLICT (kind, key) DO UPDATE SET dirty = true,
			due_at = CASE WHEN NOT work.dirty THEN EXCLUDED.due_at WHEN work.due_at IS NULL THEN NULL
				ELSE least(work.due_at, EXCLUDED.due_at) END`, kind, key, at)
	return err
}

// MarkWorkIdle marks key's work of kind dirty, due now — making its row if it has none — if it is
// not marked and no one holds it, or its holder's lease lapsed: a sweep's mark, for work that is
// new or whose holder died before marking it again, which leaves work held or marked as it is.
func MarkWorkIdle(ctx context.Context, q Querier, kind string, key int64) error {
	_, err := q.Exec(ctx, `INSERT INTO work (kind, key, dirty) VALUES ($1, $2, true)
		ON CONFLICT (kind, key) DO UPDATE SET dirty = true, due_at = NULL
		WHERE NOT work.dirty AND (work.holder IS NULL OR work.lease_until < now())`, kind, key)
	return err
}

// TakeWork leases key's work to holder for ttl, clearing its mark, if no one holds it or its
// holder's lease lapsed — making its row if it has none; it reports whether it did.
func TakeWork(ctx context.Context, q Querier, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	tag, err := q.Exec(ctx, `INSERT INTO work (kind, key, holder, lease_until) VALUES ($1, $2, $3, now() + make_interval(secs => $4))
		ON CONFLICT (kind, key) DO UPDATE SET holder = EXCLUDED.holder, lease_until = EXCLUDED.lease_until, dirty = false, due_at = NULL
		WHERE work.holder IS NULL OR work.lease_until < now()`, kind, key, holder, ttl.Seconds())
	return tag.RowsAffected() > 0, err
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
// mark due later stays, for whoever takes it then. Nothing happens if the lease is another's.
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
// (work.Claim) held while it was marked is done by whoever takes it next. Nothing happens if the
// lease is another's.
func LetWork(ctx context.Context, q Querier, kind string, key int64, holder string) error {
	_, err := q.Exec(ctx, `UPDATE work SET holder = NULL, lease_until = NULL WHERE kind = $1 AND key = $2 AND holder = $3`,
		kind, key, holder)
	return err
}

// DueWork are up to limit keys of kind's work that is marked, due, and held by no one, or whose
// holder's lease lapsed, by key.
func DueWork(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, kind string, limit int) ([]int64, error) {
	rows, err := q.Query(ctx, `SELECT key FROM work WHERE kind = $1 AND (dirty OR holder IS NOT NULL)
		AND ((dirty AND holder IS NULL AND (due_at IS NULL OR due_at <= now())) OR lease_until < now())
		ORDER BY key LIMIT $2`, kind, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
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
// statement of its own, as spannerdata.Work is on Spanner.
func Work(pool *pgxpool.Pool) work.Store { return workStore{pool} }

type workStore struct{ pool *pgxpool.Pool }

func (w workStore) Mark(ctx context.Context, kind string, key int64) error {
	return MarkWork(ctx, w.pool, kind, key)
}

func (w workStore) MarkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return MarkWorkAt(ctx, w.pool, kind, key, at)
}

func (w workStore) Take(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
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

func (w workStore) Due(ctx context.Context, kind string, limit int) ([]int64, error) {
	return DueWork(ctx, w.pool, kind, limit)
}
