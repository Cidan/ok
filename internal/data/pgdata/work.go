package pgdata

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// WorkSchema makes the table a role keeps its keyed work in (internal/work): a row per kind and
// key, marked dirty when what the work acts on changes, and leased to one holder at a time. A role
// that does keyed work includes it in its migrations.
const WorkSchema = `CREATE TABLE work (
    kind        text        NOT NULL,
    key         bigint      NOT NULL,
    dirty       boolean     NOT NULL DEFAULT false,
    holder      text,
    lease_until timestamptz,
    PRIMARY KEY (kind, key)
);
CREATE INDEX work_due ON work (kind, key) WHERE dirty OR holder IS NOT NULL;`

// MarkWork marks key's work of kind dirty.
func MarkWork(ctx context.Context, q Querier, kind string, key int64) error {
	_, err := q.Exec(ctx, `INSERT INTO work (kind, key, dirty) VALUES ($1, $2, true)
		ON CONFLICT (kind, key) DO UPDATE SET dirty = true`, kind, key)
	return err
}

// TakeWork leases key's work to holder for ttl, clearing its mark, if no one holds it or its
// holder's lease lapsed; it reports whether it did.
func TakeWork(ctx context.Context, q Querier, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE work SET holder = $3, lease_until = now() + make_interval(secs => $4), dirty = false
		WHERE kind = $1 AND key = $2 AND (holder IS NULL OR lease_until < now())`, kind, key, holder, ttl.Seconds())
	return tag.RowsAffected() > 0, err
}

// RenewWork extends holder's lease on key's work by ttl from now; it reports false if the lease is
// another's.
func RenewWork(ctx context.Context, q Querier, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE work SET lease_until = now() + make_interval(secs => $4)
		WHERE kind = $1 AND key = $2 AND holder = $3`, kind, key, holder, ttl.Seconds())
	return tag.RowsAffected() > 0, err
}

// ReleaseWork ends holder's lease on key's work — unless the work was marked meanwhile, when it
// clears the mark and keeps the lease, reporting again: the holder does the work again. Neither
// happens if the lease is another's.
func ReleaseWork(ctx context.Context, q Querier, kind string, key int64, holder string) (again bool, err error) {
	err = q.QueryRow(ctx, `UPDATE work SET
			holder = CASE WHEN dirty THEN holder END,
			lease_until = CASE WHEN dirty THEN lease_until END,
			dirty = false
		WHERE kind = $1 AND key = $2 AND holder = $3
		RETURNING holder IS NOT NULL`, kind, key, holder).Scan(&again)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return again, err
}

// DueWork are up to limit keys of kind's work that is marked and held by no one, or whose
// holder's lease lapsed, by key.
func DueWork(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, kind string, limit int) ([]int64, error) {
	rows, err := q.Query(ctx, `SELECT key FROM work WHERE kind = $1 AND (dirty OR holder IS NOT NULL)
		AND ((dirty AND holder IS NULL) OR lease_until < now()) ORDER BY key LIMIT $2`, kind, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}
