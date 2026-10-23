package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func (x queries) RunIDsOf(ctx context.Context, repo, after int64, limit int) ([]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT id FROM run WHERE repository_id = $1 AND id > $2 ORDER BY id LIMIT $3`, repo, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func (x queries) DeleteRepository(ctx context.Context, repo int64) error {
	// In the order the references allow: main's commits name submissions, attempts' candidates
	// and runs name plans; the rest cascades.
	for _, q := range []string{
		`DELETE FROM main_commit WHERE repository_id = $1`,
		`DELETE FROM landed_change WHERE repository_id = $1`,
		`DELETE FROM submission WHERE repository_id = $1`,
		`DELETE FROM account_slot WHERE repository_id = $1`,
		`DELETE FROM run WHERE repository_id = $1`,
		`DELETE FROM plan WHERE repository_id = $1`,
		`DELETE FROM locks WHERE bucket = 'plans' AND id LIKE ($1::bigint)::text || '/%'`, // its CLs' (LockCL)
		`DELETE FROM locks WHERE bucket = 'main' AND id = ($1::bigint)::text`,             // its main's (LockMain)
	} {
		if _, err := x.q.Exec(ctx, q, repo); err != nil {
			return err
		}
	}
	return nil
}
