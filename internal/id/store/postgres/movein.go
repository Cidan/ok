package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) TenancyState(ctx context.Context) (string, string, error) {
	var mode string
	var movedIn *string
	err := x.q.QueryRow(ctx, `SELECT mode, moved_in FROM tenancy_state`).Scan(&mode, &movedIn)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil
	}
	if movedIn == nil {
		return mode, "", err
	}
	return mode, *movedIn, err
}

func (x queries) PutTenancyState(ctx context.Context, mode, movedIn string, at time.Time) error {
	var m *string
	var when *time.Time
	if movedIn != "" {
		m, when = &movedIn, &at
	}
	_, err := x.q.Exec(ctx, `INSERT INTO tenancy_state (singleton, mode, moved_in, moved_at) VALUES (true, $1, $2, $3)
		ON CONFLICT (singleton) DO UPDATE SET mode = EXCLUDED.mode, moved_in = COALESCE(EXCLUDED.moved_in, tenancy_state.moved_in),
			moved_at = COALESCE(EXCLUDED.moved_at, tenancy_state.moved_at)`, mode, m, when)
	return err
}

func (x queries) InsertAlias(ctx context.Context, old, new string) error {
	_, err := x.q.Exec(ctx, `INSERT INTO name_aliases (old_name, new_name) VALUES ($1, $2)
		ON CONFLICT (old_name) DO UPDATE SET new_name = EXCLUDED.new_name`, old, new)
	return err
}

func (x queries) Alias(ctx context.Context, old string) (string, error) {
	var n string
	err := x.q.QueryRow(ctx, `SELECT new_name FROM name_aliases WHERE old_name = $1`, old).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrNotFound
	}
	return n, err
}

func (x queries) Aliases(ctx context.Context) (map[string]string, error) {
	rows, err := x.q.Query(ctx, `SELECT old_name, new_name FROM name_aliases`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var o, n string
		if err := rows.Scan(&o, &n); err != nil {
			return nil, err
		}
		out[o] = n
	}
	return out, rows.Err()
}

func (x queries) RenameGrantSubject(ctx context.Context, old, new string) ([]int64, error) {
	rows, err := x.q.Query(ctx, `UPDATE grants SET subject = $2 WHERE subject = $1 RETURNING repository_id`, old, new)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func (x queries) DeleteSyncedGroup(ctx context.Context, name string) error {
	_, err := x.q.Exec(ctx, `DELETE FROM synced_groups WHERE name = $1`, name)
	return err
}

func (x queries) ClearSyncState(ctx context.Context) error {
	_, err := x.q.Exec(ctx, `DELETE FROM sync_state`)
	return err
}

func (x queries) MoveSCIM(ctx context.Context, org string) error {
	for _, q := range []string{`UPDATE scim_users SET scope = $1 WHERE scope = ''`, `UPDATE scim_groups SET scope = $1 WHERE scope = ''`} {
		if _, err := x.q.Exec(ctx, q, org); err != nil {
			return err
		}
	}
	return nil
}
