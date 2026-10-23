package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
)

func scanDeletion(row pgx.Row) (store.Deletion, error) {
	var d store.Deletion
	err := row.Scan(&d.Principal, &d.Requested, &d.By)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Deletion{}, store.ErrNotFound
	}
	return d, err
}

func (x queries) InsertDeletion(ctx context.Context, d store.Deletion) error {
	_, err := x.q.Exec(ctx, `INSERT INTO deletions (principal_id, requested_at, requested_by) VALUES ($1, $2, $3)`, d.Principal, d.Requested, d.By)
	if unique(err) {
		return store.ErrTaken
	}
	return err
}

func (x queries) DeletionOf(ctx context.Context, principal string) (store.Deletion, error) {
	return scanDeletion(x.q.QueryRow(ctx, `SELECT principal_id, requested_at, requested_by FROM deletions WHERE principal_id = $1`, principal))
}

func (x queries) RemoveDeletion(ctx context.Context, principal string) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM deletions WHERE principal_id = $1`, principal)
	return tag.RowsAffected() == 1, err
}

func (x queries) DueDeletions(ctx context.Context, before time.Time, limit int) ([]store.Deletion, error) {
	rows, err := x.q.Query(ctx, `SELECT principal_id, requested_at, requested_by FROM deletions WHERE requested_at < $1
		ORDER BY requested_at LIMIT $2 FOR UPDATE SKIP LOCKED`, before, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Deletion, error) { return scanDeletion(row) })
}

func (x queries) RemoveFromTeams(ctx context.Context, principal string) error {
	_, err := x.q.Exec(ctx, `UPDATE teams SET members = array_remove(members, $1) WHERE $1 = ANY(members)`, principal)
	return err
}

func (x queries) DeleteMembersOf(ctx context.Context, org string) error {
	_, err := x.q.Exec(ctx, `DELETE FROM memberships WHERE organization_id = $1`, org)
	return err
}

func (x queries) DeleteMembershipsOf(ctx context.Context, principal string) error {
	_, err := x.q.Exec(ctx, `DELETE FROM memberships WHERE principal_id = $1`, principal)
	return err
}

func (x queries) RevokeTokensOf(ctx context.Context, principal string, at time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE tokens SET revoked_at = $2 WHERE principal_id = $1 AND revoked_at IS NULL`, principal, at)
	return err
}

func (x queries) DeleteSignInsOf(ctx context.Context, principal string) error {
	for _, q := range []string{`DELETE FROM passwords WHERE principal_id = $1`, `DELETE FROM login_links WHERE principal_id = $1`,
		`DELETE FROM passkeys WHERE principal_id = $1`, `DELETE FROM email_tokens WHERE principal_id = $1`} {
		if _, err := x.q.Exec(ctx, q, principal); err != nil {
			return err
		}
	}
	return nil
}
