package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// tokenColumns are a token's, its installation's organization with them (t: tokens, i: its
// installation).
const tokenColumns = `t.id, t.principal_id, t.kind, t.label, t.role, t.repositories, t.created_at, t.expires_at, t.last_used_at,
	t.revoked_at, COALESCE(t.installation_id, ''), COALESCE(i.organization_id, '')`

const tokenFrom = `tokens t LEFT JOIN app_installations i ON i.id = t.installation_id`

func scanToken(row pgx.Row) (store.Token, error) {
	var t store.Token
	var role string
	var lastUsed, revoked *time.Time
	err := row.Scan(&t.ID, &t.Principal, &t.Kind, &t.Label, &role, &t.Repositories, &t.Created, &t.Expires, &lastUsed, &revoked,
		&t.Installation, &t.Owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Token{}, store.ErrNotFound
	}
	if err != nil {
		return store.Token{}, err
	}
	if t.Role, err = tenancy.ParseRole(role); err != nil {
		return store.Token{}, err
	}
	if lastUsed != nil {
		t.LastUsed = *lastUsed
	}
	if revoked != nil {
		t.Revoked = *revoked
	}
	return t, nil
}

func (x queries) InsertToken(ctx context.Context, t store.Token, hash []byte) error {
	repos := t.Repositories
	if repos == nil {
		repos = []int64{}
	}
	var installation *string
	if t.Installation != "" {
		installation = &t.Installation
	}
	_, err := x.q.Exec(ctx, `INSERT INTO tokens (id, token_hash, principal_id, kind, label, role, repositories, created_at, expires_at, installation_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`, t.ID, hash, t.Principal, t.Kind, t.Label, t.Role.String(), repos, t.Created, t.Expires,
		installation)
	return err
}

func (x queries) TokenByHash(ctx context.Context, hash []byte) (store.Token, error) {
	return scanToken(x.q.QueryRow(ctx, `SELECT `+tokenColumns+` FROM `+tokenFrom+` WHERE t.token_hash = $1`, hash))
}

func (x queries) TouchToken(ctx context.Context, id string, at time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE tokens SET last_used_at = $2 WHERE id = $1`, id, at)
	return err
}

func (x queries) Tokens(ctx context.Context, principal string) ([]store.Token, error) {
	rows, err := x.q.Query(ctx, `SELECT `+tokenColumns+` FROM `+tokenFrom+` WHERE t.principal_id = $1 ORDER BY t.created_at DESC, t.id`, principal)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Token, error) { return scanToken(row) })
}

func (x queries) RevokeToken(ctx context.Context, principal, id string, at time.Time) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE tokens SET revoked_at = $3 WHERE id = $2 AND principal_id = $1 AND revoked_at IS NULL`, principal, id, at)
	return tag.RowsAffected() == 1, err
}
