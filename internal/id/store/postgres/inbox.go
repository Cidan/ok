package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) InboxVersion(ctx context.Context, repo, number int64) (int64, error) {
	var v int64
	err := x.q.QueryRow(ctx, `SELECT version FROM inbox_version WHERE repository_id = $1 AND number = $2 FOR UPDATE`, repo, number).Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

func (x queries) ReplaceInbox(ctx context.Context, repo, number, version int64, entries []store.InboxEntry) error {
	if _, err := x.q.Exec(ctx, `INSERT INTO inbox_version (repository_id, number, version) VALUES ($1, $2, $3)
		ON CONFLICT (repository_id, number) DO UPDATE SET version = $3`, repo, number, version); err != nil {
		return err
	}
	if _, err := x.q.Exec(ctx, `DELETE FROM inbox WHERE repository_id = $1 AND number = $2`, repo, number); err != nil {
		return err
	}
	for _, e := range entries {
		if _, err := x.q.Exec(ctx, `INSERT INTO inbox (principal_id, repository_id, number, relation, attention, repository, title, status, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (principal_id, repository_id, number, relation) DO NOTHING`, e.Principal, repo, number, int16(e.Relation), e.Attention,
			e.RepositoryName, e.Title, e.Status, e.UpdatedAt); err != nil {
			return err
		}
	}
	return nil
}

func (x queries) InboxOf(ctx context.Context, principal string, relation store.InboxRelation, closed bool, limit int) ([]store.InboxEntry, error) {
	rows, err := x.q.Query(ctx, `SELECT principal_id, repository_id, number, relation, attention, repository, title, status, updated_at
		FROM inbox WHERE principal_id = $1 AND ($2 = 0 OR relation = $2) AND ($3 OR status IN ('draft', 'open'))
		ORDER BY attention DESC, updated_at DESC, repository_id, number, relation LIMIT $4`, principal, int16(relation), closed, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.InboxEntry, error) {
		var e store.InboxEntry
		var rel int16
		err := row.Scan(&e.Principal, &e.Repository, &e.Number, &rel, &e.Attention, &e.RepositoryName, &e.Title, &e.Status, &e.UpdatedAt)
		e.Relation = store.InboxRelation(rel)
		return e, err
	})
}
