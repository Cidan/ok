package spannerstore

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) InboxVersion(ctx context.Context, repo, number int64) (int64, error) {
	var v int64
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		row, err := tx.ReadRow(ctx, "inbox_version", spanner.Key{repo, number}, []string{"version"})
		if spanner.ErrCode(err) == codes.NotFound {
			v = 0
			return nil
		}
		if err != nil {
			return err
		}
		return row.Columns(&v)
	})
	return v, err
}

func (x queries) ReplaceInbox(ctx context.Context, repo, number, version int64, entries []store.InboxEntry) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `DELETE FROM inbox WHERE repository_id = @repo AND number = @number`,
			map[string]any{"repo": repo, "number": number}); err != nil {
			return err
		}
		ms := []*spanner.Mutation{spanner.InsertOrUpdate("inbox_version", []string{"repository_id", "number", "version"},
			[]any{repo, number, version})}
		for _, e := range entries {
			ms = append(ms, spanner.InsertOrUpdate("inbox", []string{"principal_id", "repository_id", "number", "relation", "attention", "repository",
				"title", "status", "updated_at"}, []any{e.Principal, repo, number, int64(e.Relation), e.Attention, e.RepositoryName, e.Title, e.Status,
				e.UpdatedAt}))
		}
		return tx.BufferWrite(ms)
	})
}

func (x queries) InboxOf(ctx context.Context, principal string, relation store.InboxRelation, closed bool, limit int) ([]store.InboxEntry, error) {
	var out []store.InboxEntry
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT principal_id, repository_id, number, relation, attention, repository, title, status, updated_at
			FROM inbox WHERE principal_id = @principal AND (@relation = 0 OR relation = @relation) AND (@closed OR status IN ('draft', 'open'))
			ORDER BY attention DESC, updated_at DESC, repository_id, number, relation LIMIT @limit`,
			map[string]any{"principal": principal, "relation": int64(relation), "closed": closed, "limit": int64(limit)}, func(row *spanner.Row) error {
				var e store.InboxEntry
				var rel int64
				if err := row.Columns(&e.Principal, &e.Repository, &e.Number, &rel, &e.Attention, &e.RepositoryName, &e.Title, &e.Status,
					&e.UpdatedAt); err != nil {
					return err
				}
				e.Relation = store.InboxRelation(rel)
				out = append(out, e)
				return nil
			})
	})
	return out, err
}
