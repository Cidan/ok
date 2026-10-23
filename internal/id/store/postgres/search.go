package postgres

import (
	"context"

	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) SearchRepositories(ctx context.Context, owner, name string, limit int) ([]store.Repository, error) {
	return x.repositories(ctx, `SELECT r.id, r.owner_id, r.name, r.visibility, r.propose, COALESCE(r.parent_id, 0), r.network_id, r.cell, r.state,
			r.state_at, r.created_by, r.created_at
		FROM repositories r JOIN principals p ON p.id = r.owner_id
		WHERE lower(r.name) LIKE '%' || $1 || '%' AND ($2 = '' OR lower(split_part(p.name, '.', 1)) LIKE $2 || '%')
		ORDER BY length(r.name), r.name LIMIT $3`, store.LikeEscape(name), store.LikeEscape(owner), limit)
}

func (x queries) SearchPrincipals(ctx context.Context, q string, limit int) ([]store.Principal, error) {
	return collectPrincipals(x.q.Query(ctx, `SELECT `+principalColumns+` FROM principals
		WHERE kind IN ('human', 'org') AND status = 'active'
			AND (lower(split_part(name, '.', 1)) LIKE '%' || $1 || '%' OR lower(display_name) LIKE '%' || $1 || '%')
		ORDER BY length(name), name LIMIT $2`, store.LikeEscape(q), limit))
}
