package spannerstore

import (
	"context"

	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) SearchRepositories(ctx context.Context, owner, name string, limit int) ([]store.Repository, error) {
	where := `TRUE`
	params := map[string]any{"limit": int64(limit), "owner": owner}
	if name != "" {
		where, params["name"] = `SEARCH_SUBSTRING(r.name_tokens, @name)`, name
	}
	return x.repositories(ctx, `SELECT r.repository_id, r.owner_id, r.name, r.visibility, r.propose, IFNULL(r.parent_id, 0), r.network_id, r.cell,
			r.state, r.state_at, r.created_by, r.created_at
		FROM repositories r JOIN principals p ON p.id = r.owner_id
		WHERE `+where+` AND (@owner = '' OR STARTS_WITH(LOWER(REGEXP_EXTRACT(p.name, r'^([^.]*)')), @owner))
		ORDER BY CHAR_LENGTH(r.name), r.name LIMIT @limit`, params)
}

func (x queries) SearchPrincipals(ctx context.Context, q string, limit int) ([]store.Principal, error) {
	return x.principals(ctx, `SELECT `+principalColumns+` FROM principals
		WHERE kind IN ('human', 'org') AND status = 'active'
			AND (SEARCH_SUBSTRING(handle_tokens, @q) OR SEARCH_SUBSTRING(display_tokens, @q))
		ORDER BY CHAR_LENGTH(name), name LIMIT @limit`, map[string]any{"q": q, "limit": int64(limit)})
}
