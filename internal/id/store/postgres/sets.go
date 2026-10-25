package postgres

import (
	"context"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
)

// ─── Sets, by their members ───

// notTreeDeclared and notAliased hold a synced group (or an alias, for the first) to the
// directory's rules: a name the applied tree declares is the tree's set, and a synced group an
// alias names is the alias, while its team stands.
const (
	notTreeDeclared = `NOT EXISTS (SELECT 1 FROM tree_sets ts WHERE ts.name = %s)`
	notAliased      = `NOT EXISTS (SELECT 1 FROM name_aliases na JOIN teams nt ON nt.name = na.new_name WHERE na.old_name = g.name)`
)

// setsHolding is one statement: each branch a source of sets, those holding names and IDs read
// through the members' GIN indexes (&&).
var setsHolding = `
	SELECT name AS set_name, 1 AS source,
		ARRAY(SELECT m FROM unnest(members) AS m WHERE m = ANY($1::text[]) ORDER BY m) AS holds
	FROM tree_sets WHERE members && $1::text[]
	UNION ALL
	SELECT g.name, 2, ARRAY(SELECT m FROM unnest(g.members) AS m WHERE m = ANY($1::text[]) ORDER BY m)
	FROM synced_groups g
	WHERE g.members && $1::text[] AND NOT g.deleted AND ` + fmt.Sprintf(notTreeDeclared, "g.name") + ` AND ` + notAliased + `
	UNION ALL
	SELECT t.name, 3, ARRAY(SELECT m FROM unnest(t.members) AS m WHERE m = ANY($2::text[]) ORDER BY m)
	FROM teams t WHERE t.members && $2::text[]
	UNION ALL
	SELECT a.old_name, 4, ARRAY(SELECT m FROM unnest(t.members) AS m WHERE m = ANY($2::text[]) ORDER BY m)
	FROM name_aliases a JOIN teams t ON t.name = a.new_name
	WHERE t.members && $2::text[] AND ` + fmt.Sprintf(notTreeDeclared, "a.old_name") + `
	UNION ALL
	SELECT c.parent, 3, ARRAY[c.name]
	FROM teams c WHERE c.name = ANY($3::text[]) AND c.parent IS NOT NULL
	UNION ALL
	SELECT a.old_name, 4, ARRAY[c.name]
	FROM name_aliases a JOIN teams c ON c.parent = a.new_name
	WHERE c.name = ANY($3::text[]) AND ` + fmt.Sprintf(notTreeDeclared, "a.old_name") + `
	ORDER BY 1, 2`

func (x queries) SetsHolding(ctx context.Context, names, ids, teams []string) ([]store.Holding, error) {
	rows, err := x.q.Query(ctx, setsHolding, nonNilStrings(names), nonNilStrings(ids), nonNilStrings(teams))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Holding
	for rows.Next() {
		var h store.Holding
		var source int
		if err := rows.Scan(&h.Set, &source, &h.Holds); err != nil {
			return nil, err
		}
		h.Source = store.SetSource(source)
		// A set two branches find holding — a parent holding a child team, and a team holding a
		// principal — is one holding.
		if n := len(out); n > 0 && out[n-1].Set == h.Set && out[n-1].Source == h.Source {
			out[n-1].Holds = sortedUnion(out[n-1].Holds, h.Holds)
			continue
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// setRecords reads the named sets of each source; the tree's branch, and the tree's precedence,
// only when the applied tree counts.
func setRecords(tree bool) string {
	sql := ``
	shadow := func(string) string { return `` }
	if tree {
		sql = `SELECT 1 AS source, name, kind, path, owner, members, ARRAY[]::text[] AS children, '' AS team, '' AS org
			FROM tree_sets WHERE name = ANY($1::text[])
			UNION ALL `
		shadow = func(col string) string { return ` AND ` + fmt.Sprintf(notTreeDeclared, col) }
	}
	return sql + `
		SELECT 2, g.name, '', '', '', g.members, ARRAY[]::text[], '', ''
		FROM synced_groups g WHERE g.name = ANY($1::text[]) AND NOT g.deleted` + shadow("g.name") + ` AND ` + notAliased + `
		UNION ALL
		SELECT 3, t.name, '', '', '', t.members, ARRAY(SELECT c.name FROM teams c WHERE c.parent = t.name ORDER BY c.name), '',
			t.organization_id
		FROM teams t WHERE t.name = ANY($1::text[])
		UNION ALL
		SELECT 4, a.old_name, '', '', '', t.members, ARRAY(SELECT c.name FROM teams c WHERE c.parent = t.name ORDER BY c.name), t.name,
			t.organization_id
		FROM name_aliases a JOIN teams t ON t.name = a.new_name WHERE a.old_name = ANY($1::text[])` + shadow("a.old_name")
}

func (x queries) SetRecords(ctx context.Context, names []string, tree bool) ([]store.SetRecord, error) {
	rows, err := x.q.Query(ctx, setRecords(tree), nonNilStrings(names))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (store.SetRecord, error) {
		var s store.SetRecord
		var source int
		err := r.Scan(&source, &s.Name, &s.Kind, &s.Path, &s.Owner, &s.Members, &s.Children, &s.Team, &s.Organization)
		s.Source, s.Members, s.Children = store.SetSource(source), nonNilStrings(s.Members), nonNilStrings(s.Children)
		return s, err
	})
}

func (x queries) PrincipalsByID(ctx context.Context, ids []string) ([]store.Principal, error) {
	return collectPrincipals(x.q.Query(ctx, `SELECT `+principalColumns+` FROM principals WHERE id = ANY($1::text[])`, nonNilStrings(ids)))
}

// sortedUnion is a and b, each sorted, merged without repeats.
func sortedUnion(a, b []string) []string {
	out := slices.Concat(a, b)
	slices.Sort(out)
	return slices.Compact(out)
}
