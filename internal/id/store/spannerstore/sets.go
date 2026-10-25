package spannerstore

import (
	"context"
	"fmt"
	"slices"

	"cloud.google.com/go/spanner"

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
// through the members' search indexes (ARRAY_INCLUDES_ANY; `x IN UNNEST(members)` would scan).
// Each names its index: a search index holds the tokens and the key alone, and the planner, asked
// for another column (what a set holds, whether a group is deleted, a synced group's name), scans
// the table rather than join back from the index — 3,000 rows for 3 sets, against 11. A search
// index named so needs the query optimizer's version 6 or above, which Spanner Omni does not choose
// for a statement of this shape (4): the statement asks for the latest.
var setsHolding = `@{OPTIMIZER_VERSION=latest_version}
	SELECT name AS set_name, 1 AS source,
		ARRAY(SELECT m FROM UNNEST(members) AS m WHERE m IN UNNEST(@names) ORDER BY m) AS holds
	FROM tree_sets@{FORCE_INDEX=tree_sets_members} WHERE ARRAY_INCLUDES_ANY(members, @names)
	UNION ALL
	SELECT g.name, 2, ARRAY(SELECT m FROM UNNEST(g.members) AS m WHERE m IN UNNEST(@names) ORDER BY m)
	FROM synced_groups@{FORCE_INDEX=synced_groups_members} g
	WHERE ARRAY_INCLUDES_ANY(g.members, @names) AND NOT g.deleted AND ` + fmt.Sprintf(notTreeDeclared, "g.name") + ` AND ` + notAliased + `
	UNION ALL
	SELECT t.name, 3, ARRAY(SELECT m FROM UNNEST(t.members) AS m WHERE m IN UNNEST(@ids) ORDER BY m)
	FROM teams@{FORCE_INDEX=teams_members} t WHERE ARRAY_INCLUDES_ANY(t.members, @ids)
	UNION ALL
	SELECT a.old_name, 4, ARRAY(SELECT m FROM UNNEST(t.members) AS m WHERE m IN UNNEST(@ids) ORDER BY m)
	FROM teams@{FORCE_INDEX=teams_members} t JOIN name_aliases a ON a.new_name = t.name
	WHERE ARRAY_INCLUDES_ANY(t.members, @ids) AND ` + fmt.Sprintf(notTreeDeclared, "a.old_name") + `
	UNION ALL
	SELECT c.parent, 3, [c.name]
	FROM teams c WHERE c.name IN UNNEST(@teams) AND c.parent IS NOT NULL
	UNION ALL
	SELECT a.old_name, 4, [c.name]
	FROM name_aliases a JOIN teams c ON c.parent = a.new_name
	WHERE c.name IN UNNEST(@teams) AND ` + fmt.Sprintf(notTreeDeclared, "a.old_name") + `
	ORDER BY set_name, source`

func (x queries) SetsHolding(ctx context.Context, names, ids, teams []string) ([]store.Holding, error) {
	var out []store.Holding
	err := x.readAll(ctx, setsHolding, map[string]any{"names": nonNil(names), "ids": nonNil(ids), "teams": nonNil(teams)},
		func() { out = nil }, func(row *spanner.Row) error {
			var h store.Holding
			var source int64
			if err := row.Columns(&h.Set, &source, &h.Holds); err != nil {
				return err
			}
			h.Source = store.SetSource(source)
			// A set two branches find holding — a parent holding a child team, and a team holding a
			// principal — is one holding.
			if n := len(out); n > 0 && out[n-1].Set == h.Set && out[n-1].Source == h.Source {
				out[n-1].Holds = sortedUnion(out[n-1].Holds, h.Holds)
				return nil
			}
			out = append(out, h)
			return nil
		})
	return out, err
}

// setRecords reads the named sets of each source; the tree's branch, and the tree's precedence,
// only when the applied tree counts.
func setRecords(tree bool) string {
	sql := ``
	shadow := func(string) string { return `` }
	if tree {
		sql = `SELECT 1 AS source, name, kind, path, owner, members, ARRAY<STRING>[] AS children, '' AS team, '' AS org
			FROM tree_sets WHERE name IN UNNEST(@names)
			UNION ALL `
		shadow = func(col string) string { return ` AND ` + fmt.Sprintf(notTreeDeclared, col) }
	}
	return sql + `
		SELECT 2, g.name, '', '', '', g.members, ARRAY<STRING>[], '', ''
		FROM synced_groups@{FORCE_INDEX=synced_groups_name} g WHERE g.name IN UNNEST(@names) AND NOT g.deleted` + shadow("g.name") + ` AND ` + notAliased + `
		UNION ALL
		SELECT 3, t.name, '', '', '', t.members, ARRAY(SELECT c.name FROM teams c WHERE c.parent = t.name ORDER BY c.name), '',
			t.organization_id
		FROM teams t WHERE t.name IN UNNEST(@names)
		UNION ALL
		SELECT 4, a.old_name, '', '', '', t.members, ARRAY(SELECT c.name FROM teams c WHERE c.parent = t.name ORDER BY c.name), t.name,
			t.organization_id
		FROM name_aliases a JOIN teams t ON t.name = a.new_name WHERE a.old_name IN UNNEST(@names)` + shadow("a.old_name")
}

func (x queries) SetRecords(ctx context.Context, names []string, tree bool) ([]store.SetRecord, error) {
	var out []store.SetRecord
	err := x.readAll(ctx, setRecords(tree), map[string]any{"names": nonNil(names)}, func() { out = nil }, func(row *spanner.Row) error {
		var r store.SetRecord
		var source int64
		if err := row.Columns(&source, &r.Name, &r.Kind, &r.Path, &r.Owner, &r.Members, &r.Children, &r.Team, &r.Organization); err != nil {
			return err
		}
		r.Source = store.SetSource(source)
		r.Members, r.Children = nonNil(r.Members), nonNil(r.Children)
		out = append(out, r)
		return nil
	})
	return out, err
}

// principalsAtOnce bounds the IDs one statement reads.
const principalsAtOnce = 5000

func (x queries) PrincipalsByID(ctx context.Context, ids []string) ([]store.Principal, error) {
	var out []store.Principal
	for chunk := range slices.Chunk(ids, principalsAtOnce) {
		ps, err := x.principals(ctx, `SELECT `+principalColumns+` FROM principals WHERE id IN UNNEST(@ids)`, map[string]any{"ids": chunk})
		if err != nil {
			return nil, err
		}
		out = append(out, ps...)
	}
	return out, nil
}

// sortedUnion is a and b, each sorted, merged without repeats.
func sortedUnion(a, b []string) []string {
	out := slices.Concat(a, b)
	slices.Sort(out)
	return slices.Compact(out)
}
