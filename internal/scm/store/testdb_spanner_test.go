package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/data/spannerdata/spannertest"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	scmspanner "github.com/Cidan/ok/internal/scm/store/spannerstore"
)

// newSpannerDB is a new Spanner database for this test alone (spannertest), with the SCM's schema.
func newSpannerDB(t *testing.T) testDB {
	t.Helper()
	admin, client := spannertest.Database(t, scmspanner.Migrations)
	return spannerDB{admin: admin, c: client}
}

// spannerDB is a test's Spanner database.
type spannerDB struct {
	admin *database.DatabaseAdminClient
	c     *spanner.Client
}

func (d spannerDB) open(t testing.TB) backend.Backend {
	t.Helper()
	b, err := scmspanner.Open(context.Background(), d.admin, d.c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (d spannerDB) exec(t testing.TB, sql string, args map[string]any) int64 {
	t.Helper()
	var n int64
	if _, err := d.c.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = spannerdata.Update(ctx, tx, sql, args)
		return err
	}); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func (d spannerDB) scan(t testing.TB, sql string, args map[string]any, dest ...any) {
	t.Helper()
	ok, err := spannerdata.First(context.Background(), d.c.Single(), sql, args, dest...)
	if err == nil && !ok {
		err = fmt.Errorf("no row")
	}
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func (d spannerDB) count(t testing.TB, table, where string, args map[string]any) int {
	t.Helper()
	var n int64
	d.scan(t, `SELECT COUNT(*) FROM `+table+` WHERE `+where, args, &n)
	return int(n)
}

func (d spannerDB) strings(t testing.TB, sql string, args map[string]any) []string {
	t.Helper()
	var out []string
	if err := spannerdata.Each(context.Background(), d.c.Single(), sql, args, func(row *spanner.Row) error {
		var s string
		if err := row.Columns(&s); err != nil {
			return err
		}
		out = append(out, s)
		return nil
	}); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

func (d spannerDB) today(t testing.TB) time.Time {
	t.Helper()
	var day civil.Date
	d.scan(t, `SELECT CURRENT_DATE('UTC')`, nil, &day)
	return day.In(time.UTC)
}

func (d spannerDB) touched(t testing.TB, table string, repo int64, id encoding.ID) time.Time {
	t.Helper()
	var day civil.Date
	d.scan(t, `SELECT touched_at FROM `+table+` WHERE repository_id = @repo AND id = @id`, map[string]any{"repo": repo, "id": id[:]}, &day)
	return day.In(time.UTC)
}

func (d spannerDB) ageRow(t testing.TB, table string, repo int64, id encoding.ID, days int) {
	t.Helper()
	d.exec(t, `UPDATE `+table+` SET touched_at = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL @days DAY) WHERE repository_id = @repo AND id = @id`,
		map[string]any{"repo": repo, "id": id[:], "days": int64(days)})
}

func (d spannerDB) ageTail(t testing.TB, repo int64, days int) {
	t.Helper()
	for _, tbl := range []string{"commit", "object"} {
		d.exec(t, `UPDATE `+tbl+` SET touched_at = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL @days DAY) WHERE repository_id = @repo AND NOT pinned`,
			map[string]any{"repo": repo, "days": int64(days)})
	}
	d.ageOps(t, repo, days)
}

func (d spannerDB) ageOps(t testing.TB, repo int64, days int) {
	t.Helper()
	for _, tbl := range []string{"operation", "view"} {
		d.exec(t, `UPDATE `+tbl+` SET touched_at = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
			WHERE op_log_id IN (SELECT op_log_id FROM op_log WHERE repository_id = @repo)`,
			map[string]any{"repo": repo, "hours": int64(days) * 24})
	}
}

func (d spannerDB) opRows(t testing.TB, repo int64, table string) int {
	t.Helper()
	var n int64
	d.scan(t, `SELECT COUNT(*) FROM `+table+` x JOIN op_log l ON l.op_log_id = x.op_log_id WHERE l.repository_id = @repo`,
		map[string]any{"repo": repo}, &n)
	return int(n)
}

func (spannerDB) numberDictionariesFrom(t testing.TB, id int64) {
	if id != 1 {
		t.Fatalf("a network's first dictionary on Spanner is its counter's first, 1, not %d", id)
	}
}

func (d spannerDB) unusedViews(t testing.TB, repo int64) int {
	t.Helper()
	var n int64
	d.scan(t, `SELECT COUNT(*) FROM view v JOIN op_log l ON l.op_log_id = v.op_log_id
		WHERE l.repository_id = @repo AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = v.op_log_id AND o.view_id = v.id)`,
		map[string]any{"repo": repo}, &n)
	return int(n)
}

func (d spannerDB) opHeadsOf(t testing.TB, repo int64, principals ...string) int {
	t.Helper()
	var n int64
	d.scan(t, `SELECT COUNT(*) FROM op_head h JOIN op_log l ON l.op_log_id = h.op_log_id
		WHERE l.repository_id = @repo AND l.principal IN UNNEST(@principals)`, map[string]any{"repo": repo, "principals": principals}, &n)
	return int(n)
}

func (d spannerDB) packs(t testing.TB, net int64) (live, retired int) {
	t.Helper()
	var l, r int64
	d.scan(t, `SELECT COUNTIF(retired_at IS NULL), COUNTIF(retired_at IS NOT NULL) FROM pack WHERE network_id = @net`,
		map[string]any{"net": net}, &l, &r)
	return int(l), int(r)
}

func (d spannerDB) networks(t testing.TB, net int64) int {
	t.Helper()
	return d.count(t, "network", `network_id = @net`, map[string]any{"net": net})
}

func (d spannerDB) setHead(t testing.TB, repo int64, head encoding.ID) {
	t.Helper()
	d.exec(t, `UPDATE repository SET head = @head WHERE repository_id = @repo`, map[string]any{"repo": repo, "head": head[:]})
}

func (d spannerDB) plantSharedLog(t testing.TB, repo int64, principal string) int64 {
	t.Helper()
	id := data.NewID()
	d.exec(t, `INSERT INTO op_log (op_log_id, repository_id, principal, workspace, created_at) VALUES (@id, @repo, @principal, '', CURRENT_TIMESTAMP())`,
		map[string]any{"id": id, "repo": repo, "principal": principal})
	d.exec(t, `INSERT INTO op_head (op_log_id, op_id) VALUES (@log, @root)`, map[string]any{"log": id, "root": encoding.RootOperationID[:]})
	return id
}

func (d spannerDB) plantWorkspace(t testing.TB, repo int64, principal, name string, withCheckpoint bool) {
	t.Helper()
	p := map[string]any{"repo": repo, "principal": principal, "name": name}
	d.exec(t, `INSERT INTO workspace (repository_id, principal, name, epoch, created_at) VALUES (@repo, @principal, @name, 0, CURRENT_TIMESTAMP())`, p)
	if withCheckpoint {
		p["id"], p["trees"], p["base"] = data.NewID(), [][]byte{encoding.EmptyTreeID[:]}, encoding.RootCommitID[:]
		d.exec(t, `INSERT INTO checkpoint (repository_id, id, principal, workspace, machine, tree_ids, base_commit_id, fork, created_at)
			VALUES (@repo, @id, @principal, @name, 'm1', @trees, @base, FALSE, CURRENT_TIMESTAMP())`, p)
	}
}

func (d spannerDB) setCheckpointCreated(t testing.TB, id int64, at time.Time) {
	t.Helper()
	d.exec(t, `UPDATE checkpoint SET created_at = @at WHERE id = @id`, map[string]any{"id": id, "at": at})
}

// tableBytes estimates a row's bytes from its columns' types — Spanner reports no row's size — so a
// table holds none exactly when it holds no rows of the repository's.
func (d spannerDB) tableBytes(t testing.TB, repo, net int64) map[string]int64 {
	t.Helper()
	ctx := context.Background()
	type column struct{ name, typ string }
	tables := map[string][]column{}
	if err := spannerdata.Each(ctx, d.c.Single(), `SELECT table_name, column_name, spanner_type FROM information_schema.columns
		WHERE table_schema = '' ORDER BY table_name, ordinal_position`, nil, func(row *spanner.Row) error {
		var table string
		var c column
		if err := row.Columns(&table, &c.name, &c.typ); err != nil {
			return err
		}
		tables[table] = append(tables[table], c)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for table, cols := range tables {
		has := func(name string) bool {
			return slices.ContainsFunc(cols, func(c column) bool { return c.name == name })
		}
		var where string
		switch {
		case table == "repository" || table == "network":
			continue
		case has("repository_id"):
			where = `repository_id = @repo`
		case has("owner_id"):
			where = `(scope = 0 AND owner_id = @repo) OR (scope = 1 AND owner_id = @net)`
		case has("network_id"):
			where = `network_id = @net`
		default:
			continue
		}
		sizes := make([]string, len(cols))
		for i, c := range cols {
			switch {
			case strings.HasPrefix(c.typ, "ARRAY<"):
				sizes[i] = fmt.Sprintf("8 * IFNULL(ARRAY_LENGTH(%s), 0)", c.name)
			case strings.HasPrefix(c.typ, "STRING") || strings.HasPrefix(c.typ, "BYTES"):
				sizes[i] = fmt.Sprintf("IFNULL(BYTE_LENGTH(%s), 0)", c.name)
			case c.typ == "BOOL":
				sizes[i] = "1"
			case c.typ == "DATE":
				sizes[i] = "4"
			default:
				sizes[i] = "8"
			}
		}
		var n int64
		d.scan(t, fmt.Sprintf(`SELECT IFNULL(SUM(%s), 0) FROM %s WHERE %s`, strings.Join(sizes, " + "), table, where),
			map[string]any{"repo": repo, "net": net}, &n)
		if n > 0 {
			out[table] = n
		}
	}
	return out
}
