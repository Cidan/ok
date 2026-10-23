package store

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	scmpostgres "github.com/Cidan/ok/internal/scm/store/postgres"
	"github.com/Cidan/ok/internal/testdb"
)

// testDB is the database under a test's stores: what another replica's backend opens over, and what
// a test reads, ages, and plants rows of directly. OK_TEST_DATABASE picks it: postgres, the
// default, or spanner. exec, scan, count, and strings take the SQL both dialects share, their
// parameters named (@repo); what the dialects write differently has a method of its own.
type testDB interface {
	// open opens a backend over the database, as another replica does.
	open(t testing.TB) backend.Backend
	// exec runs a statement and returns how many rows it changed.
	exec(t testing.TB, sql string, args map[string]any) int64
	// scan runs a query and scans its one row into dest: bools, byte slices, int64s, strings.
	scan(t testing.TB, sql string, args map[string]any, dest ...any)
	// count counts table's rows where where holds.
	count(t testing.TB, table, where string, args map[string]any) int
	// strings are a query's rows of one string each, in order.
	strings(t testing.TB, sql string, args map[string]any) []string
	// today is the day the store's walks touch rows on.
	today(t testing.TB) time.Time
	// touched is the day a repository's commit's or object's row was last touched.
	touched(t testing.TB, table string, repo int64, id encoding.ID) time.Time
	// ageRow makes a repository's commit's or object's row touched days ago, pinned or not.
	ageRow(t testing.TB, table string, repo int64, id encoding.ID, days int)
	// ageTail makes a repository's unpinned commits and objects touched days ago, and its
	// operations and views.
	ageTail(t testing.TB, repo int64, days int)
	// ageOps makes a repository's operations and views touched days ago.
	ageOps(t testing.TB, repo int64, days int)
	// opRows counts a repository's rows of one of its operation logs' tables (operation, view,
	// op_head, op_tombstone); unusedViews its views no operation of their log names.
	opRows(t testing.TB, repo int64, table string) int
	unusedViews(t testing.TB, repo int64) int
	// numberDictionariesFrom has the next dictionary any network trains numbered id, as a network
	// moved in may hold the same as another: PostgreSQL numbers them in one sequence, Spanner by
	// each network's counter, which starts at 1 alike.
	numberDictionariesFrom(t testing.TB, id int64)
	// opHeadsOf counts the heads of a repository's logs of principals.
	opHeadsOf(t testing.TB, repo int64, principals ...string) int
	// packs counts a network's live and retired packs; networks counts its rows of network (0 or 1).
	packs(t testing.TB, net int64) (live, retired int)
	networks(t testing.TB, net int64) int
	// setHead moves a repository's main with no landing.
	setHead(t testing.TB, repo int64, head encoding.ID)
	// plantSharedLog makes an operation log of a principal's of no workspace, as each workspace had
	// none of its own once, with the root operation its head, and returns its ID; plantWorkspace a
	// workspace of theirs with no log, with a checkpoint when withCheckpoint.
	plantSharedLog(t testing.TB, repo int64, principal string) int64
	plantWorkspace(t testing.TB, repo int64, principal, name string, withCheckpoint bool)
	// setCheckpointCreated moves when a checkpoint was made.
	setCheckpointCreated(t testing.TB, id int64, at time.Time)
	// tableBytes is how many bytes a repository's rows take in each table that holds them: by
	// repository, by its scope's or its network's owner, or by its network.
	tableBytes(t testing.TB, repo, net int64) map[string]int64
}

// testDBs are the databases tests' stores are over, by store.
var testDBs sync.Map

// dbOf is the database a test's store is over.
func dbOf(s *Store) testDB {
	d, _ := testDBs.Load(s)
	return d.(testDB)
}

// newTestDB is a new database of OK_TEST_DATABASE's for this test alone, its pool (on PostgreSQL)
// of conns connections failing the test when a goroutine holding one asks for another
// (testdb.NoNestedConns).
func newTestDB(t *testing.T, conns int32) testDB {
	t.Helper()
	switch d := os.Getenv("OK_TEST_DATABASE"); d {
	case "", "postgres":
		cfg, err := pgxpool.ParseConfig(testdb.Postgres(t))
		if err != nil {
			t.Fatal(err)
		}
		cfg.MaxConns = conns
		testdb.NoNestedConns(t, cfg)
		pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		return pgDB{pool}
	case "spanner":
		return newSpannerDB(t)
	default:
		t.Fatalf("OK_TEST_DATABASE: %q is not a database the SCM's tests run on", d)
		return nil
	}
}

// pgDB is a test's PostgreSQL database.
type pgDB struct{ pool *pgxpool.Pool }

func (d pgDB) open(t testing.TB) backend.Backend {
	t.Helper()
	b, err := scmpostgres.Open(context.Background(), d.pool)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (d pgDB) exec(t testing.TB, sql string, args map[string]any) int64 {
	t.Helper()
	tag, err := d.pool.Exec(context.Background(), sql, pgx.NamedArgs(args))
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return tag.RowsAffected()
}

func (d pgDB) scan(t testing.TB, sql string, args map[string]any, dest ...any) {
	t.Helper()
	if err := d.pool.QueryRow(context.Background(), sql, pgx.NamedArgs(args)).Scan(dest...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func (d pgDB) count(t testing.TB, table, where string, args map[string]any) int {
	t.Helper()
	var n int
	d.scan(t, `SELECT count(*) FROM `+table+` WHERE `+where, args, &n)
	return n
}

func (d pgDB) strings(t testing.TB, sql string, args map[string]any) []string {
	t.Helper()
	rows, err := d.pool.Query(context.Background(), sql, pgx.NamedArgs(args))
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

func (d pgDB) today(t testing.TB) time.Time {
	t.Helper()
	var day time.Time
	d.scan(t, `SELECT current_date`, nil, &day)
	return day
}

func (d pgDB) touched(t testing.TB, table string, repo int64, id encoding.ID) time.Time {
	t.Helper()
	var day time.Time
	d.scan(t, `SELECT touched_at FROM `+table+` WHERE repository_id = @repo AND id = @id`, map[string]any{"repo": repo, "id": id[:]}, &day)
	return day
}

func (d pgDB) ageRow(t testing.TB, table string, repo int64, id encoding.ID, days int) {
	t.Helper()
	d.exec(t, `UPDATE `+table+` SET touched_at = current_date - @days::int WHERE repository_id = @repo AND id = @id`,
		map[string]any{"repo": repo, "id": id[:], "days": days})
}

func (d pgDB) ageTail(t testing.TB, repo int64, days int) {
	t.Helper()
	for _, tbl := range []string{"commit", "object"} {
		d.exec(t, `UPDATE `+tbl+` SET touched_at = current_date - @days::int WHERE repository_id = @repo AND NOT pinned`,
			map[string]any{"repo": repo, "days": days})
	}
	d.ageOps(t, repo, days)
}

func (d pgDB) ageOps(t testing.TB, repo int64, days int) {
	t.Helper()
	for _, tbl := range []string{"operation", "view"} {
		d.exec(t, `UPDATE `+tbl+` x SET touched_at = now() - make_interval(days => @days::int)
			FROM op_log l WHERE l.id = x.op_log_id AND l.repository_id = @repo`, map[string]any{"repo": repo, "days": days})
	}
}

func (d pgDB) opRows(t testing.TB, repo int64, table string) int {
	t.Helper()
	var n int
	d.scan(t, `SELECT count(*) FROM `+table+` x JOIN op_log l ON l.id = x.op_log_id WHERE l.repository_id = @repo`,
		map[string]any{"repo": repo}, &n)
	return n
}

func (d pgDB) numberDictionariesFrom(t testing.TB, id int64) {
	t.Helper()
	d.exec(t, fmt.Sprintf(`ALTER TABLE dictionary ALTER COLUMN id RESTART WITH %d`, id), nil)
}

func (d pgDB) unusedViews(t testing.TB, repo int64) int {
	t.Helper()
	var n int
	d.scan(t, `SELECT count(*) FROM view v JOIN op_log l ON l.id = v.op_log_id
		WHERE l.repository_id = @repo AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = v.op_log_id AND o.view_id = v.id)`,
		map[string]any{"repo": repo}, &n)
	return n
}

func (d pgDB) opHeadsOf(t testing.TB, repo int64, principals ...string) int {
	t.Helper()
	var n int
	d.scan(t, `SELECT count(*) FROM op_head h JOIN op_log l ON l.id = h.op_log_id
		WHERE l.repository_id = @repo AND l.principal = ANY(@principals)`, map[string]any{"repo": repo, "principals": principals}, &n)
	return n
}

func (d pgDB) packs(t testing.TB, net int64) (live, retired int) {
	t.Helper()
	d.scan(t, `SELECT count(*) FILTER (WHERE retired_at IS NULL), count(*) FILTER (WHERE retired_at IS NOT NULL)
		FROM pack WHERE network_id = @net`, map[string]any{"net": net}, &live, &retired)
	return live, retired
}

func (d pgDB) networks(t testing.TB, net int64) int {
	t.Helper()
	return d.count(t, "network", `id = @net`, map[string]any{"net": net})
}

func (d pgDB) setHead(t testing.TB, repo int64, head encoding.ID) {
	t.Helper()
	d.exec(t, `UPDATE repository SET head = @head WHERE id = @repo`, map[string]any{"repo": repo, "head": head[:]})
}

func (d pgDB) plantSharedLog(t testing.TB, repo int64, principal string) int64 {
	t.Helper()
	var id int64
	d.scan(t, `INSERT INTO op_log (id, repository_id, principal, workspace) VALUES (@id, @repo, @principal, '') RETURNING id`,
		map[string]any{"id": data.NewID(), "repo": repo, "principal": principal}, &id)
	d.exec(t, `INSERT INTO op_head (op_log_id, op_id) VALUES (@log, @root)`, map[string]any{"log": id, "root": encoding.RootOperationID[:]})
	return id
}

func (d pgDB) plantWorkspace(t testing.TB, repo int64, principal, name string, withCheckpoint bool) {
	t.Helper()
	p := map[string]any{"repo": repo, "principal": principal, "name": name}
	d.exec(t, `INSERT INTO workspace (repository_id, principal, name) VALUES (@repo, @principal, @name)`, p)
	if withCheckpoint {
		p["id"], p["tree"], p["base"] = data.NewID(), encoding.EmptyTreeID[:], encoding.RootCommitID[:]
		d.exec(t, `INSERT INTO checkpoint (id, repository_id, principal, workspace, machine, tree_ids, base_commit_id, fork)
			VALUES (@id, @repo, @principal, @name, 'm1', ARRAY[@tree::bytea], @base, false)`, p)
	}
}

func (d pgDB) setCheckpointCreated(t testing.TB, id int64, at time.Time) {
	t.Helper()
	d.exec(t, `UPDATE checkpoint SET created_at = @at WHERE id = @id`, map[string]any{"id": id, "at": at})
}

func (d pgDB) tableBytes(t testing.TB, repo, net int64) map[string]int64 {
	t.Helper()
	ctx := context.Background()
	rows, err := d.pool.Query(ctx, `SELECT table_name, column_name FROM information_schema.columns
		WHERE column_name IN ('repository_id', 'owner_id', 'network_id') AND table_schema = current_schema()
		ORDER BY table_name`)
	if err != nil {
		t.Fatal(err)
	}
	type keyed struct{ table, column string }
	var tables []keyed
	for rows.Next() {
		var k keyed
		if err := rows.Scan(&k.table, &k.column); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, k)
	}
	rows.Close()
	out := map[string]int64{}
	for _, k := range tables {
		where, args := `repository_id = @repo`, map[string]any{"repo": repo}
		switch k.column {
		case "owner_id":
			where, args = `(scope = 0 AND owner_id = @repo) OR (scope = 1 AND owner_id = @net)`, map[string]any{"repo": repo, "net": net}
		case "network_id":
			if k.table == "repository" {
				continue
			}
			where, args = `network_id = @net`, map[string]any{"net": net}
		}
		var n int64
		d.scan(t, fmt.Sprintf(`SELECT coalesce(sum(pg_column_size(t.*)), 0) FROM %s t WHERE %s`, k.table, where), args, &n)
		if n > 0 {
			out[k.table] = n
		}
	}
	return out
}
