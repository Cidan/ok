// Package roledb gives a test's roles their databases, as OK_TEST_DATABASE names them: postgres,
// the default — one PostgreSQL database for the test, a schema to a role, whose pools the test hands
// its roles — or spanner — a Spanner database to a role (internal/testdb), <prefix>-<role>, each
// made with its schema at once (spannerdata.Schema) and dropped when the test ends, which the roles
// open as their configuration says (database.type spanner). A Spanner installation's bus is another
// backend's (bustest).
package roledb

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	autospanner "github.com/Cidan/ok/internal/automation/store/spannerstore"
	"github.com/Cidan/ok/internal/config"
	ctlspanner "github.com/Cidan/ok/internal/control/db/spannerstore"
	"github.com/Cidan/ok/internal/data/spannerdata"
	deployspanner "github.com/Cidan/ok/internal/deploy/store/spannerstore"
	idspanner "github.com/Cidan/ok/internal/id/store/spannerstore"
	"github.com/Cidan/ok/internal/migrate"
	scmspanner "github.com/Cidan/ok/internal/scm/store/spannerstore"
	secretsspanner "github.com/Cidan/ok/internal/secrets/store/spannerstore"
	"github.com/Cidan/ok/internal/testdb"
)

// roles are each role's database's name in a prefix, and its schema.
var roles = map[string][]migrate.Migration{
	"identity":   idspanner.Migrations,
	"scm":        scmspanner.Migrations,
	"automation": autospanner.Migrations,
	"control":    ctlspanner.Migrations,
	"deploy":     deployspanner.Migrations,
	"secrets":    secretsspanner.Migrations,
}

// Spanner reports whether the tests' roles run on Spanner (OK_TEST_DATABASE).
func Spanner(t testing.TB) bool {
	t.Helper()
	switch d := os.Getenv("OK_TEST_DATABASE"); d {
	case "", "postgres":
		return false
	case "spanner":
		return true
	default:
		t.Fatalf("roledb: OK_TEST_DATABASE=%q is neither postgres nor spanner", d)
		return false
	}
}

// DBs are a test's roles' databases.
type DBs struct {
	url     string // PostgreSQL's
	spanner *config.SpannerDatabase
}

// New makes a test's roles' databases: on PostgreSQL one database, its schemas made as each role's
// pool opens; on Spanner every role's, made with its schema.
func New(t testing.TB) *DBs {
	t.Helper()
	if !Spanner(t) {
		return NewPostgres(t)
	}
	return NewSpanner(t)
}

// NewPostgres makes a test's roles' databases on PostgreSQL, whatever OK_TEST_DATABASE says: one
// database, its schemas made as each role's pool opens. A test of cells on both databases makes
// each cell's so.
func NewPostgres(t testing.TB) *DBs {
	t.Helper()
	return &DBs{url: testdb.Postgres(t)}
}

// NewSpanner makes a test's roles' databases on Spanner, whatever OK_TEST_DATABASE says: every
// role's, made with its schema.
func NewSpanner(t testing.TB) *DBs {
	t.Helper()
	conn, inst := testdb.SpannerConnection(t)
	prefix := testdb.SpannerPrefix()
	ddl := map[string][]string{}
	for role, ms := range roles {
		ddl[prefix+"-"+role] = spannerdata.Schema(ms)
	}
	names := testdb.SpannerDatabases(t, ddl)
	for role, ms := range roles {
		c := testdb.SpannerClient(t, names[prefix+"-"+role])
		if err := spannerdata.Applied(context.Background(), c, ms); err != nil {
			t.Fatalf("roledb: recording %s's migrations: %v", role, err)
		}
	}
	d := &DBs{spanner: &config.SpannerDatabase{Instance: inst, Prefix: prefix}}
	if conn.Omni != "" {
		d.spanner.Omni = &config.SpannerOmni{Address: conn.Omni, PlainText: conn.PlainText}
	}
	return d
}

// Configure points cfg's database at the test's: on Spanner, its instance and the test's prefix.
func (d *DBs) Configure(cfg *config.Config) {
	if d.spanner != nil {
		cfg.Database = config.Database{Type: "spanner", Spanner: d.spanner}
	}
}

// Pool is a pool of schema's on PostgreSQL, of up to conns connections (0: pgxpool's default), that
// fails the test when a goroutine holding a connection asks for another (testdb.NoNestedConns); nil
// on Spanner, where a role opens its database as the configuration says.
func (d *DBs) Pool(t testing.TB, schema string, conns int32) *pgxpool.Pool {
	t.Helper()
	if d.spanner != nil {
		return nil
	}
	return testdb.SchemaPool(t, d.url, schema, conns)
}

// OnSpanner reports whether the roles' databases are Spanner's.
func (d *DBs) OnSpanner() bool { return d.spanner != nil }

// URL is the test's PostgreSQL database's ("" on Spanner).
func (d *DBs) URL() string { return d.url }
