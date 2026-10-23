package pgdb_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	idpostgres "github.com/Cidan/ok/internal/id/store/postgres"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/pgdb"
	scmstore "github.com/Cidan/ok/internal/scm/store"
	scmpostgres "github.com/Cidan/ok/internal/scm/store/postgres"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
)

// Roles sharing one database keep their tables apart: identity's and the SCM's both have a locks
// table and their own migrations, and each opens over the other's.
func TestRolesShareADatabase(t *testing.T) {
	ctx := context.Background()
	url := testdb.Postgres(t)
	names := identity.MustNames("example.com")

	idPool, err := pgdb.Open(ctx, url, "identity", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer idPool.Close()
	if _, err := idpostgres.Open(ctx, idPool); err != nil {
		t.Fatalf("identity: %v", err)
	}
	scmPool, err := pgdb.Open(ctx, url, "scm", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer scmPool.Close()
	blobs := testobjects.Store(t)
	sb, err := scmpostgres.Open(ctx, scmPool)
	if err != nil {
		t.Fatalf("the SCM: %v", err)
	}
	st, err := scmstore.New(sb, blobs, scmstore.Config{Names: names})
	if err != nil {
		t.Fatalf("the SCM: %v", err)
	}
	defer st.Close()

	for schema, table := range map[string]string{"identity": "principals", "scm": "repository"} {
		var n int
		if err := idPool.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = $1 AND tablename IN ($2, 'locks')`, schema, table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Errorf("schema %s has %d of %s and locks", schema, n, table)
		}
	}
	// Opening again changes nothing.
	again, err := pgdb.Open(ctx, url, "identity", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if _, err := idpostgres.Open(ctx, again); err != nil {
		t.Errorf("identity again: %v", err)
	}
	if _, err := pgdb.Open(ctx, url, "Bad; DROP", 0); err == nil {
		t.Error("a schema name that is no identifier was accepted")
	}
}

// A pool beside another reaches its database, its tables in a schema of its own, and leaves the
// first as it was.
func TestBeside(t *testing.T) {
	ctx := context.Background()
	pool, err := pgdb.Open(ctx, testdb.Postgres(t), "first", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	beside, err := pgdb.Beside(ctx, pool, "second")
	if err != nil {
		t.Fatal(err)
	}
	defer beside.Close()
	for want, p := range map[string]interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}{"first": pool, "second": beside} {
		var schema, db string
		if err := p.QueryRow(ctx, `SELECT current_schema(), current_database()`).Scan(&schema, &db); err != nil || schema != want {
			t.Errorf("the %s pool's schema: %q, %v", want, schema, err)
		}
	}
	var first, second string
	pool.QueryRow(ctx, `SELECT current_database()`).Scan(&first)
	beside.QueryRow(ctx, `SELECT current_database()`).Scan(&second)
	if first == "" || first != second || beside.Config().MaxConns != 3 {
		t.Errorf("databases %q and %q, the second's pool of %d", first, second, beside.Config().MaxConns)
	}
	if _, err := pgdb.Beside(ctx, pool, "Bad; DROP"); err == nil {
		t.Error("a schema name that is no identifier was accepted")
	}
}

// Roles starting at once on a new database each make the schema they share, or find it made:
// none fails because another made it first.
func TestOpenAtOnce(t *testing.T) {
	ctx := context.Background()
	url := testdb.Postgres(t)
	for round := range 5 {
		schema := fmt.Sprintf("together_%d", round)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				p, err := pgdb.Open(ctx, url, schema, 1)
				if err != nil {
					t.Errorf("opening %s beside others: %v", schema, err)
					return
				}
				p.Close()
			})
		}
		wg.Wait()
	}
}
