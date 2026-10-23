// Package spannertest makes a Spanner database for each of a backend's test cases with the
// backend's schema at once (spannerdata.Schema), its migrations recorded as applied: the backend's
// Migrate then finds none to apply. A database Migrate brings up pays its schema changes, tens of
// seconds each on Spanner Omni; one made with its schema takes under a second. Migrate's own path
// is spannerdata's tests'.
package spannertest

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/testdb"
)

// Database is a new database for this test alone (testdb.Spanner), with migrations' schema, and
// an admin client and a client of it, all gone when the test ends.
func Database(t testing.TB, migrations []migrate.Migration) (*database.DatabaseAdminClient, *spanner.Client) {
	t.Helper()
	c := testdb.SpannerClient(t, testdb.Spanner(t, spannerdata.Schema(migrations)...))
	if err := spannerdata.Applied(context.Background(), c, migrations); err != nil {
		t.Fatalf("spannertest: recording the migrations: %v", err)
	}
	return testdb.SpannerAdmin(t), c
}
