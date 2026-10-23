package spannerdata_test

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/data/spannerdata/spannertest"
	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/testdb"
)

// A migration's statements are its file's, split at each ';', its comments gone — a ';' or '--' in
// a comment, a string, or a quoted identifier splitting nothing.
func TestStatements(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want []string
	}{
		{"-- a table\nCREATE TABLE a (\n  x INT64,\n) PRIMARY KEY (x);\n\n-- an index\nCREATE INDEX a_x ON a (x);\n",
			[]string{"CREATE TABLE a (\n  x INT64,\n) PRIMARY KEY (x)", "CREATE INDEX a_x ON a (x)"}},
		{"CREATE TABLE b (\n  x INT64, -- its x; never 0\n  y STRING(MAX) DEFAULT ('a;b--c'),\n) PRIMARY KEY (x)",
			[]string{"CREATE TABLE b (\n  x INT64, \n  y STRING(MAX) DEFAULT ('a;b--c'),\n) PRIMARY KEY (x)"}},
		{"CREATE TABLE `c;d` (x INT64) PRIMARY KEY (x); ALTER TABLE `c;d` ADD COLUMN y STRING(MAX) DEFAULT ('it\\'s;')",
			[]string{"CREATE TABLE `c;d` (x INT64) PRIMARY KEY (x)", "ALTER TABLE `c;d` ADD COLUMN y STRING(MAX) DEFAULT ('it\\'s;')"}},
	} {
		if got := spannerdata.Statements(tc.sql); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("Statements(%q) = %q, want %q", tc.sql, got, tc.want)
		}
	}
}

// Migrations apply once each, in order; a database ahead of the binary is refused; four replicas
// migrating at once apply each migration once between them.
func TestMigrate(t *testing.T) {
	ctx := context.Background()
	a := testdb.SpannerAdmin(t)
	c := testdb.SpannerClient(t, testdb.Spanner(t))
	ms := []migrate.Migration{
		{Version: 1, Name: "first", SQL: "CREATE TABLE a (x INT64 NOT NULL) PRIMARY KEY (x)"},
		{Version: 2, Name: "second", SQL: "ALTER TABLE a ADD COLUMN y INT64;\nCREATE INDEX a_y ON a (y)"},
	}
	if did, err := spannerdata.Migrate(ctx, a, c, ms[:1]); err != nil || !slices.Equal(did, []int{1}) {
		t.Fatalf("Migrate = %v, %v", did, err)
	}
	if did, err := spannerdata.Migrate(ctx, a, c, ms); err != nil || !slices.Equal(did, []int{2}) {
		t.Fatalf("Migrate again = %v, %v", did, err)
	}
	if did, err := spannerdata.Migrate(ctx, a, c, ms); err != nil || len(did) != 0 {
		t.Fatalf("Migrate a third time = %v, %v", did, err)
	}
	if _, err := spannerdata.Migrate(ctx, a, c, ms[:1]); err == nil || !strings.Contains(err.Error(), "older than the database") {
		t.Errorf("an older binary: %v", err)
	}
	if _, err := c.Apply(ctx, []*spanner.Mutation{spanner.Insert("a", []string{"x", "y"}, []any{int64(1), int64(2)})}); err != nil {
		t.Errorf("the migrated table: %v", err)
	}

	fresh := testdb.SpannerClient(t, testdb.Spanner(t))
	type result struct {
		did []int
		err error
	}
	results := make(chan result, 4)
	for range 4 {
		go func() {
			did, err := spannerdata.Migrate(ctx, a, fresh, ms)
			results <- result{did, err}
		}()
	}
	var all []int
	for range 4 {
		r := <-results
		if r.err != nil {
			t.Errorf("Migrate at once: %v", r.err)
		}
		all = append(all, r.did...)
	}
	slices.Sort(all)
	if !slices.Equal(all, []int{1, 2}) {
		t.Errorf("applied among the replicas: %v, want each once", all)
	}
}

// A database made with its schema at once (spannertest, spannerdata.Schema) is one Migrate leaves:
// Migrate applies none of its migrations, refuses an older binary, and the tables are there.
func TestSchema(t *testing.T) {
	ctx := context.Background()
	ms := []migrate.Migration{
		{Version: 1, Name: "first", SQL: "CREATE TABLE a (x INT64 NOT NULL) PRIMARY KEY (x)"},
		{Version: 2, Name: "second", SQL: "ALTER TABLE a ADD COLUMN y INT64;\nCREATE INDEX a_y ON a (y)"},
	}
	a, c := spannertest.Database(t, ms)
	if did, err := spannerdata.Migrate(ctx, a, c, ms); err != nil || len(did) != 0 {
		t.Fatalf("Migrate = %v, %v; want nothing to apply", did, err)
	}
	if _, err := spannerdata.Migrate(ctx, a, c, ms[:1]); err == nil || !strings.Contains(err.Error(), "older than the database") {
		t.Errorf("an older binary: %v", err)
	}
	if _, err := c.Apply(ctx, []*spanner.Mutation{spanner.Insert("a", []string{"x", "y"}, []any{int64(1), int64(2)})}); err != nil {
		t.Errorf("the table: %v", err)
	}
	var locks int64
	if err := c.Single().Query(ctx, spanner.Statement{SQL: `SELECT COUNT(*) FROM locks`}).Do(func(r *spanner.Row) error { return r.Columns(&locks) }); err != nil {
		t.Errorf("the locks table: %v", err)
	}
}
