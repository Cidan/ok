package data_test

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/testdb"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

const pgTables = `
CREATE TABLE t (
    id    bigint PRIMARY KEY,
    parent bigint REFERENCES t,
    n     int, s smallint, txt text, b bytea, f boolean, made timestamptz, d date, j jsonb,
    tags  text[], nums bigint[], blobs bytea[],
    seqn  bigint GENERATED ALWAYS AS IDENTITY,
    nul   text
);
CREATE TABLE c (
    t_id bigint NOT NULL REFERENCES t ON DELETE CASCADE,
    k    text NOT NULL,
    v    bytea,
    PRIMARY KEY (t_id, k)
);`

var spannerTables = []string{
	`CREATE TABLE t (
    t_id  INT64 NOT NULL,
    parent INT64,
    n INT64, s INT64, txt STRING(MAX), b BYTES(MAX), f BOOL, made TIMESTAMP, d DATE, j JSON,
    tags  ARRAY<STRING(MAX)>, nums ARRAY<INT64>, blobs ARRAY<BYTES(MAX)>,
    seqn  INT64,
    nul   STRING(MAX),
    only_spanner INT64,
    CONSTRAINT t_parent FOREIGN KEY (parent) REFERENCES t (t_id),
) PRIMARY KEY (t_id)`,
	`CREATE TABLE c (
    t_id INT64 NOT NULL,
    k    STRING(MAX) NOT NULL,
    v    BYTES(MAX),
) PRIMARY KEY (t_id, k), INTERLEAVE IN PARENT t ON DELETE CASCADE`,
}

var renames = spannerdata.Renames{"t": {"id": "t_id"}}

// pgSchema is a schema of the test's database holding the tables, empty.
func pgSchema(t *testing.T, url, schema string) *pgxpool.Pool {
	t.Helper()
	p := testdb.SchemaPool(t, url, schema, 0)
	if _, err := p.Exec(context.Background(), pgTables); err != nil {
		t.Fatal(err)
	}
	return p
}

// exportPG reads every row of a table, two to a page.
func exportPG(t *testing.T, p *pgxpool.Pool, tb *pgdata.Table) []*okpb.Row {
	t.Helper()
	var out []*okpb.Row
	var after *okpb.Row
	for {
		rows, err := pgdata.ExportRows(context.Background(), p, tb, "TRUE", nil, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, rows...)
		if len(rows) < 2 {
			return out
		}
		after = keyOf(rows[len(rows)-1], tb.Key)
	}
}

func keyOf(r *okpb.Row, key []string) *okpb.Row {
	k := &okpb.Row{}
	for _, name := range key {
		for _, c := range r.GetColumns() {
			if c.GetName() == name {
				k.Columns = append(k.Columns, c)
			}
		}
	}
	return k
}

// Rows carried from PostgreSQL to Spanner and back read as they were: every type, a column Spanner
// names otherwise and one it alone has, an interleaved child, a row whose parent comes after it by
// key, a page at a time; and a sequence that numbered rows is advanced past them.
func TestRowsBetweenDatabases(t *testing.T) {
	ctx := context.Background()
	url := testdb.Postgres(t)
	src := pgSchema(t, url, "src")
	if _, err := src.Exec(ctx, `
INSERT INTO t (id, parent, n, s, txt, b, f, made, d, j, tags, nums, blobs) VALUES
  (5, NULL, 1, 2, 'five', '\x0001ff', true, '2026-10-18 12:34:56.789+00', '2026-10-18', '{"a": [1, "x"]}', '{"p","q"}', '{7,8}', ARRAY['\x01'::bytea]),
  (9, NULL, NULL, NULL, '', '', false, NULL, NULL, 'null', '{}', '{}', '{}');
INSERT INTO t (id, parent, txt) VALUES (1, 9, 'child of a later row'), (3, 1, 'grandchild');
INSERT INTO c VALUES (5, 'a', '\xaa'), (5, 'b', NULL), (9, 'z', '\x00');`); err != nil {
		t.Fatal(err)
	}
	pgTabs, err := pgdata.Tables(ctx, src, []string{"t", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if order, err := pgdata.ParentsFirst(pgTabs); err != nil || order[0] != "t" {
		t.Fatalf("PostgreSQL's order: %v, %v", order, err)
	}
	if len(pgTabs["t"].SelfRefs) != 1 || pgTabs["t"].SelfRefs[0] != "parent" {
		t.Fatalf("t's self references: %v", pgTabs["t"].SelfRefs)
	}
	want := map[string][]*okpb.Row{"t": exportPG(t, src, pgTabs["t"]), "c": exportPG(t, src, pgTabs["c"])}
	if len(want["t"]) != 4 || len(want["c"]) != 3 {
		t.Fatalf("exported %d and %d rows", len(want["t"]), len(want["c"]))
	}

	// Into Spanner, and out of it.
	client := testdb.SpannerClient(t, testdb.Spanner(t, spannerTables...))
	ro := client.ReadOnlyTransaction()
	spTabs, err := spannerdata.Tables(ctx, ro, []string{"t", "c"}, renames)
	ro.Close()
	if err != nil {
		t.Fatal(err)
	}
	if order, err := spannerdata.ParentsFirst(spTabs); err != nil || order[0] != "t" {
		t.Fatalf("Spanner's order: %v, %v", order, err)
	}
	for _, name := range []string{"t", "c"} {
		if err := spannerdata.ImportRows(ctx, client, spTabs[name], want[name], func(v map[string]*okpb.Value) {
			if name == "t" {
				v["only_spanner"] = &okpb.Value{Kind: &okpb.Value_Int{Int: 42}}
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"t", "c"} {
		var got []*okpb.Row
		var after *okpb.Row
		for {
			rows, err := spannerdata.ExportRows(ctx, client.Single(), spTabs[name], "TRUE", nil, after, 2, map[string]bool{"only_spanner": true})
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, rows...)
			if len(rows) < 2 {
				break
			}
			after = keyOf(rows[len(rows)-1], map[string][]string{"t": {"id"}, "c": {"t_id", "k"}}[name])
		}
		// Spanner has no identity: its seqn is what PostgreSQL numbered.
		if len(got) != len(want[name]) {
			t.Fatalf("%s: %d rows from Spanner, want %d", name, len(got), len(want[name]))
		}
		for i := range got {
			if !proto.Equal(got[i], want[name][i]) {
				t.Errorf("%s row %d from Spanner:\n%v\nwant\n%v", name, i, got[i], want[name][i])
			}
		}
		want[name] = got
	}
	var only int64
	if err := client.Single().Query(ctx, spanner.Statement{SQL: `SELECT only_spanner FROM t WHERE t_id = 5`}).Do(func(r *spanner.Row) error {
		return r.Columns(&only)
	}); err != nil || only != 42 {
		t.Errorf("the column Spanner alone has: %d, %v", only, err)
	}

	// Back into PostgreSQL, whose sequence moves past the rows it brings.
	dst := pgSchema(t, url, "dst")
	dstTabs, err := pgdata.Tables(ctx, dst, []string{"t", "c"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"t", "c"} {
		if err := pgdata.ImportRows(ctx, dst, dstTabs[name], want[name]); err != nil {
			t.Fatal(err)
		}
		if err := pgdata.ImportRows(ctx, dst, dstTabs[name], want[name]); err != nil {
			t.Fatalf("importing %s again, over itself: %v", name, err)
		}
		if err := pgdata.AdvanceSequences(ctx, dst, dstTabs[name]); err != nil {
			t.Fatal(err)
		}
		got := exportPG(t, dst, dstTabs[name])
		for i := range got {
			if !proto.Equal(got[i], want[name][i]) {
				t.Errorf("%s row %d back in PostgreSQL:\n%v\nwant\n%v", name, i, got[i], want[name][i])
			}
		}
	}
	var seqn, most int64
	if err := dst.QueryRow(ctx, `INSERT INTO t (id) VALUES (100) RETURNING seqn`).Scan(&seqn); err != nil {
		t.Fatal(err)
	}
	if err := src.QueryRow(ctx, `SELECT max(seqn) FROM t`).Scan(&most); err != nil {
		t.Fatal(err)
	}
	if seqn <= most {
		t.Errorf("a row made after the import is numbered %d, not past the %d brought", seqn, most)
	}
	n, err := pgdata.DeleteRows(ctx, dst, "c", "t_id = @t", pgx.NamedArgs{"t": int64(5)}, 1)
	if err != nil || n != 1 {
		t.Errorf("deleting a page of one: %d, %v", n, err)
	}
	if n, err := spannerdata.DeleteRows(ctx, client, spTabs["c"], "t_id = @t", map[string]any{"t": int64(5)}, 10); err != nil || n != 2 {
		t.Errorf("deleting from Spanner: %d, %v", n, err)
	}
}
