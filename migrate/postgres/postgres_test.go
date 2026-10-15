package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/migrate"
)

func classes(hs []migrate.Hazard) []string {
	var out []string
	for _, h := range hs {
		if !slices.Contains(out, h.Class) {
			out = append(out, h.Class)
		}
	}
	slices.Sort(out)
	return out
}

func TestHazards(t *testing.T) {
	for sql, want := range map[string][]string{
		"CREATE TABLE t (id int);":                               nil,
		"drop table t;":                                          {migrate.DeletesData},
		"TRUNCATE t":                                             {migrate.DeletesData},
		"DELETE FROM t WHERE id < 3":                             {migrate.DeletesData},
		"ALTER TABLE t ADD COLUMN c int":                         {migrate.AccessExclusiveLock},
		"ALTER TABLE t DROP COLUMN c":                            {migrate.AccessExclusiveLock, migrate.DeletesData},
		"ALTER TABLE t DROP c, ADD d int":                        {migrate.AccessExclusiveLock, migrate.DeletesData},
		"ALTER TABLE t DROP CONSTRAINT k":                        {migrate.AccessExclusiveLock},
		"ALTER TABLE t ALTER COLUMN c DROP NOT NULL":             {migrate.AccessExclusiveLock},
		"ALTER TABLE t ALTER COLUMN c DROP DEFAULT":              {migrate.AccessExclusiveLock},
		"ALTER TABLE t ALTER COLUMN c TYPE bigint":               {migrate.AccessExclusiveLock, migrate.TableRewrite},
		"ALTER TABLE t ALTER c SET NOT NULL":                     {migrate.AccessExclusiveLock, migrate.MayFailOnData, migrate.ValidationScan},
		"ALTER TABLE t ADD CONSTRAINT k CHECK (c > 0)":           {migrate.AccessExclusiveLock, migrate.MayFailOnData, migrate.ValidationScan},
		"ALTER TABLE t ADD CONSTRAINT k CHECK (c > 0) NOT VALID": {migrate.AccessExclusiveLock},
		"CREATE INDEX i ON t (c)":                                {migrate.IndexBuild},
		"CREATE UNIQUE INDEX i ON t (c)":                         {migrate.IndexBuild, migrate.MayFailOnData},
		// What a string, a comment, or a function body holds is not what the migration does.
		"INSERT INTO notes VALUES ('DROP TABLE t; it''s gone')":                         nil,
		"-- DROP TABLE t\nSELECT 1; /* TRUNCATE t */":                                   nil,
		"CREATE FUNCTION f() RETURNS void AS $body$ DELETE FROM t; $body$ LANGUAGE sql": nil,
		// A declared hazard is the file's to say.
		"-- hazard: DELETES_DATA f removes orphans when it runs\nSELECT f();": {migrate.DeletesData},
	} {
		if got := classes(Hazards(sql)); !slices.Equal(got, want) {
			t.Errorf("Hazards(%q) = %v, want %v", sql, got, want)
		}
	}
	if h := Hazards("-- hazard: DELETES_DATA f removes orphans\nSELECT f();"); h[0].Detail != "f removes orphans" {
		t.Errorf("a declared hazard's detail: %+v", h)
	}
}

// The protocol over a real database: plan, a refusal, an allowed apply, and nothing left to do.
func TestMigrator(t *testing.T) {
	ctx := context.Background()
	url := testdb.Postgres(t)
	files := fstest.MapFS{
		"0001_create.sql": {Data: []byte("CREATE TABLE widgets (id bigint PRIMARY KEY, name text, legacy text);\n")},
		"0002_drop.sql":   {Data: []byte("ALTER TABLE widgets DROP COLUMN legacy;\n")},
	}
	run := func(args ...string) (int, migrate.Report) {
		t.Helper()
		var out bytes.Buffer
		code := migrate.Run(ctx, args, &out, func(ctx context.Context) (migrate.Migrator, error) {
			m, err := New(ctx, url, files, "widgets")
			if err == nil {
				t.Cleanup(m.Close)
			}
			return m, err
		})
		var r migrate.Report
		if err := json.Unmarshal(out.Bytes(), &r); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, out.String())
		}
		return code, r
	}
	code, r := run("plan")
	if code != 0 || len(r.Steps) != 2 || r.Steps[0].Description != "0001_create" || r.Database == "" || r.Server == "" ||
		!slices.Equal(classes(r.Steps[1].Hazards), []string{migrate.AccessExclusiveLock, migrate.DeletesData}) {
		t.Fatalf("plan: %d %+v", code, r)
	}
	if code, r = run("apply"); code != migrate.ExitRefused || r.Executed || !slices.Equal(r.Refused, []string{migrate.DeletesData}) {
		t.Fatalf("apply, not allowed: %d %+v", code, r)
	}
	// Refused, nothing applied: the plan is the same.
	if _, r = run("plan"); len(r.Steps) != 2 {
		t.Fatalf("after a refusal: %+v", r)
	}
	if code, r = run("apply", "--allow-hazard", migrate.DeletesData); code != 0 || !r.Executed {
		t.Fatalf("apply, allowed: %d %+v", code, r)
	}
	if code, r = run("plan"); code != 0 || len(r.Steps) != 0 {
		t.Fatalf("after applying: %d %+v", code, r)
	}
	// A third file is the plan's one step.
	files["0003_index.sql"] = &fstest.MapFile{Data: []byte("CREATE INDEX widgets_name ON widgets (name);\n")}
	if code, r = run("apply"); code != 0 || !r.Executed || len(r.Steps) != 1 || r.Steps[0].Description != "0003_index" {
		t.Fatalf("a new file: %d %+v", code, r)
	}
}
