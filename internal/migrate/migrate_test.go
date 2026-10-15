package migrate

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/testdb"
)

func TestLoad(t *testing.T) {
	ms, err := Load(fstest.MapFS{
		"0002_more.sql":  {Data: []byte("ALTER TABLE a ADD b int;")},
		"0001_first.sql": {Data: []byte("CREATE TABLE a (x int);")},
		"README.md":      {Data: []byte("not a migration")},
	})
	if err != nil || len(ms) != 2 || ms[0].Name != "first" || ms[1].Version != 2 {
		t.Fatalf("Load = %+v, %v", ms, err)
	}
	for name, fsys := range map[string]fstest.MapFS{
		"a gap":    {"0001_a.sql": {}, "0003_c.sql": {}},
		"a repeat": {"0001_a.sql": {}, "0001_b.sql": {}},
		"no first": {"0002_b.sql": {}},
	} {
		if _, err := Load(fsys); err == nil {
			t.Errorf("%s: Load accepted it", name)
		}
	}
}

func TestApply(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ms := []Migration{
		{1, "first", "CREATE TABLE a (x int)"},
		{2, "second", "ALTER TABLE a ADD COLUMN y int"},
	}
	did, err := Apply(ctx, pool, "test", ms[:1])
	if err != nil || !reflect.DeepEqual(did, []int{1}) {
		t.Fatalf("Apply = %v, %v", did, err)
	}
	if did, err = Apply(ctx, pool, "test", ms); err != nil || !reflect.DeepEqual(did, []int{2}) {
		t.Fatalf("Apply again = %v, %v", did, err)
	}
	if did, err = Apply(ctx, pool, "test", ms); err != nil || len(did) != 0 {
		t.Fatalf("Apply a third time = %v, %v", did, err)
	}
	if _, err := Apply(ctx, pool, "test", ms[:1]); err == nil || !strings.Contains(err.Error(), "older than the database") {
		t.Errorf("an older binary: %v", err)
	}
	bad := append(ms, Migration{3, "broken", "ALTER TABLE nope ADD z int"})
	if did, err = Apply(ctx, pool, "test", bad); err == nil || len(did) != 0 {
		t.Errorf("a failing migration: %v, %v", did, err)
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM test_migrations").Scan(&n); err != nil || n != 2 {
		t.Errorf("recorded %d migrations, %v", n, err)
	}
}
