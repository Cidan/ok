package testdb_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/testdb"
)

// recorder is a test whose errors are recorded rather than failing it.
type recorder struct {
	testing.TB
	mu   sync.Mutex
	errs []string
}

func (r *recorder) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func (r *recorder) reported() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.errs)
}

// A goroutine holding a pool's connection that asks the pool for another is reported; using it
// connection after connection, another goroutine using it meanwhile, and asking a second pool
// opened beside it are not.
func TestNoNestedConns(t *testing.T) {
	ctx := context.Background()
	rec := &recorder{TB: t}
	cfg, err := pgxpool.ParseConfig(testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	testdb.NoNestedConns(rec, cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	beside, err := pgdb.Beside(ctx, pool, "beside")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(beside.Close)
	one := func(p *pgxpool.Pool) {
		t.Helper()
		var n int
		if err := p.QueryRow(ctx, `SELECT 1`).Scan(&n); err != nil {
			t.Fatal(err)
		}
	}

	one(pool)
	one(pool)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Go(func() { one(pool) })
	wg.Wait()
	one(beside)
	if n := rec.reported(); n != 0 {
		t.Fatalf("%d reported without a nested acquisition: %v", n, rec.errs)
	}
	one(pool)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n := rec.reported(); n != 1 || !strings.Contains(rec.errs[0], "holding 1 of a pool's connections acquires another") {
		t.Fatalf("a nested acquisition: %d reported, %v", n, rec.errs)
	}
	one(pool)
	if n := rec.reported(); n != 1 {
		t.Errorf("after the transaction ended: %d reported", n)
	}
}
