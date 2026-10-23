package postgres_test

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/id/store/postgres"
	"github.com/Cidan/ok/internal/id/store/storetest"
	"github.com/Cidan/ok/internal/testdb"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Backend {
		ctx := context.Background()
		pool := testdb.Pool(t)
		b, err := postgres.Open(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := postgres.Open(ctx, pool); err != nil {
			t.Fatalf("opening again: %v", err)
		}
		return b
	})
}

// Search's trigram indexes are made where pg_trgm can be, as on the test server.
func TestSearchIndexes(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t)
	if _, err := postgres.Open(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes
		WHERE indexname IN ('repositories_name_trgm', 'principals_handle_trgm', 'principals_display_trgm')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("%d of search's 3 trigram indexes", n)
	}
}
