package postgres_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/id/store/postgres"
	"github.com/Cidan/ok/internal/id/store/storetest"
	"github.com/Cidan/ok/internal/testdb"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Backend {
		ctx := context.Background()
		pool, err := pgxpool.New(ctx, testdb.Postgres(t))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
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
