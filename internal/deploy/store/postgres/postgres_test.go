package postgres_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/deploy/store/postgres"
	"github.com/Cidan/ok/internal/deploy/store/storetest"
	"github.com/Cidan/ok/internal/testdb"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Backend {
		pool, err := pgxpool.New(context.Background(), testdb.Postgres(t))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		b, err := postgres.Open(context.Background(), pool)
		if err != nil {
			t.Fatal(err)
		}
		return b
	})
}

// The place in the SCM's feed the role reached before it followed each repository's log is read
// once, then forgotten.
func TestLegacyCursor(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `INSERT INTO feed_cursor (seq) VALUES (42)`); err != nil {
		t.Fatal(err)
	}
	if at, err := b.LegacyCursor(ctx); err != nil || at != 42 {
		t.Fatalf("the legacy cursor: %d, %v", at, err)
	}
	if err := b.ForgetLegacyCursor(ctx); err != nil {
		t.Fatal(err)
	}
	if at, err := b.LegacyCursor(ctx); err != nil || at != 0 {
		t.Errorf("the legacy cursor once forgotten: %d, %v", at, err)
	}
}
