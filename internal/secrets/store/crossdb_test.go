package store_test

import (
	"context"
	"os"
	"testing"

	"github.com/Cidan/ok/internal/data/spannerdata/spannertest"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/secrets/store/postgres"
	"github.com/Cidan/ok/internal/secrets/store/spannerstore"
	"github.com/Cidan/ok/internal/secrets/store/storetest"
	"github.com/Cidan/ok/internal/testdb"
)

// A repository moves between a PostgreSQL cell's ok secrets and a Spanner cell's, each way, when
// the environment has both.
func TestMovesBetweenDatabases(t *testing.T) {
	if os.Getenv("PGHOST") == "" || os.Getenv("OK_TEST_SPANNER_OMNI") == "" && os.Getenv("OK_TEST_SPANNER_INSTANCE") == "" {
		t.Skip("the environment has one kind of database")
	}
	pg := func(t *testing.T) store.Backend {
		b, err := postgres.Open(context.Background(), testdb.Pool(t))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	sp := func(t *testing.T) store.Backend {
		admin, client := spannertest.Database(t, spannerstore.Migrations)
		b, err := spannerstore.Open(context.Background(), admin, client)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	t.Run("PostgreSQL to Spanner", func(t *testing.T) { storetest.Moves(t, pg(t), sp(t)) })
	t.Run("Spanner to PostgreSQL", func(t *testing.T) { storetest.Moves(t, sp(t), pg(t)) })
}
