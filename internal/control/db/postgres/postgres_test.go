package postgres_test

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/db/dbtest"
	"github.com/Cidan/ok/internal/control/db/postgres"
	"github.com/Cidan/ok/internal/testdb"
)

func TestConformance(t *testing.T) {
	dbtest.Run(t, func(t *testing.T) db.Store {
		pool := testdb.Pool(t)
		s, err := postgres.Open(context.Background(), pool)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}
