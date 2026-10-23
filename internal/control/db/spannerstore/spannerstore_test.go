package spannerstore_test

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/db/dbtest"
	"github.com/Cidan/ok/internal/control/db/spannerstore"
	"github.com/Cidan/ok/internal/data/spannerdata/spannertest"
)

func TestConformance(t *testing.T) {
	dbtest.Run(t, func(t *testing.T) db.Store {
		admin, client := spannertest.Database(t, spannerstore.Migrations)
		s, err := spannerstore.Open(context.Background(), admin, client)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}
