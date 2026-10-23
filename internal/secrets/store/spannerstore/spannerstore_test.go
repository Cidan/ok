package spannerstore_test

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/data/spannerdata/spannertest"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/secrets/store/spannerstore"
	"github.com/Cidan/ok/internal/secrets/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Backend {
		admin, client := spannertest.Database(t, spannerstore.Migrations)
		b, err := spannerstore.Open(context.Background(), admin, client)
		if err != nil {
			t.Fatal(err)
		}
		return b
	})
}
