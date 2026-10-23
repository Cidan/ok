package spannerstore_test

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/data/spannerdata/spannertest"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/id/store/spannerstore"
	"github.com/Cidan/ok/internal/id/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Backend {
		ctx := context.Background()
		admin, client := spannertest.Database(t, spannerstore.Migrations)
		b, err := spannerstore.Open(ctx, admin, client)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := spannerstore.Open(ctx, admin, client); err != nil {
			t.Fatalf("opening again: %v", err)
		}
		return b
	})
}
