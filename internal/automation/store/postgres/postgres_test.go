package postgres_test

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/automation/store/postgres"
	"github.com/Cidan/ok/internal/automation/store/storetest"
	"github.com/Cidan/ok/internal/testdb"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Backend {
		pool := testdb.Pool(t)
		b, err := postgres.Open(context.Background(), pool)
		if err != nil {
			t.Fatal(err)
		}
		return b
	})
}
