package postgres_test

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/secrets/store/postgres"
	"github.com/Cidan/ok/internal/secrets/store/storetest"
	"github.com/Cidan/ok/internal/testdb"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Backend {
		b, err := postgres.Open(context.Background(), testdb.Pool(t))
		if err != nil {
			t.Fatal(err)
		}
		return b
	})
}
