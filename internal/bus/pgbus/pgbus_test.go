package pgbus_test

import (
	"context"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/bus/pgbus"
	"github.com/Cidan/ok/internal/testdb"
)

// open is a bus over a database of the test's own.
func open(t *testing.T, opts pgbus.Options) *pgbus.Bus {
	t.Helper()
	ctx := context.Background()
	pool := testdb.SchemaPool(t, testdb.Postgres(t), pgbus.Schema, 20)
	opts.Poll = 200 * time.Millisecond
	opts.Lease = 2 * time.Second
	b, err := pgbus.Open(ctx, pool, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	return b
}

func TestStream(t *testing.T) {
	bustest.RunStream(t, func(t *testing.T) bus.Stream { return open(t, pgbus.Options{}) }, bustest.StreamOptions{})
}

func TestQueue(t *testing.T) {
	bustest.RunQueue(t, func(t *testing.T, maxAttempts int) bus.Queue {
		return open(t, pgbus.Options{MaxAttempts: maxAttempts})
	}, bustest.QueueOptions{Dedupe: true, DetectsLostLease: true})
}

// Each replica is a bus of its own over one database.
func TestSignals(t *testing.T) {
	url := testdb.Postgres(t)
	bustest.RunSignals(t, func(t *testing.T) bus.Signals {
		pool := testdb.SchemaPool(t, url, pgbus.Schema, 4)
		b, err := pgbus.Open(context.Background(), pool, pgbus.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.Close)
		return b
	})
}
