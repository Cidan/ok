package pgbus_test

import (
	"context"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/bus/pgbus"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/testdb"
)

// open is a bus over a database of the test's own.
func open(t *testing.T, opts pgbus.Options) *pgbus.Bus {
	t.Helper()
	ctx := context.Background()
	pool, err := pgdb.Open(ctx, testdb.Postgres(t), pgbus.Schema, 20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
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
