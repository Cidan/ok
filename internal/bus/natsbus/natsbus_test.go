package natsbus_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/bus/natsbus"
)

// serve connects to a NATS server with JetStream started in the test's process.
func serve(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(bustest.NATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func TestQueue(t *testing.T) {
	nc := serve(t)
	bustest.RunQueue(t, func(t *testing.T, maxAttempts int) bus.Queue {
		b, err := natsbus.Open(nc, natsbus.Options{AckWait: time.Second, MaxAttempts: maxAttempts})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}, bustest.QueueOptions{Dedupe: true, DetectsLostLease: true, MinVisibility: time.Second, Shared: true})
}

// A server that comes back without its streams has them made anew: a replica enqueues again, with no restart of its own.
func TestAServerThatLostItsStreams(t *testing.T) {
	ctx := context.Background()
	nc := serve(t)
	b, err := natsbus.Open(nc, natsbus.Options{AckWait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(ctx, bus.Job{ID: "a", Queue: "work.lost"}); err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	for name := range js.StreamNames(ctx).Name() {
		if err := js.DeleteStream(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	natsbus.ForgetStreams(b)
	if err := b.Enqueue(ctx, bus.Job{ID: "b", Queue: "work.lost"}); err != nil {
		t.Fatalf("enqueueing after the server lost them: %v", err)
	}
	got, err := b.Lease(ctx, "work.lost", bus.LeaseOptions{Max: 1, Visibility: time.Minute, Wait: 5 * time.Second})
	if err != nil || len(got) != 1 || got[0].Job().ID != "b" {
		t.Fatalf("leasing after the server lost them: %v, %v", got, err)
	}
}
