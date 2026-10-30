package natsbus_test

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"

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
