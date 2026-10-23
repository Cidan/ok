package pubsubbus_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/bus/pubsubbus"
)

// client is a Pub/Sub client of a project on a Pub/Sub fake in the test's process.
func client(t *testing.T) *pubsub.Client {
	t.Helper()
	c, err := pubsub.NewClient(context.Background(), "ok-test", option.WithEndpoint(bustest.PubSub(t)),
		option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestStream(t *testing.T) {
	c := client(t)
	bustest.RunStream(t, func(t *testing.T) bus.Stream {
		b := pubsubbus.Open(c, pubsubbus.Options{})
		t.Cleanup(b.Close)
		return b
	}, bustest.StreamOptions{Concurrent: 8})
}

func TestQueue(t *testing.T) {
	c := client(t)
	bustest.RunQueue(t, func(t *testing.T, maxAttempts int) bus.Queue {
		b := pubsubbus.Open(c, pubsubbus.Options{MaxAttempts: maxAttempts})
		t.Cleanup(b.Close)
		return b
	}, bustest.QueueOptions{DetectsLostLease: true, MinVisibility: 10 * time.Second})
}

// Each replica is a client of its own, of one fake.
func TestSignals(t *testing.T) {
	addr := bustest.PubSub(t)
	bustest.RunSignals(t, func(t *testing.T) bus.Signals {
		c, err := pubsub.NewClient(context.Background(), "ok-test", option.WithEndpoint(addr),
			option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		b := pubsubbus.Open(c, pubsubbus.Options{})
		t.Cleanup(b.Close)
		return b
	})
}
