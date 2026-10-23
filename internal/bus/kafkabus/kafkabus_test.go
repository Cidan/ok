package kafkabus_test

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/bus/kafkabus"
)

func TestStream(t *testing.T) {
	brokers := bustest.Kafka(t)
	bustest.RunStream(t, func(t *testing.T) bus.Stream {
		b, err := kafkabus.Open([]kgo.Opt{kgo.SeedBrokers(brokers...)}, kafkabus.Options{Partitions: 4})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.Close)
		return b
	}, bustest.StreamOptions{Concurrent: 8})
}

func TestSignals(t *testing.T) {
	brokers := bustest.Kafka(t)
	bustest.RunSignals(t, func(t *testing.T) bus.Signals {
		b, err := kafkabus.Open([]kgo.Opt{kgo.SeedBrokers(brokers...)}, kafkabus.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.Close)
		return b
	})
}
