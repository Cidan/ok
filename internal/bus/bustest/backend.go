package bustest

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/Cidan/ok/internal/bus/bustest/awsfake"
	"github.com/Cidan/ok/internal/config"
)

// Config is the bus a test's roles run on, as OK_TEST_BUS names it: postgres (the default), in the
// test's database; nats, a NATS server with JetStream this process runs on its loopback for the test
// alone; pubsub, a Pub/Sub fake (pstest) likewise; aws, a fake of SNS and SQS (awsfake) likewise; or
// kafka, a Kafka cluster (kfake) likewise for the stream, beside PostgreSQL's queue. A test that runs roles takes its bus from here, so the suites run on each
// (docs/hosted.md, "Testing"). Roles on Spanner (OK_TEST_DATABASE) have no PostgreSQL for a bus:
// theirs is nats unless OK_TEST_BUS names another, and Kafka's queue is NATS's.
func Config(t testing.TB) config.Bus {
	t.Helper()
	k := os.Getenv("OK_TEST_BUS")
	if os.Getenv("OK_TEST_DATABASE") == "spanner" {
		switch k {
		case "":
			k = "nats"
		case "postgres":
			t.Fatal("bustest: OK_TEST_BUS=postgres with OK_TEST_DATABASE=spanner: a Spanner installation's bus is another backend's")
		case "kafka":
			n := &config.NATSBus{URL: NATS(t), AckWait: time.Second}
			return config.Bus{Stream: config.BusBackend{Type: "kafka", Kafka: &config.KafkaBus{Brokers: Kafka(t), Partitions: 4}},
				Queue: config.BusBackend{Type: "nats", NATS: n}}
		}
	}
	switch k {
	case "", "postgres":
		return config.Bus{Stream: config.BusBackend{Type: "postgres"}, Queue: config.BusBackend{Type: "postgres"}}
	case "nats":
		n := &config.NATSBus{URL: NATS(t), AckWait: time.Second}
		return config.Bus{Stream: config.BusBackend{Type: "nats", NATS: n}, Queue: config.BusBackend{Type: "nats", NATS: n}}
	case "pubsub":
		p := &config.PubSubBus{Project: "ok-test", EmulatorHost: PubSub(t)}
		return config.Bus{Stream: config.BusBackend{Type: "pubsub", PubSub: p}, Queue: config.BusBackend{Type: "pubsub", PubSub: p}}
	case "aws":
		dir := t.TempDir()
		key, secret := filepath.Join(dir, "access-key"), filepath.Join(dir, "secret-key")
		if os.WriteFile(key, []byte("test\n"), 0o600) != nil || os.WriteFile(secret, []byte("test\n"), 0o600) != nil {
			t.Fatal("bustest: writing the fake's key")
		}
		a := &config.AWSBus{Region: awsfake.Region, Endpoint: AWS(t), AccessKeyFile: key, SecretKeyFile: secret}
		return config.Bus{Stream: config.BusBackend{Type: "aws", AWS: a}, Queue: config.BusBackend{Type: "aws", AWS: a}}
	case "kafka":
		// Kafka carries the stream alone: the queue stays on PostgreSQL, as an installation's would.
		return config.Bus{Stream: config.BusBackend{Type: "kafka", Kafka: &config.KafkaBus{Brokers: Kafka(t), Partitions: 4}},
			Queue: config.BusBackend{Type: "postgres"}}
	default:
		t.Fatalf("bustest: OK_TEST_BUS=%q is none of postgres, nats, pubsub, aws, and kafka", k)
		return config.Bus{}
	}
}

// AWS starts a fake of SQS and SNS (awsfake) in the test's process, stopped when the test ends, and
// returns its endpoint.
func AWS(t testing.TB) string {
	t.Helper()
	s, err := awsfake.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s.URL
}

// Kafka starts a Kafka cluster (kfake) of three brokers in the test's process, stopped when the test
// ends, and returns their addresses.
func Kafka(t testing.TB) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(3))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

// PubSub starts a Pub/Sub fake (pstest) in the test's process, stopped when the test ends, and
// returns its address.
func PubSub(t testing.TB) string {
	t.Helper()
	srv := pstest.NewServerWithAddress("127.0.0.1:0") // NewServer's localhost needs a resolver
	t.Cleanup(func() { srv.Close() })
	return srv.Addr
}

// NATS starts a NATS server with JetStream in the test's process, stopped when the test ends, and
// returns its URL.
func NATS(t testing.TB) string {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("bustest: the NATS server did not start")
	}
	t.Cleanup(s.Shutdown)
	return s.ClientURL()
}
