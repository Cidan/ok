package bustest

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/nats-io/nats-server/v2/server"

	"github.com/Cidan/ok/internal/bus/bustest/awsfake"
	"github.com/Cidan/ok/internal/config"
)

// Config is the bus a test's roles run on, as OK_TEST_BUS names it: nats (the default), a NATS
// server with JetStream this process runs on its loopback for the test alone; pubsub, a Pub/Sub fake
// (pstest) likewise; or aws, a fake of SQS (awsfake) likewise. A test that runs roles takes
// its bus from here, so the suites run on each (docs/hosted.md, "Testing").
func Config(t testing.TB) config.Bus {
	t.Helper()
	switch k := os.Getenv("OK_TEST_BUS"); k {
	case "", "nats":
		return config.Bus{Type: "nats", NATS: &config.NATSBus{URL: NATS(t), AckWait: time.Second}}
	case "pubsub":
		return config.Bus{Type: "pubsub", PubSub: &config.PubSubBus{Project: "ok-test", EmulatorHost: PubSub(t)}}
	case "aws":
		dir := t.TempDir()
		key, secret := filepath.Join(dir, "access-key"), filepath.Join(dir, "secret-key")
		if os.WriteFile(key, []byte("test\n"), 0o600) != nil || os.WriteFile(secret, []byte("test\n"), 0o600) != nil {
			t.Fatal("bustest: writing the fake's key")
		}
		return config.Bus{Type: "aws", AWS: &config.AWSBus{Region: awsfake.Region, Endpoint: AWS(t), AccessKeyFile: key, SecretKeyFile: secret}}
	default:
		t.Fatalf("bustest: OK_TEST_BUS=%q is none of nats, pubsub, and aws", k)
		return config.Bus{}
	}
}

// AWS starts a fake of SQS (awsfake) in the test's process, stopped when the test ends, and
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
