// Package busconfig opens the bus (internal/bus) an ok configuration names.
package busconfig

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub/v2"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/twmb/franz-go/pkg/kgo"
	saslaws "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/awsbus"
	"github.com/Cidan/ok/internal/bus/kafkabus"
	"github.com/Cidan/ok/internal/bus/natsbus"
	"github.com/Cidan/ok/internal/bus/pgbus"
	"github.com/Cidan/ok/internal/bus/pubsubbus"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/pgdb"
)

// Bus is an open bus.
type Bus struct {
	Stream bus.Stream
	Queue  bus.Queue
	// Signals ride the stream's backend (docs/hosted.md, "Signals").
	Signals bus.Signals
	closers []func()
}

// Close closes what Open opened.
func (b *Bus) Close() {
	for i := len(b.closers) - 1; i >= 0; i-- {
		b.closers[i]()
	}
}

// natsServers is what a NATS connection is to: the stream and the queue share one when it is the
// same.
type natsServers struct{ url, credentials, ca string }

// pubsubProject is what a Pub/Sub client is to: the stream and the queue share one when it is the
// same.
type pubsubProject struct{ project, credentials, endpoint, emulator string }

// awsAccount is what SNS and SQS clients are to: the stream and the queue share them when it is the
// same.
type awsAccount struct{ region, endpoint, accessKey, secretKey string }

// Open opens the bus c names. A postgres backend is in the database the role's pool reaches, in
// the bus's own schema; the stream and queue share one connection to it when both are postgres,
// one to NATS when both are nats on the same servers, one Pub/Sub client when both are pubsub in the
// same project, and SNS and SQS clients when both are aws in the same region. Kafka carries the
// stream alone.
func Open(ctx context.Context, c config.Bus, role *pgxpool.Pool) (*Bus, error) {
	b := &Bus{}
	var pg *pgbus.Bus
	postgres := func() (*pgbus.Bus, error) {
		if pg != nil {
			return pg, nil
		}
		pool, err := pgdb.Beside(ctx, role, pgbus.Schema)
		if err != nil {
			return nil, fmt.Errorf("the bus: %w", err)
		}
		if pg, err = pgbus.Open(ctx, pool, pgbus.Options{}); err != nil {
			pool.Close()
			return nil, err
		}
		b.closers = append(b.closers, pool.Close, pg.Close)
		return pg, nil
	}
	conns := map[natsServers]*nats.Conn{}
	natsBus := func(field string, n *config.NATSBus) (*natsbus.Bus, error) {
		servers := natsServers{n.URL, n.CredentialsFile, n.CAFile}
		nc := conns[servers]
		if nc == nil {
			opts := []nats.Option{nats.Name("ok"), nats.MaxReconnects(-1)}
			if n.CredentialsFile != "" {
				opts = append(opts, nats.UserCredentials(n.CredentialsFile))
			}
			if n.CAFile != "" {
				opts = append(opts, nats.RootCAs(n.CAFile))
			}
			var err error
			if nc, err = nats.Connect(n.URL, opts...); err != nil {
				return nil, fmt.Errorf("%s.nats: %w", field, err)
			}
			conns[servers] = nc
			b.closers = append(b.closers, nc.Close)
		}
		return natsbus.Open(nc, natsbus.Options{Prefix: n.Prefix, Replicas: n.Replicas, Retention: n.Retention,
			AckWait: n.AckWait, MaxAttempts: n.MaxAttempts, Concurrency: n.Concurrency})
	}
	clients := map[pubsubProject]*pubsub.Client{}
	pubsubBus := func(field string, p *config.PubSubBus) (*pubsubbus.Bus, error) {
		project := pubsubProject{p.Project, p.CredentialsFile, p.Endpoint, p.EmulatorHost}
		c := clients[project]
		if c == nil {
			var opts []option.ClientOption
			switch {
			case p.EmulatorHost != "":
				opts = append(opts, option.WithEndpoint(p.EmulatorHost), option.WithoutAuthentication(),
					option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
			case p.Endpoint != "":
				opts = append(opts, option.WithEndpoint(p.Endpoint))
			}
			if p.CredentialsFile != "" && p.EmulatorHost == "" {
				opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, p.CredentialsFile))
			}
			var err error
			if c, err = pubsub.NewClient(ctx, p.Project, opts...); err != nil {
				return nil, fmt.Errorf("%s.pubsub: %w", field, err)
			}
			clients[project] = c
			b.closers = append(b.closers, func() { c.Close() })
		}
		pb := pubsubbus.Open(c, pubsubbus.Options{Prefix: p.Prefix, Retention: p.Retention, MaxAttempts: p.MaxAttempts, Concurrency: p.Concurrency})
		b.closers = append(b.closers, pb.Close)
		return pb, nil
	}
	type awsClients struct {
		sqs *sqs.Client
		sns *sns.Client
	}
	accounts := map[awsAccount]awsClients{}
	awsBus := func(field string, a *config.AWSBus) (*awsbus.Bus, error) {
		access, err := config.Secret(a.AccessKeyFile)
		if err != nil {
			return nil, fmt.Errorf("%s.aws.access_key_file: %w", field, err)
		}
		secret, err := config.Secret(a.SecretKeyFile)
		if err != nil {
			return nil, fmt.Errorf("%s.aws.secret_key_file: %w", field, err)
		}
		account := awsAccount{a.Region, a.Endpoint, access, secret}
		c, ok := accounts[account]
		if !ok {
			opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(a.Region)}
			if access != "" {
				opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(access, secret, "")))
			}
			cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
			if err != nil {
				return nil, fmt.Errorf("%s.aws: %w", field, err)
			}
			var endpoint *string
			if a.Endpoint != "" {
				endpoint = &a.Endpoint
			}
			c = awsClients{
				sqs: sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = endpoint }),
				sns: sns.NewFromConfig(cfg, func(o *sns.Options) { o.BaseEndpoint = endpoint }),
			}
			accounts[account] = c
		}
		return awsbus.New(c.sqs, c.sns, awsbus.Options{Prefix: a.Prefix, Retention: a.Retention, MaxAttempts: a.MaxAttempts,
			Concurrency: a.Concurrency}), nil
	}
	kafkaBus := func(field string, k *config.KafkaBus) (*kafkabus.Bus, error) {
		conn := []kgo.Opt{kgo.SeedBrokers(k.Brokers...)}
		if k.TLS {
			tc := &tls.Config{MinVersion: tls.VersionTLS12}
			if k.CAFile != "" {
				pem, err := os.ReadFile(k.CAFile)
				if err != nil {
					return nil, fmt.Errorf("%s.kafka.ca_file: %w", field, err)
				}
				tc.RootCAs = x509.NewCertPool()
				if !tc.RootCAs.AppendCertsFromPEM(pem) {
					return nil, fmt.Errorf("%s.kafka.ca_file: no certificate in it", field)
				}
			}
			conn = append(conn, kgo.DialTLSConfig(tc))
		}
		if s := k.SASL; s != nil {
			switch s.Mechanism {
			case "aws-msk-iam":
				cfg, err := awsconfig.LoadDefaultConfig(ctx)
				if err != nil {
					return nil, fmt.Errorf("%s.kafka.sasl: %w", field, err)
				}
				conn = append(conn, kgo.SASL(saslaws.ManagedStreamingIAM(func(ctx context.Context) (saslaws.Auth, error) {
					c, err := cfg.Credentials.Retrieve(ctx)
					return saslaws.Auth{AccessKey: c.AccessKeyID, SecretKey: c.SecretAccessKey, SessionToken: c.SessionToken}, err
				})))
			default:
				password, err := config.Secret(s.PasswordFile)
				if err != nil {
					return nil, fmt.Errorf("%s.kafka.sasl.password_file: %w", field, err)
				}
				switch s.Mechanism {
				case "plain":
					conn = append(conn, kgo.SASL(plain.Auth{User: s.Username, Pass: password}.AsMechanism()))
				case "scram-sha-256":
					conn = append(conn, kgo.SASL(scram.Auth{User: s.Username, Pass: password}.AsSha256Mechanism()))
				case "scram-sha-512":
					conn = append(conn, kgo.SASL(scram.Auth{User: s.Username, Pass: password}.AsSha512Mechanism()))
				default:
					return nil, fmt.Errorf("%s.kafka.sasl.mechanism: %q is none of plain, scram-sha-256, scram-sha-512, and aws-msk-iam", field, s.Mechanism)
				}
			}
		}
		kb, err := kafkabus.Open(conn, kafkabus.Options{Prefix: k.Prefix, Partitions: k.Partitions, ReplicationFactor: k.ReplicationFactor,
			Retention: k.Retention, Concurrency: k.Concurrency})
		if err != nil {
			return nil, fmt.Errorf("%s.kafka: %w", field, err)
		}
		b.closers = append(b.closers, kb.Close)
		return kb, nil
	}
	for _, s := range []struct {
		field   string
		backend config.BusBackend
		set     func(stream bus.Stream, queue bus.Queue)
	}{
		{"bus.stream", c.Stream, func(s bus.Stream, _ bus.Queue) { b.Stream, b.Signals = s, s.(bus.Signals) }},
		{"bus.queue", c.Queue, func(_ bus.Stream, q bus.Queue) { b.Queue = q }},
	} {
		switch s.backend.Type {
		case "", "postgres":
			p, err := postgres()
			if err != nil {
				b.Close()
				return nil, err
			}
			s.set(p, p)
		case "nats":
			if s.backend.NATS == nil {
				b.Close()
				return nil, fmt.Errorf("%s.nats: the NATS servers, for type nats", s.field)
			}
			n, err := natsBus(s.field, s.backend.NATS)
			if err != nil {
				b.Close()
				return nil, err
			}
			s.set(n, n)
		case "pubsub":
			if s.backend.PubSub == nil {
				b.Close()
				return nil, fmt.Errorf("%s.pubsub: the Google Cloud project, for type pubsub", s.field)
			}
			p, err := pubsubBus(s.field, s.backend.PubSub)
			if err != nil {
				b.Close()
				return nil, err
			}
			s.set(p, p)
		case "aws":
			if s.backend.AWS == nil {
				b.Close()
				return nil, fmt.Errorf("%s.aws: the AWS region, for type aws", s.field)
			}
			a, err := awsBus(s.field, s.backend.AWS)
			if err != nil {
				b.Close()
				return nil, err
			}
			s.set(a, a)
		case "kafka":
			if s.field == "bus.queue" || s.backend.Kafka == nil {
				b.Close()
				return nil, fmt.Errorf("%s: kafka carries the stream alone, from its brokers", s.field)
			}
			k, err := kafkaBus(s.field, s.backend.Kafka)
			if err != nil {
				b.Close()
				return nil, err
			}
			s.set(k, nil)
		default:
			b.Close()
			return nil, fmt.Errorf("%s.type: %q is none of postgres, nats, pubsub, aws, and kafka", s.field, s.backend.Type)
		}
	}
	return b, nil
}
