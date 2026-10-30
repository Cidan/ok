// Package busconfig opens the bus (internal/bus) an ok configuration names.
package busconfig

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/pubsub/v2"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/nats-io/nats.go"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/awsbus"
	"github.com/Cidan/ok/internal/bus/natsbus"
	"github.com/Cidan/ok/internal/bus/pubsubbus"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/work"
)

// Bus is an open bus.
type Bus struct {
	Queue    bus.Queue
	closers  []func()
	withdraw []func()
}

// RegisterWork has a store's keyed work's jobs go on the bus, under prefix (work.RegisterStore),
// until the bus closes.
func (b *Bus) RegisterWork(s work.Store, prefix string) error {
	withdraw, err := work.RegisterStore(s, work.Jobs{Queue: b.Queue, Prefix: prefix})
	if err == nil {
		b.withdraw = append(b.withdraw, withdraw)
	}
	return err
}

// Close withdraws what was registered on the bus — another replica's registration of the same
// database, a test's beside it, stands — closes the process's inboxes, removing the queue that
// carried them, and what Open opened.
func (b *Bus) Close() {
	for _, w := range b.withdraw {
		w()
	}
	b.withdraw = nil
	if b.Queue != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := b.Queue.CloseInboxes(ctx); err != nil {
			slog.Warn("the bus: closing the process's inboxes", "err", err)
		}
		cancel()
	}
	for i := len(b.closers) - 1; i >= 0; i-- {
		b.closers[i]()
	}
}

// Open opens the bus c names.
func Open(ctx context.Context, c config.Bus) (*Bus, error) {
	b := &Bus{}
	var err error
	switch c.Type {
	case "":
		return nil, fmt.Errorf("bus.type: name the bus's backend — nats, pubsub, or aws: ok keeps its work on it (docs/state.md)")
	case "nats":
		if c.NATS == nil {
			return nil, fmt.Errorf("bus.nats: the NATS servers, for type nats")
		}
		b.Queue, err = b.nats(c.NATS)
	case "pubsub":
		if c.PubSub == nil {
			return nil, fmt.Errorf("bus.pubsub: the Google Cloud project, for type pubsub")
		}
		b.Queue, err = b.pubsub(ctx, c.PubSub)
	case "aws":
		if c.AWS == nil {
			return nil, fmt.Errorf("bus.aws: the AWS region, for type aws")
		}
		b.Queue, err = b.aws(ctx, c.AWS)
	default:
		return nil, fmt.Errorf("bus.type: %q is none of nats, pubsub, and aws", c.Type)
	}
	if err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

func (b *Bus) nats(n *config.NATSBus) (bus.Queue, error) {
	opts := []nats.Option{nats.Name("ok"), nats.MaxReconnects(-1)}
	if n.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(n.CredentialsFile))
	}
	if n.CAFile != "" {
		opts = append(opts, nats.RootCAs(n.CAFile))
	}
	nc, err := nats.Connect(n.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("bus.nats: %w", err)
	}
	b.closers = append(b.closers, nc.Close)
	return natsbus.Open(nc, natsbus.Options{Prefix: n.Prefix, Replicas: n.Replicas, Retention: n.Retention,
		AckWait: n.AckWait, MaxAttempts: n.MaxAttempts})
}

func (b *Bus) pubsub(ctx context.Context, p *config.PubSubBus) (bus.Queue, error) {
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
	c, err := pubsub.NewClient(ctx, p.Project, opts...)
	if err != nil {
		return nil, fmt.Errorf("bus.pubsub: %w", err)
	}
	b.closers = append(b.closers, func() { c.Close() })
	pb := pubsubbus.Open(c, pubsubbus.Options{Prefix: p.Prefix, Retention: p.Retention, MaxAttempts: p.MaxAttempts})
	b.closers = append(b.closers, pb.Close)
	return pb, nil
}

func (b *Bus) aws(ctx context.Context, a *config.AWSBus) (bus.Queue, error) {
	access, err := config.Secret(a.AccessKeyFile)
	if err != nil {
		return nil, fmt.Errorf("bus.aws.access_key_file: %w", err)
	}
	secret, err := config.Secret(a.SecretKeyFile)
	if err != nil {
		return nil, fmt.Errorf("bus.aws.secret_key_file: %w", err)
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(a.Region)}
	if access != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(access, secret, "")))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("bus.aws: %w", err)
	}
	var endpoint *string
	if a.Endpoint != "" {
		endpoint = &a.Endpoint
	}
	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = endpoint })
	return awsbus.New(client, awsbus.Options{Prefix: a.Prefix, Retention: a.Retention, MaxAttempts: a.MaxAttempts}), nil
}
