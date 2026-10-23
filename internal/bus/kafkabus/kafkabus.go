// Package kafkabus is the bus's stream (internal/bus) on Kafka (docs/hosted.md, "Backends"): Apache
// Kafka, Amazon MSK, Confluent, Redpanda. Kafka acknowledges no single message and keeps no
// visibility, so it carries no queue: an installation on Kafka takes its queue from another backend.
//
// A topic is a Kafka topic, its events keyed by what they are about, and a group a consumer group of
// its own on it, which CreateGroup starts at the topic's end. A member handles a fetch's events up
// to Concurrency at once — the stream promises no order, and a consumer applies each by its key's
// cursor — and commits what it fetched once every event of it is handled or set aside. A handler's error sets its event aside on the group's retry topic,
// due after a backoff, which a second consumer of the same Subscribe handles once due — so a refused
// event delays no other event behind it, and a set-aside one is handled at least once more.
package kafkabus

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/Cidan/ok/internal/bus"
)

// Options shape a Bus.
type Options struct {
	// Prefix begins every topic and group name, so installations sharing a cluster keep apart.
	// Default "ok".
	Prefix string
	// Partitions is how many partitions a topic is made with. Default 12.
	Partitions int32
	// ReplicationFactor is how many brokers keep each partition; -1, the default, is the cluster's.
	ReplicationFactor int16
	// Retention is how long a topic keeps its events. Default seven days.
	Retention time.Duration
	// Concurrency is how many of a group's events a member handles at once, and fetches at most.
	// Default 32.
	Concurrency int
}

func (o *Options) defaults() {
	if o.Prefix == "" {
		o.Prefix = "ok"
	}
	if o.Partitions <= 0 {
		o.Partitions = 12
	}
	if o.ReplicationFactor == 0 {
		o.ReplicationFactor = -1
	}
	if o.Retention <= 0 {
		o.Retention = 7 * 24 * time.Hour
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 32
	}
}

// Headers an event carries.
const (
	hdrKey       = "ok-key"
	hdrSeq       = "ok-seq"
	hdrAttempt   = "ok-attempt"    // deliveries so far, on the retry topic
	hdrNotBefore = "ok-not-before" // when it is due, in Unix milliseconds, on the retry topic
)

// maxName bounds a topic's name, Kafka's 249 bytes.
const maxName = 249

// Bus is a bus.Stream on a Kafka cluster.
type Bus struct {
	conn []kgo.Opt // how to reach the cluster: its brokers, TLS, SASL
	cl   *kgo.Client
	adm  *kadm.Client
	opts Options

	mu     sync.Mutex
	topics map[string]bool // made
}

var _ bus.Stream = (*Bus)(nil)

// Open is a bus on the cluster conn reaches.
func Open(conn []kgo.Opt, opts Options) (*Bus, error) {
	opts.defaults()
	cl, err := kgo.NewClient(conn...)
	if err != nil {
		return nil, fmt.Errorf("kafkabus: %w", err)
	}
	return &Bus{conn: conn, cl: cl, adm: kadm.NewClient(cl), opts: opts, topics: map[string]bool{}}, nil
}

// Close closes the bus's client, flushing what it holds.
func (b *Bus) Close() { b.cl.Close() }

// token is s as part of a Kafka name: letters, digits, and '-' as they are, every other byte '_'
// and its two hex digits.
func token(s string) string {
	var out strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' {
			out.WriteByte(c)
		} else {
			fmt.Fprintf(&out, "_%02x", c)
		}
	}
	return out.String()
}

// name joins parts with '.' as a name of at most maxName bytes: one longer keeps its start and ends
// in a hash of the whole, so every replica names it alike.
func name(parts ...string) string {
	for i, p := range parts {
		parts[i] = token(p)
	}
	n := strings.Join(parts, ".")
	if len(n) <= maxName {
		return n
	}
	sum := sha256.Sum256([]byte(n))
	return n[:maxName-17] + "." + hex.EncodeToString(sum[:8])
}

func (b *Bus) eventTopic(topic string) string { return name(b.opts.Prefix, "ev", topic) }
func (b *Bus) retryTopic(topic, group string) string {
	return name(b.opts.Prefix, "retry", topic, group)
}
func (b *Bus) groupID(topic, group string) string { return name(b.opts.Prefix, "ev", topic, group) }
func (b *Bus) retryGroupID(topic, group string) string {
	return name(b.opts.Prefix, "retry", topic, group)
}

// ensureTopic makes a topic of partitions unless this bus made it or it exists.
func (b *Bus) ensureTopic(ctx context.Context, topic string, partitions int32) error {
	b.mu.Lock()
	done := b.topics[topic]
	b.mu.Unlock()
	if done {
		return nil
	}
	ms := strconv.FormatInt(b.opts.Retention.Milliseconds(), 10)
	resp, err := b.adm.CreateTopic(ctx, partitions, b.opts.ReplicationFactor, map[string]*string{"retention.ms": &ms}, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("kafkabus: topic %s: %w", topic, err)
	}
	b.mu.Lock()
	b.topics[topic] = true
	b.mu.Unlock()
	return nil
}

func header(r *kgo.Record, k string) string {
	for _, h := range r.Headers {
		if h.Key == k {
			return string(h.Value)
		}
	}
	return ""
}

// Publish publishes events, each keyed by what it is about, waiting until the cluster holds them.
func (b *Bus) Publish(ctx context.Context, events ...bus.Event) error {
	recs := make([]*kgo.Record, len(events))
	for i, e := range events {
		topic := b.eventTopic(e.Topic)
		if err := b.ensureTopic(ctx, topic, b.opts.Partitions); err != nil {
			return err
		}
		recs[i] = &kgo.Record{Topic: topic, Key: binary.BigEndian.AppendUint64(nil, uint64(e.Key)), Value: e.Body, Headers: []kgo.RecordHeader{
			{Key: hdrKey, Value: []byte(strconv.FormatInt(e.Key, 10))}, {Key: hdrSeq, Value: []byte(strconv.FormatInt(e.Seq, 10))}}}
	}
	if err := b.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("kafkabus: publishing: %w", err)
	}
	return nil
}

// CreateGroup makes group on topic: its consumer group starts at the topic's end, and its retry
// topic is made. A group that has committed a partition's offset keeps it. Two replicas making a
// new group at once each start it at the end they list, the later's skipping what is published
// between — which a consumer, reading its repositories' logs from its cursors, fills.
func (b *Bus) CreateGroup(ctx context.Context, topic, group string) error {
	t := b.eventTopic(topic)
	if err := b.ensureTopic(ctx, t, b.opts.Partitions); err != nil {
		return err
	}
	if err := b.ensureTopic(ctx, b.retryTopic(topic, group), 1); err != nil {
		return err
	}
	gid := b.groupID(topic, group)
	// A group no member has joined has committed nothing; brokers answer with no offsets, or that
	// they do not know it.
	committed, err := b.adm.FetchOffsets(ctx, gid)
	if err != nil && !errors.Is(err, kerr.GroupIDNotFound) {
		return fmt.Errorf("kafkabus: group %s: %w", gid, err)
	}
	ends, err := b.adm.ListEndOffsets(ctx, t)
	if err != nil {
		return fmt.Errorf("kafkabus: group %s: %w", gid, err)
	}
	var start kadm.Offsets
	ends.Each(func(lo kadm.ListedOffset) {
		if c, ok := committed.Lookup(lo.Topic, lo.Partition); !ok || c.Err != nil || c.At < 0 {
			start.Add(kadm.Offset{Topic: lo.Topic, Partition: lo.Partition, At: lo.Offset, LeaderEpoch: -1})
		}
	})
	if len(start) == 0 {
		return nil
	}
	err = b.adm.CommitAllOffsets(ctx, gid, start)
	// A group with members commits for itself: its partitions with no offset are read from their
	// start, which delivers again only what a consumer has, by its cursor.
	if err != nil && !errors.Is(err, kerr.UnknownMemberID) && !errors.Is(err, kerr.IllegalGeneration) && !errors.Is(err, kerr.RebalanceInProgress) {
		return fmt.Errorf("kafkabus: starting group %s at the end: %w", gid, err)
	}
	return nil
}

// backoff is how long an event set aside on its attempt-th delivery waits.
func backoff(attempt int) time.Duration {
	return min(time.Second<<min(max(attempt-1, 0), 6), time.Minute)
}

// deliver hands r, an event of topic, to h; a record not of this package's is nothing to hand.
func deliver(ctx context.Context, topic string, r *kgo.Record, h bus.Handler) error {
	key, kerr := strconv.ParseInt(header(r, hdrKey), 10, 64)
	seq, serr := strconv.ParseInt(header(r, hdrSeq), 10, 64)
	if kerr != nil || serr != nil {
		return nil
	}
	return h(ctx, bus.Event{Topic: topic, Key: key, Seq: seq, Body: r.Value})
}

// setAside puts r on the group's retry topic, its attempt-th delivery done, due after a backoff.
func (b *Bus) setAside(ctx context.Context, retry string, r *kgo.Record, attempt int) error {
	headers := []kgo.RecordHeader{
		{Key: hdrKey, Value: []byte(header(r, hdrKey))}, {Key: hdrSeq, Value: []byte(header(r, hdrSeq))},
		{Key: hdrAttempt, Value: []byte(strconv.Itoa(attempt))},
		{Key: hdrNotBefore, Value: []byte(strconv.FormatInt(time.Now().Add(backoff(attempt)).UnixMilli(), 10))},
	}
	if err := b.cl.ProduceSync(ctx, &kgo.Record{Topic: retry, Key: r.Key, Value: r.Value, Headers: headers}).FirstErr(); err != nil {
		return fmt.Errorf("kafkabus: setting an event aside: %w", err)
	}
	return nil
}

// fetchErr is the first error of fetches that is not ctx's ending.
func fetchErr(ctx context.Context, fs kgo.Fetches) error {
	for _, e := range fs.Errors() {
		if ctx.Err() != nil || errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, kgo.ErrClientClosed) {
			continue
		}
		return fmt.Errorf("kafkabus: fetching %s/%d: %w", e.Topic, e.Partition, e.Err)
	}
	return nil
}

func (b *Bus) consumer(gid, topic string, opts ...kgo.Opt) (*kgo.Client, error) {
	cl, err := kgo.NewClient(append(slices.Clone(b.conn), append([]kgo.Opt{kgo.ConsumerGroup(gid), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxWait(time.Second)}, opts...)...)...)
	if err != nil {
		return nil, fmt.Errorf("kafkabus: consumer of %s: %w", gid, err)
	}
	return cl, nil
}

// Subscribe handles topic's events as a member of group until ctx ends: those the topic carries,
// and those the group set aside once they are due.
func (b *Bus) Subscribe(ctx context.Context, topic, group string, h bus.Handler) error {
	if err := b.CreateGroup(ctx, topic, group); err != nil {
		return err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return b.consume(gctx, topic, group, h) })
	g.Go(func() error { return b.retry(gctx, topic, group, h) })
	err := g.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// consume handles the topic's events, a fetch's up to Concurrency at once, a refused event set
// aside; then it commits what it fetched, rebalancing only between fetches.
func (b *Bus) consume(ctx context.Context, topic, group string, h bus.Handler) error {
	cl, err := b.consumer(b.groupID(topic, group), b.eventTopic(topic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	if err != nil {
		return err
	}
	// Leaving the group is a rebalance, which the last poll may hold.
	defer func() { cl.AllowRebalance(); cl.Close() }()
	retry := b.retryTopic(topic, group)
	for {
		fs := cl.PollRecords(ctx, b.opts.Concurrency)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fetchErr(ctx, fs); err != nil {
			return err
		}
		var g errgroup.Group
		g.SetLimit(b.opts.Concurrency)
		for _, r := range fs.Records() {
			g.Go(func() error {
				if deliver(ctx, topic, r, h) == nil {
					return nil
				}
				return b.setAside(ctx, retry, r, 1)
			})
		}
		// An event neither handled nor set aside leaves the fetch uncommitted, delivered again.
		if err := g.Wait(); err != nil {
			return err
		}
		if err := cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			return fmt.Errorf("kafkabus: committing %s/%s: %w", topic, group, err)
		}
		cl.AllowRebalance()
	}
}

// retry handles the group's set-aside events as they come due, setting aside again, a longer
// backoff on, any refused anew. It marks each as it finishes it, which its client commits.
func (b *Bus) retry(ctx context.Context, topic, group string, h bus.Handler) error {
	retry := b.retryTopic(topic, group)
	cl, err := b.consumer(b.retryGroupID(topic, group), retry, kgo.AutoCommitMarks())
	if err != nil {
		return err
	}
	defer cl.Close()
	for {
		fs := cl.PollRecords(ctx, 100)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fetchErr(ctx, fs); err != nil {
			return err
		}
		for _, r := range fs.Records() {
			if ms, err := strconv.ParseInt(header(r, hdrNotBefore), 10, 64); err == nil {
				if wait := time.Until(time.UnixMilli(ms)); wait > 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(wait):
					}
				}
			}
			if deliver(ctx, topic, r, h) != nil {
				attempt, _ := strconv.Atoi(header(r, hdrAttempt))
				if err := b.setAside(ctx, retry, r, max(attempt, 1)+1); err != nil {
					return err
				}
			}
			cl.MarkCommitRecords(r)
		}
	}
}
