// Package pubsubbus is the bus (internal/bus) on Google Pub/Sub (docs/hosted.md, "Backends").
//
// A topic is a Pub/Sub topic of its own, and a group a subscription to it, which receives what is
// published after it was made; a handler's error nacks its event, delivered again by the
// subscription's retry policy.
//
// A queue is a topic and a subscription read by synchronous pulls. A job's ack deadline is its
// lease: set to the visibility asked for, up to Pub/Sub's ten minutes, and renewed until the
// visibility has passed — at least ten seconds, Pub/Sub's shortest deadline. Past it a lease is
// ended, and Ack, Extend, and Nack say bus.ErrLeaseLost. Pub/Sub counts a message's deliveries only
// on a subscription with a dead-letter policy, so each queue's has one, at its attempts (no fewer
// than Pub/Sub's five); a job nacked on its last attempt is published to the queue's dead letters —
// a topic that keeps what it is sent — and acknowledged. Pub/Sub delays nothing, so a job not due
// waits on the queue's delay topic, hidden by its ack deadline, until a Lease moves it onto the
// queue — so a job is due within the subscriptions' retention, seven days at most — and a Nack
// delayed past ten minutes goes the same way, carrying its attempts with it.
// Pub/Sub drops no duplicate publish, so the queue drops no duplicate ID.
package pubsubbus

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/Cidan/ok/internal/bus"
)

// Options shape a Bus.
type Options struct {
	// Prefix begins every topic and subscription name, so installations sharing a project keep
	// apart. Default "ok".
	Prefix string
	// Retention is how long a subscription keeps what it has not acknowledged, and the dead
	// letters their topic: at most seven days. Default seven days.
	Retention time.Duration
	// MaxAttempts is how many deliveries a job gets before it is a dead letter: at most 100.
	// Default 10.
	MaxAttempts int
	// AckDeadline is a group's deliveries' deadline, which the client extends while a handler
	// works. Default a minute.
	AckDeadline time.Duration
	// Concurrency is how many of a group's deliveries a member handles at once, and holds
	// unacknowledged. Default 32.
	Concurrency int
}

func (o *Options) defaults() {
	if o.Prefix == "" {
		o.Prefix = "ok"
	}
	if o.Retention <= 0 || o.Retention > 7*24*time.Hour {
		o.Retention = 7 * 24 * time.Hour
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 10
	}
	o.MaxAttempts = min(o.MaxAttempts, 100)
	if o.AckDeadline <= 0 {
		o.AckDeadline = time.Minute
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 32
	}
}

// Pub/Sub's bounds on an ack deadline.
const (
	minDeadline = 10 * time.Second
	maxDeadline = 600 * time.Second
)

// Attributes a message carries.
const (
	attrKey       = "ok-key"
	attrSeq       = "ok-seq"
	attrID        = "ok-id"
	attrNotBefore = "ok-not-before"
	attrAttempts  = "ok-attempts" // deliveries before it was moved onto a topic anew
)

// Bus is a bus.Stream and a bus.Queue on one Pub/Sub client.
type Bus struct {
	c    *pubsub.Client
	opts Options

	mu         sync.Mutex
	made       map[string]bool // topics and subscriptions made
	publishers map[string]*pubsub.Publisher
}

var (
	_ bus.Stream  = (*Bus)(nil)
	_ bus.Queue   = (*Bus)(nil)
	_ bus.Signals = (*Bus)(nil)
)

// Open is a bus on c's project.
func Open(c *pubsub.Client, opts Options) *Bus {
	opts.defaults()
	return &Bus{c: c, opts: opts, made: map[string]bool{}, publishers: map[string]*pubsub.Publisher{}}
}

// Close stops the bus's publishers, sending what they hold.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.publishers {
		p.Stop()
	}
}

// token is name as part of a Pub/Sub resource name: letters, digits, '_', and '-' as they are,
// every other byte '~' and its two hex digits.
func token(name string) string {
	var s strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			s.WriteByte(c)
		} else {
			fmt.Fprintf(&s, "~%02x", c)
		}
	}
	return s.String()
}

func (b *Bus) topicName(id string) string { return "projects/" + b.c.Project() + "/topics/" + id }
func (b *Bus) subName(id string) string   { return "projects/" + b.c.Project() + "/subscriptions/" + id }

// ensureTopic makes a topic unless this bus made it or it exists; retention, when set, has it keep
// what it is sent though no subscription takes it.
func (b *Bus) ensureTopic(ctx context.Context, name string, retention time.Duration) error {
	b.mu.Lock()
	done := b.made[name]
	b.mu.Unlock()
	if done {
		return nil
	}
	t := &pubsubpb.Topic{Name: name}
	if retention > 0 {
		t.MessageRetentionDuration = durationpb.New(retention)
	}
	if _, err := b.c.TopicAdminClient.CreateTopic(ctx, t); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("pubsubbus: topic %s: %w", name, err)
	}
	b.mu.Lock()
	b.made[name] = true
	b.mu.Unlock()
	return nil
}

// ensureSub makes a subscription unless this bus made it or it exists; one that exists is taken as
// it is.
func (b *Bus) ensureSub(ctx context.Context, s *pubsubpb.Subscription) error {
	b.mu.Lock()
	done := b.made[s.Name]
	b.mu.Unlock()
	if done {
		return nil
	}
	if _, err := b.c.SubscriptionAdminClient.CreateSubscription(ctx, s); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("pubsubbus: subscription %s: %w", s.Name, err)
	}
	b.mu.Lock()
	b.made[s.Name] = true
	b.mu.Unlock()
	return nil
}

func (b *Bus) publisher(topic string) *pubsub.Publisher {
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.publishers[topic]
	if p == nil {
		p = b.c.Publisher(topic)
		b.publishers[topic] = p
	}
	return p
}

// publish publishes messages to topics, waiting until Pub/Sub holds each.
func (b *Bus) publish(ctx context.Context, topics []string, msgs []*pubsub.Message) error {
	results := make([]*pubsub.PublishResult, len(msgs))
	for i, m := range msgs {
		results[i] = b.publisher(topics[i]).Publish(ctx, m)
	}
	for i, r := range results {
		if _, err := r.Get(ctx); err != nil {
			return fmt.Errorf("pubsubbus: publishing to %s: %w", topics[i], err)
		}
	}
	return nil
}

// ─── The stream ───

func (b *Bus) eventTopic(topic string) string {
	return b.topicName(b.opts.Prefix + "-ev-" + token(topic))
}
func (b *Bus) groupSub(topic, group string) string {
	return b.subName(b.opts.Prefix + "-ev-" + token(topic) + "-" + token(group))
}

// Publish publishes events.
func (b *Bus) Publish(ctx context.Context, events ...bus.Event) error {
	topics := make([]string, len(events))
	msgs := make([]*pubsub.Message, len(events))
	for i, e := range events {
		topics[i] = b.eventTopic(e.Topic)
		if err := b.ensureTopic(ctx, topics[i], 0); err != nil {
			return err
		}
		msgs[i] = &pubsub.Message{Data: e.Body, Attributes: map[string]string{
			attrKey: strconv.FormatInt(e.Key, 10), attrSeq: strconv.FormatInt(e.Seq, 10)}}
	}
	return b.publish(ctx, topics, msgs)
}

// CreateGroup makes group's subscription to topic, which receives what is published from now on.
func (b *Bus) CreateGroup(ctx context.Context, topic, group string) error {
	t := b.eventTopic(topic)
	if err := b.ensureTopic(ctx, t, 0); err != nil {
		return err
	}
	return b.ensureSub(ctx, &pubsubpb.Subscription{
		Name: b.groupSub(topic, group), Topic: t, AckDeadlineSeconds: int32(b.opts.AckDeadline / time.Second),
		MessageRetentionDuration: durationpb.New(b.opts.Retention),
		RetryPolicy:              &pubsubpb.RetryPolicy{MinimumBackoff: durationpb.New(time.Second), MaximumBackoff: durationpb.New(time.Minute)},
	})
}

// Subscribe handles topic's events as a member of group until ctx ends.
func (b *Bus) Subscribe(ctx context.Context, topic, group string, h bus.Handler) error {
	if err := b.CreateGroup(ctx, topic, group); err != nil {
		return err
	}
	s := b.c.Subscriber(b.groupSub(topic, group))
	// The client runs a callback for each delivery it holds, up to what flow control lets it hold:
	// the stream promises no order, and a consumer applies each by its key's cursor.
	s.ReceiveSettings.MaxOutstandingMessages = b.opts.Concurrency
	err := s.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		key, kerr := strconv.ParseInt(m.Attributes[attrKey], 10, 64)
		seq, serr := strconv.ParseInt(m.Attributes[attrSeq], 10, 64)
		if kerr != nil || serr != nil {
			m.Ack() // not an event of this package's: nothing a retry would change
			return
		}
		if h(ctx, bus.Event{Topic: topic, Key: key, Seq: seq, Body: m.Data}) == nil {
			m.Ack()
		} else {
			m.Nack()
		}
	})
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("pubsubbus: receiving %s/%s: %w", topic, group, err)
	}
	return ctx.Err()
}

// ─── The queue ───

func (b *Bus) queueTopic(queue string) string {
	return b.topicName(b.opts.Prefix + "-q-" + token(queue))
}
func (b *Bus) queueSub(queue string) string { return b.subName(b.opts.Prefix + "-q-" + token(queue)) }
func (b *Bus) laterTopic(queue string) string {
	return b.topicName(b.opts.Prefix + "-q-" + token(queue) + "-later")
}
func (b *Bus) laterSub(queue string) string {
	return b.subName(b.opts.Prefix + "-q-" + token(queue) + "-later")
}
func (b *Bus) deadTopic(queue string) string {
	return b.topicName(b.opts.Prefix + "-dead-" + token(queue))
}

// ensureQueue makes a queue's topics and subscriptions.
func (b *Bus) ensureQueue(ctx context.Context, queue string) error {
	dead := b.deadTopic(queue)
	if err := b.ensureTopic(ctx, dead, b.opts.Retention); err != nil {
		return err
	}
	// Only the queue's subscription counts deliveries: a delayed job is delivered every ten minutes
	// while it waits, which no attempt is.
	for _, q := range []struct {
		topic, sub string
		dead       *pubsubpb.DeadLetterPolicy
	}{
		{b.queueTopic(queue), b.queueSub(queue), &pubsubpb.DeadLetterPolicy{DeadLetterTopic: dead, MaxDeliveryAttempts: int32(max(b.opts.MaxAttempts, 5))}},
		{b.laterTopic(queue), b.laterSub(queue), nil},
	} {
		if err := b.ensureTopic(ctx, q.topic, 0); err != nil {
			return err
		}
		if err := b.ensureSub(ctx, &pubsubpb.Subscription{
			Name: q.sub, Topic: q.topic, AckDeadlineSeconds: int32(minDeadline / time.Second),
			MessageRetentionDuration: durationpb.New(b.opts.Retention), DeadLetterPolicy: q.dead,
		}); err != nil {
			return err
		}
	}
	return nil
}

func jobMessage(j bus.Job, attempts int) *pubsub.Message {
	m := &pubsub.Message{Data: j.Body, Attributes: map[string]string{attrKey: strconv.FormatInt(j.Key, 10), attrID: j.ID}}
	if attempts > 0 {
		m.Attributes[attrAttempts] = strconv.Itoa(attempts)
	}
	if !j.NotBefore.IsZero() {
		m.Attributes[attrNotBefore] = j.NotBefore.UTC().Format(time.RFC3339Nano)
	}
	return m
}

// Enqueue adds jobs: those due onto their queues, the rest onto their delay topics.
func (b *Bus) Enqueue(ctx context.Context, jobs ...bus.Job) error {
	topics := make([]string, len(jobs))
	msgs := make([]*pubsub.Message, len(jobs))
	for i, j := range jobs {
		if err := b.ensureQueue(ctx, j.Queue); err != nil {
			return err
		}
		topics[i] = b.queueTopic(j.Queue)
		if j.NotBefore.After(time.Now()) {
			topics[i] = b.laterTopic(j.Queue)
		}
		msgs[i] = jobMessage(j, 0)
	}
	return b.publish(ctx, topics, msgs)
}

// pull takes up to n messages of a subscription, waiting up to wait for one.
func (b *Bus) pull(ctx context.Context, sub string, n int, wait time.Duration) ([]*pubsubpb.ReceivedMessage, error) {
	deadline := time.Now().Add(wait)
	for {
		pctx, cancel := context.WithTimeout(ctx, max(min(time.Until(deadline), 10*time.Second), time.Second))
		resp, err := b.c.SubscriptionAdminClient.Pull(pctx, &pubsubpb.PullRequest{Subscription: sub, MaxMessages: int32(n), ReturnImmediately: wait <= 0})
		cancel()
		if err != nil && status.Code(err) != codes.DeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("pubsubbus: pulling %s: %w", sub, err)
		}
		if resp != nil && len(resp.ReceivedMessages) > 0 {
			return resp.ReceivedMessages, nil
		}
		if wait <= 0 || !time.Now().Before(deadline) || ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
}

func (b *Bus) modify(ctx context.Context, sub string, ackIDs []string, d time.Duration) error {
	return b.c.SubscriptionAdminClient.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: ackIDs, AckDeadlineSeconds: int32(min(d, maxDeadline) / time.Second)})
}

func (b *Bus) ack(ctx context.Context, sub string, ackIDs ...string) error {
	return b.c.SubscriptionAdminClient.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{Subscription: sub, AckIds: ackIDs})
}

// promote moves a queue's delayed jobs that are due onto it, and hides the rest until they are, as
// far as an ack deadline reaches.
func (b *Bus) promote(ctx context.Context, queue string) error {
	sub := b.laterSub(queue)
	msgs, err := b.pull(ctx, sub, 100, 0)
	if err != nil {
		return err
	}
	for _, rm := range msgs {
		m := rm.Message
		due, err := time.Parse(time.RFC3339Nano, m.Attributes[attrNotBefore])
		if err != nil || !due.After(time.Now()) {
			attrs := map[string]string{}
			for k, v := range m.Attributes {
				if k != attrNotBefore {
					attrs[k] = v
				}
			}
			if err := b.publish(ctx, []string{b.queueTopic(queue)}, []*pubsub.Message{{Data: m.Data, Attributes: attrs}}); err != nil {
				return err
			}
			if err := b.ack(ctx, sub, rm.AckId); err != nil {
				return fmt.Errorf("pubsubbus: moving a due job onto %s: %w", queue, err)
			}
			continue
		}
		if err := b.modify(ctx, sub, []string{rm.AckId}, max(time.Until(due), minDeadline)); err != nil {
			return fmt.Errorf("pubsubbus: holding a delayed job of %s: %w", queue, err)
		}
	}
	return nil
}

// Lease moves queue's due delayed jobs onto it, then takes up to o.Max of its ready jobs.
func (b *Bus) Lease(ctx context.Context, queue string, o bus.LeaseOptions) ([]bus.Leased, error) {
	if err := b.ensureQueue(ctx, queue); err != nil {
		return nil, err
	}
	if err := b.promote(ctx, queue); err != nil {
		return nil, err
	}
	sub := b.queueSub(queue)
	msgs, err := b.pull(ctx, sub, max(o.Max, 1), o.Wait)
	if err != nil || len(msgs) == 0 {
		return nil, err
	}
	vis := max(o.Visibility, minDeadline)
	ackIDs := make([]string, len(msgs))
	for i, rm := range msgs {
		ackIDs[i] = rm.AckId
	}
	if err := b.modify(ctx, sub, ackIDs, vis); err != nil {
		return nil, fmt.Errorf("pubsubbus: leasing from %s: %w", queue, err)
	}
	out := make([]bus.Leased, len(msgs))
	for i, rm := range msgs {
		m := rm.Message
		key, _ := strconv.ParseInt(m.Attributes[attrKey], 10, 64)
		base, _ := strconv.Atoi(m.Attributes[attrAttempts])
		l := &leased{b: b, sub: sub, ackID: rm.AckId, attempt: base + max(int(rm.DeliveryAttempt), 1),
			job: bus.Job{ID: m.Attributes[attrID], Queue: queue, Key: key, Body: m.Data}, deadline: time.Now().Add(vis)}
		l.start()
		out[i] = l
	}
	return out, nil
}

// leased is a job a worker holds until its deadline, its ack deadline renewed until then.
type leased struct {
	b       *Bus
	sub     string
	ackID   string
	job     bus.Job
	attempt int

	mu       sync.Mutex
	deadline time.Time
	finished bool
	stop     chan struct{}
	stopped  chan struct{}
}

func (l *leased) Job() bus.Job { return l.job }
func (l *leased) Attempt() int { return l.attempt }

// start renews the ack deadline every few seconds until the lease's deadline passes or it is
// finished, each renewal reaching at least past the deadline or the most Pub/Sub keeps.
func (l *leased) start() {
	l.stop, l.stopped = make(chan struct{}), make(chan struct{})
	go func() {
		defer close(l.stopped)
		t := time.NewTicker(minDeadline / 3)
		defer t.Stop()
		for {
			select {
			case <-l.stop:
				return
			case <-t.C:
			}
			l.mu.Lock()
			left := time.Until(l.deadline)
			l.mu.Unlock()
			if left < 0 {
				return
			}
			_ = l.b.modify(context.Background(), l.sub, []string{l.ackID}, max(left, minDeadline))
		}
	}()
}

// end finishes the lease, reporting bus.ErrLeaseLost if it had passed its deadline or ended.
func (l *leased) end() error {
	l.mu.Lock()
	if l.finished || time.Now().After(l.deadline) {
		l.mu.Unlock()
		return bus.ErrLeaseLost
	}
	l.finished = true
	l.mu.Unlock()
	close(l.stop)
	<-l.stopped
	return nil
}

func (l *leased) Extend(ctx context.Context, d time.Duration) error {
	d = max(d, minDeadline)
	l.mu.Lock()
	if l.finished || time.Now().After(l.deadline) {
		l.mu.Unlock()
		return bus.ErrLeaseLost
	}
	l.deadline = time.Now().Add(d)
	l.mu.Unlock()
	if err := l.b.modify(ctx, l.sub, []string{l.ackID}, d); err != nil {
		return fmt.Errorf("pubsubbus: extending: %w", err)
	}
	return nil
}

func (l *leased) Ack(ctx context.Context) error {
	if err := l.end(); err != nil {
		return err
	}
	if err := l.b.ack(ctx, l.sub, l.ackID); err != nil {
		return fmt.Errorf("pubsubbus: acknowledging: %w", err)
	}
	return nil
}

func (l *leased) Nack(ctx context.Context, delay time.Duration) error {
	if err := l.end(); err != nil {
		return err
	}
	switch {
	case l.attempt >= l.b.opts.MaxAttempts:
		if err := l.b.publish(ctx, []string{l.b.deadTopic(l.job.Queue)}, []*pubsub.Message{jobMessage(l.job, l.attempt)}); err != nil {
			return err
		}
	case delay > maxDeadline:
		// Past an ack deadline's reach: onto the delay topic, its attempts kept.
		j := l.job
		j.NotBefore = time.Now().Add(delay)
		if err := l.b.publish(ctx, []string{l.b.laterTopic(j.Queue)}, []*pubsub.Message{jobMessage(j, l.attempt)}); err != nil {
			return err
		}
	default:
		if err := l.b.modify(ctx, l.sub, []string{l.ackID}, delay); err != nil {
			return fmt.Errorf("pubsubbus: returning a job: %w", err)
		}
		return nil
	}
	if err := l.b.ack(ctx, l.sub, l.ackID); err != nil {
		return fmt.Errorf("pubsubbus: acknowledging: %w", err)
	}
	return nil
}
