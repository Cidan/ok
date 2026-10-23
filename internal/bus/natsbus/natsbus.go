// Package natsbus is the bus (internal/bus) on NATS JetStream (docs/hosted.md, "Backends").
//
// A topic is a stream of its own, and a group a durable pull consumer on it that starts with what is
// published after it was made; a handler's error has its event delivered again after a backoff.
//
// The queues share one work-queue stream, a subject and a pull consumer each, the consumer's
// MaxDeliver the queue's attempts and job IDs dropped within the stream's duplicate window. A job not
// due yet is a message schedule, which the server publishes when it is due, so waiting costs no
// attempt. A consumer's ack wait is the floor of every lease: a lease renews itself (in-progress
// acks) until the visibility it was given has passed, at least an ack wait, so a worker that dies
// holds its job at most one ack wait longer, and the server delivers it again only after that. A lease that has passed its visibility is ended — the server cannot tell an
// acknowledgement of an ended delivery from one of the current — and Ack, Extend, and Nack report
// bus.ErrLeaseLost. A job nacked on its last attempt is copied to the queue's dead letters, a stream
// of their own, and terminated.
package natsbus

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/inflight"
)

// Options shape a Bus.
type Options struct {
	// Prefix begins every subject and stream name, so installations sharing an account keep apart.
	// Default "ok".
	Prefix string
	// Replicas is how many servers keep each stream: 3 in a cluster. Default 1.
	Replicas int
	// Retention is how long a topic keeps its events, and the queues their jobs and dead letters.
	// Default seven days.
	Retention time.Duration
	// MaxAttempts is how many deliveries a job gets before it is a dead letter. Default 10.
	MaxAttempts int
	// AckWait is the floor of a lease's visibility: how long a delivery is invisible unrenewed.
	// Default 30 seconds.
	AckWait time.Duration
	// Dedupe is how long a job's ID is remembered, so a job enqueued again with it is dropped.
	// Default two minutes.
	Dedupe time.Duration
	// Concurrency is how many of a group's deliveries a member handles at once. Default 16.
	Concurrency int
}

func (o *Options) defaults() {
	if o.Prefix == "" {
		o.Prefix = "ok"
	}
	if o.Replicas <= 0 {
		o.Replicas = 1
	}
	if o.Retention <= 0 {
		o.Retention = 7 * 24 * time.Hour
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 10
	}
	if o.AckWait <= 0 {
		o.AckWait = 30 * time.Second
	}
	if o.Dedupe <= 0 {
		o.Dedupe = 2 * time.Minute
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 16
	}
}

// Headers a message carries.
const (
	hdrKey = "Ok-Key"
	hdrSeq = "Ok-Seq"
	hdrID  = "Ok-Id"
)

// Bus is a bus.Stream and a bus.Queue on one NATS connection.
type Bus struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	opts Options

	mu        sync.Mutex
	streams   map[string]bool // the streams made
	consumers map[string]jetstream.Consumer
}

var (
	_ bus.Stream  = (*Bus)(nil)
	_ bus.Queue   = (*Bus)(nil)
	_ bus.Signals = (*Bus)(nil)
)

// Open is a bus on nc's JetStream.
func Open(nc *nats.Conn, opts Options) (*Bus, error) {
	opts.defaults()
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("natsbus: %w", err)
	}
	return &Bus{nc: nc, js: js, opts: opts, streams: map[string]bool{}, consumers: map[string]jetstream.Consumer{}}, nil
}

// token is name as one subject token, and a valid stream or consumer name: letters, digits, '_', and
// '-' as they are, every other byte '~' and its two hex digits.
func token(name string) string {
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "~%02x", c)
		}
	}
	if b.Len() == 0 {
		return "~"
	}
	return b.String()
}

// ensureStream makes a stream of cfg unless this bus made it already or it exists.
func (b *Bus) ensureStream(ctx context.Context, cfg jetstream.StreamConfig) error {
	b.mu.Lock()
	done := b.streams[cfg.Name]
	b.mu.Unlock()
	if done {
		return nil
	}
	if _, err := b.js.Stream(ctx, cfg.Name); errors.Is(err, jetstream.ErrStreamNotFound) {
		if _, err := b.js.CreateStream(ctx, cfg); err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
			return fmt.Errorf("natsbus: stream %s: %w", cfg.Name, err)
		}
	} else if err != nil {
		return fmt.Errorf("natsbus: stream %s: %w", cfg.Name, err)
	}
	b.mu.Lock()
	b.streams[cfg.Name] = true
	b.mu.Unlock()
	return nil
}

// consumer is a stream's durable consumer of cfg, made unless it exists; one that exists is taken as
// it is, so a group made earlier keeps its place.
func (b *Bus) consumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	key := stream + "/" + cfg.Durable
	b.mu.Lock()
	c := b.consumers[key]
	b.mu.Unlock()
	if c != nil {
		return c, nil
	}
	c, err := b.js.Consumer(ctx, stream, cfg.Durable)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		c, err = b.js.CreateConsumer(ctx, stream, cfg)
		if errors.Is(err, jetstream.ErrConsumerExists) {
			c, err = b.js.Consumer(ctx, stream, cfg.Durable)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("natsbus: consumer %s: %w", key, err)
	}
	b.mu.Lock()
	b.consumers[key] = c
	b.mu.Unlock()
	return c, nil
}

// ─── The stream ───

func (b *Bus) topicStream(topic string) (name, subject string) {
	return b.opts.Prefix + "-ev-" + token(topic), b.opts.Prefix + ".ev." + token(topic)
}

func (b *Bus) ensureTopic(ctx context.Context, topic string) (stream, subject string, err error) {
	stream, subject = b.topicStream(topic)
	return stream, subject, b.ensureStream(ctx, jetstream.StreamConfig{
		Name: stream, Subjects: []string{subject}, Retention: jetstream.LimitsPolicy,
		MaxAge: b.opts.Retention, Replicas: b.opts.Replicas, Storage: jetstream.FileStorage,
	})
}

// Publish publishes events, waiting until the server holds each.
func (b *Bus) Publish(ctx context.Context, events ...bus.Event) error {
	futures := make([]jetstream.PubAckFuture, 0, len(events))
	for _, e := range events {
		_, subject, err := b.ensureTopic(ctx, e.Topic)
		if err != nil {
			return err
		}
		m := nats.NewMsg(subject)
		m.Data = e.Body
		m.Header.Set(hdrKey, strconv.FormatInt(e.Key, 10))
		m.Header.Set(hdrSeq, strconv.FormatInt(e.Seq, 10))
		f, err := b.js.PublishMsgAsync(m)
		if err != nil {
			return fmt.Errorf("natsbus: publishing to %s: %w", e.Topic, err)
		}
		futures = append(futures, f)
	}
	for _, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return fmt.Errorf("natsbus: publishing: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// CreateGroup makes group's consumer on topic, delivering what is published from now on.
func (b *Bus) CreateGroup(ctx context.Context, topic, group string) error {
	_, err := b.group(ctx, topic, group)
	return err
}

func (b *Bus) group(ctx context.Context, topic, group string) (jetstream.Consumer, error) {
	stream, _, err := b.ensureTopic(ctx, topic)
	if err != nil {
		return nil, err
	}
	return b.consumer(ctx, stream, jetstream.ConsumerConfig{
		Durable: token(group), DeliverPolicy: jetstream.DeliverNewPolicy, AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: b.opts.AckWait, MaxDeliver: -1, MaxAckPending: -1,
	})
}

// backoff is how long an event a handler refused waits, by its deliveries so far.
func backoff(delivered uint64) time.Duration {
	return min(time.Second<<min(delivered-1, 6), time.Minute)
}

// Subscribe handles topic's events as a member of group until ctx ends.
func (b *Bus) Subscribe(ctx context.Context, topic, group string, h bus.Handler) error {
	c, err := b.group(ctx, topic, group)
	if err != nil {
		return err
	}
	// Deliveries are handled up to Concurrency at once: the stream promises no order, and a
	// consumer applies each by its key's cursor. A fetch takes only as many as there are free
	// slots, so none waits for one unrenewed.
	slots := inflight.New(b.opts.Concurrency)
	defer slots.Wait()
	for ctx.Err() == nil {
		reserved, err := slots.Reserve(ctx, b.opts.Concurrency)
		if err != nil {
			break
		}
		fctx, cancel := context.WithTimeout(ctx, time.Second)
		batch, err := c.Fetch(reserved, jetstream.FetchContext(fctx))
		if err != nil {
			cancel()
			slots.Release(reserved)
			if ctx.Err() != nil {
				break
			}
			return fmt.Errorf("natsbus: fetching %s/%s: %w", topic, group, err)
		}
		for m := range batch.Messages() {
			reserved--
			slots.Run(func() error { return b.handle(ctx, topic, m, h) })
		}
		slots.Release(reserved)
		cancel()
		if err := batch.Error(); err != nil && ctx.Err() == nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("natsbus: fetching %s/%s: %w", topic, group, err)
		}
		if err := slots.Err(); err != nil && ctx.Err() == nil {
			return err
		}
	}
	return ctx.Err()
}

// handle hands one delivery to h, renewing it while h works, and acknowledges it or has it
// delivered again after a backoff.
func (b *Bus) handle(ctx context.Context, topic string, m jetstream.Msg, h bus.Handler) error {
	key, kerr := strconv.ParseInt(m.Headers().Get(hdrKey), 10, 64)
	seq, serr := strconv.ParseInt(m.Headers().Get(hdrSeq), 10, 64)
	if kerr != nil || serr != nil {
		// Not an event of this package's: nothing a retry would change.
		return m.Term()
	}
	stop := renew(m, b.opts.AckWait)
	err := h(ctx, bus.Event{Topic: topic, Key: key, Seq: seq, Body: m.Data()})
	stop()
	if err == nil {
		return m.Ack()
	}
	delivered := uint64(1)
	if md, merr := m.Metadata(); merr == nil {
		delivered = md.NumDelivered
	}
	return m.NakWithDelay(backoff(delivered))
}

// renew keeps m invisible with an in-progress ack a third of every ack wait, until stop is called.
func renew(m jetstream.Msg, ackWait time.Duration) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(ackWait / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = m.InProgress()
			}
		}
	}()
	return func() { close(done); <-finished }
}

// ─── The queue ───

func (b *Bus) queueStream() string { return b.opts.Prefix + "-q" }
func (b *Bus) deadStream() string  { return b.opts.Prefix + "-dead" }

func (b *Bus) readySubject(queue string) string {
	return b.opts.Prefix + ".q." + token(queue) + ".ready"
}

func (b *Bus) ensureQueues(ctx context.Context) error {
	if err := b.ensureStream(ctx, jetstream.StreamConfig{
		Name: b.queueStream(), Subjects: []string{b.opts.Prefix + ".q.>"}, Retention: jetstream.WorkQueuePolicy,
		MaxAge: b.opts.Retention, Replicas: b.opts.Replicas, Storage: jetstream.FileStorage,
		Duplicates: b.opts.Dedupe, AllowMsgSchedules: true,
	}); err != nil {
		return err
	}
	return b.ensureStream(ctx, jetstream.StreamConfig{
		Name: b.deadStream(), Subjects: []string{b.opts.Prefix + ".dead.>"}, Retention: jetstream.LimitsPolicy,
		MaxAge: b.opts.Retention, Replicas: b.opts.Replicas, Storage: jetstream.FileStorage,
	})
}

// Enqueue adds jobs; one not due yet waits as a message schedule.
func (b *Bus) Enqueue(ctx context.Context, jobs ...bus.Job) error {
	if err := b.ensureQueues(ctx); err != nil {
		return err
	}
	futures := make([]jetstream.PubAckFuture, 0, len(jobs))
	for _, j := range jobs {
		ready := b.readySubject(j.Queue)
		m := nats.NewMsg(ready)
		m.Data = j.Body
		m.Header.Set(hdrKey, strconv.FormatInt(j.Key, 10))
		m.Header.Set(hdrID, j.ID)
		if j.ID != "" {
			m.Header.Set(jetstream.MsgIDHeader, token(j.Queue)+"/"+j.ID)
		}
		if j.NotBefore.After(time.Now()) {
			m.Subject = b.opts.Prefix + ".q." + token(j.Queue) + ".later." + nuid.Next()
			m.Header.Set("Nats-Schedule", "@at "+j.NotBefore.UTC().Format(time.RFC3339Nano))
			m.Header.Set("Nats-Schedule-Target", ready)
		}
		f, err := b.js.PublishMsgAsync(m)
		if err != nil {
			return fmt.Errorf("natsbus: enqueueing on %s: %w", j.Queue, err)
		}
		futures = append(futures, f)
	}
	for _, f := range futures {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return fmt.Errorf("natsbus: enqueueing: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Lease takes up to o.Max ready jobs of queue.
func (b *Bus) Lease(ctx context.Context, queue string, o bus.LeaseOptions) ([]bus.Leased, error) {
	if err := b.ensureQueues(ctx); err != nil {
		return nil, err
	}
	c, err := b.consumer(ctx, b.queueStream(), jetstream.ConsumerConfig{
		Durable: token(queue), FilterSubject: b.readySubject(queue), AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: b.opts.AckWait, MaxDeliver: b.opts.MaxAttempts, MaxAckPending: -1,
	})
	if err != nil {
		return nil, err
	}
	n := max(o.Max, 1)
	var batch jetstream.MessageBatch
	if o.Wait <= 0 {
		batch, err = c.FetchNoWait(n)
	} else {
		fctx, cancel := context.WithTimeout(ctx, o.Wait)
		defer cancel()
		batch, err = c.Fetch(n, jetstream.FetchContext(fctx))
	}
	if err != nil {
		return nil, fmt.Errorf("natsbus: leasing from %s: %w", queue, err)
	}
	var out []bus.Leased
	for m := range batch.Messages() {
		key, _ := strconv.ParseInt(m.Headers().Get(hdrKey), 10, 64)
		attempt := 1
		if md, err := m.Metadata(); err == nil {
			attempt = int(md.NumDelivered)
		}
		l := &leased{b: b, m: m, attempt: attempt, deadline: time.Now().Add(max(o.Visibility, b.opts.AckWait)),
			job: bus.Job{ID: m.Headers().Get(hdrID), Queue: queue, Key: key, Body: m.Data()}}
		l.start()
		out = append(out, l)
	}
	if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return out, fmt.Errorf("natsbus: leasing from %s: %w", queue, err)
	}
	return out, ctx.Err()
}

// leased is a job a worker holds until its deadline.
type leased struct {
	b       *Bus
	m       jetstream.Msg
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

// start renews the delivery a third of every ack wait until its deadline passes, ending the lease,
// or the lease is finished: the server delivers it again only after the deadline, so an ended
// lease's holder, told so, never acknowledges another's delivery.
func (l *leased) start() {
	l.stop, l.stopped = make(chan struct{}), make(chan struct{})
	ackWait := l.b.opts.AckWait
	go func() {
		defer close(l.stopped)
		t := time.NewTicker(ackWait / 3)
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
			_ = l.m.InProgress()
		}
	}()
}

// end stops renewing, reporting bus.ErrLeaseLost if the lease had passed its deadline or ended.
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
	l.mu.Lock()
	if l.finished || time.Now().After(l.deadline) {
		l.mu.Unlock()
		return bus.ErrLeaseLost
	}
	l.deadline = time.Now().Add(max(d, l.b.opts.AckWait))
	l.mu.Unlock()
	if err := l.m.InProgress(); err != nil {
		return fmt.Errorf("natsbus: extending: %w", err)
	}
	return nil
}

func (l *leased) Ack(ctx context.Context) error {
	if err := l.end(); err != nil {
		return err
	}
	if err := l.m.DoubleAck(ctx); err != nil {
		return fmt.Errorf("natsbus: acknowledging: %w", err)
	}
	return nil
}

func (l *leased) Nack(ctx context.Context, delay time.Duration) error {
	if err := l.end(); err != nil {
		return err
	}
	if l.attempt >= l.b.opts.MaxAttempts {
		m := nats.NewMsg(l.b.opts.Prefix + ".dead." + token(l.job.Queue))
		m.Data = l.job.Body
		m.Header = l.m.Headers()
		m.Header.Del(jetstream.MsgIDHeader)
		if _, err := l.b.js.PublishMsg(ctx, m); err != nil {
			return fmt.Errorf("natsbus: keeping a dead letter of %s: %w", l.job.Queue, err)
		}
		return l.m.TermWithReason("its attempts are spent")
	}
	if delay <= 0 {
		return l.m.Nak()
	}
	return l.m.NakWithDelay(delay)
}
