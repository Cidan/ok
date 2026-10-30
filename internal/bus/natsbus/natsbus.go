// Package natsbus is the bus (internal/bus) on NATS JetStream (docs/hosted.md, "Backends").
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
	"golang.org/x/sync/singleflight"

	"github.com/Cidan/ok/internal/bus"
)

// Options shape a Bus.
type Options struct {
	// Prefix begins every subject and stream name, so installations sharing an account keep apart.
	// Default "ok".
	Prefix string
	// Replicas is how many servers keep each stream: 3 in a cluster. Default 1.
	Replicas int
	// Retention is how long the queues keep their jobs and dead letters.
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
}

// Headers a message carries.
const (
	hdrKey = "Ok-Key"
	hdrID  = "Ok-Id"
)

// Bus is a bus.Queue on one NATS connection.
type Bus struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	opts Options

	mu        sync.Mutex
	streams   map[string]bool // the streams made
	consumers map[string]jetstream.Consumer
	// making is a stream's or a consumer's lookup or creation under way: the leases and
	// publishes that need one before it is known wait for the one request, rather than each
	// asking the server, which the first moments of a replica's many leases would swamp.
	making singleflight.Group
	// renewing are the resumed leases this process keeps for longer than an ack wait, by their
	// deliveries' reply subjects.
	renewing map[string]*renewal
	// inboxes are this process's inboxes, on a queue of its own (bus.Inboxes).
	inboxes *bus.Inboxes
}

var _ bus.Queue = (*Bus)(nil)

// publishTimeout bounds the wait for a job's acknowledgement from the server: the client fails no
// pending acknowledgement when its connection closes, so an enqueue — made in a transaction,
// holding a database connection — would otherwise wait for one forever.
const publishTimeout = 30 * time.Second

// Open is a bus on nc's JetStream.
func Open(nc *nats.Conn, opts Options) (*Bus, error) {
	opts.defaults()
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncTimeout(publishTimeout))
	if err != nil {
		return nil, fmt.Errorf("natsbus: %w", err)
	}
	b := &Bus{nc: nc, js: js, opts: opts, streams: map[string]bool{}, consumers: map[string]jetstream.Consumer{}, renewing: map[string]*renewal{}}
	b.inboxes = bus.NewInboxes(b, b.prepareInboxes, b.dropQueue)
	before := nc.Opts.ReconnectedCB
	nc.SetReconnectHandler(func(c *nats.Conn) {
		b.forgetStreams()
		if before != nil {
			before(c)
		}
	})
	return b, nil
}

// forgetStreams has every stream and consumer looked for anew: a server that came back may have lost them, and until they are made again
// it answers "stream not found" to every enqueue and lease.
func (b *Bus) forgetStreams() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.streams, b.consumers = map[string]bool{}, map[string]jetstream.Consumer{}
}

// Inbox opens an inbox, on this process's queue of inboxes: a consumer the server removes once no
// one has leased from it for inboxIdle, should the process stop before it closes its inboxes.
func (b *Bus) Inbox(ctx context.Context) (bus.Inbox, error) { return b.inboxes.Open(ctx) }

// CloseInboxes closes the process's inboxes and removes their queue.
func (b *Bus) CloseInboxes(ctx context.Context) error { return b.inboxes.Close(ctx) }

// inboxIdle is how long a process's queue of inboxes lasts unleased.
const inboxIdle = time.Hour

// dropQueue removes a queue's consumer and its jobs.
func (b *Bus) dropQueue(ctx context.Context, queue string) error {
	key := b.queueStream() + "/" + token(queue)
	b.mu.Lock()
	delete(b.consumers, key)
	made := b.streams[b.queueStream()]
	b.mu.Unlock()
	if !made {
		return nil // nothing was ever enqueued or leased here
	}
	if err := b.js.DeleteConsumer(ctx, b.queueStream(), token(queue)); err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return fmt.Errorf("natsbus: removing %s: %w", queue, err)
	}
	s, err := b.js.Stream(ctx, b.queueStream())
	if err != nil {
		return fmt.Errorf("natsbus: removing %s: %w", queue, err)
	}
	if err := s.Purge(ctx, jetstream.WithPurgeSubject(b.readySubject(queue))); err != nil {
		return fmt.Errorf("natsbus: removing %s's jobs: %w", queue, err)
	}
	return nil
}

// prepareInboxes makes the process's queue of inboxes: its consumer, looked up anew, which the
// server removes once unleased for inboxIdle. A job enqueued on it before then waits in the stream.
func (b *Bus) prepareInboxes(ctx context.Context, queue string) error {
	if err := b.ensureQueues(ctx); err != nil {
		return err
	}
	b.forgetConsumer(b.queueStream(), token(queue))
	_, err := b.consumer(ctx, b.queueStream(), b.queueConsumer(queue))
	return err
}

// forgetConsumer drops a consumer this bus holds, to look it up anew: the server removed it.
func (b *Bus) forgetConsumer(stream, durable string) {
	b.mu.Lock()
	delete(b.consumers, stream+"/"+durable)
	b.mu.Unlock()
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
	_, err, _ := b.making.Do("stream/"+cfg.Name, func() (any, error) {
		if _, err := b.js.Stream(ctx, cfg.Name); errors.Is(err, jetstream.ErrStreamNotFound) {
			if _, err := b.js.CreateStream(ctx, cfg); err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
				return nil, fmt.Errorf("natsbus: stream %s: %w", cfg.Name, err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("natsbus: stream %s: %w", cfg.Name, err)
		}
		b.mu.Lock()
		b.streams[cfg.Name] = true
		b.mu.Unlock()
		return nil, nil
	})
	return err
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
	made, err, _ := b.making.Do("consumer/"+key, func() (any, error) {
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
	})
	if err != nil {
		return nil, err
	}
	return made.(jetstream.Consumer), nil
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
		j, _ = bus.InboxJob(j)
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

// queueConsumer is a queue's consumer: a process's queue of inboxes' lasting while it is leased.
func (b *Bus) queueConsumer(queue string) jetstream.ConsumerConfig {
	cfg := jetstream.ConsumerConfig{
		Durable: token(queue), FilterSubject: b.readySubject(queue), AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: b.opts.AckWait, MaxDeliver: b.opts.MaxAttempts, MaxAckPending: -1,
	}
	if bus.IsInboxQueue(queue) {
		cfg.InactiveThreshold = inboxIdle
	}
	return cfg
}

// Lease takes up to o.Max ready jobs of queue.
func (b *Bus) Lease(ctx context.Context, queue string, o bus.LeaseOptions) ([]bus.Leased, error) {
	if err := b.ensureQueues(ctx); err != nil {
		return nil, err
	}
	cfg := b.queueConsumer(queue)
	c, err := b.consumer(ctx, b.queueStream(), cfg)
	if err != nil {
		return nil, err
	}
	msgs, err := b.fetch(ctx, c, max(o.Max, 1), o.Wait)
	if errors.Is(err, jetstream.ErrConsumerNotFound) || errors.Is(err, jetstream.ErrConsumerDeleted) {
		b.forgetConsumer(b.queueStream(), cfg.Durable) // the server removed it: the next lease makes it anew
	}
	var out []bus.Leased
	for _, m := range msgs {
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
	if err != nil {
		return out, fmt.Errorf("natsbus: leasing from %s: %w", queue, err)
	}
	return out, ctx.Err()
}

// fetch takes up to n of a consumer's messages: those ready at once, or — waiting up to wait for
// the first — the first, and those ready by the time it came. One pull request of n waits until n
// arrive or its wait ends, which kept a lease of many as long as its wait for one job.
func (b *Bus) fetch(ctx context.Context, c jetstream.Consumer, n int, wait time.Duration) ([]jetstream.Msg, error) {
	collect := func(batch jetstream.MessageBatch, err error, into []jetstream.Msg) ([]jetstream.Msg, error) {
		if err != nil {
			return into, err
		}
		for m := range batch.Messages() {
			into = append(into, m)
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, context.Canceled) {
			return into, err
		}
		return into, nil
	}
	batch, err := c.FetchNoWait(n)
	msgs, err := collect(batch, err, nil)
	if err != nil || len(msgs) > 0 || wait <= 0 {
		return msgs, err
	}
	fctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	batch, err = c.Fetch(1, jetstream.FetchContext(fctx))
	if msgs, err = collect(batch, err, nil); err != nil || len(msgs) == 0 || n == 1 {
		return msgs, err
	}
	batch, err = c.FetchNoWait(n - 1)
	return collect(batch, err, msgs)
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

// Handle names the delivery by its reply subject, which acknowledges it from any connection.
func (l *leased) Handle() []byte { return bus.EncodeHandle(l.job.Queue, l.job.ID, l.m.Reply()) }

// Resume is the lease of the delivery whose reply subject the handle names: acknowledged, returned,
// and kept in progress by publishing to it, as the holder's connection would.
func (b *Bus) Resume(handle []byte) (bus.Leased, error) {
	parts, err := bus.DecodeHandle(handle, 3)
	if err != nil {
		return nil, err
	}
	return &resumed{b: b, job: bus.Job{Queue: parts[0], ID: parts[1]}, reply: parts[2], handle: handle}, nil
}

// resumed is a delivery leased by another process, or by this one before.
type resumed struct {
	b      *Bus
	job    bus.Job
	reply  string
	handle []byte
}

func (r *resumed) Job() bus.Job   { return r.job }
func (r *resumed) Attempt() int   { return 0 }
func (r *resumed) Handle() []byte { return r.handle }

// Extend keeps the delivery in progress: once, which keeps it an ack wait, and past that by
// renewing it here a third of each ack wait until d has passed.
func (r *resumed) Extend(ctx context.Context, d time.Duration) error {
	if err := r.b.nc.Publish(r.reply, []byte("+WPI")); err != nil {
		return fmt.Errorf("natsbus: extending: %w", err)
	}
	if d > r.b.opts.AckWait {
		r.b.renew(r.reply, time.Now().Add(d))
	} else {
		r.b.stopRenewing(r.reply)
	}
	return nil
}

func (r *resumed) Ack(ctx context.Context) error {
	r.b.stopRenewing(r.reply)
	if _, err := r.b.nc.RequestWithContext(ctx, r.reply, []byte("+ACK")); err != nil {
		return fmt.Errorf("natsbus: acknowledging: %w", err)
	}
	return nil
}

func (r *resumed) Nack(ctx context.Context, delay time.Duration) error {
	r.b.stopRenewing(r.reply)
	body := []byte("-NAK")
	if delay > 0 {
		body = fmt.Appendf(nil, "-NAK {\"delay\": %d}", delay.Nanoseconds())
	}
	if err := r.b.nc.Publish(r.reply, body); err != nil {
		return fmt.Errorf("natsbus: returning a job: %w", err)
	}
	return nil
}

// renewal keeps a resumed delivery in progress until its deadline.
type renewal struct {
	mu       sync.Mutex
	deadline time.Time
	stop     chan struct{}
}

// renew keeps reply's delivery in progress until deadline, from this process.
func (b *Bus) renew(reply string, deadline time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r := b.renewing[reply]; r != nil {
		r.mu.Lock()
		r.deadline = deadline
		r.mu.Unlock()
		return
	}
	r := &renewal{deadline: deadline, stop: make(chan struct{})}
	b.renewing[reply] = r
	go func() {
		t := time.NewTicker(b.opts.AckWait / 3)
		defer t.Stop()
		for {
			select {
			case <-r.stop:
				return
			case <-t.C:
			}
			r.mu.Lock()
			left := time.Until(r.deadline)
			r.mu.Unlock()
			if left < 0 {
				b.stopRenewing(reply)
				return
			}
			_ = b.nc.Publish(reply, []byte("+WPI"))
		}
	}()
}

func (b *Bus) stopRenewing(reply string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r := b.renewing[reply]; r != nil {
		close(r.stop)
		delete(b.renewing, reply)
	}
}

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
