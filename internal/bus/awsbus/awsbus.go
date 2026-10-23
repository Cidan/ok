// Package awsbus is the bus (internal/bus) on Amazon SNS and SQS (docs/hosted.md, "Backends").
//
// A topic is an SNS topic, and a group an SQS queue subscribed to it with raw message delivery, its
// policy letting the topic send to it; SNS delivers a subscription what is published after it was
// made. A handler's error returns its event to the group's queue after a backoff.
//
// A queue is an SQS standard queue, with another of its dead letters. A lease is a receive's
// visibility timeout, at most SQS's twelve hours from the first receive; past it a lease is ended
// and says bus.ErrLeaseLost, since SQS cannot tell whether a receipt handle is still the latest. A
// job's attempts are SQS's receive count, plus any it carries from before it was sent again. A job
// nacked on its last attempt is sent to the dead letters by the bus; the queue's redrive policy, at
// the same count, takes a job whose workers die holding it. SQS delays a message at most fifteen
// minutes: a job due later, or nacked past the twelve hours, is sent again carrying its attempts,
// and a Lease that receives it early sends it on. SQS standard queues drop no duplicates.
//
// SQS's and SNS's bodies are text, so an event's or job's bytes travel in a binary message
// attribute — base64 on the wire, as both protocols carry binary — under a fixed body.
package awsbus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"golang.org/x/sync/errgroup"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/inflight"
)

// Options shape a Bus.
type Options struct {
	// Prefix begins every topic and queue name, so installations sharing an account keep apart.
	// Default "ok".
	Prefix string
	// Retention is how long a queue keeps what it holds: SQS's minute to fourteen days. Default
	// seven days.
	Retention time.Duration
	// MaxAttempts is how many deliveries a job gets before it is a dead letter: at most 1,000.
	// Default 10.
	MaxAttempts int
	// Visibility is how long a group's delivery is invisible to the group's other members, renewed
	// while a handler works. Default 30 seconds.
	Visibility time.Duration
	// Concurrency is how many of a group's deliveries a member handles at once. Default 32.
	Concurrency int
}

func (o *Options) defaults() {
	if o.Prefix == "" {
		o.Prefix = "ok"
	}
	if o.Retention <= 0 {
		o.Retention = 7 * 24 * time.Hour
	}
	o.Retention = min(max(o.Retention, time.Minute), 14*24*time.Hour)
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 10
	}
	o.MaxAttempts = min(o.MaxAttempts, 1000)
	if o.Visibility <= 0 {
		o.Visibility = 30 * time.Second
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 32
	}
}

// SQS's and SNS's bounds.
const (
	maxDelay      = 15 * time.Minute // a message's delay
	maxVisibility = 12 * time.Hour   // a message's visibility, from its first receive
	maxWait       = 20 * time.Second // a receive's long poll
	maxBatch      = 10               // entries in a batch
	maxBatchBytes = 256 << 10        // a batch's bytes, counting attributes
	maxName       = 80               // a queue's name; a topic's may be longer
)

// Attributes a message carries, and its body.
const (
	attrKey       = "ok-key"
	attrSeq       = "ok-seq"
	attrID        = "ok-id"
	attrBody      = "ok-body"
	attrNotBefore = "ok-not-before"
	attrAttempts  = "ok-attempts" // deliveries before it was sent again
	body          = "ok"
)

// Bus is a bus.Stream and a bus.Queue on SNS and SQS clients.
type Bus struct {
	sqs  *sqs.Client
	sns  *sns.Client
	opts Options

	mu     sync.Mutex
	topics map[string]string   // a topic's ARN, by name
	queues map[string]string   // a queue's URL, by name
	arns   map[string]string   // a queue's ARN, by URL
	groups map[string]struct{} // groups subscribed
}

var (
	_ bus.Stream = (*Bus)(nil)
	_ bus.Queue  = (*Bus)(nil)
)

// New is a bus on the clients' account and region.
func New(sqsClient *sqs.Client, snsClient *sns.Client, opts Options) *Bus {
	opts.defaults()
	return &Bus{sqs: sqsClient, sns: snsClient, opts: opts, topics: map[string]string{}, queues: map[string]string{},
		arns: map[string]string{}, groups: map[string]struct{}{}}
}

// token is s as part of an SQS or SNS name: letters, digits, and '-' as they are, every other byte
// '_' and its two hex digits.
func token(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "_%02x", c)
		}
	}
	return b.String()
}

// name joins parts as a name of at most maxName bytes: one longer keeps its start and ends in a hash
// of the whole, so every replica names it alike.
func name(parts ...string) string {
	for i, p := range parts {
		parts[i] = token(p)
	}
	n := strings.Join(parts, "-")
	if len(n) <= maxName {
		return n
	}
	sum := sha256.Sum256([]byte(n))
	return n[:maxName-17] + "-" + hex.EncodeToString(sum[:8])
}

func seconds(d time.Duration) int32 { return int32((d + time.Second - 1) / time.Second) }

// ensureQueue makes a queue of attrs unless this bus made it; one that exists with others is set to
// them.
func (b *Bus) ensureQueue(ctx context.Context, qname string, attrs map[string]string) (string, error) {
	b.mu.Lock()
	u := b.queues[qname]
	b.mu.Unlock()
	if u != "" {
		return u, nil
	}
	out, err := b.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(qname), Attributes: attrs})
	if errors.As(err, new(*sqstypes.QueueNameExists)) {
		got, gerr := b.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(qname)})
		if gerr != nil {
			return "", fmt.Errorf("awsbus: queue %s: %w", qname, gerr)
		}
		if _, err := b.sqs.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: got.QueueUrl, Attributes: attrs}); err != nil {
			return "", fmt.Errorf("awsbus: queue %s: %w", qname, err)
		}
		out, err = &sqs.CreateQueueOutput{QueueUrl: got.QueueUrl}, nil
	}
	if err != nil {
		return "", fmt.Errorf("awsbus: queue %s: %w", qname, err)
	}
	b.mu.Lock()
	b.queues[qname] = *out.QueueUrl
	b.mu.Unlock()
	return *out.QueueUrl, nil
}

func (b *Bus) queueArn(ctx context.Context, url string) (string, error) {
	b.mu.Lock()
	arn := b.arns[url]
	b.mu.Unlock()
	if arn != "" {
		return arn, nil
	}
	out, err := b.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(url),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	if err != nil {
		return "", fmt.Errorf("awsbus: queue %s: %w", url, err)
	}
	arn = out.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	b.mu.Lock()
	b.arns[url] = arn
	b.mu.Unlock()
	return arn, nil
}

// retention is a queue's retention, as SQS takes it.
func (b *Bus) retention() string { return strconv.Itoa(int(b.opts.Retention / time.Second)) }

// sized splits entries, each n bytes, into batches SQS and SNS take.
func sized[T any](entries []T, n func(T) int) [][]T {
	var out [][]T
	var cur []T
	bytes := 0
	for _, e := range entries {
		if len(cur) == maxBatch || (len(cur) > 0 && bytes+n(e) > maxBatchBytes) {
			out, cur, bytes = append(out, cur), nil, 0
		}
		cur = append(cur, e)
		bytes += n(e)
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

// ─── The stream ───

func (b *Bus) topicArn(ctx context.Context, topic string) (string, error) {
	b.mu.Lock()
	arn := b.topics[topic]
	b.mu.Unlock()
	if arn != "" {
		return arn, nil
	}
	out, err := b.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(name(b.opts.Prefix, "ev", topic))})
	if err != nil {
		return "", fmt.Errorf("awsbus: topic %s: %w", topic, err)
	}
	b.mu.Lock()
	b.topics[topic] = *out.TopicArn
	b.mu.Unlock()
	return *out.TopicArn, nil
}

func eventAttrs(e bus.Event) map[string]snstypes.MessageAttributeValue {
	attrs := map[string]snstypes.MessageAttributeValue{
		attrKey: {DataType: aws.String("Number"), StringValue: aws.String(strconv.FormatInt(e.Key, 10))},
		attrSeq: {DataType: aws.String("Number"), StringValue: aws.String(strconv.FormatInt(e.Seq, 10))},
	}
	if len(e.Body) > 0 {
		attrs[attrBody] = snstypes.MessageAttributeValue{DataType: aws.String("Binary"), BinaryValue: e.Body}
	}
	return attrs
}

// Publish publishes events, a batch at a time.
func (b *Bus) Publish(ctx context.Context, events ...bus.Event) error {
	byTopic := map[string][]bus.Event{}
	var order []string
	for _, e := range events {
		if _, ok := byTopic[e.Topic]; !ok {
			order = append(order, e.Topic)
		}
		byTopic[e.Topic] = append(byTopic[e.Topic], e)
	}
	for _, topic := range order {
		arn, err := b.topicArn(ctx, topic)
		if err != nil {
			return err
		}
		for _, batch := range sized(byTopic[topic], func(e bus.Event) int { return len(body) + len(e.Body) + 128 }) {
			entries := make([]snstypes.PublishBatchRequestEntry, len(batch))
			for i, e := range batch {
				entries[i] = snstypes.PublishBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), Message: aws.String(body), MessageAttributes: eventAttrs(e)}
			}
			out, err := b.sns.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: aws.String(arn), PublishBatchRequestEntries: entries})
			if err != nil {
				return fmt.Errorf("awsbus: publishing to %s: %w", topic, err)
			}
			if len(out.Failed) > 0 {
				f := out.Failed[0]
				return fmt.Errorf("awsbus: publishing %d of %d events to %s: %s: %s", len(out.Failed), len(batch), topic,
					aws.ToString(f.Code), aws.ToString(f.Message))
			}
		}
	}
	return nil
}

// policy lets topic's SNS send to a queue.
func policy(queueArn, topicArn string) (string, error) {
	p, err := json.Marshal(map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Sid": "ok-topic", "Effect": "Allow", "Principal": map[string]string{"Service": "sns.amazonaws.com"},
			"Action": "sqs:SendMessage", "Resource": queueArn,
			"Condition": map[string]any{"ArnEquals": map[string]string{"aws:SourceArn": topicArn}},
		}},
	})
	return string(p), err
}

// CreateGroup makes group's queue on topic, subscribed to it: it receives what is published from
// now on.
func (b *Bus) CreateGroup(ctx context.Context, topic, group string) error {
	_, err := b.group(ctx, topic, group)
	return err
}

func (b *Bus) group(ctx context.Context, topic, group string) (string, error) {
	arn, err := b.topicArn(ctx, topic)
	if err != nil {
		return "", err
	}
	qname := name(b.opts.Prefix, "ev", topic, group)
	url, err := b.ensureQueue(ctx, qname, map[string]string{
		"MessageRetentionPeriod": b.retention(), "VisibilityTimeout": strconv.Itoa(int(seconds(b.opts.Visibility)))})
	if err != nil {
		return "", err
	}
	b.mu.Lock()
	_, done := b.groups[qname]
	b.mu.Unlock()
	if done {
		return url, nil
	}
	qarn, err := b.queueArn(ctx, url)
	if err != nil {
		return "", err
	}
	p, err := policy(qarn, arn)
	if err != nil {
		return "", err
	}
	// The policy first: the subscription delivers from the moment it is made.
	if _, err := b.sqs.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: aws.String(url), Attributes: map[string]string{"Policy": p}}); err != nil {
		return "", fmt.Errorf("awsbus: group %s/%s: %w", topic, group, err)
	}
	if _, err := b.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("sqs"), Endpoint: aws.String(qarn),
		Attributes: map[string]string{"RawMessageDelivery": "true"}, ReturnSubscriptionArn: true}); err != nil {
		return "", fmt.Errorf("awsbus: group %s/%s: %w", topic, group, err)
	}
	b.mu.Lock()
	b.groups[qname] = struct{}{}
	b.mu.Unlock()
	return url, nil
}

// backoff is how long an event a handler refused waits, by its receives so far.
func backoff(receives int) time.Duration {
	return min(time.Second<<min(max(receives-1, 0), 6), time.Minute)
}

func (b *Bus) receive(ctx context.Context, url string, n int, vis time.Duration, wait time.Duration) ([]sqstypes.Message, error) {
	out, err := b.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(url), MaxNumberOfMessages: int32(n), VisibilityTimeout: seconds(vis), WaitTimeSeconds: seconds(wait),
		MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameApproximateReceiveCount},
		MessageAttributeNames:       []string{"All"},
	})
	if err != nil {
		return nil, err
	}
	return out.Messages, nil
}

func receives(m sqstypes.Message) int {
	n, _ := strconv.Atoi(m.Attributes[string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)])
	return max(n, 1)
}

func stringAttr(m sqstypes.Message, k string) string {
	return aws.ToString(m.MessageAttributes[k].StringValue)
}

// Subscribe handles topic's events as a member of group until ctx ends, up to Concurrency at once:
// the stream promises no order, and a consumer applies each by its key's cursor. Receivers, each
// long-polling for up to ten, receive only as many as they reserved free slots for, so none waits
// for one, its visibility running, and hand each over as its receive returns; what is handled is
// deleted ten a request.
func (b *Bus) Subscribe(ctx context.Context, topic, group string, h bus.Handler) error {
	url, err := b.group(ctx, topic, group)
	if err != nil {
		return err
	}
	del := b.deleter(url)
	defer del.close()
	slots := inflight.New(b.opts.Concurrency)
	defer slots.Wait()
	g, gctx := errgroup.WithContext(ctx)
	for range (b.opts.Concurrency + maxBatch - 1) / maxBatch {
		g.Go(func() error {
			for gctx.Err() == nil {
				reserved, err := slots.Reserve(gctx, maxBatch)
				if err != nil {
					return nil
				}
				msgs, err := b.receive(gctx, url, reserved, b.opts.Visibility, maxWait)
				for _, m := range msgs {
					slots.Run(func() error { return b.handle(ctx, url, topic, m, h, del) })
				}
				slots.Release(reserved - len(msgs))
				if err != nil && gctx.Err() == nil {
					return fmt.Errorf("awsbus: receiving %s/%s: %w", topic, group, err)
				}
				if err := slots.Err(); err != nil && gctx.Err() == nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil && ctx.Err() == nil {
		return err
	}
	return ctx.Err()
}

// deleter deletes a queue's handled deliveries, ten a request, flushing a few milliseconds after
// the first of a batch: a delivery not yet deleted stays invisible, and one whose delete fails is
// delivered again, which a delivery may be.
type deleter struct {
	b    *Bus
	url  string
	in   chan *string
	done chan struct{}
}

func (b *Bus) deleter(url string) *deleter {
	d := &deleter{b: b, url: url, in: make(chan *string, 4*maxBatch), done: make(chan struct{})}
	go d.run()
	return d
}

func (d *deleter) delete(receipt *string) { d.in <- receipt }

// close deletes what is pending and stops.
func (d *deleter) close() {
	close(d.in)
	<-d.done
}

func (d *deleter) run() {
	defer close(d.done)
	var batch []*string
	flush := func() {
		if len(batch) == 0 {
			return
		}
		entries := make([]sqstypes.DeleteMessageBatchRequestEntry, len(batch))
		for i, r := range batch {
			entries[i] = sqstypes.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: r}
		}
		d.b.sqs.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(d.url), Entries: entries})
		batch = batch[:0]
	}
	var timer <-chan time.Time
	for {
		select {
		case r, ok := <-d.in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, r)
			if len(batch) == maxBatch {
				flush()
				timer = nil
			} else if timer == nil {
				timer = time.After(20 * time.Millisecond)
			}
		case <-timer:
			flush()
			timer = nil
		}
	}
}

// handle hands one delivery to h, keeping it invisible while h works, and deletes it or returns it
// after a backoff.
func (b *Bus) handle(ctx context.Context, url, topic string, m sqstypes.Message, h bus.Handler, del *deleter) error {
	key, kerr := strconv.ParseInt(stringAttr(m, attrKey), 10, 64)
	seq, serr := strconv.ParseInt(stringAttr(m, attrSeq), 10, 64)
	if kerr != nil || serr != nil {
		// Not an event of this package's: nothing a retry would change.
		del.delete(m.ReceiptHandle)
		return nil
	}
	stop := b.renew(url, m.ReceiptHandle)
	err := h(ctx, bus.Event{Topic: topic, Key: key, Seq: seq, Body: m.MessageAttributes[attrBody].BinaryValue})
	stop()
	if err == nil {
		del.delete(m.ReceiptHandle)
		return nil
	}
	if _, err := b.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle,
		VisibilityTimeout: seconds(backoff(receives(m)))}); err != nil {
		return fmt.Errorf("awsbus: returning %s's event: %w", topic, err)
	}
	return nil
}

// renew keeps a group's delivery invisible a third of every visibility, until stop is called.
func (b *Bus) renew(url string, receipt *string) (stop func()) {
	done, finished := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(b.opts.Visibility / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				b.sqs.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(url),
					ReceiptHandle: receipt, VisibilityTimeout: seconds(b.opts.Visibility)})
			}
		}
	}()
	return func() { close(done); <-finished }
}

// ─── The queue ───

// jobQueue makes a queue and its dead letters, returning their URLs.
func (b *Bus) jobQueue(ctx context.Context, queue string) (url, dead string, err error) {
	dead, err = b.ensureQueue(ctx, name(b.opts.Prefix, "dead", queue), map[string]string{"MessageRetentionPeriod": b.retention()})
	if err != nil {
		return "", "", err
	}
	deadArn, err := b.queueArn(ctx, dead)
	if err != nil {
		return "", "", err
	}
	redrive, err := json.Marshal(map[string]string{"deadLetterTargetArn": deadArn, "maxReceiveCount": strconv.Itoa(b.opts.MaxAttempts)})
	if err != nil {
		return "", "", err
	}
	url, err = b.ensureQueue(ctx, name(b.opts.Prefix, "q", queue), map[string]string{
		"MessageRetentionPeriod": b.retention(), "VisibilityTimeout": "30", "RedrivePolicy": string(redrive)})
	return url, dead, err
}

// jobEntry is a job as a message: due at notBefore, having had attempts deliveries already.
func jobEntry(id string, j bus.Job, attempts int) sqstypes.SendMessageBatchRequestEntry {
	attrs := map[string]sqstypes.MessageAttributeValue{
		attrKey: {DataType: aws.String("Number"), StringValue: aws.String(strconv.FormatInt(j.Key, 10))},
	}
	if j.ID != "" {
		attrs[attrID] = sqstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(j.ID)}
	}
	if len(j.Body) > 0 {
		attrs[attrBody] = sqstypes.MessageAttributeValue{DataType: aws.String("Binary"), BinaryValue: j.Body}
	}
	if attempts > 0 {
		attrs[attrAttempts] = sqstypes.MessageAttributeValue{DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(attempts))}
	}
	e := sqstypes.SendMessageBatchRequestEntry{Id: aws.String(id), MessageBody: aws.String(body), MessageAttributes: attrs}
	if wait := time.Until(j.NotBefore); wait > 0 {
		e.DelaySeconds = min(seconds(wait), seconds(maxDelay))
		if wait > maxDelay {
			attrs[attrNotBefore] = sqstypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(j.NotBefore.UTC().Format(time.RFC3339Nano))}
		}
	}
	return e
}

// send sends entries to a queue, a batch at a time.
func (b *Bus) send(ctx context.Context, url string, entries []sqstypes.SendMessageBatchRequestEntry) error {
	size := func(e sqstypes.SendMessageBatchRequestEntry) int {
		return len(body) + len(e.MessageAttributes[attrBody].BinaryValue) + 256
	}
	for _, batch := range sized(entries, size) {
		for i := range batch {
			batch[i].Id = aws.String(strconv.Itoa(i))
		}
		out, err := b.sqs.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(url), Entries: batch})
		if err != nil {
			return fmt.Errorf("awsbus: sending to %s: %w", url, err)
		}
		if len(out.Failed) > 0 {
			f := out.Failed[0]
			return fmt.Errorf("awsbus: sending %d of %d jobs to %s: %s: %s", len(out.Failed), len(batch), url, aws.ToString(f.Code), aws.ToString(f.Message))
		}
	}
	return nil
}

// Enqueue adds jobs.
func (b *Bus) Enqueue(ctx context.Context, jobs ...bus.Job) error {
	byQueue := map[string][]sqstypes.SendMessageBatchRequestEntry{}
	var order []string
	for _, j := range jobs {
		if _, ok := byQueue[j.Queue]; !ok {
			order = append(order, j.Queue)
		}
		byQueue[j.Queue] = append(byQueue[j.Queue], jobEntry("", j, 0))
	}
	for _, queue := range order {
		url, _, err := b.jobQueue(ctx, queue)
		if err != nil {
			return err
		}
		if err := b.send(ctx, url, byQueue[queue]); err != nil {
			return err
		}
	}
	return nil
}

func jobOf(queue string, m sqstypes.Message) bus.Job {
	key, _ := strconv.ParseInt(stringAttr(m, attrKey), 10, 64)
	return bus.Job{ID: stringAttr(m, attrID), Queue: queue, Key: key, Body: m.MessageAttributes[attrBody].BinaryValue}
}

// resend sends a job to url again, due at notBefore, carrying attempts, and deletes the message it
// was: a crash between the two leaves the job twice, which a job may be.
func (b *Bus) resend(ctx context.Context, url, from string, receipt *string, j bus.Job, notBefore time.Time, attempts int) error {
	j.NotBefore = notBefore
	if err := b.send(ctx, url, []sqstypes.SendMessageBatchRequestEntry{jobEntry("0", j, attempts)}); err != nil {
		return err
	}
	if _, err := b.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(from), ReceiptHandle: receipt}); err != nil {
		return fmt.Errorf("awsbus: deleting a job sent again: %w", err)
	}
	return nil
}

// Lease takes up to o.Max of queue's ready jobs, sending on any received before they are due.
func (b *Bus) Lease(ctx context.Context, queue string, o bus.LeaseOptions) ([]bus.Leased, error) {
	url, dead, err := b.jobQueue(ctx, queue)
	if err != nil {
		return nil, err
	}
	vis := min(max(o.Visibility, time.Second), maxVisibility)
	deadline := time.Now().Add(o.Wait)
	for {
		wait := time.Duration(0)
		if o.Wait > 0 {
			wait = min(max(time.Until(deadline), time.Second), maxWait)
		}
		start := time.Now()
		msgs, err := b.receive(ctx, url, min(max(o.Max, 1), maxBatch), vis, wait)
		if err != nil {
			return nil, fmt.Errorf("awsbus: leasing from %s: %w", queue, err)
		}
		var out []bus.Leased
		for _, m := range msgs {
			j := jobOf(queue, m)
			base, _ := strconv.Atoi(stringAttr(m, attrAttempts))
			if due, err := time.Parse(time.RFC3339Nano, stringAttr(m, attrNotBefore)); err == nil && due.After(time.Now()) {
				// Not an attempt: this receive was its delay's.
				if err := b.resend(ctx, url, url, m.ReceiptHandle, j, due, base+receives(m)-1); err != nil {
					return out, err
				}
				continue
			}
			out = append(out, &leased{b: b, url: url, dead: dead, receipt: m.ReceiptHandle, job: j, attempt: base + receives(m),
				received: start, deadline: start.Add(vis)})
		}
		if len(out) > 0 || o.Wait <= 0 || !time.Now().Before(deadline) || ctx.Err() != nil {
			return out, ctx.Err()
		}
	}
}

// leased is a job a worker holds until its deadline, which SQS keeps by its visibility.
type leased struct {
	b         *Bus
	url, dead string
	receipt   *string
	job       bus.Job
	attempt   int
	received  time.Time // before the receive: no later than SQS's first receive of it

	mu       sync.Mutex
	deadline time.Time
	finished bool
}

func (l *leased) Job() bus.Job { return l.job }
func (l *leased) Attempt() int { return l.attempt }

// live reports bus.ErrLeaseLost if the lease has passed its deadline or ended, and with finish ends
// it.
func (l *leased) live(finish bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finished || time.Now().After(l.deadline) {
		return bus.ErrLeaseLost
	}
	l.finished = finish
	return nil
}

func (l *leased) Extend(ctx context.Context, d time.Duration) error {
	if err := l.live(false); err != nil {
		return err
	}
	d = max(d, time.Second)
	at := time.Now()
	if at.Add(d).After(l.received.Add(maxVisibility)) {
		return fmt.Errorf("awsbus: SQS holds a lease at most %v from its receive", maxVisibility)
	}
	if _, err := l.b.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(l.url), ReceiptHandle: l.receipt,
		VisibilityTimeout: seconds(d)}); err != nil {
		return fmt.Errorf("awsbus: extending: %w", err)
	}
	l.mu.Lock()
	l.deadline = at.Add(d)
	l.mu.Unlock()
	return nil
}

func (l *leased) Ack(ctx context.Context) error {
	if err := l.live(true); err != nil {
		return err
	}
	if _, err := l.b.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(l.url), ReceiptHandle: l.receipt}); err != nil {
		return fmt.Errorf("awsbus: acknowledging: %w", err)
	}
	return nil
}

func (l *leased) Nack(ctx context.Context, delay time.Duration) error {
	if err := l.live(true); err != nil {
		return err
	}
	switch {
	case l.attempt >= l.b.opts.MaxAttempts:
		return l.b.resend(ctx, l.dead, l.url, l.receipt, l.job, time.Time{}, l.attempt)
	case time.Now().Add(delay).After(l.received.Add(maxVisibility)):
		return l.b.resend(ctx, l.url, l.url, l.receipt, l.job, time.Now().Add(delay), l.attempt)
	}
	if _, err := l.b.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(l.url), ReceiptHandle: l.receipt,
		VisibilityTimeout: seconds(max(delay, 0))}); err != nil {
		return fmt.Errorf("awsbus: returning a job: %w", err)
	}
	return nil
}
