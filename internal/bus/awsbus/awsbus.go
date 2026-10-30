// Package awsbus is the bus (internal/bus) on Amazon SQS (docs/hosted.md, "Backends").
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
// SQS's bodies are text, so a job's bytes travel in a binary message attribute — base64 on the
// wire, as the protocol carries binary — under a fixed body.
package awsbus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Cidan/ok/internal/bus"
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
	attrID        = "ok-id"
	attrBody      = "ok-body"
	attrNotBefore = "ok-not-before"
	attrAttempts  = "ok-attempts" // deliveries before it was sent again
	body          = "ok"
)

// Bus is a bus.Queue on an SQS client.
type Bus struct {
	sqs  *sqs.Client
	opts Options

	mu     sync.Mutex
	queues map[string]string // a queue's URL, by name
	arns   map[string]string // a queue's ARN, by URL
	// inboxes are this process's inboxes (bus.Inboxes), on an SQS queue of its own tagged with when
	// it was last leased (seen) — every inboxSeen at most — so that any process making its own
	// removes one unleased for inboxGone: its process stopped before it closed its inboxes.
	inboxes *bus.Inboxes
	seen    time.Time
}

var _ bus.Queue = (*Bus)(nil)

// New is a bus on the client's account and region.
func New(sqsClient *sqs.Client, opts Options) *Bus {
	opts.defaults()
	b := &Bus{sqs: sqsClient, opts: opts, queues: map[string]string{}, arns: map[string]string{}}
	b.inboxes = bus.NewInboxes(b, b.prepareInboxes, b.dropInboxes)
	return b
}

// Inbox opens an inbox, on this process's own SQS queue.
func (b *Bus) Inbox(ctx context.Context) (bus.Inbox, error) { return b.inboxes.Open(ctx) }

// CloseInboxes closes the process's inboxes and removes their queue.
func (b *Bus) CloseInboxes(ctx context.Context) error { return b.inboxes.Close(ctx) }

// How long an inbox's queue goes between marks of its being leased, and unleased before any process
// removes it.
const (
	inboxSeen = 10 * time.Minute
	inboxGone = time.Hour
	tagSeen   = "ok-seen"
)

// inboxAttrs are a process's queue of inboxes': replies kept ten minutes, and no dead letters.
var inboxAttrs = map[string]string{"MessageRetentionPeriod": "600", "VisibilityTimeout": "30"}

// prepareInboxes makes the process's queue of inboxes — looked up anew, which another process may
// have removed — marks it leased, and removes every other process's left unleased for inboxGone.
func (b *Bus) prepareInboxes(ctx context.Context, queue string) error {
	qname := name(b.opts.Prefix, "q", queue)
	b.mu.Lock()
	delete(b.queues, qname)
	b.mu.Unlock()
	url, err := b.ensureQueue(ctx, qname, inboxAttrs)
	if err != nil {
		return err
	}
	if err := b.markSeen(ctx, url, true); err != nil {
		return err
	}
	b.removeGoneInboxes(ctx, qname)
	return nil
}

// markSeen tags a queue of inboxes with now, unless it did within inboxSeen and not always.
func (b *Bus) markSeen(ctx context.Context, url string, always bool) error {
	b.mu.Lock()
	due := always || time.Since(b.seen) >= inboxSeen
	if due {
		b.seen = time.Now()
	}
	b.mu.Unlock()
	if !due {
		return nil
	}
	_, err := b.sqs.TagQueue(ctx, &sqs.TagQueueInput{QueueUrl: aws.String(url),
		Tags: map[string]string{tagSeen: strconv.FormatInt(time.Now().Unix(), 10)}})
	if err != nil {
		return fmt.Errorf("awsbus: marking %s leased: %w", url, err)
	}
	return nil
}

// removeGoneInboxes removes the queues of inboxes other than own that no process has leased for
// inboxGone. A failure is logged: the next process to make its inboxes removes them.
func (b *Bus) removeGoneInboxes(ctx context.Context, own string) {
	prefix := name(b.opts.Prefix, "q", "inbox.")
	var next *string
	for {
		out, err := b.sqs.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: aws.String(prefix), NextToken: next})
		if err != nil {
			slog.Warn("awsbus: listing queues of inboxes", "err", err)
			return
		}
		for _, url := range out.QueueUrls {
			if strings.HasSuffix(url, "/"+own) {
				continue
			}
			tags, err := b.sqs.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: aws.String(url)})
			if err != nil {
				continue
			}
			at, err := strconv.ParseInt(tags.Tags[tagSeen], 10, 64)
			if err == nil && time.Since(time.Unix(at, 0)) < inboxGone {
				continue
			}
			if _, err := b.sqs.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(url)}); err != nil {
				slog.Warn("awsbus: removing a queue of inboxes no process leases", "queue", url, "err", err)
			}
		}
		if out.NextToken == nil {
			return
		}
		next = out.NextToken
	}
}

func (b *Bus) dropInboxes(ctx context.Context, queue string) error {
	qname := name(b.opts.Prefix, "q", queue)
	b.mu.Lock()
	url := b.queues[qname]
	delete(b.queues, qname)
	b.mu.Unlock()
	if url == "" {
		return nil
	}
	if _, err := b.sqs.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(url)}); err != nil &&
		!errors.As(err, new(*sqstypes.QueueDoesNotExist)) {
		return fmt.Errorf("awsbus: removing %s: %w", queue, err)
	}
	return nil
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

// ─── The queue ───

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

// jobQueue makes a queue and its dead letters, returning their URLs.
func (b *Bus) jobQueue(ctx context.Context, queue string) (url, dead string, err error) {
	if bus.IsInboxQueue(queue) {
		url, err = b.ensureQueue(ctx, name(b.opts.Prefix, "q", queue), inboxAttrs)
		return url, "", err
	}
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
		j, _ = bus.InboxJob(j)
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
	if bus.IsInboxQueue(queue) {
		if err := b.markSeen(ctx, url, false); err != nil {
			return nil, err
		}
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

// Handle names the delivery by its queue's URL and receipt handle, with which any client changes
// its visibility or deletes it.
func (l *leased) Handle() []byte {
	return bus.EncodeHandle(l.job.Queue, l.job.ID, l.url, aws.ToString(l.receipt))
}

// Resume is the lease of the delivery whose queue URL and receipt handle the handle names. SQS
// keeps a message invisible at most maxVisibility from its first receive, past which extending it
// fails; its queue's redrive policy keeps it past its attempts.
func (b *Bus) Resume(handle []byte) (bus.Leased, error) {
	parts, err := bus.DecodeHandle(handle, 4)
	if err != nil {
		return nil, err
	}
	return &resumed{b: b, job: bus.Job{Queue: parts[0], ID: parts[1]}, url: parts[2], receipt: parts[3], handle: handle}, nil
}

// resumed is a delivery leased by another process, or by this one before.
type resumed struct {
	b            *Bus
	job          bus.Job
	url, receipt string
	handle       []byte
}

func (r *resumed) Job() bus.Job   { return r.job }
func (r *resumed) Attempt() int   { return 0 }
func (r *resumed) Handle() []byte { return r.handle }

func (r *resumed) Extend(ctx context.Context, d time.Duration) error {
	if _, err := r.b.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(r.url),
		ReceiptHandle: aws.String(r.receipt), VisibilityTimeout: seconds(max(d, time.Second))}); err != nil {
		return fmt.Errorf("awsbus: extending: %w", err)
	}
	return nil
}

func (r *resumed) Ack(ctx context.Context) error {
	if _, err := r.b.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(r.url), ReceiptHandle: aws.String(r.receipt)}); err != nil {
		return fmt.Errorf("awsbus: acknowledging: %w", err)
	}
	return nil
}

func (r *resumed) Nack(ctx context.Context, delay time.Duration) error {
	if _, err := r.b.sqs.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(r.url),
		ReceiptHandle: aws.String(r.receipt), VisibilityTimeout: seconds(max(delay, 0))}); err != nil {
		return fmt.Errorf("awsbus: returning a job: %w", err)
	}
	return nil
}

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
