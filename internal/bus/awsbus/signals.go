package awsbus

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Cidan/ok/internal/bus"
)

// Signals are one SNS topic, <prefix>-signals, a message's channel and payload its attributes (SNS
// takes no empty message, so its body says only "signal"). Each listener is an SQS queue of its own
// subscribed to the topic with raw delivery and a filter policy naming its channels, made as it
// starts and unsubscribed and deleted as it stops; a replica that dies first leaves its queue,
// which keeps what it is sent a minute (SQS's shortest retention), for an operator to remove. The
// listener checks its channels too.

var _ bus.Signals = (*Bus)(nil)

const (
	attrChannel = "ok-channel"
	attrPayload = "ok-payload"
)

const signalTopicKey = "\x00signals"

func (b *Bus) signalTopic(ctx context.Context) (string, error) {
	b.mu.Lock()
	arn := b.topics[signalTopicKey]
	b.mu.Unlock()
	if arn != "" {
		return arn, nil
	}
	out, err := b.sns.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(b.opts.Prefix + "-signals")})
	if err != nil {
		return "", fmt.Errorf("awsbus: the signal topic: %w", err)
	}
	b.mu.Lock()
	b.topics[signalTopicKey] = *out.TopicArn
	b.mu.Unlock()
	return *out.TopicArn, nil
}

func (b *Bus) Signal(ctx context.Context, channel, payload string) error {
	arn, err := b.signalTopic(ctx)
	if err != nil {
		return err
	}
	attrs := map[string]snstypes.MessageAttributeValue{attrChannel: {DataType: aws.String("String"), StringValue: aws.String(channel)}}
	if payload != "" {
		attrs[attrPayload] = snstypes.MessageAttributeValue{DataType: aws.String("Binary"), BinaryValue: []byte(payload)}
	}
	if _, err := b.sns.Publish(ctx, &sns.PublishInput{TopicArn: aws.String(arn), Message: aws.String("signal"), MessageAttributes: attrs}); err != nil {
		return fmt.Errorf("awsbus: signaling: %w", err)
	}
	return nil
}

func (b *Bus) Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error {
	arn, err := b.signalTopic(ctx)
	if err != nil {
		return err
	}
	qname := b.opts.Prefix + "-sig-" + strings.ToLower(rand.Text())
	q, err := b.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(qname),
		Attributes: map[string]string{"MessageRetentionPeriod": "60", "VisibilityTimeout": "30"}})
	if err != nil {
		return fmt.Errorf("awsbus: a signal queue: %w", err)
	}
	url := *q.QueueUrl
	cleanup := func(sub *string) {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		if sub != nil {
			_, _ = b.sns.Unsubscribe(dctx, &sns.UnsubscribeInput{SubscriptionArn: sub})
		}
		_, _ = b.sqs.DeleteQueue(dctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(url)})
	}
	qarn, err := b.queueArn(ctx, url)
	if err != nil {
		cleanup(nil)
		return err
	}
	p, err := policy(qarn, arn)
	if err != nil {
		cleanup(nil)
		return err
	}
	if _, err := b.sqs.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: aws.String(url), Attributes: map[string]string{"Policy": p}}); err != nil {
		cleanup(nil)
		return fmt.Errorf("awsbus: a signal queue: %w", err)
	}
	filter, err := json.Marshal(map[string][]string{attrChannel: channels})
	if err != nil {
		cleanup(nil)
		return err
	}
	sub, err := b.sns.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("sqs"), Endpoint: aws.String(qarn),
		Attributes: map[string]string{"RawMessageDelivery": "true", "FilterPolicy": string(filter)}, ReturnSubscriptionArn: true})
	if err != nil {
		cleanup(nil)
		return fmt.Errorf("awsbus: a signal subscription: %w", err)
	}
	defer cleanup(sub.SubscriptionArn)
	want := map[string]bool{}
	for _, c := range channels {
		want[c] = true
	}
	ready()
	var mu sync.Mutex
	for {
		ms, err := b.receive(ctx, url, 10, 30*time.Second, 20*time.Second)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("awsbus: receiving signals: %w", err)
		}
		var done []sqstypes.DeleteMessageBatchRequestEntry
		for i, m := range ms {
			if c := stringAttr(m, attrChannel); want[c] {
				mu.Lock()
				deliver(c, string(m.MessageAttributes[attrPayload].BinaryValue))
				mu.Unlock()
			}
			done = append(done, sqstypes.DeleteMessageBatchRequestEntry{Id: aws.String(fmt.Sprint(i)), ReceiptHandle: m.ReceiptHandle})
		}
		if len(done) > 0 {
			_, _ = b.sqs.DeleteMessageBatch(context.WithoutCancel(ctx), &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(url), Entries: done})
		}
	}
}
