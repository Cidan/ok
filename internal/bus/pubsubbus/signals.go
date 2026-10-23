package pubsubbus

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Signals are one topic, <prefix>-signals, its messages carrying their channel as an attribute and
// their payload as data. Each listener is a subscription of its own, made as it starts and deleted
// as it stops — or, if its replica dies first, by Pub/Sub a day later (its shortest expiry) —
// keeping what it is sent ten minutes at most (Pub/Sub's shortest retention). Its filter names its
// channels where it fits Pub/Sub's 256 characters; the listener checks them either way.

const (
	attrChannel = "ok-channel"
	// filterMax is the longest filter Pub/Sub takes.
	filterMax = 256
)

func (b *Bus) signalTopic() string { return b.topicName(b.opts.Prefix + "-signals") }

func (b *Bus) Signal(ctx context.Context, channel, payload string) error {
	t := b.signalTopic()
	if err := b.ensureTopic(ctx, t, 0); err != nil {
		return err
	}
	return b.publish(ctx, []string{t}, []*pubsub.Message{{Data: []byte(payload), Attributes: map[string]string{attrChannel: channel}}})
}

// signalFilter is a filter admitting channels, or "" for none: one too long, or a channel no
// filter's string literal can hold plainly.
func signalFilter(channels []string) string {
	terms := make([]string, len(channels))
	for i, c := range channels {
		if strings.ContainsAny(c, "\"\\") {
			return ""
		}
		terms[i] = fmt.Sprintf("attributes.%s = %q", attrChannel, c)
	}
	f := strings.Join(terms, " OR ")
	if len(f) > filterMax {
		return ""
	}
	return f
}

func (b *Bus) Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error {
	t := b.signalTopic()
	if err := b.ensureTopic(ctx, t, 0); err != nil {
		return err
	}
	want := map[string]bool{}
	for _, c := range channels {
		want[c] = true
	}
	name := b.subName(b.opts.Prefix + "-sig-" + strings.ToLower(rand.Text()))
	if _, err := b.c.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name: name, Topic: t, AckDeadlineSeconds: 10, Filter: signalFilter(channels),
		MessageRetentionDuration: durationpb.New(10 * time.Minute),
		ExpirationPolicy:         &pubsubpb.ExpirationPolicy{Ttl: durationpb.New(24 * time.Hour)},
	}); err != nil {
		return fmt.Errorf("pubsubbus: a signal subscription: %w", err)
	}
	defer func() {
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = b.c.SubscriptionAdminClient.DeleteSubscription(dctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: name})
	}()
	ready()
	var mu sync.Mutex // one delivery at a time
	err := b.c.Subscriber(name).Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		m.Ack()
		if c := m.Attributes[attrChannel]; want[c] {
			mu.Lock()
			defer mu.Unlock()
			deliver(c, string(m.Data))
		}
	})
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("pubsubbus: receiving signals: %w", err)
	}
	return ctx.Err()
}
