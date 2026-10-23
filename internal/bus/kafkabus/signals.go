package kafkabus

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Cidan/ok/internal/bus"
)

// Signals are one topic, <prefix>.signals, a record's key and header its channel and its value its
// payload. A listener is a client of its own, in no consumer group, reading every partition from the
// end it listed as it started: nothing sent after it is ready is missed, and nothing it has read is
// committed anywhere.

var _ bus.Signals = (*Bus)(nil)

const hdrChannel = "Ok-Channel"

func (b *Bus) signalTopic() string { return name(b.opts.Prefix, "signals") }

func (b *Bus) Signal(ctx context.Context, channel, payload string) error {
	t := b.signalTopic()
	if err := b.ensureTopic(ctx, t, 1); err != nil {
		return err
	}
	r := &kgo.Record{Topic: t, Key: []byte(channel), Value: []byte(payload), Headers: []kgo.RecordHeader{{Key: hdrChannel, Value: []byte(channel)}}}
	if err := b.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		return fmt.Errorf("kafkabus: signaling: %w", err)
	}
	return nil
}

func (b *Bus) Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error {
	t := b.signalTopic()
	if err := b.ensureTopic(ctx, t, 1); err != nil {
		return err
	}
	ends, err := b.adm.ListEndOffsets(ctx, t)
	if err != nil {
		return fmt.Errorf("kafkabus: the end of %s: %w", t, err)
	}
	from := map[int32]kgo.Offset{}
	ends.Each(func(lo kadm.ListedOffset) { from[lo.Partition] = kgo.NewOffset().At(lo.Offset) })
	cl, err := kgo.NewClient(append(slices.Clone(b.conn), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{t: from}),
		kgo.FetchMaxWait(time.Second))...)
	if err != nil {
		return fmt.Errorf("kafkabus: a signal listener: %w", err)
	}
	defer cl.Close()
	want := map[string]bool{}
	for _, c := range channels {
		want[c] = true
	}
	ready()
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fetchErr(ctx, fs); err != nil {
			return err
		}
		fs.EachRecord(func(r *kgo.Record) {
			if c := header(r, hdrChannel); want[c] {
				deliver(c, string(r.Value))
			}
		})
	}
}
