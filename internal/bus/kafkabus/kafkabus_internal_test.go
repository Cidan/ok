package kafkabus

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/bustest"
)

func open(t *testing.T, brokers []string) *Bus {
	t.Helper()
	b, err := Open([]kgo.Opt{kgo.SeedBrokers(brokers...)}, Options{Partitions: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	return b
}

// An event its handler refuses delays none behind it in its partition: they are handled first, and
// it once due, after being set aside twice — and a group made again keeps its place.
func TestSetAside(t *testing.T) {
	brokers := bustest.Kafka(t)
	b := open(t, brokers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.CreateGroup(ctx, "t", "g"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var order []int64
	refusals := 0
	done := make(chan struct{})
	h := func(_ context.Context, e bus.Event) error {
		mu.Lock()
		defer mu.Unlock()
		if e.Seq == 1 && refusals < 2 {
			refusals++
			return errors.New("not yet")
		}
		order = append(order, e.Seq)
		if len(order) == 5 {
			close(done)
		}
		return nil
	}
	sub := make(chan error, 1)
	go func() { sub <- b.Subscribe(ctx, "t", "g", h) }()
	var es []bus.Event
	for s := int64(1); s <= 5; s++ {
		es = append(es, bus.Event{Topic: "t", Key: 7, Seq: s, Body: []byte{byte(s)}})
	}
	if err := b.Publish(ctx, es...); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("handled %v", order)
	}
	mu.Lock()
	if order[len(order)-1] != 1 || refusals != 2 {
		t.Errorf("handled in the order %v after %d refusals: the refused event last, after two", order, refusals)
	}
	mu.Unlock()
	cancel()
	if err := <-sub; !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe: %v", err)
	}

	// Made again, the group starts where it was: nothing is handled twice.
	again := open(t, brokers)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := again.CreateGroup(ctx2, "t", "g"); err != nil {
		t.Fatal(err)
	}
	var more []int64
	sub2 := make(chan error, 1)
	go func() {
		sub2 <- again.Subscribe(ctx2, "t", "g", func(_ context.Context, e bus.Event) error {
			mu.Lock()
			defer mu.Unlock()
			more = append(more, e.Seq)
			return nil
		})
	}()
	if err := again.Publish(ctx2, bus.Event{Topic: "t", Key: 7, Seq: 6}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		mu.Lock()
		n := len(more)
		mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	cancel2()
	<-sub2
	mu.Lock()
	defer mu.Unlock()
	if len(more) != 1 || more[0] != 6 {
		t.Errorf("the group made again handled %v, want only the event published since", more)
	}
}

// Names keep to Kafka's alphabet and length, and a long one is cut the same way every time.
func TestNames(t *testing.T) {
	if got := name("ok", "ev", "scm.events", "control_follow"); got != "ok.ev.scm_2eevents.control_5ffollow" {
		t.Errorf("a name: %q", got)
	}
	long := name("ok", "ev", strings.Repeat("t", 200), strings.Repeat("g", 200))
	if len(long) != maxName || long != name("ok", "ev", strings.Repeat("t", 200), strings.Repeat("g", 200)) ||
		strings.Trim(long, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.") != "" {
		t.Errorf("a long name: %q (%d)", long, len(long))
	}
}
