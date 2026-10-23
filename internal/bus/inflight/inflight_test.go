package inflight_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus/inflight"
)

// Slots run at most their number at once, Reserve waits for a free one, Release returns what a
// receiver did not use, and Err is the first error.
func TestSlots(t *testing.T) {
	ctx := context.Background()
	s := inflight.New(3)
	var running, peak atomic.Int64
	release := make(chan struct{})
	if n, err := s.Reserve(ctx, 5); err != nil || n != 3 {
		t.Fatalf("reserving 5 of 3: %d, %v", n, err)
	}
	s.Release(1)
	if n, err := s.Reserve(ctx, 5); err != nil || n != 1 {
		t.Fatalf("reserving the one released: %d, %v", n, err)
	}
	for range 3 {
		s.Run(func() error {
			n := running.Add(1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			<-release
			running.Add(-1)
			return errors.New("first")
		})
	}
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := s.Reserve(short, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reserving with every slot busy: %v", err)
	}
	close(release)
	s.Wait()
	if n, err := s.Reserve(ctx, 3); err != nil || n != 3 {
		t.Fatalf("reserving after the handlers finished: %d, %v", n, err)
	}
	if peak.Load() != 3 || s.Err() == nil {
		t.Fatalf("peak %d, err %v", peak.Load(), s.Err())
	}
}
