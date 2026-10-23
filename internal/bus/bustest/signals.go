package bustest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
)

// heard records what a listener was delivered.
type heard struct {
	mu  sync.Mutex
	got []string // channel, a space, payload
}

func (h *heard) deliver(channel, payload string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.got = append(h.got, channel+" "+payload)
}

func (h *heard) count(s string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, g := range h.got {
		if g == s {
			n++
		}
	}
	return n
}

func (h *heard) all() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.got...)
}

// listen runs a listener on channels until the returned stop, waiting until it is ready; stop
// returns Listen's error.
func listen(t *testing.T, s bus.Signals, channels []string, h *heard) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{}, 8)
	done := make(chan error, 1)
	go func() {
		done <- s.Listen(ctx, channels, func() { ready <- struct{}{} }, h.deliver)
	}()
	select {
	case <-ready:
	case err := <-done:
		cancel()
		t.Fatalf("Listen ended before it was ready: %v", err)
	case <-time.After(patience):
		cancel()
		t.Fatal("Listen was never ready")
	}
	stopped := false
	var stopErr error
	stop = func() error {
		if !stopped {
			stopped = true
			cancel()
			select {
			case stopErr = <-done:
			case <-time.After(patience):
				t.Error("Listen did not return once its context ended")
			}
		}
		return stopErr
	}
	t.Cleanup(func() { stop() })
	return stop
}

func signal(t *testing.T, s bus.Signals, channel, payload string) {
	t.Helper()
	if err := s.Signal(context.Background(), channel, payload); err != nil {
		t.Fatalf("Signal(%s, %q): %v", channel, payload, err)
	}
}

// quiet is how long a test waits to see that nothing more is delivered.
const quiet = 2 * time.Second

// RunSignals is the suite every bus.Signals passes: a signal sent after a listener is ready
// reaches it, and every other listener on its channel — another replica's, made by open again —
// with its payload as sent, the empty one included; a listener hears no channel it does not listen
// on; a listener stops when its context ends, and one made again is ready again.
func RunSignals(t *testing.T, open func(t *testing.T) bus.Signals) {
	t.Run("FanOut", func(t *testing.T) {
		a, b := open(t), open(t)
		x, y, z := name("signal"), name("signal"), name("signal")
		var ha, hb heard
		listen(t, a, []string{x, y}, &ha)
		listen(t, b, []string{x}, &hb)
		for _, p := range []string{"1", "", "a payload: with spaces, ünïcode, and\ttabs"} {
			signal(t, a, x, p)
		}
		signal(t, b, y, "2")
		signal(t, a, z, "3")
		for _, want := range []string{x + " 1", x + " ", x + " a payload: with spaces, ünïcode, and\ttabs"} {
			eventually(t, "both listeners to hear "+want, func() bool { return ha.count(want) > 0 && hb.count(want) > 0 })
		}
		eventually(t, "the listener on y to hear it", func() bool { return ha.count(y+" 2") > 0 })
		time.Sleep(quiet)
		for _, g := range hb.all() {
			if !strings.HasPrefix(g, x+" ") {
				t.Errorf("a listener on %s heard %q", x, g)
			}
		}
		for _, g := range ha.all() {
			if strings.HasPrefix(g, z+" ") {
				t.Errorf("a listener on %s and %s heard %q", x, y, g)
			}
		}
	})

	t.Run("Many", func(t *testing.T) {
		s := open(t)
		x := name("signal")
		var h heard
		listen(t, s, []string{x}, &h)
		for i := range 200 {
			signal(t, s, x, fmt.Sprint(i))
		}
		eventually(t, "every signal to be heard", func() bool {
			for i := range 200 {
				if h.count(fmt.Sprintf("%s %d", x, i)) == 0 {
					return false
				}
			}
			return true
		})
	})

	t.Run("StopAndListenAgain", func(t *testing.T) {
		s := open(t)
		x := name("signal")
		var first heard
		stop := listen(t, s, []string{x}, &first)
		signal(t, s, x, "before")
		eventually(t, "the first listener to hear it", func() bool { return first.count(x+" before") > 0 })
		if err := stop(); err != nil && err != context.Canceled {
			t.Errorf("Listen once its context ended: %v", err)
		}
		signal(t, s, x, "between")
		var again heard
		listen(t, s, []string{x}, &again)
		signal(t, s, x, "after")
		eventually(t, "the listener made again to hear it", func() bool { return again.count(x+" after") > 0 })
		time.Sleep(quiet)
		if first.count(x+" after") > 0 {
			t.Error("a stopped listener heard a signal")
		}
	})
}
