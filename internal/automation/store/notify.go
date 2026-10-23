package store

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// notifier wakes the store's waiters on the database's notifications: a worker waiting for a
// queued run, a reader following a log. Each wait is also bounded by the poll interval, so a
// lost notification — or no Listen at all — delays a waiter and never strands it.
type notifier struct {
	mu      sync.Mutex
	waiters map[string]chan struct{} // closed by the next matching notification
}

func newNotifier() *notifier { return &notifier{waiters: map[string]chan struct{}{}} }

func waiterKey(channel, payload string) string { return channel + "\x00" + payload }

// watch returns a channel closed by the next notification on channel whose payload is
// payload, or any notification on it when payload is "".
func (n *notifier) watch(channel, payload string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	k := waiterKey(channel, payload)
	ch, ok := n.waiters[k]
	if !ok {
		ch = make(chan struct{})
		n.waiters[k] = ch
	}
	return ch
}

// fire wakes the waiters for a notification.
func (n *notifier) fire(channel, payload string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, k := range []string{waiterKey(channel, payload), waiterKey(channel, "")} {
		if ch, ok := n.waiters[k]; ok {
			close(ch)
			delete(n.waiters, k)
		}
	}
}

// fireAll wakes every waiter: after the listener reconnects, any notification may have been
// missed.
func (n *notifier) fireAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, ch := range n.waiters {
		close(ch)
		delete(n.waiters, k)
	}
}

// wait returns when ch is closed, max passes, or ctx ends (its error).
func (n *notifier) wait(ctx context.Context, ch <-chan struct{}, max time.Duration) error {
	return n.waitAny(ctx, max, ch)
}

// waitAny returns when any of chs is closed, max passes, or ctx ends (its error).
func (n *notifier) waitAny(ctx context.Context, max time.Duration, chs ...<-chan struct{}) error {
	t := time.NewTimer(max)
	defer t.Stop()
	woke := make(chan struct{})
	stop := make(chan struct{})
	defer close(stop)
	for _, ch := range chs {
		go func() {
			select {
			case <-ch:
				select {
				case woke <- struct{}{}:
				case <-stop:
				}
			case <-stop:
			}
		}()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	case <-woke:
	}
	return nil
}

// Listen delivers the store's signals to its waiters until ctx ends, listening again after a
// failure.
func (s *Store) Listen(ctx context.Context) error {
	for ctx.Err() == nil {
		err := s.cfg.Signals.Listen(ctx, []string{chanQueue, chanLog, chanDone, chanSubmission}, s.notify.fireAll, s.notify.fire)
		if ctx.Err() != nil {
			return nil
		}
		slog.Warn("automation: listening for notifications", "err", err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Second):
		}
	}
	return nil
}
