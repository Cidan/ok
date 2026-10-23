package bus

import (
	"context"
	"log/slog"
	"sync"
)

// Signals carry wake-ups to every replica (docs/hosted.md, "Signals"): a run was queued, a log
// grew, a repository's segments changed. Unlike a stream's events, a signal is no row of any log:
// it may be lost, late, or doubled, so nothing that waits on one rests on it — whatever waits also
// looks again on its own, and whatever caches by one re-reads where a stale cache cannot be right.
type Signals interface {
	// Signal tells every replica listening on channel, once and at some point, in no promised
	// order. A transaction's signals are sent once it commits (Pending), never before.
	Signal(ctx context.Context, channel, payload string) error
	// Listen calls deliver with each signal on channels until ctx ends or the backend fails,
	// returning ctx's error or the failure. It calls ready once it listens — after which no
	// signal sent is missed until it returns — so a caller that listens again first takes
	// everything it waits on as changed.
	Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error
}

// Local is Signals within one process: every listener in it hears every signal. It is for tests,
// and for roles that share a process with all the replicas they have.
type Local struct {
	mu        sync.Mutex
	listeners map[*localListener]bool
}

type localListener struct {
	channels map[string]bool
	deliver  func(channel, payload string)
	mu       sync.Mutex // one delivery at a time, as every backend's listener delivers
}

// NewLocal makes signals within the process.
func NewLocal() *Local { return &Local{listeners: map[*localListener]bool{}} }

func (l *Local) Signal(_ context.Context, channel, payload string) error {
	l.mu.Lock()
	var to []*localListener
	for ll := range l.listeners {
		if ll.channels[channel] {
			to = append(to, ll)
		}
	}
	l.mu.Unlock()
	for _, ll := range to {
		ll.mu.Lock()
		ll.deliver(channel, payload)
		ll.mu.Unlock()
	}
	return nil
}

func (l *Local) Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error {
	ll := &localListener{channels: map[string]bool{}, deliver: deliver}
	for _, c := range channels {
		ll.channels[c] = true
	}
	l.mu.Lock()
	l.listeners[ll] = true
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		delete(l.listeners, ll)
		l.mu.Unlock()
	}()
	ready()
	<-ctx.Done()
	return ctx.Err()
}

// Pending collects a transaction's signals, to be sent once it commits. Reset it as each attempt
// of the transaction begins: an attempt that aborts sends nothing.
type Pending struct {
	signals []signal
}

type signal struct{ channel, payload string }

// Add records a signal to send after commit; the same signal twice is sent once.
func (p *Pending) Add(channel, payload string) {
	for _, s := range p.signals {
		if s.channel == channel && s.payload == payload {
			return
		}
	}
	p.signals = append(p.signals, signal{channel, payload})
}

// Reset forgets every signal recorded: a new attempt begins.
func (p *Pending) Reset() { p.signals = p.signals[:0] }

// Send sends what was recorded on s, and forgets it. A signal that cannot be sent is logged, not
// returned: the transaction has committed, and whatever waits on the signal looks again on its own.
func (p *Pending) Send(ctx context.Context, s Signals) {
	for _, sg := range p.signals {
		if err := s.Signal(context.WithoutCancel(ctx), sg.channel, sg.payload); err != nil {
			slog.Warn("bus: sending a signal", "channel", sg.channel, "err", err)
		}
	}
	p.Reset()
}
