package natsbus

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Signals are core NATS subjects, <prefix>.signal.<channel>, which every subscriber hears: no
// stream keeps them. A listener whose connection drops returns, since what is sent while it is
// down is not delivered to it, and the caller listens again as from the start.

func (b *Bus) signalSubject(channel string) string {
	return b.opts.Prefix + ".signal." + token(channel)
}

func (b *Bus) Signal(_ context.Context, channel, payload string) error {
	return b.nc.Publish(b.signalSubject(channel), []byte(payload))
}

func (b *Bus) Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error {
	status := b.nc.StatusChanged(nats.DISCONNECTED, nats.CLOSED)
	defer b.nc.RemoveStatusListener(status)
	var mu sync.Mutex // one delivery at a time
	var subs []*nats.Subscription
	defer func() {
		for _, sub := range subs {
			_ = sub.Unsubscribe()
		}
	}()
	for _, ch := range channels {
		sub, err := b.nc.Subscribe(b.signalSubject(ch), func(m *nats.Msg) {
			mu.Lock()
			defer mu.Unlock()
			deliver(ch, string(m.Data))
		})
		if err != nil {
			return err
		}
		subs = append(subs, sub)
	}
	// Once the server has the subscriptions, nothing published after is missed.
	fctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := b.nc.FlushWithContext(fctx)
	cancel()
	if err != nil {
		return err
	}
	ready()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case st := <-status:
		return errors.New("natsbus: the connection was " + st.String() + ": signals sent meanwhile are lost")
	}
}
