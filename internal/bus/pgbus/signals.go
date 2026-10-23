package pgbus

import (
	"context"
	"fmt"

	"github.com/Cidan/ok/internal/data/pgdata"
)

// Signals are PostgreSQL's notifications: the bus of an installation on PostgreSQL alone has no
// other server to carry them, and its database's notifications reach every connection to it, every
// role's replicas among them. A listener holds a connection of its own; one that loses it returns,
// and is listened again as from the start.

func (b *Bus) Signal(ctx context.Context, channel, payload string) error {
	if _, err := b.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, channel, payload); err != nil {
		return fmt.Errorf("pgbus: signaling %s: %w", channel, err)
	}
	return nil
}

func (b *Bus) Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error {
	err := pgdata.Listen(ctx, b.pool, channels, ready, deliver)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}
