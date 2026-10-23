// Package busconfig opens the bus (internal/bus) an ok configuration names.
package busconfig

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/pgbus"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/pgdb"
)

// Bus is an open bus.
type Bus struct {
	Stream  bus.Stream
	Queue   bus.Queue
	closers []func()
}

// Close closes what Open opened.
func (b *Bus) Close() {
	for i := len(b.closers) - 1; i >= 0; i-- {
		b.closers[i]()
	}
}

// Open opens the bus c names. A postgres backend is in the database the role's pool reaches, in
// the bus's own schema; the stream and queue share one connection to it when both are postgres.
func Open(ctx context.Context, c config.Bus, role *pgxpool.Pool) (*Bus, error) {
	b := &Bus{}
	var pg *pgbus.Bus
	postgres := func() (*pgbus.Bus, error) {
		if pg != nil {
			return pg, nil
		}
		pool, err := pgdb.Beside(ctx, role, pgbus.Schema)
		if err != nil {
			return nil, fmt.Errorf("the bus: %w", err)
		}
		if pg, err = pgbus.Open(ctx, pool, pgbus.Options{}); err != nil {
			pool.Close()
			return nil, err
		}
		b.closers = append(b.closers, pool.Close, pg.Close)
		return pg, nil
	}
	for _, s := range []struct {
		field string
		typ   string
		set   func(*pgbus.Bus)
	}{
		{"bus.stream", c.Stream.Type, func(p *pgbus.Bus) { b.Stream = p }},
		{"bus.queue", c.Queue.Type, func(p *pgbus.Bus) { b.Queue = p }},
	} {
		switch s.typ {
		case "", "postgres":
			p, err := postgres()
			if err != nil {
				b.Close()
				return nil, err
			}
			s.set(p)
		default:
			b.Close()
			return nil, fmt.Errorf("%s.type: %q is not postgres", s.field, s.typ)
		}
	}
	return b, nil
}
