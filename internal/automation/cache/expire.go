package cache

import (
	"context"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/work"
)

// ExpireWork is the cache's expiry, as keyed work: one replica at a time sweeps it, every
// expireEvery.
const ExpireWork = "cache"

const expireEvery = time.Hour

// Expire sweeps what the cache wrote ttl ago, every hour, on one replica at a time, until ctx ends.
func Expire(ctx context.Context, s *Server, ws work.Store, ttl time.Duration, log *slog.Logger) error {
	if err := ws.MarkAt(ctx, ExpireWork, 0, time.Now().Add(expireEvery)); err != nil && ctx.Err() == nil {
		log.Warn("cache: scheduling its expiry", "err", err)
	}
	return (&work.Runner{
		Kind:  ExpireWork,
		Store: ws,
		Do: func(ctx context.Context, _ int64) error {
			n, err := s.Sweep(ctx, time.Now().Add(-ttl))
			if n > 0 {
				log.Info("cache: expired entries", "deleted", n)
			}
			if err != nil {
				return err
			}
			return ws.MarkAt(ctx, ExpireWork, 0, time.Now().Add(expireEvery))
		},
		Idle: expireEvery,
		Log:  log,
	}).Run(ctx)
}
