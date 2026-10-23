package tunnel

import (
	"context"
	"errors"
	"fmt"

	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// Migrate asks the agent to run a migration, and waits for what it did. An error is one worth
// trying again: the agent could not run it, or the link ended; the migration's own failure is in
// the result.
func Migrate(ctx context.Context, link Link, req *deploypb.MigrationRequest) (*deploypb.MigrationResult, error) {
	req.Id = NewID()
	got := make(chan *deploypb.MigrationResult, 1)
	stop := link.Watch(req.Id, func(up *deploypb.AgentUp) {
		if m := up.GetMigration(); m != nil {
			select {
			case got <- m:
			default:
			}
		}
	})
	defer stop()
	if err := link.Send(ctx, &deploypb.AgentDown{Kind: &deploypb.AgentDown_Migration{Migration: req}}); err != nil {
		return nil, fmt.Errorf("tunnel: asking for the migration: %w", err)
	}
	select {
	case m := <-got:
		if m.GetError() != "" {
			return nil, fmt.Errorf("tunnel: the agent could not run %s: %s", req.GetTarget(), m.GetError())
		}
		return m, nil
	case <-ctx.Done():
		_ = link.Send(context.WithoutCancel(ctx), cancel(req.Id))
		return nil, ctx.Err()
	case <-link.Done():
		if err := link.Err(); err != nil && !errors.Is(err, ErrEnded) {
			return nil, fmt.Errorf("%w: %v", ErrEnded, err)
		}
		return nil, ErrEnded
	}
}
