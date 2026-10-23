package pgdata

import (
	"context"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/lock"
)

// CheckFence checks, in the transaction q is, the fence ctx carries, if any (data.Fence): the
// first thing a store's transaction does that writes under a lease.
func CheckFence(ctx context.Context, q Querier) error {
	f, ok := data.FenceOf(ctx)
	if !ok {
		return nil
	}
	if f.Lock {
		return lock.CheckFence(ctx, q, f)
	}
	return CheckWorkFence(ctx, q, f)
}
