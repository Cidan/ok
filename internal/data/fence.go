package data

import (
	"context"
	"errors"
	"fmt"
)

// A Fence names the lease a transaction's writes rest on (docs/hosted.md, "Shared primitives"): a
// lock's (internal/lock) or a piece of keyed work's (internal/work). A store whose context carries
// one checks, in the transaction that writes, that its holder still holds it — and commits nothing
// if not — so a holder that stalled past its lease, by a pause or a partition, never writes after
// another has taken the lease: at worst it wastes its work. The lease's row is in the database
// the transaction writes, so the check and the writes are one transaction, and the check holds the
// row against a takeover until it commits.
type Fence struct {
	// Lock is a lock's lease, in bucket Bucket, named ID; otherwise keyed work's, of kind Kind and
	// key Key.
	Lock   bool
	Bucket string
	ID     string
	Kind   string
	Key    int64
	// Holder is what the lease's row says while its holder holds it: the lock's refresh ID, or the
	// work's holder.
	Holder string
}

func (f Fence) String() string {
	if f.Lock {
		return fmt.Sprintf("lock %s/%s", f.Bucket, f.ID)
	}
	return fmt.Sprintf("work %s/%d", f.Kind, f.Key)
}

// ErrFenced is a write refused because the lease it rests on is no longer its holder's.
var ErrFenced = errors.New("data: the lease the write rests on is no longer held")

type fenceKey struct{}

// WithFence is ctx carrying f: every store write made with it is fenced by it. A context carries
// one fence; a new one replaces any it carried.
func WithFence(ctx context.Context, f Fence) context.Context {
	return context.WithValue(ctx, fenceKey{}, f)
}

// FenceOf is the fence ctx carries, if any.
func FenceOf(ctx context.Context) (Fence, bool) {
	f, ok := ctx.Value(fenceKey{}).(Fence)
	return f, ok
}

// Unfenced is ctx carrying no fence: for writes that do not rest on the lease the work holds —
// recording, after the lease was lost, that the work stopped.
func Unfenced(ctx context.Context) context.Context { return context.WithValue(ctx, fenceKey{}, nil) }
