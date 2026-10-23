package review

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// An OWNERS read that could not reach the SCM is the caller's to try again; one that read an
// invalid OWNERS file fails Approval closed.
func TestUnreachable(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("read OWNERS: %w", status.Error(codes.Unavailable, "connection refused")), true},
		{fmt.Errorf("read OWNERS: %w", status.Error(codes.DeadlineExceeded, "slow")), true},
		{fmt.Errorf("read OWNERS: %w", context.Canceled), true},
		{status.Error(codes.Aborted, "transaction aborted"), true},
		{fmt.Errorf("read OWNERS: %w", status.Error(codes.PermissionDenied, "no")), false},
		{errors.New("OWNERS:3: unknown directive"), false},
	} {
		if got := unreachable(c.err); got != c.want {
			t.Errorf("unreachable(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}
