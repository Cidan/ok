package data_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Cidan/ok/internal/data"
)

var errAgain = errors.New("again")

func again(err error) bool { return errors.Is(err, errAgain) }

func TestRetry(t *testing.T) {
	ctx := context.Background()
	n := 0
	if err := data.Retry(ctx, again, func() error {
		if n++; n < 3 {
			return errAgain
		}
		return nil
	}); err != nil || n != 3 {
		t.Errorf("an attempt that succeeds on its third: %v after %d", err, n)
	}

	n = 0
	other := errors.New("other")
	if err := data.Retry(ctx, again, func() error { n++; return other }); !errors.Is(err, other) || n != 1 {
		t.Errorf("an error it does not retry: %v after %d attempts", err, n)
	}

	n = 0
	if err := data.Retry(ctx, again, func() error { n++; return errAgain }); !errors.Is(err, errAgain) || n != data.Attempts {
		t.Errorf("an attempt that never succeeds: %v after %d attempts, want %d", err, n, data.Attempts)
	}

	cctx, cancel := context.WithCancel(ctx)
	n = 0
	if err := data.Retry(cctx, again, func() error { n++; cancel(); return errAgain }); !errors.Is(err, context.Canceled) || n != 1 {
		t.Errorf("a context that ends: %v after %d attempts", err, n)
	}
}
