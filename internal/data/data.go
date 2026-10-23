// Package data is what every role's database needs, on any backend (docs/hosted.md, "Shared
// primitives"); pgdata is PostgreSQL's side of it.
package data

import (
	"context"
	"math/rand/v2"
	"time"
)

// Attempts is how many times Retry runs an attempt before it gives up.
const Attempts = 10

// Retry runs attempt until it returns nil or an error retryable refuses, ctx ends, or it has run
// Attempts times — waiting between attempts a little longer each time, at random within that, so
// transactions that aborted one another do not meet again. attempt holds no effects outside the
// transaction it runs.
func Retry(ctx context.Context, retryable func(error) bool, attempt func() error) error {
	for n := 1; ; n++ {
		err := attempt()
		if err == nil || !retryable(err) || n == Attempts {
			return err
		}
		backoff := time.Duration(n*n) * 5 * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff/2 + rand.N(backoff/2+1)):
		}
	}
}
