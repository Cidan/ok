// Package data is what every role's database needs, on any backend (docs/hosted.md, "Shared
// primitives"); pgdata is PostgreSQL's side of it.
package data

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"math/rand/v2"
	"strings"
	"time"
)

// NewID is a random identifier, from 1 to 2^63-1: unique across every database that makes one
// with no counter to share, so what it names can move between them (docs/hosted.md, "Shared
// primitives").
func NewID() int64 {
	var b [8]byte
	for {
		crand.Read(b[:])
		if id := int64(binary.BigEndian.Uint64(b[:]) >> 1); id != 0 {
			return id
		}
	}
}

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

// CanonicalJSON is JSON as a move carries it between databases, which spell it differently: its
// objects' keys sorted, no space, and its numbers as written.
func CanonicalJSON(s string) (string, error) {
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return "", err
	}
	b, err := json.Marshal(v)
	return string(b), err
}
