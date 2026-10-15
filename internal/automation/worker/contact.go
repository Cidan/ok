package worker

import (
	"sync/atomic"
	"time"
)

// ReadyWithin is how recently ok automation must have answered a worker for it to be ready:
// an idle worker hears from automation at least once a lease, a busy one at every heartbeat.
const ReadyWithin = 2 * leaseWait

// Contact is when ok automation last answered the worker — a lease, with a run or without,
// or a run's heartbeat — which is the worker's readiness: a worker that cannot reach automation
// takes no runs, though it keeps trying and never fails on its own.
type Contact struct {
	last atomic.Int64 // Unix nanoseconds; 0 until automation first answers
}

// heard records an answer from automation at now. A nil Contact records nothing.
func (c *Contact) heard(now time.Time) {
	if c != nil {
		c.last.Store(now.UnixNano())
	}
}

// Within reports whether automation answered within d before now.
func (c *Contact) Within(now time.Time, d time.Duration) bool {
	last := c.last.Load()
	return last != 0 && now.Sub(time.Unix(0, last)) <= d
}
