package edge

import (
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/Cidan/ok/internal/config"
)

// limiter is a token bucket per key — a principal, an address — at this replica, forgetting a key
// idle for limiterIdle.
type limiter struct {
	r rate.Limit
	b int

	mu   sync.Mutex
	keys map[string]*bucket
	next time.Time // when idle keys are next swept
}

type bucket struct {
	l    *rate.Limiter
	seen time.Time
}

// limiterIdle is how long a key goes unseen before its bucket is dropped, full again.
const limiterIdle = 10 * time.Minute

func newLimiter(r config.Rate) *limiter {
	return &limiter{r: rate.Limit(r.PerSecond), b: r.Burst, keys: map[string]*bucket{}}
}

// allow takes a token of key's bucket, reporting false when it has none.
func (l *limiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.After(l.next) {
		for k, b := range l.keys {
			if now.Sub(b.seen) > limiterIdle {
				delete(l.keys, k)
			}
		}
		l.next = now.Add(limiterIdle)
	}
	b, ok := l.keys[key]
	if !ok {
		b = &bucket{l: rate.NewLimiter(l.r, l.b)}
		l.keys[key] = b
	}
	b.seen = now
	return b.l.AllowN(now, 1)
}
