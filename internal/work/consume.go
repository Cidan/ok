package work

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/bus"
)

// Step does the step a job names (Pend): nil when it is done, or the job is of nothing — an
// attempt that never committed; Later when what the job names is not there yet, or another is
// taking it, to look again after; another error to try again a while later.
type Step func(ctx context.Context, key int64, body []byte, age time.Duration) error

// Later is a step's answer that what its job names is not there yet — its attempt still
// committing — or is another's for now: the job is looked at again after After.
type Later struct{ After time.Duration }

func (l Later) Error() string { return "work: looked at again after " + l.After.String() }

// Source is where a database's jobs are: its keyed work's store, read at each lease, so a consumer
// moves to the bus its database is registered with now (Register).
type Source interface{ Jobs() Jobs }

// Consume does kind's steps on src's bus as their jobs come, workers at once, until ctx ends. A
// step that fails is tried again RetryAfter later; a step's job is acknowledged only once it is
// done, so one whose consumer died is brought again.
func Consume(ctx context.Context, src Source, kind string, workers int, retryAfter time.Duration, do Step, log *slog.Logger) {
	if workers <= 0 {
		workers = 4
	}
	if retryAfter <= 0 {
		retryAfter = 10 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for ctx.Err() == nil {
				jobs := src.Jobs()
				ls, err := jobs.Queue.Lease(ctx, jobs.QueueOf(kind), bus.LeaseOptions{Max: 1, Visibility: 5 * time.Minute, Wait: wait})
				if err != nil {
					if ctx.Err() == nil {
						log.Warn("work: leasing a step's job", "kind", kind, "err", err)
						sleep(ctx, time.Second)
					}
					continue
				}
				for _, l := range ls {
					step(ctx, jobs, kind, l, retryAfter, do, log)
				}
			}
		})
	}
	wg.Wait()
}

func step(ctx context.Context, jobs Jobs, kind string, l bus.Leased, retryAfter time.Duration, do Step, log *slog.Logger) {
	j := l.Job()
	if len(j.Body) < 8 {
		_ = l.Ack(context.WithoutCancel(ctx))
		return
	}
	made := time.UnixMilli(int64(binary.BigEndian.Uint64(j.Body)))
	err := do(ctx, j.Key, j.Body[8:], time.Since(made))
	bg := context.WithoutCancel(ctx)
	var later Later
	switch {
	case err == nil:
		if err := l.Ack(bg); err != nil && !errors.Is(err, bus.ErrLeaseLost) {
			log.Warn("work: acknowledging a step's job", "kind", kind, "err", err)
		}
		return
	case errors.As(err, &later):
	case ctx.Err() != nil:
		later.After = 0
	default:
		log.Warn("work: a step", "kind", kind, "key", j.Key, "err", err)
		later.After = retryAfter
	}
	// Again as a fresh job, which spends none of this one's attempts, before this one goes; that
	// failing, this one returned.
	again := j
	again.ID, again.NotBefore = newToken()+newToken(), time.Now().Add(later.After)
	if err := jobs.Queue.Enqueue(bg, again); err != nil {
		if err := l.Nack(bg, later.After); err != nil && !errors.Is(err, bus.ErrLeaseLost) {
			log.Warn("work: returning a step's job", "kind", kind, "err", err)
		}
		return
	}
	if err := l.Ack(bg); err != nil && !errors.Is(err, bus.ErrLeaseLost) {
		log.Warn("work: acknowledging a step's job", "kind", kind, "err", err)
	}
}
