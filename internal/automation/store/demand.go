package store

import (
	"context"
	"time"

	"github.com/Cidan/ok/internal/workflow"
)

// Demand is what a pool of workers is asked for (docs/hosted.md, "Hosted automation, as built"):
// its runs queued, and those of them admission could claim now — of accounts under their quotas,
// up to what each quota leaves — its runs running, when the oldest queued run was queued, and the
// runs each worker holds.
type Demand struct {
	Queued, Claimable, Running int64
	Oldest                     time.Time
	Workers                    map[string]int64
}

// PoolDemand is a pool's demand now.
func (s *Store) PoolDemand(ctx context.Context, pool string) (Demand, error) {
	if pool == "" {
		pool = workflow.DefaultPool
	}
	queued, oldest, err := s.b.QueuedByAccount(ctx, pool)
	if err != nil {
		return Demand{}, err
	}
	running, err := s.b.RunningByAccount(ctx, pool)
	if err != nil {
		return Demand{}, err
	}
	workers, err := s.b.RunningOnWorkers(ctx, pool)
	if err != nil {
		return Demand{}, err
	}
	accounts := make([]string, 0, len(queued))
	for a := range queued {
		accounts = append(accounts, a)
	}
	seconds, err := s.b.MonthSeconds(ctx, MonthOf(time.Now()), accounts)
	if err != nil {
		return Demand{}, err
	}
	quotas, err := s.quotas(ctx, accounts)
	if err != nil {
		return Demand{}, err
	}
	d := Demand{Oldest: oldest, Workers: workers}
	for _, r := range running {
		d.Running += r.InPool
	}
	for a, n := range queued {
		d.Queued += n
		q := quotas[a]
		if !q.admits(running[a].Total, seconds[a]) {
			continue
		}
		if q.Concurrent > 0 {
			n = min(n, q.Concurrent-running[a].Total-q.RunningElsewhere)
		}
		d.Claimable += max(n, 0)
	}
	return d, nil
}
