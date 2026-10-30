package store

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/Cidan/ok/internal/move"
)

// Admission at the claim (docs/hosted.md, "Hosted automation, as built"): a run dispatched to a
// worker (dispatch.go) is claimed in a transaction that locks only it, and, when its account has a
// concurrent quota, takes a slot of the account's, of at most its quota, with it; an account at
// its quota has the run parked until one of its runs stops.

// Quota is an account's quota of CI, and what the other cells last reported of its use: its
// concurrent runs and run seconds a month (zero for unlimited).
type Quota struct {
	Concurrent int64
	Seconds    int64
	// RunningElsewhere and SecondsElsewhere are the account's running runs and month's run
	// seconds in the other cells, as they last reported them.
	RunningElsewhere, SecondsElsewhere int64
}

// Quotas reads accounts' quotas, by account ("" for repositories the directory has not named).
type Quotas func(ctx context.Context, accounts []string) (map[string]Quota, error)

// admits reports whether an account may start another run: its running runs and month's seconds
// here, and elsewhere, under its quota.
func (q Quota) admits(running, seconds int64) bool {
	return (q.Concurrent <= 0 || running+q.RunningElsewhere < q.Concurrent) && (q.Seconds <= 0 || seconds+q.SecondsElsewhere < q.Seconds)
}

// MonthOf is a time's month, as usage is counted: YYYY-MM, UTC.
func MonthOf(t time.Time) string { return t.UTC().Format("2006-01") }

// errGiveBack is a claim whose run's repository a move holds: the claim gives it back.
var errGiveBack = errors.New("the claimed run's repository is moving")

// slotTries is how many free slots a claim tries, each another claim may take first.
const slotTries = 4

// quotas are accounts' quotas: Config.Quotas', or every account's unlimited.
func (s *Store) quotas(ctx context.Context, accounts []string) (map[string]Quota, error) {
	if s.cfg.Quotas == nil {
		return map[string]Quota{}, nil
	}
	return s.cfg.Quotas(ctx, accounts)
}

// freeSlot is a slot of an account's no running run holds, within its quota here — what the other
// cells run counting against it — chosen at random among them, so claims beside each other take
// different ones; ok is false when every one is held.
func (s *Store) freeSlot(ctx context.Context, account string, q Quota) (int64, bool, error) {
	n := q.Concurrent - q.RunningElsewhere
	if n <= 0 {
		return 0, false, nil
	}
	held, err := s.b.Slots(ctx, account)
	if err != nil {
		return 0, false, err
	}
	taken := make(map[int64]bool, len(held))
	for _, h := range held {
		taken[h] = true
	}
	var free []int64
	for i := int64(0); i < n; i++ {
		if !taken[i] {
			free = append(free, i)
		}
	}
	if len(free) == 0 {
		return 0, false, nil
	}
	return free[rand.IntN(len(free))], true, nil
}

// claimRun leases a run under a delivery of its dispatch job, if it is still queued in the pool, in
// a transaction of its own that takes the account's slot too (slot < 0: none) and gates the run's
// repository: errGiveBack while a move holds it, ErrSlotTaken when another run holds the slot.
func (s *Store) claimRun(ctx context.Context, id int64, job, pool, worker, account string, slot int64, handle []byte) (*Run, error) {
	key := make([]byte, 32)
	if _, err := crand.Read(key); err != nil {
		return nil, err
	}
	keyHash := sha256.Sum256(key)
	var out *Run
	err := s.txn(ctx, func(tx Ops) error {
		out = nil
		run, err := tx.ClaimRun(ctx, id, job, pool, worker, s.leaseTTL(), account, slot, keyHash[:], handle)
		if err != nil || run == nil {
			return err
		}
		if slot >= 0 {
			if err := tx.TakeSlot(ctx, account, slot, id); err != nil {
				return err
			}
		}
		// Its repository's network may have frozen since it was read: then it is the move's.
		if err := tx.Gate(ctx, run.RepositoryID); err != nil {
			if move.IsFrozen(err) || moved(err) {
				return errGiveBack
			}
			return err
		}
		run.LeaseKey = key
		out = run
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MonthUsage is this month's use by each account that ran or runs here, for the directory's
// sum over the cells.
func (s *Store) MonthUsage(ctx context.Context) (string, []Usage, error) {
	month := MonthOf(time.Now())
	u, err := s.b.MonthUsage(ctx, month)
	return month, u, err
}

// OwnerOf is a repository's account; "" for one the directory's events have not named.
func (s *Store) OwnerOf(ctx context.Context, repo int64) (string, error) {
	return s.b.OwnerOf(ctx, repo)
}

// SetOwner records a repository's account, as the directory's events name it.
func (s *Store) SetOwner(ctx context.Context, repo int64, account string) error {
	return s.b.SetOwner(move.Untagged(ctx), repo, account)
}
