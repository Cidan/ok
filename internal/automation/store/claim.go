package store

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"errors"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/workflow"
)

// Admission and fair share at the claim (docs/hosted.md, "Hosted automation, as built"): a worker's
// lease admits the next run of its pool — of the accounts with runs queued there and under their
// quotas, the one with the fewest running in the pool for its weight, its head. What a claim reads
// to choose, it reads outside its transaction, which locks only the run it takes; an account with a
// concurrent quota runs each run in a slot of its own, of at most its quota, so claims beside each
// other never pass it.

// Quota is an account's quota of CI, and what the other cells last reported of its use: its
// concurrent runs and run seconds a month (zero for unlimited), and its weight in fair share.
type Quota struct {
	Concurrent int64
	Seconds    int64
	Weight     int64
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

func (q Quota) weight() int64 { return max(q.Weight, 1) }

// MonthOf is a time's month, as usage is counted: YYYY-MM, UTC.
func MonthOf(t time.Time) string { return t.UTC().Format("2006-01") }

// Candidate is an account a claim may serve: its head in the pool, its running runs, its month's
// run seconds here, and its quota.
type Candidate struct {
	Account string
	Head    Head
	Running Running
	Seconds int64
	Quota   Quota
}

// Admit orders the accounts a claim serves: those under their quotas, the fewest running in the
// pool for their weight first — ties to the higher priority's head, then the older, then by
// account.
func Admit(cs []Candidate) []Candidate {
	var out []Candidate
	for _, c := range cs {
		if c.Quota.admits(c.Running.Total, c.Seconds) {
			out = append(out, c)
		}
	}
	slices.SortStableFunc(out, func(a, b Candidate) int {
		// a.InPool / a.weight against b.InPool / b.weight, without division.
		if l, r := a.Running.InPool*b.Quota.weight(), b.Running.InPool*a.Quota.weight(); l != r {
			if l < r {
				return -1
			}
			return 1
		}
		if a.Head.Priority != b.Head.Priority {
			return int(b.Head.Priority) - int(a.Head.Priority)
		}
		if c := a.Head.Oldest.Compare(b.Head.Oldest); c != 0 {
			return c
		}
		return strings.Compare(a.Account, b.Account)
	})
	return out
}

// errGiveBack is a claim whose run's repository a move holds: the claim gives it back.
var errGiveBack = errors.New("the claimed run's repository is moving")

// claim admits and leases a queued run of a pool to a worker; nil when none may run.
func (s *Store) claim(ctx context.Context, worker, pool string) (*Run, error) {
	if pool == "" {
		pool = workflow.DefaultPool
	}
	heads, err := s.b.QueuedHeads(ctx, pool)
	if err != nil || len(heads) == 0 {
		return nil, err
	}
	byAccount := map[string]Head{}
	for _, h := range heads {
		cur, ok := byAccount[h.Account]
		if !ok || h.Priority > cur.Priority || h.Priority == cur.Priority && h.Oldest.Before(cur.Oldest) {
			byAccount[h.Account] = h
		}
	}
	accounts := make([]string, 0, len(byAccount))
	for a := range byAccount {
		accounts = append(accounts, a)
	}
	slices.Sort(accounts)
	running, err := s.b.RunningByAccount(ctx, pool)
	if err != nil {
		return nil, err
	}
	month := MonthOf(time.Now())
	seconds, err := s.b.MonthSeconds(ctx, month, accounts)
	if err != nil {
		return nil, err
	}
	quotas, err := s.quotas(ctx, accounts)
	if err != nil {
		return nil, err
	}
	var cs []Candidate
	for _, a := range accounts {
		cs = append(cs, Candidate{Account: a, Head: byAccount[a], Running: running[a], Seconds: seconds[a], Quota: quotas[a]})
	}
	for _, c := range Admit(cs) {
		run, err := s.claimOf(ctx, c.Account, pool, worker, c.Quota)
		if err != nil || run != nil {
			return run, err
		}
	}
	return nil, nil
}

// Queued is a queued run a claim may take, and its priority.
type Queued struct {
	ID       int64
	Priority int16
}

// claimCandidates are how many of an account's next runs a claim tries; claimSpread how many of the
// first of its head's priority it goes on at, at random, once another claim took its head — workers
// claiming at once then meet less on one run, while a claim alone takes the oldest — and slotTries
// how many free slots it tries, each another claim may take first.
const (
	claimCandidates = 8
	claimSpread     = 4
	slotTries       = 4
)

// spread orders a claim's candidates after its head: from one of the first few of the head's
// priority, at random, keeping every higher priority before every lower.
func spread(qs []Queued) []int64 {
	if len(qs) == 0 {
		return nil
	}
	lead := 1
	for lead < min(len(qs), claimSpread) && qs[lead].Priority == qs[0].Priority {
		lead++
	}
	start := rand.IntN(lead)
	out := make([]int64, 0, len(qs))
	for _, q := range qs[start:lead] {
		out = append(out, q.ID)
	}
	for _, q := range qs[:start] {
		out = append(out, q.ID)
	}
	for _, q := range qs[lead:] {
		out = append(out, q.ID)
	}
	return out
}

// quotas are accounts' quotas: Config.Quotas', or every account's unlimited.
func (s *Store) quotas(ctx context.Context, accounts []string) (map[string]Quota, error) {
	if s.cfg.Quotas == nil {
		return map[string]Quota{}, nil
	}
	return s.cfg.Quotas(ctx, accounts)
}

// claimOf leases one of an account's next runs in a pool to a worker — in a slot of the account's,
// when it has a concurrent quota — or nil when another claim took each first, its slots are full,
// or a move holds the run's repository.
func (s *Store) claimOf(ctx context.Context, account, pool, worker string, q Quota) (*Run, error) {
	ctx = move.Untagged(ctx)
	qs, err := s.b.ClaimCandidates(ctx, account, pool, claimCandidates)
	if err != nil || len(qs) == 0 {
		return nil, err
	}
	ids := append([]int64{qs[0].ID}, spread(qs[1:])...)
	for tries := 0; ; tries++ {
		slot := int64(-1)
		if q.Concurrent > 0 {
			var ok bool
			if slot, ok, err = s.freeSlot(ctx, account, q); err != nil || !ok {
				return nil, err
			}
		}
		run, err := s.claimFirst(ctx, ids, pool, worker, account, slot)
		if errors.Is(err, ErrSlotTaken) && tries+1 < slotTries {
			continue // another claim took the slot first: another
		}
		if errors.Is(err, ErrSlotTaken) {
			return nil, nil
		}
		return run, err
	}
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

// claimFirst leases the first of the runs still queued, in a transaction of its own each, which
// takes the slot too and gates the run's repository.
func (s *Store) claimFirst(ctx context.Context, ids []int64, pool, worker, account string, slot int64) (*Run, error) {
	key := make([]byte, 32)
	if _, err := crand.Read(key); err != nil {
		return nil, err
	}
	keyHash := sha256.Sum256(key)
	for _, id := range ids {
		var out *Run
		err := s.txn(ctx, func(tx Ops) error {
			out = nil
			run, err := tx.ClaimRun(ctx, id, pool, worker, s.leaseTTL(), account, slot, keyHash[:])
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
		switch {
		case errors.Is(err, errGiveBack):
			continue
		case err != nil:
			return nil, err
		case out != nil:
			return out, nil
		}
	}
	return nil, nil
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
