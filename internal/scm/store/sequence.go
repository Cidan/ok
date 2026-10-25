package store

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/scm/store/backend"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Sequencing (docs/scm.md, "Sequencing"). A repository's CL numbers and the places of its events in
// its log follow the order its transactions commit in. On a backend that takes them from the
// repository's counters inside the transaction (PostgreSQL), a transaction holds those rows from
// then until it commits; on one that sequences after commit (Spanner), where a row every request
// of a repository writes would abort the requests that meet on it, a transaction leaves them
// pending instead — each CL it makes numbered by its ID negated, each event unplaced — and the
// repository's sequencing pass, a short transaction run just after, numbers the CLs and places the
// events in commit order. A request answers only once its pass has run; a provisional number is
// never shown outside the SCM.

// pendingKey is the key of the next row t leaves pending in repo.
func (t *txn) pendingKey(repo int64) backend.PendingKey {
	if t.sequence == nil {
		t.sequence = map[int64]bool{}
	}
	t.sequence[repo] = true
	t.places++
	return backend.PendingKey{Txn: t.tie, Place: t.places}
}

// newChangeLists gives n CLs about to be made in tx their IDs and numbers: taken now from the
// repository's counter, or provisional — their IDs negated — and left pending, in order, for the
// pass.
func (s *Store) newChangeLists(ctx context.Context, tx *txn, repo int64, n int) (ids, numbers []int64, err error) {
	ids, numbers = make([]int64, n), make([]int64, n)
	for i := range ids {
		ids[i] = data.NewID()
	}
	if !tx.after {
		last, err := tx.NextChangeListNumbers(ctx, repo, int64(n))
		if err != nil {
			return nil, nil, err
		}
		for i := range numbers {
			numbers[i] = last - int64(n) + 1 + int64(i)
		}
		return ids, numbers, nil
	}
	rows := make([]backend.Pending, n)
	for i, id := range ids {
		numbers[i] = -id
		rows[i] = backend.Pending{Repository: repo, Key: tx.pendingKey(repo), ChangeList: id}
	}
	return ids, numbers, tx.Pend(ctx, rows)
}

// pendEvents leaves evs pending in tx, for the pass to place in the log.
func pendEvents(ctx context.Context, tx *txn, repo int64, at time.Time, evs []*scmpb.Event) error {
	rows := make([]backend.Pending, len(evs))
	for i, ev := range evs {
		ev.RepositoryId, ev.RepositorySeq, ev.At = repo, 0, timestamppb.New(at)
		body, err := proto.Marshal(ev)
		if err != nil {
			return err
		}
		rows[i] = backend.Pending{Repository: repo, Key: tx.pendingKey(repo), At: at, Body: body}
		tx.appended = append(tx.appended, appended{repo, ev})
	}
	return tx.Pend(ctx, rows)
}

// appended is an event a transaction left pending, as its caller holds it.
type appended struct {
	repo int64
	ev   *scmpb.Event
}

// changeListRef is the field of ev that names a CL by number: provisional when the transaction that
// wrote it found the CL so — one it made, or one another made and the pass has yet to number.
func changeListRef(ev *scmpb.Event) *int64 {
	switch k := ev.GetKind().(type) {
	case *scmpb.Event_Uploaded:
		return &k.Uploaded.Number
	case *scmpb.Event_Transitioned:
		return &k.Transitioned.Number
	case *scmpb.Event_Landed:
		return &k.Landed.Number
	}
	return nil
}

// sequencing is the context a pass runs in: its own, as it sequences writes already committed, which
// no caller's end, nor a move's gate, holds back (a move sequences its network before it exports it).
func sequencing() context.Context { return context.Background() }

// sequenced finishes, after it committed, what t left pending: the passes of its repositories,
// then its events' places and the numbers they name, filled into the events its caller holds.
func (s *Store) sequenced(t *txn) error {
	ctx := sequencing()
	need := time.Now()
	if s.beforeSequenced != nil {
		s.beforeSequenced()
	}
	for repo := range t.sequence {
		if err := s.sequence(ctx, repo, need); err != nil {
			return fmt.Errorf("sequencing repository %d: %w", repo, err)
		}
	}
	byRepo := map[int64][]*scmpb.Event{}
	for _, a := range t.appended {
		byRepo[a.repo] = append(byRepo[a.repo], a.ev)
	}
	for repo, evs := range byRepo {
		seqs, err := s.ops(ctx).TransactionEvents(ctx, repo, t.tie)
		if err != nil {
			return err
		}
		if len(seqs) != len(evs) {
			return fmt.Errorf("repository %d's log holds %d of the %d events its transaction wrote", repo, len(seqs), len(evs))
		}
		var ids []int64
		for i, ev := range evs {
			ev.RepositorySeq = seqs[i]
			if n := changeListRef(ev); n != nil && *n < 0 {
				ids = append(ids, -*n)
			}
		}
		if len(ids) == 0 {
			continue
		}
		numbers, err := s.ops(ctx).ChangeListNumbers(ctx, repo, ids)
		if err != nil {
			return err
		}
		for _, ev := range evs {
			if n := changeListRef(ev); n != nil && *n < 0 {
				*n = numbers[-*n]
			}
		}
	}
	return nil
}

// sequencer runs a repository's passes one at a time in this replica, each serving every caller
// that needs what committed before it began.
type sequencer struct {
	mu    sync.Mutex
	repos map[int64]*repoSequence
}

type repoSequence struct {
	mu    sync.Mutex
	began time.Time // when the last pass to finish began
}

func (q *sequencer) of(repo int64) *repoSequence {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.repos == nil {
		q.repos = map[int64]*repoSequence{}
	}
	r, ok := q.repos[repo]
	if !ok {
		r = &repoSequence{}
		q.repos[repo] = r
	}
	return r
}

// sequencePage is how many pending rows one pass's transaction sequences.
const sequencePage = 500

// sequence runs repo's pass, unless one that began at or after need has finished in this replica:
// that one sequenced every row committed before need. A pass sequences what is pending, a page at a
// time, until none is; what another replica's pass took meanwhile it leaves.
func (s *Store) sequence(ctx context.Context, repo int64, need time.Time) error {
	if !s.b.SequencesAfterCommit() {
		return nil
	}
	r := s.seq.of(repo)
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.began.Before(need) {
		return nil
	}
	began := time.Now()
	for {
		rows, err := s.b.PendingRows(ctx, repo, sequencePage)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		if err := s.sequencePass(ctx, repo, rows); err != nil {
			return err
		}
		if len(rows) < sequencePage {
			break
		}
	}
	r.began = began
	if s.sequencePasses != nil {
		s.sequencePasses(repo)
	}
	return nil
}

// sequencePass sequences rows, read unlocked in commit order, in one transaction: of them, those
// still pending — held, so no other pass takes them meanwhile — get the repository's next numbers
// and places, in order. Every row committed before the read is among rows or after them, and every
// row committed after it carries a later timestamp, so what this pass gives follows what every
// earlier pass gave, whichever replica ran it.
func (s *Store) sequencePass(ctx context.Context, repo int64, rows []backend.Pending) error {
	keys := make([]backend.PendingKey, len(rows))
	for i, r := range rows {
		keys[i] = r.Key
	}
	return s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		held, err := tx.PendingHeld(ctx, repo, keys)
		if err != nil {
			return err
		}
		var cls []int64
		var evs []backend.Pending
		var taken []backend.PendingKey
		for _, r := range rows {
			if !held[r.Key] {
				continue
			}
			taken = append(taken, r.Key)
			if r.ChangeList != 0 {
				cls = append(cls, r.ChangeList)
			} else {
				evs = append(evs, r)
			}
		}
		if len(taken) == 0 {
			return nil
		}
		numbers := map[int64]int64{}
		if len(cls) > 0 {
			last, err := tx.NextChangeListNumbers(ctx, repo, int64(len(cls)))
			if err != nil {
				return err
			}
			given := make([]int64, len(cls))
			for i, id := range cls {
				given[i] = last - int64(len(cls)) + 1 + int64(i)
				numbers[id] = given[i]
			}
			if err := tx.NumberChangeLists(ctx, repo, cls, given); err != nil {
				return err
			}
		}
		if len(evs) > 0 {
			last, err := tx.ReserveRepositoryEvents(ctx, repo, len(evs))
			if err != nil {
				return err
			}
			placed := make([]*scmpb.Event, len(evs))
			var earlier []int64
			for i, r := range evs {
				ev := &scmpb.Event{}
				if err := proto.Unmarshal(r.Body, ev); err != nil {
					return fmt.Errorf("a pending event of repository %d: %w", repo, err)
				}
				placed[i] = ev
				if n := changeListRef(ev); n != nil && *n < 0 {
					if _, ok := numbers[-*n]; !ok {
						earlier = append(earlier, -*n)
					}
				}
			}
			// A CL an event names provisionally was made by a transaction committed no later than
			// the event's, so numbered by this pass or one before it.
			if len(earlier) > 0 {
				got, err := tx.ChangeListNumbers(ctx, repo, earlier)
				if err != nil {
					return err
				}
				for id, n := range got {
					numbers[id] = n
				}
			}
			out := make([]backend.LogEvent, len(evs))
			for i, ev := range placed {
				if n := changeListRef(ev); n != nil && *n < 0 {
					number, ok := numbers[-*n]
					if !ok || number < 0 {
						return fmt.Errorf("repository %d's event names CL %d, which is not numbered", repo, -*n)
					}
					*n = number
				}
				ev.RepositoryId, ev.RepositorySeq = repo, last-int64(len(evs))+1+int64(i)
				body, err := proto.Marshal(ev)
				if err != nil {
					return err
				}
				out[i] = backend.LogEvent{Seq: ev.RepositorySeq, At: evs[i].At, Txn: evs[i].Key.Txn, Body: body}
			}
			if err := tx.InsertEvents(ctx, repo, out); err != nil {
				return err
			}
			if err := tx.Notify(ctx, eventChannel, fmt.Sprint(repo)); err != nil {
				return err
			}
		}
		return tx.DeletePending(ctx, repo, taken)
	})
}

// sweepAfter is how long a pending row waits for its writer's pass before any replica's sweep runs
// one: a writer that stopped between its commit and its pass leaves it.
const sweepAfter = 5 * time.Second

// sweepEvery is how often each replica sweeps.
const sweepEvery = 5 * time.Second

// sweep runs the pass of every repository with a row pending since before sweepAfter, every
// sweepEvery, until ctx ends.
func (s *Store) sweep(ctx context.Context) {
	if !s.b.SequencesAfterCommit() {
		return
	}
	t := time.NewTicker(sweepEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := s.sweepOnce(ctx, time.Now().Add(-sweepAfter)); err != nil && ctx.Err() == nil {
			slog.Warn("ok scm: sweeping pending sequences", slog.Any("err", err))
		}
	}
}

// sweepOnce runs the pass of every repository with a row pending that committed before before.
func (s *Store) sweepOnce(ctx context.Context, before time.Time) error {
	for {
		repos, err := s.b.PendingRepositories(ctx, before, sequencePage)
		if err != nil {
			return err
		}
		for _, repo := range repos {
			if err := s.sequence(sequencing(), repo, time.Now()); err != nil {
				return fmt.Errorf("repository %d: %w", repo, err)
			}
		}
		if len(repos) < sequencePage {
			return nil
		}
	}
}

// ensureSequenced runs repo's pass if any of rows is provisional, reporting whether it did: a read
// that found one reads again, and finds it numbered. Every such row's transaction committed before
// the read, so a pass begun after it numbers it.
func (s *Store) ensureSequenced(ctx context.Context, repo int64, rows []backend.ChangeList) (bool, error) {
	if !s.b.SequencesAfterCommit() {
		return false, nil
	}
	for _, r := range rows {
		if r.Provisional() {
			if inTxContext(ctx) {
				return false, fmt.Errorf("repository %d: a provisional CL read in a transaction, which cannot wait for its pass", repo)
			}
			return true, s.sequence(sequencing(), repo, time.Now())
		}
	}
	return false, nil
}
