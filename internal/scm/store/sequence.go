package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
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
		// Another replica's pass, heard of, is taken to serve what committed a margin before it
		// began by that replica's clock: should the clocks differ more, the transaction's rows are
		// pending still, and a pass begun now serves them.
		for tries := 0; ; tries++ {
			pending, err := s.b.TxnPending(ctx, repo, t.tie)
			if err != nil {
				return fmt.Errorf("sequencing repository %d: %w", repo, err)
			}
			if !pending {
				break
			}
			if tries == passTries {
				return fmt.Errorf("sequencing repository %d: its rows are pending after %d passes", repo, passTries)
			}
			if err := s.sequence(ctx, repo, time.Now().Add(skewMargin)); err != nil {
				return fmt.Errorf("sequencing repository %d: %w", repo, err)
			}
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

const (
	// sequencePage is how many pending rows one pass's transaction sequences.
	sequencePage = 500
	// passClaim is a repository's sequencing pass, as keyed work claimed for its length, and
	// passLease how long the claim lasts unrenewed.
	passClaim = "sequence-pass"
	passLease = 10 * time.Second
	// skewMargin is how much earlier than another replica says its pass began it is taken to have:
	// more than its clock and this one's differ by, as a rule — what a wider skew leaves pending, a
	// writer finds so, and waits for another pass (sequenced, sequenceStep).
	skewMargin = 100 * time.Millisecond
	// passTries bounds the passes a writer waits for once its own should have served it.
	passTries = 10
)

// sequencedSubject is what a pass of repo changes, ending (internal/wait): what a writer waiting for
// one waits on.
func sequencedSubject(repo int64) string { return "sequenced/" + strconv.FormatInt(repo, 10) }

// sequence runs repo's pass, unless one that began at or after need has finished: it sequenced
// every row committed before need. A repository's passes run one at a time in the installation,
// each serving every writer that needs what committed before it began: a pass holds the
// repository's claim (passClaim) and records when it began as it ends (SequencedAt), saying so
// (sequencedSubject). A writer whose repository another replica is passing waits for that pass to
// end — one that began after need serves it — or, should its holder die, for its claim to lapse.
// Staging's herd of 3,000 uploads, each running passes of its own, waited two minutes for their
// numbers, the passes meeting on the rows and counters they took.
func (s *Store) sequence(ctx context.Context, repo int64, need time.Time) error {
	if !s.b.SequencesAfterCommit() {
		return nil
	}
	var waiter *wait.Waiter
	defer func() {
		if waiter != nil {
			waiter.Close()
		}
	}()
	for {
		began, err := s.b.SequencedAt(ctx, repo)
		if err != nil {
			return err
		}
		if !began.Add(-skewMargin).Before(need) {
			return nil
		}
		ran, err := s.runPass(ctx, repo)
		if err != nil || ran {
			return err
		}
		if waiter == nil {
			// Registered first, then read again: a pass that ended meanwhile is read, not missed.
			if waiter, err = wait.Open(ctx, s.Work().Jobs().Queue, wait.Reg{Store: s.b.Waits(), Subjects: []string{sequencedSubject(repo)}}); err != nil {
				return err
			}
			continue
		}
		p, err := s.Work().Peek(ctx, passClaim, repo)
		if err != nil {
			return err
		}
		if !p.Held {
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, p.Left+10*time.Millisecond)
		_, err = waiter.Next(wctx)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
}

// runPass runs repo's pass holding its claim, and records its beginning as it ends; false when
// another replica holds the claim. What a pass of an older release, which claims nothing, took
// meanwhile it leaves.
func (s *Store) runPass(ctx context.Context, repo int64) (bool, error) {
	_, release, ok, err := work.Claim(ctx, s.Work(), passClaim, repo, s.holder, passLease)
	if err != nil || !ok {
		return false, err
	}
	defer release()
	if s.passing != nil {
		s.passing(repo, true)
		defer s.passing(repo, false)
	}
	began := time.Now()
	for {
		rows, err := s.b.PendingRows(ctx, repo, sequencePage)
		if err != nil {
			return true, err
		}
		if len(rows) == 0 {
			break
		}
		if err := s.sequencePass(ctx, repo, rows); err != nil {
			return true, err
		}
		if len(rows) < sequencePage {
			break
		}
	}
	if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		if err := tx.SetSequencedAt(ctx, repo, began); err != nil {
			return err
		}
		kind, key, body := wait.Changed(sequencedSubject(repo))
		return tx.PendStep(ctx, kind, key, time.Time{}, body)
	}); err != nil {
		return true, err
	}
	if s.sequencePasses != nil {
		s.sequencePasses(repo)
	}
	return true, nil
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
			if err := pendRelay(ctx, tx, repo, last-int64(len(evs))+1, last); err != nil {
				return err
			}
		}
		return tx.DeletePending(ctx, repo, taken)
	})
}

// sequenceKind is the step that runs a repository's pass should the writer that left rows pending
// stop between its commit and its pass: a job its transaction enqueued, due SequenceAfter later,
// which finds them pending still and runs the pass (sequenceStep), or finds them sequenced.
const sequenceKind = "sequence"

// SequenceAfter is how long after a writer leaves rows pending its sequence step looks for them:
// its own pass has run long before.
const SequenceAfter = 5 * time.Second

// pendSequence pends, in tx, the sequence step of each repository it leaves rows pending in.
func (s *Store) pendSequence(ctx context.Context, tx *txn) error {
	for repo := range tx.sequence {
		if err := tx.PendStep(ctx, sequenceKind, repo, time.Now().Add(s.sequenceAfter), binary.BigEndian.AppendUint64(nil, uint64(tx.tie))); err != nil {
			return err
		}
	}
	return nil
}

// sequenceStep runs repo's pass if the transaction of the job's tie left rows pending there that no
// pass has sequenced — or, for a tie of 0 (an upgrade's, a move's arrival), whatever is pending. A
// transaction whose rows it finds neither pending nor placed may be committing still: looked at
// again once it is a minute old.
func (s *Store) sequenceStep(ctx context.Context, repo int64, body []byte, age time.Duration) error {
	if len(body) != 8 {
		return nil
	}
	tie := int64(binary.BigEndian.Uint64(body))
	if tie != 0 {
		pending, err := s.b.TxnPending(ctx, repo, tie)
		if err != nil {
			return err
		}
		if !pending {
			placed, err := s.b.TransactionEvents(ctx, repo, tie)
			switch {
			case err != nil:
				return err
			case len(placed) > 0 || age >= commitWithin:
				return nil
			}
			return work.Later{After: commitWithin - age}
		}
	}
	if err := s.sequence(sequencing(), repo, time.Now()); err != nil || tie == 0 {
		return err
	}
	// Served by another replica's pass, by its clock, that left the rows pending: another.
	pending, err := s.b.TxnPending(ctx, repo, tie)
	if err != nil {
		return err
	}
	if pending {
		return work.Later{After: time.Second}
	}
	return nil
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
