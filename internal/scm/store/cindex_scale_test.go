package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/work/worktest"
)

// The content index's log at scale (docs/scm.md, "The content index"): a replica reads only what
// committed after its place, each row once, in the order the rows apply; a fold commits beside
// writers, and deletes what it folded after; a long tail asks for a fold.

// chunkID is a chunk's ID for these tests: n, big-endian.
func chunkID(n uint64) encoding.ID {
	var id encoding.ID
	binary.BigEndian.PutUint64(id[24:], n)
	return id
}

func putChunk(id encoding.ID) contentindex.Update {
	return contentindex.Update{Op: contentindex.Put, ID: id, Entry: contentindex.Entry{Kind: encoding.KindChunk,
		Loc: contentindex.Location{Pack: 1, Stored: 10, Raw: 10}, Day: 1}}
}

// logAs logs updates to a content index in a transaction of its own, holding the index in share
// mode, as a writer does, or alone, as a sweep does.
func logAs(t *testing.T, s *Store, sc backend.Scope, alone bool, updates ...contentindex.Update) {
	t.Helper()
	ctx := context.Background()
	if err := s.b.Txn(ctx, func(tx backend.Ops) error {
		if alone {
			if _, _, err := tx.LockContentIndex(ctx, sc); err != nil {
				return err
			}
		} else if _, _, _, err := tx.ShareContentIndex(ctx, sc); err != nil {
			return err
		}
		return tx.InsertContentLog(ctx, sc, contentindex.EncodeLog(updates))
	}); err != nil {
		t.Fatal(err)
	}
}

// viewHolds reports whether a view of a content index holds id.
func viewHolds(s *Store, sc backend.Scope, v *indexView, id encoding.ID) (bool, error) {
	got, err := s.resolveIn(context.Background(), sc, v, []encoding.ID{id})
	_, ok := got[id]
	return ok, err
}

// mustHold fails the test unless a replica's live index of a content index holds id.
func mustHold(t *testing.T, s *Store, sc backend.Scope, li *liveIndex, id encoding.ID, what string) {
	t.Helper()
	ok, err := viewHolds(s, sc, li.view.Load(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal(what)
	}
}

// refreshed is a replica's live index of a content index, refreshed now.
func refreshed(t *testing.T, s *Store, sc backend.Scope) *liveIndex {
	t.Helper()
	li := s.live(sc)
	if err := s.refreshIndex(context.Background(), sc, li); err != nil {
		t.Fatal(err)
	}
	return li
}

// A put, a sweep's drop, and the put again, read by another replica in one refresh: the chunk is
// held, as it is in the order they committed — a drop applied after the second put would drop it.
func TestLogAppliesInOrderOnce(t *testing.T) {
	s := testStore(t)
	sc := repoScope(newRepo(t, s).ID)
	other := reopen(t, s)
	refreshed(t, other, sc)
	id := chunkID(1)
	logAs(t, s, sc, false, putChunk(id))
	logAs(t, s, sc, true, contentindex.Update{Op: contentindex.Drop, ID: id, Entry: contentindex.Entry{Kind: encoding.KindChunk}})
	logAs(t, s, sc, false, putChunk(id))
	li := refreshed(t, other, sc)
	mustHold(t, other, sc, li, id, "a chunk put, dropped, and put again is not held")
	// Refreshed again with nothing new, it applies nothing: the drop is not applied twice.
	before := li.tail
	if li = refreshed(t, other, sc); li.tail != before {
		t.Errorf("a refresh with nothing committed since applied %d rows", li.tail-before)
	}
	mustHold(t, other, sc, li, id, "refreshed again, the chunk is not held")
}

// A replica's refresh applies the rows committed since its place, and no others.
func TestRefreshReadsOnlyWhatIsNew(t *testing.T) {
	s := testStore(t)
	sc := repoScope(newRepo(t, s).ID)
	other := reopen(t, s)
	first := refreshed(t, other, sc).tail
	for i := range 25 {
		logAs(t, s, sc, false, putChunk(chunkID(uint64(100+i))))
	}
	li := refreshed(t, other, sc)
	if got := li.tail - first; got != 25 {
		t.Errorf("after 25 rows, a refresh applied %d", got)
	}
	for i := range 25 {
		mustHold(t, other, sc, li, chunkID(uint64(100+i)), fmt.Sprintf("chunk %d missing", i))
	}
}

// Folds commit while writers log beside them, losing none of their rows: the next generation holds
// what each fold read, the rows after stay, and the rows folded are deleted after.
func TestFoldBesideWriters(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	sc := repoScope(newRepo(t, s).ID)
	var written atomic.Uint64
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for w := range 4 {
		writers.Go(func() {
			for i := uint64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				logAs(t, s, sc, false, putChunk(chunkID(uint64(w)<<32|i)))
				written.Add(1)
			}
		})
	}
	var gens []int64
	for range 3 {
		time.Sleep(300 * time.Millisecond)
		gen, _, _, err := s.foldIndex(ctx, sc)
		if err != nil {
			t.Fatal(err)
		}
		gens = append(gens, gen)
	}
	close(stop)
	writers.Wait()
	if slices.Contains(gens, 0) {
		t.Errorf("folds beside writers: generations %v — one changed nothing", gens)
	}
	// Every chunk any writer logged is held, read afresh.
	if n := len(indexOf(t, s, sc.ID)); uint64(n) != written.Load() {
		t.Errorf("%d chunks held, %d logged", n, written.Load())
	}
	// The log holds only what the last fold did not read.
	rows, _, _, err := s.logRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(rows) >= written.Load() {
		t.Errorf("%d log rows left of %d written: the folds deleted none", rows, written.Load())
	}
}

// A log that changed more entries than the fold's threshold asks for a fold; FoldDue folds it, and
// the rows it read are deleted.
func TestLongLogAsksForAFold(t *testing.T) {
	ctx := context.Background()
	defer func(was int) { foldLogEntries = was }(foldLogEntries)
	foldLogEntries = 10
	s := testStore(t)
	repo := newRepo(t, s).ID
	sc := repoScope(repo)
	for i := range 12 {
		logAs(t, s, sc, false, putChunk(chunkID(uint64(i))))
	}
	refreshed(t, s, sc)
	if got := worktest.Ready(t, s.Work(), FoldDueWork); !slices.Contains(got, repo) {
		t.Fatalf("a long tail: fold work's jobs %v, want the repository's", got)
	}
	st, _, err := s.ops(ctx).ContentIndexGeneration(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FoldDue(ctx, FoldDueWork, repo); err != nil {
		t.Fatal(err)
	}
	after, _, err := s.ops(ctx).ContentIndexGeneration(ctx, sc)
	if err != nil || after.Generation != st.Generation+1 {
		t.Fatalf("after the fold: generation %d (%v), want %d", after.Generation, err, st.Generation+1)
	}
	if rows, _, _, err := s.logRows(ctx, sc); err != nil || rows != 0 {
		t.Errorf("after the fold, %d log rows (%v)", rows, err)
	}
}

// logRows counts the log rows a content index has — every one, folded or not — and returns its
// generation's fold's place.
func (s *Store) logRows(ctx context.Context, sc backend.Scope) (rows int, folded backend.LogPlace, found bool, err error) {
	err = s.snapshot(ctx, func(tx backend.Ops) error {
		st, ok, err := tx.ContentIndexGeneration(ctx, sc)
		if err != nil || !ok {
			return err
		}
		folded, found = st.Folded, true
		logs, _, err := tx.ContentLogSince(ctx, sc, nil)
		rows = len(logs)
		return err
	})
	return rows, folded, found, err
}

// Readers of an index together read the database once between them, not a read each one after
// another — every read of an index reads the database — and each holds what was committed before
// it asked; a reader that asks after a refresh began reads again.
func TestRefreshesCoalesce(t *testing.T) {
	s := testStore(t)
	sc := repoScope(newRepo(t, s).ID)
	other := reopen(t, s)
	li := refreshed(t, other, sc)
	var reads atomic.Int64
	other.indexReads = func() {
		reads.Add(1)
		time.Sleep(20 * time.Millisecond) // a read that takes a while, as Spanner's does
	}
	defer func() { other.indexReads = nil }()
	const readers = 64
	for round := range 3 {
		id := chunkID(uint64(100 + round))
		logAs(t, s, sc, false, putChunk(id))
		reads.Store(0)
		var wg sync.WaitGroup
		for range readers {
			wg.Go(func() {
				_, v, err := other.contentIndex(context.Background(), sc)
				if err != nil {
					t.Error(err)
					return
				}
				if ok, err := viewHolds(other, sc, v, id); err != nil || !ok {
					t.Errorf("round %d: a reader's index lacks what was committed before it asked", round)
				}
			})
		}
		wg.Wait()
		if n := reads.Load(); n == 0 || n > 3 {
			t.Errorf("round %d: %d readers of an index read it %d times", round, readers, n)
		}
	}
	// One that asks after the last refresh began reads again, though the index is not stale.
	reads.Store(0)
	if err := other.refreshIndex(context.Background(), sc, li); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 1 {
		t.Errorf("a refresh asked for after the last read %d times", reads.Load())
	}
}

// A content index — its log and its runs, folded, and its unpinned index merged, at random points —
// holds what its whole log, applied in order from the start, says: on this replica and on one with
// nothing cached, for a repository's index (pinned for good) and a network's.
func TestIndexAgreesWithItsLog(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	for i, sc := range []backend.Scope{repoScope(repo), backend.NetworkScope(netOf(t, s, repo))} {
		t.Run(sc.Name(), func(t *testing.T) {
			rnd := rand.New(rand.NewPCG(7, uint64(i)))
			ids := make([]encoding.ID, 10)
			for n := range ids {
				ids[n] = chunkID(uint64(1000*(i+1) + n))
			}
			history := map[encoding.ID][]contentindex.Update{}
			held, pinned := map[encoding.ID]bool{}, map[encoding.ID]bool{}
			unpinnedRuns := 0
			check := func(step int, r *Store) {
				t.Helper()
				li := refreshed(t, r, sc)
				got, err := r.resolveIn(ctx, sc, li.view.Load(), ids)
				if err != nil {
					t.Fatal(err)
				}
				for _, id := range ids {
					want, holds := contentindex.Resolve(contentindex.Entry{}, false, history[id])
					e, ok := got[id]
					if ok != holds || (ok && !e.Equal(want)) {
						t.Fatalf("step %d, chunk %x: the index holds %+v %v, its log %+v %v\nlog %+v", step, id[24:], e, ok, want, holds, history[id])
					}
				}
			}
			for step := range 150 {
				var ups []contentindex.Update
				alone := false
				for range 1 + rnd.IntN(4) {
					id := ids[rnd.IntN(len(ids))]
					var u contentindex.Update
					switch op := rnd.IntN(6); {
					case !held[id] || op == 0:
						u = putChunk(id)
						u.Entry.Day = contentindex.Day(100 + rnd.IntN(20))
						u.Entry.Loc.Pack = uint64(1 + rnd.IntN(3))
						held[id] = true
					case op == 1:
						u = contentindex.Update{Op: contentindex.Touch, ID: id, Entry: contentindex.Entry{Kind: encoding.KindChunk, Day: contentindex.Day(100 + rnd.IntN(30))}}
					case op == 2:
						u = contentindex.Update{Op: contentindex.Pin, ID: id, Entry: contentindex.Entry{Kind: encoding.KindChunk}}
						pinned[id] = true
					case op == 3:
						u = contentindex.Update{Op: contentindex.Move, ID: id, Entry: contentindex.Entry{Kind: encoding.KindChunk,
							Loc: contentindex.Location{Pack: uint64(10 + rnd.IntN(5)), Stored: 10, Raw: 10}}}
						alone = true
					case !sc.Network && pinned[id]:
						// Pinned is permanent in a repository's index: no sweep drops it.
						u = contentindex.Update{Op: contentindex.Touch, ID: id, Entry: contentindex.Entry{Kind: encoding.KindChunk, Day: 125}}
					default:
						u = contentindex.Update{Op: contentindex.Drop, ID: id, Entry: contentindex.Entry{Kind: encoding.KindChunk}}
						held[id], pinned[id], alone = false, false, true
					}
					ups = append(ups, u)
					history[id] = append(history[id], u)
				}
				logAs(t, s, sc, alone, ups...)
				if rnd.IntN(8) == 0 {
					_, unpinned, _, err := s.foldIndex(ctx, sc)
					if err != nil {
						t.Fatalf("step %d: folding: %v", step, err)
					}
					if unpinned > 0 {
						unpinnedRuns++
					}
				}
				check(step, s)
				if step%25 == 24 {
					check(step, reopen(t, s))
				}
			}
			if live := len(runsOf(t, s, sc, segIndexUnpinned)); !sc.Network && (live == 0 || live >= unpinnedRuns) {
				t.Fatalf("%d unpinned runs live of the %d folds wrote: none merged", live, unpinnedRuns)
			}
		})
	}
}
