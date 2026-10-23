package store

import (
	"context"
	"encoding/binary"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
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
			if _, err := tx.LockContentIndex(ctx, sc); err != nil {
				return err
			}
		} else if _, _, err := tx.ShareContentIndex(ctx, sc); err != nil {
			return err
		}
		return tx.InsertContentLog(ctx, sc, contentindex.EncodeLog(updates))
	}); err != nil {
		t.Fatal(err)
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
	if _, ok := li.x.Load().Get(id); !ok {
		t.Fatal("a chunk put, dropped, and put again is not held")
	}
	// Refreshed again with nothing new, it applies nothing: the drop is not applied twice.
	before := li.tail
	if li = refreshed(t, other, sc); li.tail != before {
		t.Errorf("a refresh with nothing committed since applied %d rows", li.tail-before)
	}
	if _, ok := li.x.Load().Get(id); !ok {
		t.Fatal("refreshed again, the chunk is not held")
	}
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
		if _, ok := li.x.Load().Get(chunkID(uint64(100 + i))); !ok {
			t.Fatalf("chunk %d missing", i)
		}
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
	x := indexOf(t, s, sc.ID)
	n := 0
	x.Range(func(encoding.ID, contentindex.Entry) bool { n++; return true })
	if uint64(n) != written.Load() {
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

// A tail longer than the fold's threshold asks for a fold; FoldDue folds it, and the rows it read
// are deleted.
func TestLongTailAsksForAFold(t *testing.T) {
	ctx := context.Background()
	defer func(was int) { foldTailMin = was }(foldTailMin)
	foldTailMin = 10
	s := testStore(t)
	repo := newRepo(t, s).ID
	sc := repoScope(repo)
	for i := range 12 {
		logAs(t, s, sc, false, putChunk(chunkID(uint64(i))))
	}
	refreshed(t, s, sc)
	due, err := s.Work().Due(ctx, FoldDueWork, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(due, repo) {
		t.Fatalf("a long tail: fold work due %v, want the repository's", due)
	}
	gen, _, _, err := s.ops(ctx).ContentIndexGeneration(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.FoldDue(ctx, FoldDueWork, repo); err != nil {
		t.Fatal(err)
	}
	after, _, _, err := s.ops(ctx).ContentIndexGeneration(ctx, sc)
	if err != nil || after != gen+1 {
		t.Fatalf("after the fold: generation %d (%v), want %d", after, err, gen+1)
	}
	if rows, _, _, err := s.logRows(ctx, sc); err != nil || rows != 0 {
		t.Errorf("after the fold, %d log rows (%v)", rows, err)
	}
}

// logRows counts the log rows a content index has — every one, folded or not — and returns its
// generation's fold's place.
func (s *Store) logRows(ctx context.Context, sc backend.Scope) (rows int, folded backend.LogPlace, found bool, err error) {
	err = s.snapshot(ctx, func(tx backend.Ops) error {
		var err error
		if _, folded, found, err = tx.ContentIndexGeneration(ctx, sc); err != nil || !found {
			return err
		}
		logs, _, err := tx.ContentLogSince(ctx, sc, nil)
		rows = len(logs)
		return err
	})
	return rows, folded, found, err
}
