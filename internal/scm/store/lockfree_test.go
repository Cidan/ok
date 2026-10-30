package store

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// locksTaken records the content indexes s's transactions take the row lock of in share mode.
func locksTaken(s *Store) (got func() []backend.Scope) {
	var mu sync.Mutex
	var scopes []backend.Scope
	s.indexLocked = func(sc backend.Scope) {
		mu.Lock()
		defer mu.Unlock()
		scopes = append(scopes, sc)
	}
	return func() []backend.Scope {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(scopes)
	}
}

// freshFile publishes a file of new content to repo and returns the IDs its index holds of it:
// the file's, and its chunks'.
func freshFile(t *testing.T, s *Store, repo int64, seed byte) []encoding.ID {
	t.Helper()
	b := newBuilder(t)
	contents := bytesOf(40_000, seed)
	file := b.file(contents)
	b.commit(b.tree(fileEntry("f", file)), "fresh", encoding.RootCommitID)
	mustPublish(t, s, repo, alice, b, nil)
	return append([]encoding.ID{file}, chunkIDs(contents)...)
}

// ageNow makes repo's unpinned entries days old (ageIndex), a generation of its own, and brings
// s's view of the index to it, as the generation's signal does a replica's.
func ageNow(t *testing.T, s *Store, repo int64, days int) {
	t.Helper()
	ageIndex(t, s, repo, days)
	if err := s.refreshIndex(context.Background(), repoScope(repo), s.live(repoScope(repo))); err != nil {
		t.Fatal(err)
	}
}

// lockAlone takes sc's index alone in a transaction of its own, on a goroutine of its own — the
// caller's holds a connection — and reports how it went within wait.
func lockAlone(s *Store, sc backend.Scope, wait time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			_, _, err := tx.lockIndex(ctx, sc)
			return err
		})
	}()
	return <-done
}

// A transaction relying only on what garbage collection cannot take before it commits — entries
// changed in the last lockFreeDays — holds no lock on the index: what takes the index alone runs
// beside it, waiting for nothing.
func TestWritersHoldNoLock(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	ids := freshFile(t, e.s, e.repo, 7)
	// At the edge: changed lockFreeDays ago.
	ageNow(t, e.s, e.repo, int(lockFreeDays))
	locked := locksTaken(e.s)
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		lg, err := e.s.txIndex(ctx, tx, repoScope(e.repo))
		if err != nil {
			return err
		}
		if err := lg.load(ctx, ids); err != nil {
			return err
		}
		for _, id := range ids {
			if h := lg.found[id]; !h.held {
				t.Errorf("%s is not held in the view", id)
			}
		}
		if lg.locked {
			t.Error("a transaction relying on entries changed lockFreeDays ago holds the index's lock")
		}
		if err := lockAlone(e.s, repoScope(e.repo), 20*time.Second); err != nil {
			t.Errorf("taking the index alone beside a writer holding no lock: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := locked(); len(got) != 0 {
		t.Errorf("locks taken: %v", got)
	}
}

// A transaction whose lookup finds an unpinned entry changed before the last lockFreeDays — one a
// sweep may take before it commits — holds the index's lock in share mode from then on, and reads
// the index again under it: what takes the index alone waits for it.
func TestWritersNearTheCutoffLock(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	ids := freshFile(t, e.s, e.repo, 8)
	ageNow(t, e.s, e.repo, int(lockFreeDays)+1)
	locked := locksTaken(e.s)
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		lg, err := e.s.txIndex(ctx, tx, repoScope(e.repo))
		if err != nil {
			return err
		}
		if lg.locked {
			t.Error("the index locked before a lookup found what a sweep may take")
		}
		if err := lg.load(ctx, ids); err != nil {
			return err
		}
		if !lg.locked {
			t.Error("a transaction relying on an entry a sweep may take holds no lock")
		}
		for _, id := range ids {
			if h := lg.found[id]; !h.held {
				t.Errorf("%s is not held in the view read under the lock", id)
			}
		}
		if err := lockAlone(e.s, repoScope(e.repo), 2*time.Second); err == nil {
			t.Error("the index was taken alone beside a writer holding it in share mode")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := locked(); !slices.Equal(got, []backend.Scope{repoScope(e.repo)}) {
		t.Errorf("locks taken: %v; want the repository's index's, once", got)
	}
}

// A publish relying on its network's location of what its repository does not hold — which the
// network's collection drops once no repository holds it — decides under the network's lock, the
// repository's held first; one relying on the locations of what its repository holds, or storing
// what the network lacks, takes none. (A replica's first transaction of an index it has no view of
// loads it under the lock.)
func TestPublishLocksTheNetworkForWhatItDoesNotHold(t *testing.T) {
	p := newCLEnv(t)
	p.landChange("base", map[string]string{"a": "a0\n"})
	fr := newFork(t, p.s, p.repo)
	shared := bytesOf(40_000, 9)
	publishShared := func(repo int64) {
		t.Helper()
		b := newBuilder(t)
		b.commit(b.tree(fileEntry("shared", b.file(shared))), "shared", encoding.RootCommitID)
		mustPublish(t, p.s, repo, alice, b, nil)
	}
	publishShared(p.repo)
	freshFile(t, p.s, fr.ID, 10)
	locked := locksTaken(p.s)
	freshFile(t, p.s, fr.ID, 14)
	freshFile(t, p.s, p.repo, 15)
	if got := locked(); len(got) != 0 {
		t.Fatalf("publishes storing what the network lacks took locks: %v", got)
	}
	// The parent's content is unpinned: the fork does not hold it, and stores it again, the
	// network's location of it relied on.
	publishShared(fr.ID)
	net := backend.NetworkScope(netOf(t, p.s, p.repo))
	if got := locked(); !slices.Equal(got, []backend.Scope{repoScope(fr.ID), net}) {
		t.Fatalf("locks taken by the fork's publish: %v; want its repository's index's, then its network's", got)
	}
	if got := readAll(t, reopen(t, p.s), fr.ID, fileID(t, string(shared))); string(got) != string(shared) {
		t.Fatal("the fork's file reads back wrong")
	}
	clean(t, p.s, p.repo)
	clean(t, p.s, fr.ID)
}

// A transaction holding no lock that would commit more than lockFreeFor after it read the index is
// refused, and runs again; one holding the index's lock is not.
func TestLockFreeViewsExpire(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	ids := freshFile(t, e.s, e.repo, 11)
	was := lockFreeFor
	lockFreeFor = 100 * time.Millisecond
	defer func() { lockFreeFor = was }()
	attempts := 0
	e.s.txAttempts = func() { attempts++ }
	defer func() { e.s.txAttempts = nil }()
	rely := func() error {
		return e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			lg, err := e.s.txIndex(ctx, tx, repoScope(e.repo))
			if err != nil {
				return err
			}
			if err := lg.load(ctx, ids); err != nil {
				return err
			}
			if attempts == 1 {
				time.Sleep(3 * lockFreeFor)
			}
			return nil
		})
	}
	if err := rely(); err != nil || attempts != 2 {
		t.Fatalf("a transaction outlasting its view: %d attempts, %v; want refused once, then committed", attempts, err)
	}
	ageNow(t, e.s, e.repo, int(lockFreeDays)+1)
	attempts = 0
	if err := rely(); err != nil || attempts != 1 {
		t.Fatalf("a transaction holding the index's lock: %d attempts, %v; want it committed", attempts, err)
	}
}

// A fold by another replica since this one's view was read has the next transaction load the new
// generation, its runs and the log since its fold, holding no lock: the log is read with the
// generation it follows, so a fold that slips between them is seen.
func TestLockFreeViewsAfterAFold(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	ids := freshFile(t, e.s, e.repo, 12)
	if err := e.s.refreshIndex(ctx, repoScope(e.repo), e.s.live(repoScope(e.repo))); err != nil {
		t.Fatal(err)
	}
	gen, _, _, err := reopen(t, e.s).RebuildContentIndex(ctx, e.repo)
	if err != nil || gen == 0 {
		t.Fatalf("another replica's fold: generation %d, %v", gen, err)
	}
	locked := locksTaken(e.s)
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		lg, err := e.s.txIndex(ctx, tx, repoScope(e.repo))
		if err != nil {
			return err
		}
		if lg.locked || lg.view.log.Generation() != gen {
			t.Errorf("the view after another's fold: locked %v, generation %d; want generation %d, holding no lock",
				lg.locked, lg.view.log.Generation(), gen)
		}
		if err := lg.load(ctx, ids); err != nil {
			return err
		}
		for _, id := range ids {
			if h := lg.found[id]; !h.held {
				t.Errorf("%s is not held in the new generation", id)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := locked(); len(got) != 0 {
		t.Errorf("locks taken: %v", got)
	}
}

// Transactions on a replica asking for its view at once share the reads: one read begun since a
// transaction asked serves it, so a busy index costs a replica a read at a time, not one a write.
func TestLockFreeReadsCoalesce(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	ids := freshFile(t, e.s, e.repo, 16)
	if err := e.s.refreshIndex(ctx, repoScope(e.repo), e.s.live(repoScope(e.repo))); err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	e.s.indexReads = func() {
		reads.Add(1)
		time.Sleep(50 * time.Millisecond) // a slow read: the transactions behind it pile up
	}
	defer func() { e.s.indexReads = nil }()
	const writers = 16
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
				lg, err := e.s.txIndex(ctx, tx, repoScope(e.repo))
				if err != nil {
					return err
				}
				if lg.locked {
					t.Error("a writer's view was read under the lock")
				}
				return lg.load(ctx, ids)
			}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if n := reads.Load(); n > writers/2 {
		t.Errorf("%d writers at once read the database %d times; want the reads shared", writers, n)
	}
}

// A transaction holding the index alone reads its view under that lock, and its log rows keep the
// stamp the lock gave them — past every writer's before it — not a writer's (PostgreSQL's order).
func TestHeldAloneKeepsItsStamp(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	if e.s.b.SequencesAfterCommit() {
		t.Skip("Spanner orders log rows by their commit timestamps")
	}
	ids := freshFile(t, e.s, e.repo, 13)
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		if _, _, err := tx.lockIndex(ctx, repoScope(e.repo)); err != nil {
			return err
		}
		lg, err := e.s.txIndex(ctx, tx, repoScope(e.repo))
		if err != nil {
			return err
		}
		if !lg.locked {
			t.Error("a transaction holding the index alone reads it as one holding no lock")
		}
		e.s.logIndex(tx, repoScope(e.repo), pinUpdate(encoding.KindFile, ids[0]))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var stamp, seq int64
	dbOf(e.s).scan(t, `SELECT max(stamp) FROM content_log WHERE scope = 0 AND owner_id = @repo`, map[string]any{"repo": e.repo}, &stamp)
	dbOf(e.s).scan(t, `SELECT log_seq FROM content_index WHERE scope = 0 AND owner_id = @repo`, map[string]any{"repo": e.repo}, &seq)
	if stamp != seq-1 || stamp%2 != 1 {
		t.Errorf("the row of a transaction holding the index alone is stamped %d, the index's sequence %d; want the sequence less one", stamp, seq)
	}
}
