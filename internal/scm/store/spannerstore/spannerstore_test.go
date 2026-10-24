package spannerstore_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata/spannertest"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/scm/store/spannerstore"
)

// idOf is the i'th of a test's IDs.
func idOf(tag string, i int) encoding.ID {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(i))
	return encoding.ID(sha256.Sum256(append([]byte(tag), b[:]...)))
}

// txn runs fn in a transaction of b's, again when Spanner aborts it, as the store does.
func txn(ctx context.Context, b *spannerstore.Backend, fn func(backend.Ops) error) error {
	return data.Retry(ctx, b.Retryable, func() error { return b.Txn(ctx, fn) })
}

// Every write of many rows stays within a statement's mutations — Spanner refuses one of more than
// 80,000, each column a row writes and each index entry counting — however many rows the store
// hands it: inserts, pins, compaction's deletes, an operation log's, and a repository's whole tail.
// Spanner Omni allows more than Spanner does, so this proves the bound on a real instance.
func TestManyRows(t *testing.T) {
	ctx := context.Background()
	admin, client := spannertest.Database(t, spannerstore.Migrations)
	b, err := spannerstore.Open(ctx, admin, client)
	if err != nil {
		t.Fatal(err)
	}
	const n = 30000
	repo, net := int64(7), backend.Network(8)
	if _, err := b.InsertRepository(ctx, repo, net, encoding.RootCommitID, "alice"); err != nil {
		t.Fatal(err)
	}
	objs := make([]backend.Object, n)
	commits := make([]backend.Commit, n)
	edges := make([]backend.ParentEdge, n)
	paths := make([]string, 2*n)
	trees := make([]encoding.ID, n)
	for i := range n {
		objs[i] = backend.Object{ID: idOf("tree", i), Kind: encoding.KindTree, Body: []byte(fmt.Sprintf("tree %d", i))}
		trees[i] = objs[i].ID
		change := idOf("change", i)
		commits[i] = backend.Commit{ID: idOf("commit", i), ChangeID: change[:16], Body: []byte("commit"), Generation: int64(i + 1),
			CommittedAt: time.Now()}
		edges[i] = backend.ParentEdge{Commit: idOf("commit", i), Parent: encoding.RootCommitID}
		paths[2*i], paths[2*i+1] = fmt.Sprintf("d%05d", i), fmt.Sprintf("d%05d/f", i)
	}
	// Each in a transaction of its own: together they are more than a transaction's 100 MiB.
	for _, step := range []struct {
		what string
		do   func(tx backend.Ops) (int, error)
	}{
		{"InsertObjects", func(tx backend.Ops) (int, error) {
			inserted, err := tx.InsertObjects(ctx, repo, objs)
			return len(inserted), err
		}},
		{"InsertCommits", func(tx backend.Ops) (int, error) {
			inserted, err := tx.InsertCommits(ctx, repo, commits, "alice")
			return len(inserted), err
		}},
		{"InsertCommitParents", func(tx backend.Ops) (int, error) { return n, tx.InsertCommitParents(ctx, repo, edges) }},
		{"InsertCommitPaths", func(tx backend.Ops) (int, error) {
			return n, tx.InsertCommitPaths(ctx, repo, 1, idOf("commit", 0), paths)
		}},
		{"pinning", func(tx backend.Ops) (int, error) {
			pinned, _, err := tx.WalkObjects(ctx, repo, encoding.KindTree, trees, backend.PinWalk)
			return len(pinned), err
		}},
	} {
		var got int
		if err := txn(ctx, b, func(tx backend.Ops) error {
			var err error
			got, err = step.do(tx)
			return err
		}); err != nil || got != n {
			t.Fatalf("%s: %d, %v", step.what, got, err)
		}
	}

	// Compaction's deletes of half of them.
	var c backend.Compacted
	for i := range n / 2 {
		c.Objects = append(c.Objects, idOf("tree", i))
	}
	if err := txn(ctx, b, func(tx backend.Ops) error { return tx.DeleteCompacted(ctx, repo, c) }); err != nil {
		t.Fatalf("DeleteCompacted: %v", err)
	}
	if objs, err := b.AllObjects(ctx, repo); err != nil || len(objs) != n-n/2 {
		t.Fatalf("after compaction: %d objects, %v", len(objs), err)
	}

	// A shared operation log of many operations, deleted whole as no workspace's.
	log, _, err := b.InsertOpLog(ctx, repo, "alice", "")
	if err != nil {
		t.Fatal(err)
	}
	ops := make([]backend.Operation, n)
	for i := range ops {
		ops[i] = backend.Operation{ID: idOf("op", i), View: idOf("view", 0), Body: []byte("op")}
	}
	if err := b.PutOperations(ctx, log, ops); err != nil {
		t.Fatalf("PutOperations: %v", err)
	}
	if deleted, err := b.DeleteOrphanOpLogs(ctx, repo, false); err != nil || deleted != 1 {
		t.Fatalf("DeleteOrphanOpLogs: %d, %v", deleted, err)
	}

	// The repository, and its tail with it.
	if _, ok, err := b.DeleteRepository(ctx, repo); err != nil || !ok {
		t.Fatalf("DeleteRepository: %v, %v", ok, err)
	}
	if ok, err := b.RepositoryExists(ctx, repo); err != nil || ok {
		t.Fatalf("after deletion: %v, %v", ok, err)
	}
}

// A transaction that logs more than it writes itself stages the rest of its log rows: none reads
// before it commits, all read after, in order; a transaction that does not commit leaves rows that
// never read. A reader at a place reads only what committed after it; a fold's deletion takes the
// rows committed by its place, the markers of their batches, and the staged rows of no committed
// batch.
func TestStagedContentLog(t *testing.T) {
	ctx := context.Background()
	admin, client := spannertest.Database(t, spannerstore.Migrations)
	b, err := spannerstore.Open(ctx, admin, client)
	if err != nil {
		t.Fatal(err)
	}
	spannerstore.SetStageLogBytes(b, 10)
	sc := backend.RepositoryScope(7)
	if err := b.InsertContentIndex(ctx, sc, 1); err != nil {
		t.Fatal(err)
	}
	since := func(place backend.LogPlace) ([]string, backend.LogPlace) {
		t.Helper()
		var got []string
		var at backend.LogPlace
		if err := b.Snapshot(ctx, func(tx backend.Ops) error {
			logs, p, err := tx.ContentLogSince(ctx, sc, place)
			for _, l := range logs {
				got = append(got, string(l.Body))
			}
			at = p
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return got, at
	}
	rows := func(table string) int64 {
		t.Helper()
		var n int64
		if err := client.Single().Query(ctx, spanner.Statement{SQL: `SELECT COUNT(*) FROM ` + table}).Do(func(r *spanner.Row) error {
			return r.Columns(&n)
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	logAll := func(bodies []string, fail error) error {
		return b.Txn(ctx, func(tx backend.Ops) error {
			if _, _, err := tx.ShareContentIndex(ctx, sc); err != nil {
				return err
			}
			for _, body := range bodies {
				if err := tx.InsertContentLog(ctx, sc, []byte(body)); err != nil {
					return err
				}
			}
			return fail
		})
	}
	bodies := []string{"first 8b", "second 8", "third 8b"}
	err = b.Txn(ctx, func(tx backend.Ops) error {
		if _, _, err := tx.ShareContentIndex(ctx, sc); err != nil {
			return err
		}
		for _, body := range bodies {
			if err := tx.InsertContentLog(ctx, sc, []byte(body)); err != nil {
				return err
			}
		}
		if got, _ := since(nil); len(got) != 0 {
			t.Errorf("before the transaction commits, %d log rows read", len(got))
		}
		if n := rows("content_log"); n != 2 {
			t.Errorf("before the transaction commits, %d rows staged, want the 2 past its first 10 bytes", n)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, place := since(nil)
	if !slices.Equal(got, bodies) {
		t.Fatalf("after it commits: %q, want %q", got, bodies)
	}

	// One that does not commit: its staged rows never read.
	errAbandon := errors.New("abandoned")
	if err := logAll(bodies, errAbandon); !errors.Is(err, errAbandon) {
		t.Fatalf("the abandoned transaction: %v", err)
	}
	if got, _ := since(nil); len(got) != 3 {
		t.Errorf("after a transaction that did not commit, %d log rows read, want 3", len(got))
	}
	if got, _ := since(place); len(got) != 0 {
		t.Errorf("from the place after the first, with nothing committed since: %q", got)
	}
	if n := rows("content_log"); n != 5 {
		t.Errorf("%d rows, want the 3 committed and 2 staged by the transaction that did not", n)
	}

	// From the place, a reader reads only what committed after it.
	if err := logAll([]string{"fourth"}, nil); err != nil {
		t.Fatal(err)
	}
	got, folded := since(place)
	if !slices.Equal(got, []string{"fourth"}) {
		t.Errorf("from the place: %q, want the fourth alone", got)
	}

	// A fold's deletion: what committed by its place, the folded batch's marker, and — once old
	// enough — the staged rows of no committed batch.
	defer spannerstore.SetStagedOrphanAge(0)()
	if _, err := b.DeleteFoldedContentLogs(ctx, sc, folded, 1000); err != nil {
		t.Fatal(err)
	}
	if n, m := rows("content_log"), rows("content_log_batch"); n != 0 || m != 0 {
		t.Errorf("after the fold: %d log rows and %d batch markers left", n, m)
	}
	if got, _ := since(nil); len(got) != 0 {
		t.Errorf("after the fold's deletion, %q read", got)
	}
}

// A replica reserves a network's pack numbers a block at a time, once however many publishes find
// its block spent together: each waits for the one reservation, rather than reserving a block of
// its own on the one counter row, so the counter holds one block's end, and every pack a number of
// its own.
func TestPackNumbersReservedOnce(t *testing.T) {
	ctx := context.Background()
	admin, client := spannertest.Database(t, spannerstore.Migrations)
	b, err := spannerstore.Open(ctx, admin, client)
	if err != nil {
		t.Fatal(err)
	}
	net := backend.Network(9)
	if _, err := b.InsertRepository(ctx, 10, net, encoding.RootCommitID, "alice"); err != nil {
		t.Fatal(err)
	}
	const packs = 200
	nums := make([]uint64, packs)
	errs := make(chan error, packs)
	for i := range packs {
		go func() {
			errs <- txn(ctx, b, func(o backend.Ops) error {
				var err error
				nums[i], err = o.InsertPack(ctx, net, idOf("pack", i), 100, nil)
				return err
			})
		}()
	}
	for range packs {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var last int64
	if err := client.Single().Query(ctx, spanner.Statement{SQL: `SELECT last FROM counter WHERE name = 'pack' AND scope_id = @net`,
		Params: map[string]any{"net": int64(net)}}).Do(func(r *spanner.Row) error { return r.Columns(&last) }); err != nil {
		t.Fatal(err)
	}
	if last != 1024 {
		t.Errorf("%d packs at once reserved through %d: more than one block", packs, last)
	}
	slices.Sort(nums)
	if nums = slices.Compact(nums); len(nums) != packs {
		t.Errorf("%d packs share numbers", packs-len(nums))
	}
}
