package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
)

func TestPublishRoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s)
	b := newBuilder(t)
	big := bytesOf(200_000, 1) // four chunks
	small := []byte("hello there\n")
	fBig, fSmall := b.file(big), b.file(small)
	link := b.symlink("../elsewhere")
	sub := b.tree(fileEntry("small.txt", fSmall))
	root := b.tree(fileEntry("big.bin", fBig), treeEntry("sub", sub),
		encoding.TreeEntry{Name: "link", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: link}})
	c1 := b.commit(root, "first\n", encoding.RootCommitID)
	c2 := b.commit(root, "second\n", c1)
	v := b.view(c2)
	op := b.operation(v, "commit", encoding.RootOperationID)

	heads := mustPublish(t, s, repo.ID, "alice", b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op})
	if len(heads) != 1 || heads[0] != op {
		t.Fatalf("heads = %v, want [%s]", heads, op)
	}

	refs := []ObjectRef{{encoding.KindTree, root}, {encoding.KindTree, sub}, {encoding.KindSymlink, link},
		{encoding.KindCommit, c1}, {encoding.KindCommit, c2}, {encoding.KindTree, encoding.EmptyTreeID}, {encoding.KindCommit, encoding.ID{9}}}
	res, err := s.ReadObjects(ctx, repo.ID, refs)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range res[:5] {
		if !r.Found || !bytes.Equal(r.Body, b.body(refs[i])) {
			t.Errorf("ReadObjects %s: found %v", refs[i], r.Found)
		}
	}
	if !res[5].Found || !bytes.Equal(res[5].Body, emptyTreeBody) {
		t.Error("the empty tree is not served virtually")
	}
	if res[6].Found {
		t.Error("an unknown commit was found")
	}

	var got []byte
	var frames int
	err = s.ReadFiles(ctx, repo.ID, []FileRange{{FileID: fBig}}, RawChunks, func(f FileFrame) error {
		if frames == 0 && f.Size != uint64(len(big)) {
			t.Errorf("first frame size %d", f.Size)
		}
		if uint64(len(got)) != f.Offset {
			t.Errorf("frame at %d, have %d bytes", f.Offset, len(got))
		}
		got = append(got, f.Data...)
		frames++
		return nil
	})
	if err != nil || !bytes.Equal(got, big) || frames != 4 {
		t.Fatalf("ReadFiles: %d frames, equal %v, err %v", frames, bytes.Equal(got, big), err)
	}
	var ranged []FileFrame
	if err := s.ReadFiles(ctx, repo.ID, []FileRange{{FileID: fBig, Offset: 70_000, Length: 10}, {FileID: encoding.ID{7}}}, RawChunks, func(f FileFrame) error {
		ranged = append(ranged, f)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ranged) != 2 || ranged[0].Offset != testChunk || !ranged[0].Last || !ranged[1].NotFound {
		t.Fatalf("ranged read frames = %+v", ranged)
	}

	ops, err := s.ReadOperations(ctx, repo.ID, "alice", testWorkspace, []ObjectRef{{encoding.KindOperation, op}, {encoding.KindView, v}})
	if err != nil || !ops[0].Found || !ops[1].Found {
		t.Fatalf("ReadOperations = %+v, %v", ops, err)
	}
	if other, err := s.ReadOperations(ctx, repo.ID, "bob", testWorkspace, []ObjectRef{{encoding.KindOperation, op}}); err != nil || other[0].Found {
		t.Fatalf("another principal read alice's operation: %+v, %v", other, err)
	}

	// Publishing the same things again sends nothing and changes nothing.
	p, err := s.BeginPublish(ctx, repo.ID, "alice", testWorkspace, b.order, b.manifests())
	if err != nil {
		t.Fatal(err)
	}
	if objs, chunks := p.Missing(); len(objs) != 0 || len(chunks) != 0 {
		t.Fatalf("republish missing %d objects, %d chunks", len(objs), len(chunks))
	}
	if heads, err := p.Commit(ctx, &OpHeadsUpdate{Old: []encoding.ID{op}, New: op}); err != nil || len(heads) != 1 || heads[0] != op {
		t.Fatalf("republish heads = %v, %v", heads, err)
	}

	var gen int64
	if dbOf(s).scan(t, `SELECT generation FROM commit WHERE repository_id = @repo AND id = @id`, map[string]any{"repo": repo.ID, "id": c2[:]}, &gen); gen != 2 {
		t.Fatalf("c2 generation = %d", gen)
	}
	report, err := s.Fsck(ctx, repo.ID, false)
	if err != nil || len(report.Problems) != 0 {
		t.Fatalf("fsck: %v, %v", report, err)
	}
}

func TestPublishRefusesBadObjects(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s)
	b := newBuilder(t)
	contents := []byte("contents\n")
	f := b.file(contents)
	tr := b.tree(fileEntry("f", f))
	trRef := ObjectRef{encoding.KindTree, tr}

	begin := func() *Publication {
		p, err := s.BeginPublish(ctx, repo.ID, "alice", testWorkspace, b.order, b.manifests())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(p.Close)
		return p
	}
	p := begin()
	body := b.body(trRef)
	if err := p.AddObject(EncodedObject{Kind: encoding.KindTree, ID: encoding.ID{1}, Body: body}); !errors.Is(err, ErrInvalid) {
		t.Errorf("unrequested object: %v", err)
	}
	if err := p.AddObject(EncodedObject{Kind: encoding.KindTree, ID: tr, Body: append(bytes.Clone(body), 0)}); !errors.Is(err, ErrInvalid) {
		t.Errorf("non-canonical body: %v", err)
	}
	tampered := bytes.Clone(body)
	tampered[len(tampered)-1] ^= 1
	if err := p.AddObject(EncodedObject{Kind: encoding.KindTree, ID: tr, Body: tampered}); !errors.Is(err, ErrInvalid) {
		t.Errorf("body hashing to another ID: %v", err)
	}
	if err := p.AddChunk(ctx, pack.ChunkID(contents), []byte("other bytes")); !errors.Is(err, ErrInvalid) {
		t.Errorf("chunk of the wrong length: %v", err)
	}
	if err := p.AddChunk(ctx, pack.ChunkID(contents), []byte("CONTENTS\n")); !errors.Is(err, ErrInvalid) {
		t.Errorf("chunk hashing to another ID: %v", err)
	}
	if _, err := p.Commit(ctx, nil); !errors.Is(err, ErrInvalid) {
		t.Errorf("commit with nothing sent: %v", err)
	}

	// A file whose chunks do not hash to its ID.
	lying := []FileManifest{{FileID: encoding.ID{5}, Size: uint64(len(contents)), Chunks: []ChunkRef{{pack.ChunkID(contents), uint32(len(contents))}}}}
	p, err := s.BeginPublish(ctx, repo.ID, "alice", testWorkspace, nil, lying)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.AddChunk(ctx, pack.ChunkID(contents), contents); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Commit(ctx, nil); !errors.Is(err, ErrInvalid) {
		t.Errorf("file whose chunks hash elsewhere: %v", err)
	}

	// A file over 50 MiB, refused from its manifest before any byte of it is sent.
	var huge []ChunkRef
	for i := 0; i*MaxChunkLen <= MaxFileSize; i++ {
		huge = append(huge, ChunkRef{encoding.ID{7, byte(i), byte(i >> 8)}, MaxChunkLen})
	}
	_, err = s.BeginPublish(ctx, repo.ID, "alice", testWorkspace, nil, []FileManifest{{FileID: encoding.ID{6}, Size: uint64(len(huge) * MaxChunkLen), Chunks: huge, PathHint: "data/huge.bin"}})
	if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "data/huge.bin is 52690944 bytes: ok holds files up to 50 MiB") {
		t.Errorf("a file over 50 MiB: %v", err)
	}
	// One of exactly 50 MiB is held.
	var limit []ChunkRef
	for i := 0; i*MaxChunkLen < MaxFileSize; i++ {
		limit = append(limit, ChunkRef{encoding.ID{8, byte(i), byte(i >> 8)}, MaxChunkLen})
	}
	if p, err := s.BeginPublish(ctx, repo.ID, "alice", testWorkspace, nil, []FileManifest{{FileID: encoding.ID{9}, Size: MaxFileSize, Chunks: limit}}); err != nil {
		t.Errorf("a file of exactly 50 MiB: %v", err)
	} else {
		p.Close()
	}

	for name, m := range map[string][]FileManifest{
		"size mismatch": {{FileID: f, Size: 1, Chunks: []ChunkRef{{pack.ChunkID(contents), uint32(len(contents))}}}},
		"empty chunk":   {{FileID: f, Size: 0, Chunks: []ChunkRef{{pack.ChunkID(nil), 0}}}},
		"oversize":      {{FileID: f, Size: MaxChunkLen + 1, Chunks: []ChunkRef{{encoding.ID{3}, MaxChunkLen + 1}}}},
	} {
		if _, err := s.BeginPublish(ctx, repo.ID, "alice", testWorkspace, nil, m); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := s.BeginPublish(ctx, repo.ID, "alice", testWorkspace, []ObjectRef{{encoding.KindCommit, encoding.RootCommitID}}, nil); !errors.Is(err, ErrInvalid) {
		t.Errorf("staging the virtual root: %v", err)
	}
	// A parentless commit is only ever the virtual root.
	orphan := newBuilder(t)
	orphan.commit(encoding.EmptyTreeID, "no parents")
	if _, err := publish(ctx, s, repo.ID, "alice", orphan, nil); !errors.Is(err, ErrInvalid) {
		t.Errorf("parentless commit: %v", err)
	}
}

func TestPublishClosureRejection(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s)

	absentFile := encoding.ID{0xf1}
	absentParent := encoding.ID{0xc1}
	absentOp := encoding.ID{0x0a}
	b := newBuilder(t)
	tr := b.tree(fileEntry("f", absentFile))
	c := b.commit(tr, "dangling\n", absentParent)
	v := b.view(c)
	op := b.operation(v, "dangling", absentOp)

	_, err := publish(ctx, s, repo.ID, "alice", b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op})
	var rej *Rejection
	if !errors.As(err, &rej) {
		t.Fatalf("publish error = %v, want a Rejection", err)
	}
	want := map[ObjectRef]bool{
		{encoding.KindFile, absentFile}:     true,
		{encoding.KindCommit, absentParent}: true,
		{encoding.KindOperation, absentOp}:  true,
	}
	if len(rej.Missing) != len(want) {
		t.Fatalf("missing = %v, want %v", rej.Missing, want)
	}
	for _, m := range rej.Missing {
		if !want[m] {
			t.Errorf("unexpected missing %s", m)
		}
	}
	// Nothing from the refused publish is visible.
	res, err := s.ReadObjects(ctx, repo.ID, []ObjectRef{{encoding.KindTree, tr}, {encoding.KindCommit, c}})
	if err != nil || res[0].Found || res[1].Found {
		t.Fatalf("refused objects visible: %+v, %v", res, err)
	}
	if heads, err := s.GetOpHeads(ctx, repo.ID, "alice", testWorkspace); err != nil || len(heads) != 1 || heads[0] != encoding.RootOperationID {
		t.Fatalf("heads after a refused publish = %v, %v", heads, err)
	}
	// A heads update to an operation the log does not hold is refused too.
	_, err = publish(ctx, s, repo.ID, "alice", newBuilder(t), &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: encoding.ID{0x0b}})
	if !errors.As(err, &rej) || len(rej.Missing) != 1 || rej.Missing[0] != (ObjectRef{encoding.KindOperation, encoding.ID{0x0b}}) {
		t.Fatalf("heads update to an unknown operation: %v", err)
	}
	// Another principal's operation does not resolve in this log.
	ob := newBuilder(t)
	bobOp := ob.operation(ob.view(), "bob's", encoding.RootOperationID)
	mustPublish(t, s, repo.ID, "bob", ob, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: bobOp})
	if _, err := publish(ctx, s, repo.ID, "alice", newBuilder(t), &OpHeadsUpdate{New: bobOp}); !errors.As(err, &rej) {
		t.Fatalf("heads update to another log's operation: %v", err)
	}
}

// Publishers race over overlapping graphs: every one lands, and the result is
// consistent.
func TestConcurrentPublishers(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s)
	base := newBuilder(t)
	shared := base.file(bytesOf(150_000, 9))
	baseTree := base.tree(fileEntry("shared", shared))
	baseCommit := base.commit(baseTree, "base\n", encoding.RootCommitID)

	const writers = 24
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range 3 {
		if _, err := s.CreateWorkspace(ctx, fmt.Sprintf("p%d", i), repo.ID, testWorkspace); err != nil {
			t.Fatal(err)
		}
	}
	for i := range writers {
		wg.Go(func() {
			b := newBuilder(t)
			// Everyone re-sends the shared base (built exactly as base built it); half the
			// writers share their extra file's chunks.
			b.file(bytesOf(150_000, 9))
			b.tree(fileEntry("shared", shared))
			if b.commit(baseTree, "base\n", encoding.RootCommitID) != baseCommit {
				errs <- fmt.Errorf("writer %d built a different base", i)
				return
			}
			b.seq = 1000 * (i + 1)
			own := b.file(bytesOf(70_000+i, byte(i%2)))
			tr := b.tree(fileEntry("shared", shared), fileEntry(fmt.Sprintf("own%d", i), own))
			c := b.commit(tr, fmt.Sprintf("writer %d\n", i), baseCommit)
			principal := fmt.Sprintf("p%d", i%3)
			var heads *OpHeadsUpdate
			if i%2 == 0 {
				v := b.view(c)
				op := b.operation(v, fmt.Sprintf("writer %d", i), encoding.RootOperationID)
				heads = &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op}
			}
			if _, err := publish(ctx, s, repo.ID, principal, b, heads); err != nil {
				errs <- fmt.Errorf("writer %d: %w", i, err)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	commits := dbOf(s).count(t, "commit", `repository_id = @repo`, map[string]any{"repo": repo.ID})
	if commits != writers+1 {
		t.Errorf("%d commits stored, want %d", commits, writers+1)
	}
	heads := dbOf(s).opHeadsOf(t, repo.ID, "p0", "p1", "p2")
	// Each of three logs saw four or so root->op updates, all divergent: every new op is
	// a head and the root is gone.
	if heads != writers/2 {
		t.Errorf("%d op heads, want %d", heads, writers/2)
	}
	report, err := s.Fsck(ctx, repo.ID, false)
	if err != nil || len(report.Problems) != 0 {
		t.Fatalf("fsck after concurrent publishes: %v, %v", report, err)
	}
}

// Two transactions take the same two row locks in opposite orders. PostgreSQL aborts
// one as the deadlock victim, and inTx re-runs it to success.
func TestDeadlockRetry(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	a, bRepo := newRepo(t, s), newRepo(t, s)
	var attempts atomic.Int32
	s.txAttempts = func() { attempts.Add(1) }
	defer func() { s.txAttempts = nil }()

	var locked sync.WaitGroup
	locked.Add(2)
	var once [2]sync.Once
	lockPair := func(side int, first, second int64) error {
		return s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			if _, err := tx.LockRepository(ctx, first); err != nil {
				return err
			}
			once[side].Do(func() {
				locked.Done()
				locked.Wait() // both hold their first lock before either asks for its second
			})
			_, err := tx.LockRepository(ctx, second)
			return err
		})
	}
	var wg sync.WaitGroup
	var errA, errB error
	wg.Go(func() { errA = lockPair(0, a.ID, bRepo.ID) })
	wg.Go(func() { errB = lockPair(1, bRepo.ID, a.ID) })
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("transactions failed: %v, %v", errA, errB)
	}
	if n := attempts.Load(); n < 3 {
		t.Fatalf("%d attempts: the deadlock victim was not retried", n)
	}
}

func touchedToday(t *testing.T, s *Store, repo int64, tableName string, id encoding.ID) bool {
	t.Helper()
	today := dbOf(s).today(t)
	if tableName == "file" || tableName == "chunk" {
		e, ok := indexOf(t, s, repo).Get(id)
		return ok && e.Day == contentindex.DayOf(today)
	}
	if tableName == "object" {
		touched, _ := objectState(t, s, repo, id)
		return touched.Equal(today)
	}
	return dbOf(s).touched(t, tableName, repo, id).Equal(today)
}

func TestTouchIsTransitiveAndStopsAtPinned(t *testing.T) {
	s := testStore(t)
	repo := newRepo(t, s)
	b := newBuilder(t)
	f := b.file([]byte("leaf\n"))
	pinnedFile := b.file([]byte("pinned\n"))
	inner := b.tree(fileEntry("leaf", f))
	pinnedTree := b.tree(fileEntry("pinned", pinnedFile))
	outer := b.tree(treeEntry("inner", inner), treeEntry("p", pinnedTree))
	mustPublish(t, s, repo.ID, "alice", b, nil)

	// The trees and files are the content index's: a publish stored them.
	age := func(ids ...encoding.ID) {
		rewriteIndex(t, s, repo.ID, func(eid encoding.ID, e *contentindex.Entry) {
			if slices.Contains(ids, eid) {
				e.Day -= 20
			}
		})
	}
	age(outer, inner, pinnedTree, f, pinnedFile)
	rewriteIndex(t, s, repo.ID, func(eid encoding.ID, e *contentindex.Entry) {
		if eid == pinnedTree {
			e.Pinned = true
		}
	})
	// A new commit referencing the outer tree touches it and everything unpinned beneath.
	nb := newBuilder(t)
	nb.commit(outer, "touch\n", encoding.RootCommitID)
	mustPublish(t, s, repo.ID, "alice", nb, nil)
	for name, want := range map[string]bool{"outer": true, "inner": true, "leaf file": true, "pinned tree": false, "file under pinned": false} {
		var got bool
		switch name {
		case "outer":
			got = touchedToday(t, s, repo.ID, "object", outer)
		case "inner":
			got = touchedToday(t, s, repo.ID, "object", inner)
		case "leaf file":
			got = touchedToday(t, s, repo.ID, "file", f)
		case "pinned tree":
			got = touchedToday(t, s, repo.ID, "object", pinnedTree)
		case "file under pinned":
			got = touchedToday(t, s, repo.ID, "file", pinnedFile)
		}
		if got != want {
			t.Errorf("%s touched today = %v, want %v", name, got, want)
		}
	}
	// Once touched today, a subgraph is not walked again: age the leaf alone, and a second
	// reference to outer (already fresh) leaves it stale.
	age(f)
	nb2 := newBuilder(t)
	nb2.commit(outer, "again\n", encoding.RootCommitID)
	mustPublish(t, s, repo.ID, "alice", nb2, nil)
	if touchedToday(t, s, repo.ID, "file", f) {
		t.Error("a walk recursed through a row it did not update")
	}
}
