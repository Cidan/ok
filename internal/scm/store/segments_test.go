package store

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// pinned is content a test pinned by uploading it: a commit whose tree holds a file with a copy
// history descended from another, a symlink, and a subtree.
type pinned struct {
	commit, root, sub, link encoding.ID
	parentCopy, copy        encoding.ID
	b                       *builder
}

func pinContent(t *testing.T, e *clEnv, seed string) pinned {
	t.Helper()
	b := newBuilder(t)
	var p pinned
	p.b = b
	p.parentCopy = b.add(&encoding.CopyHistory{CurrentPath: seed + "/old.txt"})
	p.copy = b.add(&encoding.CopyHistory{CurrentPath: seed + "/new.txt", Parents: []encoding.ID{p.parentCopy}})
	p.link = b.symlink(seed + "-target")
	f := b.file([]byte(seed + " file\n"))
	p.sub = b.tree(fileEntry("inner.txt", b.file([]byte(seed+" inner\n"))))
	p.root = b.tree(
		encoding.TreeEntry{Name: "new.txt", Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: f, CopyID: &p.copy}},
		encoding.TreeEntry{Name: "l", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: p.link}},
		treeEntry("sub", p.sub))
	p.commit = b.commit(p.root, "pinned "+seed, encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	e.mustUpload(alice, Ready, p.commit)
	return p
}

// readAllObjects reads refs through s, requiring each.
func readAllObjects(t *testing.T, s *Store, repo int64, b *builder, refs ...ObjectRef) {
	t.Helper()
	got, err := s.ReadObjects(context.Background(), repo, refs)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range got {
		if !r.Found || !bytes.Equal(r.Body, b.body(refs[i])) {
			t.Fatalf("%s: found %v, body right %v", refs[i], r.Found, bytes.Equal(r.Body, b.body(refs[i])))
		}
	}
}

func (p pinned) refs() []ObjectRef {
	return []ObjectRef{{encoding.KindTree, p.root}, {encoding.KindTree, p.sub}, {encoding.KindSymlink, p.link},
		{encoding.KindCopyHistory, p.copy}, {encoding.KindCopyHistory, p.parentCopy}}
}

// Compaction moves the tail's pinned objects, trees' sizes records, and copies' edges into
// segments and deletes their rows; leaves what is unpinned; and every read — of objects, of
// trees' sizes, of related copies, publishing what is held, a pin's walk, fsck — finds what it
// moved, on this replica and on one with nothing cached.
func TestSegmentCompaction(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	p := pinContent(t, e, "a")
	if _, err := e.s.TreeSizes(ctx, e.repo, []encoding.ID{p.root}); err != nil {
		t.Fatal(err)
	}
	// Unpinned: published, never uploaded.
	ub := newBuilder(t)
	loose := ub.tree(fileEntry("loose.txt", ub.file([]byte("loose\n"))))
	looseCopy := ub.add(&encoding.CopyHistory{CurrentPath: "a/newer.txt", Parents: []encoding.ID{p.copy}})
	mustPublish(t, e.s, e.repo, alice, ub, nil)

	if got, err := e.s.CompactSegments(ctx, e.repo, 1000); err != nil || got.Written != 0 {
		t.Fatalf("compacting under the minimum: %+v, %v", got, err)
	}
	r, err := e.s.CompactSegments(ctx, e.repo, 1)
	if err != nil {
		t.Fatal(err)
	}
	if r.Objects != 3 || r.TreeSizes != 1 || r.CopyEdges != 1 || r.Commits != 1 || r.Index != 12 || r.Written != 7 || r.More {
		t.Fatalf("compaction: %+v; want 3 objects (the trees a publish stores are the content index's), the root's sizes, "+
			"1 copy edge, 1 commit, 2 pinned files, 2 chunks, and 2 trees, their network's 6 locations — the unpinned chunk's "+
			"and tree's too: a network's fold writes every location — 7 segments", r)
	}
	left, err := e.s.ops(ctx).PinnedObjects(ctx, e.repo, 100)
	if err != nil || len(left) != 0 {
		t.Fatalf("pinned rows left in the tail: %d, %v", len(left), err)
	}
	if ent, ok := indexOf(t, e.s, e.repo).Get(loose); !ok || ent.Pinned {
		t.Fatal("the unpinned tree is not the content index's, unpinned")
	}

	for name, s := range map[string]*Store{"this replica": e.s, "a replica with nothing cached": reopen(t, e.s)} {
		t.Run(name, func(t *testing.T) {
			readAllObjects(t, s, e.repo, p.b, p.refs()...)
			sizes, err := s.TreeSizes(ctx, e.repo, []encoding.ID{p.root, p.sub})
			if err != nil || len(sizes) != 2 {
				t.Fatalf("trees' sizes: %d, %v", len(sizes), err)
			}
			rel, err := s.GetRelatedCopies(ctx, e.repo, p.parentCopy)
			if err != nil {
				t.Fatal(err)
			}
			var ids []encoding.ID
			for _, o := range rel {
				ids = append(ids, o.ID)
			}
			if len(ids) != 3 || !slices.Contains(ids, p.copy) || !slices.Contains(ids, looseCopy) {
				t.Fatalf("related copies of the moved parent: %v; want it, its moved child, and the tail's grandchild", ids)
			}
		})
	}

	// Publishing the same content again sends nothing: the SCM has it all.
	ex, err := reopen(t, e.s).BeginPublish(ctx, e.repo, alice, testWorkspace, p.b.order, p.b.manifests())
	if err != nil {
		t.Fatal(err)
	}
	defer ex.Close()
	if objs, _ := ex.Missing(); len(objs) != 0 {
		t.Errorf("publishing moved objects again asks for %d of them: %v", len(objs), objs)
	}

	// A commit on the moved tree, uploaded: its pin walks through objects in segments, which are
	// pinned, and finds none missing.
	c2 := e.commit(alice, newChange(), p.commit, "on top", map[string]string{"x": "y"})
	e.mustUpload(alice, Ready, c2)

	fr, err := e.s.Fsck(ctx, e.repo, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(fr.Problems) != 0 {
		t.Errorf("fsck after compaction: %v", fr.Problems)
	}
}

// A tree's sizes recorded after it moved into a segment are kept in the tail, then moved by the
// next compaction: its tree is pinned, being gone from the tail.
func TestSegmentTreeSizesAfterTheMove(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	p := pinContent(t, e, "b")
	if _, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.TreeSizes(ctx, e.repo, []encoding.ID{p.root}); err != nil {
		t.Fatal(err)
	}
	r, err := e.s.CompactSegments(ctx, e.repo, 1)
	if err != nil || r.TreeSizes < 1 || r.Objects != 0 {
		t.Fatalf("compacting the sizes of moved trees: %+v, %v", r, err)
	}
	if sizes, err := reopen(t, e.s).TreeSizes(ctx, e.repo, []encoding.ID{p.root, p.sub}); err != nil || len(sizes) != 2 {
		t.Fatalf("trees' sizes from segments: %d, %v", len(sizes), err)
	}
}

// Four segments of a kind at one level merge into one at the next, and every record reads as
// before; the merged-away segments stay a day for readers that still name them, then go.
func TestSegmentMerge(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	small := mustOpen(t, dbOf(e.s), e.s.blobs, Config{SegmentBatch: 2})
	var all []pinned
	for i := range 6 {
		all = append(all, pinContent(t, e, fmt.Sprint("m", i)))
		for {
			r, err := small.CompactSegments(ctx, e.repo, 1)
			if err != nil {
				t.Fatal(err)
			}
			if r.Written == 0 && !r.More {
				break
			}
		}
	}
	segs, err := e.s.ops(ctx).Segments(ctx, repoScope(e.repo), segObjects)
	if err != nil {
		t.Fatal(err)
	}
	levels, top := map[int16]int{}, int16(0)
	for _, sg := range segs {
		levels[sg.Level]++
		top = max(top, sg.Level)
	}
	for l, n := range levels {
		if n >= mergeAt {
			t.Fatalf("object segments by level: %v; %d at level %d were not merged", levels, n, l)
		}
	}
	if top == 0 {
		t.Fatalf("object segments by level: %v; none merged", levels)
	}
	fresh := reopen(t, e.s)
	for _, p := range all {
		readAllObjects(t, fresh, e.repo, p.b, p.refs()...)
	}
	net := netOf(t, e.s, e.repo)
	retired, err := e.s.ops(ctx).RetiredSegmentsBefore(ctx, time.Now().Add(time.Minute), &net)
	if err != nil || len(retired) == 0 {
		t.Fatalf("retired segments: %d, %v", len(retired), err)
	}
	if _, n, err := e.s.DeleteRetiredOf(ctx, e.repo, time.Now()); err != nil || n != 0 {
		t.Fatalf("deleted %d merged-away segments within their grace (%v)", n, err)
	}
	if _, n, err := e.s.DeleteRetiredOf(ctx, e.repo, time.Now().Add(RetiredSegmentGrace+time.Hour)); err != nil || n != len(retired) {
		t.Fatalf("deleted %d merged-away segments past their grace, want %d (%v)", n, len(retired), err)
	}
	for _, sg := range retired {
		if _, err := e.s.blobs.Stat(ctx, segmentKey(net, sg.Kind, sg.ID)); err == nil {
			t.Errorf("merged-away segment %x is still in the bucket", sg.ID[:4])
		}
	}
	for _, p := range all {
		readAllObjects(t, reopen(t, e.s), e.repo, p.b, p.refs()...)
	}
}

// A segment in the bucket that no row records — a compaction that failed after writing it — is
// deleted once a day old, and not before.
func TestOrphanSegment(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	sg, ok, err := e.s.writeSegment(ctx, repoScope(e.repo), segObjects, 0, segment.Records([]segment.Record{{Key: make([]byte, 32), Value: []byte{3}}}))
	if err != nil || !ok {
		t.Fatal(err)
	}
	key := segmentKey(netOf(t, e.s, e.repo), sg.Kind, sg.ID)
	if _, n, err := e.s.deleteSegments(ctx, e.repo, time.Now(), false); err != nil || n != 0 {
		t.Fatalf("deleted %d orphans younger than a day (%v)", n, err)
	}
	if _, n, err := e.s.deleteSegments(ctx, e.repo, time.Now().Add(OrphanSegmentGrace+time.Hour), false); err != nil || n != 1 {
		t.Fatalf("deleted %d orphans a day old, want 1 (%v)", n, err)
	}
	if _, err := e.s.blobs.Stat(ctx, key); err == nil {
		t.Error("the orphan is still in the bucket")
	}
}

// A replica that read a repository's manifest before another compacted finds what moved: its
// manifest is read anew on a miss, whether or not the announcement reached it.
func TestSegmentMoveSeenByAnotherReplica(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	p := pinContent(t, e, "r")
	reader := reopen(t, e.s)
	// Its manifest of objects is read, empty: nothing has moved yet.
	if got, err := reader.segmentObjects(ctx, e.repo, encoding.KindTree, []encoding.ID{{9}}); err != nil || len(got) != 0 {
		t.Fatalf("an absent tree: %v, %v", got, err)
	}
	if _, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil {
		t.Fatal(err)
	}
	readAllObjects(t, reader, e.repo, p.b, p.refs()...)
}

// A sweep takes a swept tree's sizes record and a swept copy's edges with it, as the rows' foreign
// keys once did. The tree is a row, as a landing's splice makes one.
func TestSweepTakesSizesAndEdges(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	parent := b.add(&encoding.CopyHistory{CurrentPath: "p"})
	cp := b.add(&encoding.CopyHistory{CurrentPath: "c", Parents: []encoding.ID{parent}})
	f := b.file([]byte("f\n"))
	mustPublish(t, e.s, e.repo, alice, b, nil)
	tb := newBuilder(t)
	tree := tb.tree(fileEntry("f", f))
	if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		_, err := tx.InsertObjects(ctx, e.repo, []backend.Object{{ID: tree, Kind: encoding.KindTree,
			Body: tb.body(ObjectRef{Kind: encoding.KindTree, ID: tree})}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.TreeSizes(ctx, e.repo, []encoding.ID{tree}); err != nil {
		t.Fatal(err)
	}
	if kept, err := e.s.ops(ctx).TreeSizes(ctx, e.repo, []encoding.ID{tree}); err != nil || len(kept) != 1 {
		t.Fatalf("the tree's sizes record, kept: %d, %v", len(kept), err)
	}
	later := time.Now().Add(30 * 24 * time.Hour)
	for _, id := range []encoding.ID{tree, cp, parent} {
		if n, err := e.s.ops(ctx).SweepObjects(ctx, e.repo, []encoding.ID{id}, later, false); err != nil || n != 1 {
			t.Fatalf("sweeping %s: %d, %v", id, n, err)
		}
	}
	if kept, err := e.s.ops(ctx).TreeSizes(ctx, e.repo, []encoding.ID{tree}); err != nil || len(kept) != 0 {
		t.Errorf("a swept tree's sizes record: %d, %v", len(kept), err)
	}
	if edges, err := e.s.ops(ctx).CopyChildren(ctx, e.repo, []encoding.ID{parent}); err != nil || len(edges) != 0 {
		t.Errorf("a swept copy's edges: %v, %v", edges, err)
	}
}

// Garbage collection after compaction: its mark walks unpinned work through references into
// segments, which it finds pinned, not missing; it sweeps the stale work and keeps what moved.
func TestGCAfterCompaction(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	p := pinContent(t, e, "g")
	if _, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil {
		t.Fatal(err)
	}
	// Unpinned work on top of the moved content: one commit a view keeps, one nothing does.
	b := newBuilder(t)
	keptTree := b.tree(treeEntry("moved", p.sub), fileEntry("k", b.file([]byte("kept\n"))))
	kept := b.commit(keptTree, "kept", p.commit)
	op := b.operation(b.view(kept), "snapshot", encoding.RootOperationID)
	goneTree := b.tree(treeEntry("moved", p.root), fileEntry("g", b.file([]byte("gone\n"))))
	gone := b.commit(goneTree, "gone", p.commit)
	mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op})
	age(t, e.s, e.repo, 20)
	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.CommitsSwept != 1 || r.ObjectsSwept != 1 {
		t.Fatalf("pass: %s; want the unreachable commit and its tree swept", r)
	}
	if has(t, e.s, e.repo, "commit", gone) || !has(t, e.s, e.repo, "commit", kept) {
		t.Fatal("the pass swept the wrong commit")
	}
	readAllObjects(t, reopen(t, e.s), e.repo, p.b, p.refs()...)
	if again, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{}); err != nil || again.CommitsSwept != 0 || again.ObjectsSwept != 0 {
		t.Fatalf("a second pass: %v, %v", again, err)
	}
	fr, err := e.s.Fsck(ctx, e.repo, false)
	if err != nil || len(fr.Problems) != 0 {
		t.Errorf("fsck after collecting: %v, %v", fr.Problems, err)
	}
}

// windowReads records the windows compaction's reads of settled operations look through.
type windowReads struct {
	backend.Backend
	mu    *sync.Mutex
	scans *[]int
}

func (w windowReads) SettledOpsIn(ctx context.Context, log int64, kind encoding.Kind, after []byte, before time.Time, scan, limit int) ([]backend.OpRecord, []byte, int, bool, error) {
	w.mu.Lock()
	*w.scans = append(*w.scans, scan)
	w.mu.Unlock()
	return w.Backend.SettledOpsIn(ctx, log, kind, after, before, scan, limit)
}

// Compaction looks through a window of the tail's operations and views a pass — at most a batch of
// them, going on from where the last pass looked — rather than the whole tail each pass: across
// passes it moves every settled one, and goes past those not settled. Compacting fully looks through
// the whole tail from its start, and moves every one settled, those behind where the passes were.
func TestCompactionLooksThroughAWindow(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	// Twelve operations, each with its view, in one log, all settled; then half of them fresh.
	b := newBuilder(t)
	head := encoding.RootOperationID
	var ops []encoding.ID
	for i := range 12 {
		work := b.commit(b.tree(fileEntry("f", b.file([]byte(fmt.Sprint("work ", i))))), fmt.Sprint("work ", i), encoding.RootCommitID)
		op := b.operation(b.view(work), fmt.Sprint("snapshot ", i), head)
		mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{Old: []encoding.ID{head}, New: op})
		head, ops = op, append(ops, op)
	}
	age(t, e.s, e.repo, 20)
	db := dbOf(e.s)
	fresh := func(ids []encoding.ID) {
		t.Helper()
		for _, id := range ids {
			db.exec(t, `UPDATE operation SET touched_at = @now WHERE id = @id`, map[string]any{"now": db.today(t).Add(48 * time.Hour), "id": id[:]})
		}
	}
	// The six first in the log's order: the first windows hold none settled.
	slices.SortFunc(ops, func(a, b encoding.ID) int { return bytes.Compare(a[:], b[:]) })
	fresh(ops[:6])
	before := db.today(t).Add(-opSettled + 24*time.Hour)
	settled := func() int {
		return db.count(t, "operation", "touched_at < @before", map[string]any{"before": before}) +
			db.count(t, "view", "touched_at < @before", map[string]any{"before": before})
	}
	if n := settled(); n < 18 {
		t.Fatalf("%d operations and views settled, want 18 at least", n)
	}
	const batch = 3
	var mu sync.Mutex
	var scans []int
	small, err := New(windowReads{dbOf(e.s).open(t), &mu, &scans}, e.s.blobs, Config{Names: testNames,
		SegmentBatch: batch})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(small.Close)
	for passes := 0; settled() > 0; passes++ {
		if passes > 100 {
			t.Fatalf("%d passes, %d settled operations and views left: the window does not go past those not settled", passes, settled())
		}
		if _, err := small.CompactSegments(ctx, e.repo, 1); err != nil {
			t.Fatal(err)
		}
	}
	if left := db.count(t, "operation", "TRUE", nil); left != 6 {
		t.Errorf("%d operations left, want the 6 not settled", left)
	}
	for _, s := range scans {
		if s > batch {
			t.Fatalf("a pass looked through %d operations, past its batch of %d: %v", s, batch, scans)
		}
	}
	// The rest settled, and the passes' window past the first of them: compacting fully moves them
	// all, from the tail's start.
	age(t, e.s, e.repo, 20)
	logs, err := small.ops(ctx).AllOpLogs(ctx, e.repo)
	if err != nil {
		t.Fatal(err)
	}
	for i, l := range logs {
		if db.count(t, "operation", "op_log_id = @log", map[string]any{"log": l}) > 0 {
			if err := small.ops(ctx).SetCompactionCursor(ctx, e.repo, tailCursor{logs: logs, log: i, op: ops[0][:]}.encode()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := small.compactFully(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	if n := db.count(t, "operation", "TRUE", nil) + db.count(t, "view", "TRUE", nil); n != 0 {
		t.Fatalf("compacting fully left %d operations and views in the tail", n)
	}
}

// The tail's windows of sizes records and copy edges, looked through a row at a time from the start
// — each after the last — take what one window over the whole tail takes, in order: those of what is
// pinned, among those of what is not.
func TestTailWindows(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	pa, pb := pinContent(t, e, "a"), pinContent(t, e, "b")
	// Not pinned: copies published alone, and trees as rows, with their sizes kept.
	b := newBuilder(t)
	var files []encoding.ID
	for i := range 3 {
		parent := b.add(&encoding.CopyHistory{CurrentPath: fmt.Sprint("u", i)})
		b.add(&encoding.CopyHistory{CurrentPath: fmt.Sprint("v", i), Parents: []encoding.ID{parent}})
		files = append(files, b.file([]byte(fmt.Sprint("unpinned ", i, "\n"))))
	}
	mustPublish(t, e.s, e.repo, alice, b, nil)
	trees := []encoding.ID{pa.root, pa.sub, pb.root, pb.sub}
	for _, f := range files {
		tb := newBuilder(t)
		tree := tb.tree(fileEntry("f", f))
		if err := e.s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			_, err := tx.InsertObjects(ctx, e.repo, []backend.Object{{ID: tree, Kind: encoding.KindTree,
				Body: tb.body(ObjectRef{Kind: encoding.KindTree, ID: tree})}})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		trees = append(trees, tree)
	}
	if sizes, err := e.s.TreeSizes(ctx, e.repo, trees); err != nil || len(sizes) != len(trees) {
		t.Fatalf("the trees' sizes: %d, %v", len(sizes), err)
	}
	ops := e.s.ops(ctx)
	whole, _, more, err := ops.PinnedTreeSizes(ctx, e.repo, nil, 1000, 1000)
	if err != nil || more || len(whole) != 4 {
		t.Fatalf("one window over the whole tail's sizes: %d, more %v, %v; want the 4 pinned trees'", len(whole), more, err)
	}
	var walked []backend.Object
	for after, steps := []byte(nil), 0; ; steps++ {
		got, last, more, err := ops.PinnedTreeSizes(ctx, e.repo, after, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		walked = append(walked, got...)
		if !more {
			break
		}
		if steps > 2*len(trees) {
			t.Fatalf("%d windows of the sizes and no end", steps)
		}
		after = last
	}
	if !slices.EqualFunc(walked, whole, func(a, b backend.Object) bool { return a.ID == b.ID && bytes.Equal(a.Body, b.Body) }) {
		t.Errorf("the sizes a row at a time: %d records, want the whole window's %d", len(walked), len(whole))
	}
	wholeEdges, _, more, err := ops.PinnedCopyEdges(ctx, e.repo, nil, 1000, 1000)
	if err != nil || more || len(wholeEdges) != 2 {
		t.Fatalf("one window over the whole tail's edges: %v, more %v, %v; want the 2 pinned copies'", wholeEdges, more, err)
	}
	var walkedEdges [][2]encoding.ID
	for after, steps := (*[2]encoding.ID)(nil), 0; ; steps++ {
		got, last, more, err := ops.PinnedCopyEdges(ctx, e.repo, after, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		walkedEdges = append(walkedEdges, got...)
		if !more {
			break
		}
		if steps > 20 {
			t.Fatalf("%d windows of the edges and no end", steps)
		}
		after = last
	}
	if !slices.Equal(walkedEdges, wholeEdges) {
		t.Errorf("the edges a row at a time: %v, want %v", walkedEdges, wholeEdges)
	}
}

// A compaction's cursor reads back from its row as it was written; a row it cannot read, or none,
// is the tail's start.
func TestTailCursorEncoding(t *testing.T) {
	edges := [2]encoding.ID{{1, 2}, {3, 4}}
	for _, c := range []tailCursor{
		{},
		{sizes: []byte("a/b\x00\x01"), edges: &edges, logs: []int64{-5, 7, 1 << 40}, log: 2, views: true, op: []byte{9, 9}},
		{logs: []int64{3}},
	} {
		if got := decodeCursor(c.encode()); !reflect.DeepEqual(got, c) {
			t.Errorf("%+v read back as %+v", c, got)
		}
	}
	for _, bad := range [][]byte{nil, {}, {2}, {1, 5}, tailCursor{logs: []int64{1, 2}}.encode()[:4]} {
		if got := decodeCursor(bad); !reflect.DeepEqual(got, tailCursor{}) {
			t.Errorf("%v read as %+v, want the start", bad, got)
		}
	}
}
