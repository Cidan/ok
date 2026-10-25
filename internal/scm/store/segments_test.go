package store

import (
	"bytes"
	"context"
	"fmt"
	"slices"
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
	small := mustOpen(t, dbOf(e.s), e.s.blobs, Config{SegmentBatch: 2, Signals: e.s.signals})
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
