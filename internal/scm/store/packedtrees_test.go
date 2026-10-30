package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// A publish stores its trees in its pack, beside its chunks — no row of the object table — the
// repository's content index holding them and its network's locating them; a replica with nothing
// cached reads them from the pack. An upload pins them in both indexes, and compaction folds them
// into the pinned runs forks take.
func TestTreesInPacks(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	sub := b.tree(fileEntry("x", b.file([]byte("x\n"))))
	root := b.tree(fileEntry("a", b.file([]byte("a\n"))), treeEntry("sub", sub))
	c := b.commit(root, "trees", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	trees := []encoding.ID{root, sub}
	if n := dbOf(e.s).count(t, "object", `repository_id = @repo AND kind = @kind`,
		map[string]any{"repo": e.repo, "kind": int64(encoding.KindTree)}); n != 0 {
		t.Fatalf("%d trees stored as rows", n)
	}
	netScope := backend.NetworkScope(netOf(t, e.s, e.repo))
	check := func(stage string, pinned bool) {
		t.Helper()
		held, locs := indexOf(t, e.s, e.repo), readWhole(t, e.s, netScope)
		for _, id := range trees {
			if ent, ok := held.Get(id); !ok || ent.Kind != encoding.KindTree || ent.Pinned != pinned {
				t.Fatalf("%s: the content index's tree %s: %+v %v; want it held, pinned %v", stage, id, ent, ok, pinned)
			}
			if loc, ok := locs.Get(id); !ok || loc.Kind != encoding.KindTree || loc.Loc.Pack == 0 || loc.Pinned != pinned {
				t.Fatalf("%s: the network's location of tree %s: %+v %v; want one, pinned %v", stage, id, loc, ok, pinned)
			}
		}
		refs := []ObjectRef{{Kind: encoding.KindTree, ID: root}, {Kind: encoding.KindTree, ID: sub}}
		readAllObjects(t, reopen(t, e.s), e.repo, b, refs...)
	}
	check("published", false)

	e.mustUpload(alice, Ready, c)
	check("uploaded", true)

	if _, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil {
		t.Fatal(err)
	}
	if runs := runsOf(t, e.s, repoScope(e.repo), segIndex); len(runs) == 0 {
		t.Fatal("no pinned run after compaction folded the pinned trees")
	}
	check("folded", true)
	if r := clean(t, e.s, e.repo); r.PackedTrees != len(trees) {
		t.Fatalf("fsck verified %d packed trees, want %d", r.PackedTrees, len(trees))
	}
}

// A publish of a tree another publish stored meanwhile records the pack's copy as dead bytes, and
// keeps the tree it found, touched: the repository holds it once.
func TestTreeStoredTwice(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	tree := b.tree(fileEntry("f", b.file([]byte("f\n"))))
	first, err := e.s.BeginPublish(ctx, e.repo, alice, testWorkspace, b.order, b.manifests())
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.s.BeginPublish(ctx, e.repo, alice, testWorkspace, b.order, b.manifests())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []*Publication{first, second} {
		objs, chunks := p.Missing()
		for _, r := range objs {
			if err := p.AddObject(EncodedObject{Kind: r.Kind, ID: r.ID, Body: b.body(r)}); err != nil {
				t.Fatal(err)
			}
		}
		for _, c := range chunks {
			if err := p.AddChunk(ctx, c, b.chunk(c)); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := p.Commit(ctx, nil); err != nil {
			t.Fatal(err)
		}
	}
	if ent, ok := indexOf(t, e.s, e.repo).Get(tree); !ok || ent.Kind != encoding.KindTree {
		t.Fatalf("the tree: %+v, %v", ent, ok)
	}
	r := clean(t, e.s, e.repo)
	if r.PackedTrees != 1 || r.DeadBytes == 0 {
		t.Fatalf("fsck: %d packed trees, %d dead bytes; want the one tree, the second pack's copy dead", r.PackedTrees, r.DeadBytes)
	}
}

// A publish of trees alone — every file they name held already — writes a pack of trees: neither
// compaction nor the deletion of retired packs takes it while a tree in it is held.
func TestPackOfTreesAlone(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	f := b.file([]byte("held\n"))
	b.tree(fileEntry("f", f))
	mustPublish(t, e.s, e.repo, alice, b, nil)
	before, _ := packsOf(t, e.s, e.repo)

	tb := newBuilder(t)
	tb.files[f] = []byte("held\n")
	tree := tb.tree(fileEntry("again", f))
	mustPublish(t, e.s, e.repo, alice, tb, nil)
	if live, _ := packsOf(t, e.s, e.repo); live != before+1 {
		t.Fatalf("%d live packs after a publish of a tree alone, want %d", live, before+1)
	}
	r := &GCReport{}
	if err := e.s.compact(ctx, e.repo, GCOptions{}, r); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.s.DeleteRetiredOf(ctx, e.repo, time.Now().Add(2*RetiredPackGrace)); err != nil {
		t.Fatal(err)
	}
	if res, err := reopen(t, e.s).ReadObjects(ctx, e.repo, []ObjectRef{{Kind: encoding.KindTree, ID: tree}}); err != nil || !res[0].Found {
		t.Fatalf("the tree after compaction and the deletion of retired packs: %+v, %v (%s)", res, err, r)
	}
	clean(t, e.s, e.repo)
}

// A content index refreshed through a read-only snapshot holds no more than the snapshot saw: a
// tree published after the snapshot began, which the snapshot cannot see, is found by the next
// reader that is not in one — the refresh neither cleared the index's staleness for it nor claimed
// to hold it — as an upload's pin, planned in a snapshot, then a later upload's, need.
func TestRefreshThroughASnapshot(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	sc := repoScope(repo)
	a := newBuilder(t)
	treeA := a.tree(fileEntry("a", a.file([]byte("a\n"))))
	mustPublish(t, s, repo, alice, a, nil)
	if _, held, err := s.contentEntries(ctx, sc, encoding.KindTree, []encoding.ID{treeA}); err != nil || len(held) != 1 {
		t.Fatalf("tree A: %v, %v", held, err)
	}
	b := newBuilder(t)
	treeB := b.tree(fileEntry("b", b.file([]byte("b\n"))))
	err := s.inSnapshot(ctx, func(ctx context.Context, tx *txn) error {
		// The snapshot's first read fixes what it sees.
		if _, _, err := tx.ContentIndexGeneration(ctx, sc); err != nil {
			return err
		}
		published := make(chan error, 1)
		go func() {
			_, err := publish(context.Background(), s, repo, alice, b, nil)
			published <- err
		}()
		if err := <-published; err != nil {
			return err
		}
		// Stale for tree B's publish: refreshed through the snapshot, which cannot see it.
		_, held, err := s.contentEntries(ctx, sc, encoding.KindTree, []encoding.ID{treeA, treeB})
		if err == nil && len(held) == 0 {
			err = fmt.Errorf("the snapshot's lookup found neither tree")
		}
		if err != nil {
			return err
		}
		// The view lacks tree B — no refresh since B committed saw it — and claims to hold no more
		// than the snapshot saw, so the next reader not in a snapshot reads past it.
		if !s.live(sc).readFrom.Before(time.Now()) {
			return fmt.Errorf("the lookup through the snapshot claimed to hold what came after it")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, held, err := s.contentEntries(ctx, sc, encoding.KindTree, []encoding.ID{treeB}); err != nil || len(held) != 1 {
		t.Fatalf("tree B, published after the snapshot began, after it: %v, %v", held, err)
	}
}
