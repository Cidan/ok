package store

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/layer"
	"github.com/Cidan/ok/internal/scm/pack"
)

// readCheckout reads a commit's checkout as a client does: its header and the layers sent, decoded.
func readCheckout(t *testing.T, s *Store, repo int64, commit encoding.ID, held ...encoding.ID) (Checkout, map[encoding.ID][]encoding.LayerRecord) {
	t.Helper()
	co, _, sent := readCheckoutFrom(t, s, repo, commit, encoding.ID{}, held...)
	return co, sent
}

// readCheckoutFrom reads a commit's checkout as a client holding have's does: its header, the tree
// it completes, and the layers sent.
func readCheckoutFrom(t *testing.T, s *Store, repo int64, commit, have encoding.ID, held ...encoding.ID) (Checkout, encoding.ID, map[encoding.ID][]encoding.LayerRecord) {
	t.Helper()
	var co Checkout
	var since encoding.ID
	sent := map[encoding.ID][]byte{}
	err := s.ReadCheckout(context.Background(), repo, commit, held, have, func(c Checkout, sn encoding.ID) error {
		co, since = c, sn
		return nil
	}, func(id encoding.ID, data []byte, last bool) error {
		sent[id] = append(sent[id], data...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	out := map[encoding.ID][]encoding.LayerRecord{}
	for id, b := range sent {
		if encoding.Hash(b) != id {
			t.Fatalf("layer %s does not hash to its ID", id)
		}
		recs, err := layer.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		out[id] = recs
	}
	return co, since, out
}

// A client moving from a tree it holds is sent one layer of what the commit changes from it,
// naming that tree, and nothing for its own tree or for a kept checkout whose layers it holds; from
// a tree the repository does not hold, the checkout.
func TestCheckoutSince(t *testing.T) {
	s := testStore(t)
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	build, a1, a2 := b.file([]byte("cc_library()\n")), b.file([]byte("one\n")), b.file([]byte("two\n"))
	keep := b.tree(fileEntry("BUILD", b.file(bytesOf(20_000, 5))))
	srcBuild := b.file([]byte("go_library()\n"))
	src1 := b.tree(fileEntry("a.go", a1))
	src2 := b.tree(fileEntry("a.go", a2), fileEntry("BUILD", srcBuild))
	root1 := b.tree(fileEntry("BUILD", build), treeEntry("keep", keep), treeEntry("src", src1))
	root2 := b.tree(fileEntry("BUILD", build), treeEntry("keep", keep), treeEntry("src", src2))
	c1 := b.commit(root1, "one", encoding.RootCommitID)
	c2 := b.commit(root2, "two", c1)
	mustPublish(t, s, repo, "alice", b, nil)

	co, since, sent := readCheckoutFrom(t, s, repo, c2, root1)
	if since != root1 || co.Tree != root2 || len(co.Layers) != 1 || len(sent) != 1 {
		t.Fatalf("from root1: %+v since %s, %d sent; want one layer since root1", co, since, len(sent))
	}
	recs := sent[co.Layers[0].ID]
	if !holds(recs, encoding.KindTree, root2) || !holds(recs, encoding.KindTree, src2) || !holds(recs, encoding.KindFile, srcBuild) ||
		holds(recs, encoding.KindTree, keep) || holds(recs, encoding.KindFile, build) {
		t.Errorf("the layer since root1 holds %d records: want root2, src, and src/BUILD alone", len(recs))
	}
	// Its own tree: nothing.
	if co, since, sent := readCheckoutFrom(t, s, repo, c2, root2); since != root2 || len(co.Layers) != 0 || len(sent) != 0 {
		t.Errorf("from its own tree: %+v since %s, %d sent", co, since, len(sent))
	}
	// A kept checkout whose layers the client holds: nothing.
	kept, _ := readCheckout(t, s, repo, c1)
	var held []encoding.ID
	for _, l := range kept.Layers {
		held = append(held, l.ID)
	}
	if co, since, sent := readCheckoutFrom(t, s, repo, c1, root2, held...); since != (encoding.ID{}) || len(sent) != 0 || len(co.Layers) != len(held) {
		t.Errorf("back to c1, holding its layers: %+v since %s, %d sent", co, since, len(sent))
	}
	// From a tree the repository does not hold: the checkout.
	if co, since, sent := readCheckoutFrom(t, s, repo, c2, encoding.Hash([]byte{byte(encoding.KindTree), 7})); since != (encoding.ID{}) || len(co.Layers) == 0 || len(sent) != len(co.Layers) {
		t.Errorf("from a stranger: %+v since %s, %d sent", co, since, len(sent))
	}
}

func holds(recs []encoding.LayerRecord, kind encoding.Kind, id encoding.ID) bool {
	return slices.ContainsFunc(recs, func(r encoding.LayerRecord) bool { return r.Kind == kind && r.ID == id })
}

// A commit's checkout is its parent's layers and one of what it changed: every tree under it with
// its sizes, its symlinks, and the files planning reads, whole, and nothing the parent's layers
// hold; a client holding the parent's is sent the new layer alone, and another replica reads the
// same checkout from the bucket.
func TestCheckoutLayers(t *testing.T) {
	s := testStore(t)
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	build := b.file([]byte("cc_library(name = \"a\")\n"))
	subBuild := b.file([]byte("go_library(name = \"sub\")\n"))
	settings := b.file([]byte("name: mono\n"))
	src1, src2 := b.file([]byte("package a\n")), b.file([]byte("package a // edited\n"))
	link := b.symlink("../sub")
	sub := b.tree(fileEntry("BUILD.bazel", subBuild), fileEntry("x.go", b.file([]byte("package sub\n"))))
	dotOK := b.tree(fileEntry("repository.yaml", settings))
	// A large file planning reads, which both commits hold: the whole layer outweighs what changed.
	bulk := b.tree(fileEntry("BUILD", b.file(bytesOf(20_000, 9))))
	root1 := b.tree(fileEntry("BUILD", build), treeEntry("src", b.tree(fileEntry("a.go", src1))), treeEntry("sub", sub), treeEntry(".ok", dotOK),
		treeEntry("bulk", bulk), encoding.TreeEntry{Name: "link", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: link}})
	srcTree2 := b.tree(fileEntry("a.go", src2), fileEntry("BUILD", build))
	root2 := b.tree(fileEntry("BUILD", build), treeEntry("src", srcTree2), treeEntry("sub", sub), treeEntry(".ok", dotOK),
		treeEntry("bulk", bulk), encoding.TreeEntry{Name: "link", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: link}})
	c1 := b.commit(root1, "one", encoding.RootCommitID)
	c2 := b.commit(root2, "two", c1)
	mustPublish(t, s, repo, "alice", b, nil)

	co, sent := readCheckout(t, s, repo, c2)
	if co.Tree != root2 || len(co.Layers) != 2 || len(sent) != 2 {
		t.Fatalf("checkout %+v, %d layers sent; want root2's in two layers, both sent", co, len(sent))
	}
	whole, top := sent[co.Layers[0].ID], sent[co.Layers[1].ID]
	for _, tr := range []encoding.ID{root1, sub, dotOK} {
		if !holds(whole, encoding.KindTree, tr) || !holds(whole, encoding.KindTreeSizes, tr) {
			t.Errorf("the parent's layer lacks tree %s or its sizes", tr)
		}
	}
	for _, f := range []encoding.ID{build, subBuild, settings} {
		if !holds(whole, encoding.KindFile, f) {
			t.Errorf("the parent's layer lacks planning's file %s", f)
		}
	}
	if holds(whole, encoding.KindFile, src1) || !holds(whole, encoding.KindSymlink, link) {
		t.Error("the parent's layer holds a file planning does not read, or lacks the symlink")
	}
	// The new layer: the trees that changed, and no file planning reads that is where it was —
	// but BUILD, new to src/, is.
	if !holds(top, encoding.KindTree, root2) || !holds(top, encoding.KindTree, srcTree2) || holds(top, encoding.KindTree, sub) {
		t.Errorf("the new layer's trees are wrong: %d records", len(top))
	}
	if !holds(top, encoding.KindFile, build) || holds(top, encoding.KindFile, subBuild) || holds(top, encoding.KindSymlink, link) {
		t.Error("the new layer holds what the parent's does, or lacks src/BUILD")
	}

	// Holding the parent's layer, the new one alone.
	co1, _ := readCheckout(t, s, repo, c1)
	var held []encoding.ID
	for _, l := range co1.Layers {
		held = append(held, l.ID)
	}
	if _, sent := readCheckout(t, s, repo, c2, held...); len(sent) != 1 || sent[co.Layers[1].ID] == nil {
		t.Errorf("holding c1's layers, sent %d", len(sent))
	}
	// Another replica reads the checkout kept.
	if co2, _ := readCheckout(t, reopen(t, s), repo, c2); !slices.Equal(co2.Layers, co.Layers) {
		t.Errorf("another replica's checkout %+v, want %+v", co2, co)
	}
	// The root commit's checkout is the empty tree's, of no layers.
	if co, sent := readCheckout(t, s, repo, encoding.RootCommitID); co.Tree != encoding.EmptyTreeID || len(sent) != 0 {
		t.Errorf("the root commit's checkout %+v", co)
	}
}

// A chain of commits stacks small layers on one whole one, merging them geometrically: the whole
// layer stays the chain's bottom, a checkout's layers stay few, and a client walking the chain
// fetches less than one whole layer more; a commit that changes most of what the whole one holds
// is whole again. A commit with conflicts has no checkout.
func TestCheckoutChains(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	// A large file planning reads makes the whole layer large beside what each commit changes.
	big := b.tree(fileEntry("BUILD", b.file(bytesOf(20_000, 3))), fileEntry("data", b.file(bytesOf(8192, 1))))
	parent := encoding.RootCommitID
	var commits []encoding.ID
	for i := range 40 {
		root := b.tree(treeEntry("big", big), fileEntry("n", b.file([]byte{byte(i)})))
		parent = b.commit(root, "c", parent)
		commits = append(commits, parent)
	}
	rebuilt := b.commit(b.tree(treeEntry("big", b.tree(fileEntry("BUILD", b.file(bytesOf(20_000, 4))))), fileEntry("n", b.file([]byte{1}))),
		"most of it", parent)
	conflicted := b.add(&encoding.Commit{Parents: []encoding.ID{parent}, RootTree: encoding.Merge[encoding.ID]{big, big, big},
		ConflictLabels: encoding.Merge[string]{"", "", ""}, Description: "conflict",
		Author: encoding.Signature{Name: "t", Email: "t@example.com"}, Committer: encoding.Signature{Name: "t", Email: "t@example.com"}})
	mustPublish(t, s, repo, "alice", b, nil)
	held := map[encoding.ID]bool{}
	var bottom LayerRef
	var fetched int64
	for i, c := range commits {
		co, err := s.Checkout(ctx, repo, c)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			bottom = co.Layers[0]
		}
		if co.Layers[0] != bottom || len(co.Layers) > 7 || co.Layers[len(co.Layers)-1].Tree != co.Tree {
			t.Fatalf("commit %d's layers %+v: want the first whole layer at the bottom, at most 7, the top of its tree", i, co.Layers)
		}
		for k := 1; k < len(co.Layers); k++ {
			if co.Layers[k-1].Size <= 2*co.Layers[k].Size {
				t.Errorf("commit %d: layer %d (%d bytes) is not more than twice layer %d (%d)", i, k-1, co.Layers[k-1].Size, k, co.Layers[k].Size)
			}
		}
		for _, l := range co.Layers {
			if !held[l.ID] {
				held[l.ID] = true
				if i > 0 {
					fetched += l.Size
				}
			}
		}
	}
	if fetched >= bottom.Size {
		t.Errorf("walking 39 commits fetched %d bytes beyond the first checkout's %d", fetched, bottom.Size)
	}
	if co, err := s.Checkout(ctx, repo, rebuilt); err != nil || len(co.Layers) != 1 || co.Layers[0] == bottom {
		t.Errorf("a commit changing most of it: %+v, %v; want one new whole layer", co, err)
	}
	if _, err := s.Checkout(ctx, repo, conflicted); !errors.Is(err, ErrInvalid) {
		t.Errorf("a conflicted commit's checkout: %v", err)
	}
}

// Pruning keeps main's head's checkout and those younger than CheckoutRetention, and the layers
// they name; the rest go, and a checkout read after its layers went derives again.
func TestPruneCheckouts(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	c1 := b.commit(b.tree(fileEntry("BUILD", b.file([]byte("one\n")))), "one", encoding.RootCommitID)
	c2 := b.commit(b.tree(fileEntry("BUILD", b.file([]byte("two\n")))), "two", c1)
	mustPublish(t, s, repo, "alice", b, nil)
	for _, c := range []encoding.ID{c1, c2} {
		if _, err := s.Checkout(ctx, repo, c); err != nil {
			t.Fatal(err)
		}
	}
	// Another replica that read c2's checkout before its layers went, and not its layers.
	other := reopen(t, s)
	if _, ok, err := other.storedCheckout(ctx, repo, c2); err != nil || !ok {
		t.Fatalf("another replica reads c2's checkout: %v, %v", ok, err)
	}
	if n, l, err := s.pruneCheckouts(ctx, repo, time.Now(), false); err != nil || n != 0 || l != 0 {
		t.Fatalf("pruning fresh checkouts: %d, %d, %v", n, l, err)
	}
	n, l, err := s.pruneCheckouts(ctx, repo, time.Now().Add(CheckoutRetention+time.Hour), false)
	if err != nil || n != 2 || l != 2 {
		t.Fatalf("pruning old checkouts: %d checkouts, %d layers, %v; want 2 and 2", n, l, err)
	}
	// That replica's memory still names c2's layers: its read finds them gone and derives again.
	if co, sent := readCheckout(t, other, repo, c2); len(co.Layers) == 0 || len(sent) != len(co.Layers) {
		t.Errorf("after pruning, c2's checkout %+v, %d sent", co, len(sent))
	}
	if co, sent := readCheckout(t, reopen(t, s), repo, c1); len(co.Layers) == 0 || len(sent) != len(co.Layers) {
		t.Errorf("after pruning, a cold replica's c1 %+v, %d sent", co, len(sent))
	}
}

// A closure with a base sends what the base does not hold at the same path: the changed trees and
// a new symlink, not a subtree the base has where it is, nor the base's own symlink.
func TestReadTreeClosureWithBase(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	keep := b.tree(fileEntry("y", b.file([]byte("y\n"))))
	old := b.symlink("old")
	sub1 := b.tree(fileEntry("x", b.file([]byte("x\n"))))
	sub2 := b.tree(fileEntry("x", b.file([]byte("x2\n"))))
	moved := b.tree(fileEntry("z", b.file([]byte("z\n"))))
	link := func(name string, id encoding.ID) encoding.TreeEntry {
		return encoding.TreeEntry{Name: name, Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: id}}
	}
	fresh := b.symlink("fresh")
	base := b.tree(treeEntry("keep", keep), treeEntry("sub", sub1), link("l", old), treeEntry("m", moved))
	root := b.tree(treeEntry("keep", keep), treeEntry("sub", sub2), link("l", old), link("n", fresh), treeEntry("moved", moved))
	mustPublish(t, s, repo, "alice", b, nil)
	var got []ObjectRef
	if err := s.ReadTreeClosure(ctx, repo, root, base, nil, false, func(o EncodedObject) error {
		got = append(got, ObjectRef{Kind: o.Kind, ID: o.ID})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []ObjectRef{{encoding.KindTree, root}, {encoding.KindSymlink, fresh}, {encoding.KindTree, sub2}, {encoding.KindTree, moved}}
	for _, w := range want {
		if !slices.Contains(got, w) {
			t.Errorf("not sent: %s %s", w.Kind, w.ID)
		}
	}
	for _, no := range []ObjectRef{{encoding.KindTree, keep}, {encoding.KindSymlink, old}, {encoding.KindTree, base}} {
		if slices.Contains(got, no) {
			t.Errorf("sent what the base holds there: %s %s", no.Kind, no.ID)
		}
	}
	// Its own base, nothing.
	got = nil
	if err := s.ReadTreeClosure(ctx, repo, root, root, nil, false, func(o EncodedObject) error {
		got = append(got, ObjectRef{Kind: o.Kind, ID: o.ID})
		return nil
	}); err != nil || len(got) != 0 {
		t.Errorf("a tree against itself sent %v, %v", got, err)
	}
}

// A file read stored, naming a base the repository holds, comes as one delta frame where that is
// smaller than its chunks, which applies to the base; a file the delta would not shrink, a base
// the repository lacks, and a ranged read come as chunks.
func TestReadFilesDeltas(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	var text []byte
	for i := range 2000 {
		text = append(text, []byte("line "+strings.Repeat("ab", i%7)+" of the file\n")...)
	}
	v1 := text
	v2 := bytes.Replace(append([]byte{}, text...), []byte("line ab of"), []byte("LINE AB OF"), 3)
	big1, big2 := bytesOf(100_000, 1), bytesOf(100_000, 2)
	id1, id2, rid1, rid2 := b.file(v1), b.file(v2), b.file(big1), b.file(big2)
	b.commit(b.tree(fileEntry("v1", id1), fileEntry("v2", id2), fileEntry("r1", rid1), fileEntry("r2", rid2)), "files", encoding.RootCommitID)
	mustPublish(t, s, repo, "alice", b, nil)
	stranger := encoding.Hash([]byte{byte(encoding.KindFile), 'q'})
	ranges := []FileRange{
		{FileID: id2, Base: id1},
		{FileID: rid2, Base: rid1},
		{FileID: id1, Base: stranger},
		{FileID: id2, Base: id1, Offset: 10, Length: 100},
	}
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	deltas, chunks := map[int]bool{}, map[int][]byte{}
	if err := s.ReadFiles(ctx, repo, ranges, StoredChunks, func(f FileFrame) error {
		if f.Base != (encoding.ID{}) {
			if deltas[f.Range] || f.Base != ranges[f.Range].Base || !f.Last || f.Size != uint64(len(v2)) {
				t.Errorf("range %d: a delta frame %+v", f.Range, f)
			}
			deltas[f.Range] = true
			d, err := dec.DecodeAll(f.Data, nil)
			if err != nil {
				return err
			}
			got, err := encoding.ApplyDelta(v1, d, uint64(f.RawLength))
			if err != nil || !bytes.Equal(got, v2) {
				t.Errorf("the delta applies to %d bytes, %v", len(got), err)
			}
			if len(f.Data) > len(v2)/20 {
				t.Errorf("a delta of %d bytes for three edits", len(f.Data))
			}
			return nil
		}
		raw, err := pack.Decompress(nil, f.ChunkID, f.Codec, f.RawLength, f.Data)
		if err != nil {
			return err
		}
		chunks[f.Range] = append(chunks[f.Range], raw...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !deltas[0] || len(deltas) != 1 {
		t.Errorf("deltas sent for ranges %v, want 0 alone", deltas)
	}
	if !bytes.Equal(chunks[1], big2) || !bytes.Equal(chunks[2], v1) || len(chunks[3]) == 0 {
		t.Errorf("chunks: random %d bytes, unknown base %d, ranged %d", len(chunks[1]), len(chunks[2]), len(chunks[3]))
	}
	// Read raw, no delta is made.
	if err := s.ReadFiles(ctx, repo, ranges[:1], RawChunks, func(f FileFrame) error {
		if f.Base != (encoding.ID{}) {
			t.Error("a delta, read raw")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
