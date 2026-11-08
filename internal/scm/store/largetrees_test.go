package store

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
)

// largeTree is a tree of n entries, each name a file of the builder's: one file under many names,
// so the tree is large and what it names small.
func largeTree(b *builder, file encoding.ID, n int, rename func(i int) string) (encoding.ID, []byte) {
	var entries []encoding.TreeEntry
	for i := range n {
		entries = append(entries, fileEntry(rename(i), file))
	}
	id := b.tree(entries...)
	return id, b.body(ObjectRef{Kind: encoding.KindTree, ID: id})
}

// publishTrees runs a publish of everything in b, the trees in large among them as their bodies'
// chunks, and returns the objects and chunks it asked for.
func publishTrees(t *testing.T, s *Store, repo int64, b *builder, large map[encoding.ID][]byte, cut func([]byte) [][]byte, heads *OpHeadsUpdate) ([]ObjectRef, []encoding.ID, error) {
	t.Helper()
	ctx := context.Background()
	manifests := b.manifests()
	chunks := map[encoding.ID][]byte{}
	for id, body := range large {
		m := FileManifest{FileID: id, Size: uint64(len(body)), Tree: true}
		for _, c := range cut(body) {
			cid := pack.ChunkID(c)
			chunks[cid] = c
			m.Chunks = append(m.Chunks, ChunkRef{ID: cid, Length: uint32(len(c))})
		}
		manifests = append(manifests, m)
	}
	p, err := s.BeginPublish(ctx, repo, alice, testWorkspace, b.order, manifests)
	if err != nil {
		return nil, nil, err
	}
	objs, missing := p.Missing()
	for _, r := range objs {
		if err := p.AddObject(EncodedObject{Kind: r.Kind, ID: r.ID, Body: b.body(r)}); err != nil {
			p.Close()
			return nil, nil, err
		}
	}
	for _, c := range missing {
		data, ok := chunks[c]
		if !ok {
			data = b.chunk(c)
		}
		if err := p.AddChunk(ctx, c, data); err != nil {
			p.Close()
			return nil, nil, err
		}
	}
	_, err = p.Commit(ctx, heads)
	return objs, missing, err
}

func canonicalCut(t *testing.T) func([]byte) [][]byte {
	return func(body []byte) [][]byte {
		cut, err := encoding.ChunkTree(body)
		if err != nil {
			t.Fatal(err)
		}
		return cut
	}
}

func treeEntryOf(t *testing.T, s *Store, repo int64, id encoding.ID) contentindex.Entry {
	t.Helper()
	_, entries, err := s.contentEntries(context.Background(), repoScope(repo), encoding.KindTree, []encoding.ID{id})
	if err != nil {
		t.Fatal(err)
	}
	e, ok := entries[id]
	if !ok {
		t.Fatalf("tree %s is not in the content index", id)
	}
	return e
}

// A large tree is published as its body's chunks, never its body: held as them in the content index,
// read back whole by a replica with nothing cached, and verified by fsck. A version that changes one
// entry sends one chunk; an upload pins its chunks with it.
func TestLargeTrees(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	f := b.file([]byte("one file\n"))
	name := func(i int) string { return fmt.Sprintf("asset-%05d.bin", i) }
	tree, body := largeTree(b, f, 6000, name)
	if len(body) <= encoding.TreeChunkThreshold {
		t.Fatalf("a tree of %d bytes is not large", len(body))
	}
	commit := b.commit(tree, "assets", encoding.RootCommitID)
	objs, chunks, err := publishTrees(t, e.s, e.repo, b, map[encoding.ID][]byte{tree: body}, canonicalCut(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range objs {
		if r.ID == tree {
			t.Fatal("the publish asked for the large tree's body")
		}
	}
	cut := canonicalCut(t)(body)
	if len(chunks) < len(cut) {
		t.Fatalf("asked for %d chunks, the tree is %d", len(chunks), len(cut))
	}
	te := treeEntryOf(t, e.s, e.repo, tree)
	if !te.Chunked || te.Located() || te.Size != uint64(len(body)) || len(te.Chunks) != len(cut) {
		t.Fatalf("the tree's entry: %+v", te)
	}
	got, err := reopen(t, e.s).readTree(ctx, e.repo, tree)
	if err != nil || len(got.Entries) != 6000 || got.Entries[17].Name != name(17) {
		t.Fatalf("read back cold: %v", err)
	}

	// One entry changed: one chunk is new.
	b2 := newBuilder(t)
	b2.files[f] = []byte("one file\n")
	g := b2.file([]byte("another\n"))
	var entries []encoding.TreeEntry
	for i := range 6000 {
		v := f
		if i == 3210 {
			v = g
		}
		entries = append(entries, fileEntry(name(i), v))
	}
	edited := b2.tree(entries...)
	b2.commit(edited, "one changed", commit)
	_, chunks, err = publishTrees(t, e.s, e.repo, b2, map[encoding.ID][]byte{edited: b2.body(ObjectRef{Kind: encoding.KindTree, ID: edited})}, canonicalCut(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	newChunks := 0
	for _, c := range chunks {
		if _, isFile := b2.chunks[c]; !isFile {
			newChunks++
		}
	}
	if newChunks != 1 {
		t.Errorf("a version of one changed entry sent %d of the tree's chunks, want 1", newChunks)
	}

	// The upload pins the tree and its chunks.
	e.mustUpload(alice, Ready, commit)
	if te := treeEntryOf(t, e.s, e.repo, tree); !te.Pinned {
		t.Fatal("the uploaded tree is not pinned")
	}
	_, ce, err := e.s.contentEntries(ctx, repoScope(e.repo), encoding.KindChunk, []encoding.ID{te.Chunks[1].ID})
	if err != nil || !ce[te.Chunks[1].ID].Pinned {
		t.Fatalf("the uploaded tree's chunk is not pinned: %+v, %v", ce, err)
	}
	if r := clean(t, e.s, e.repo); r.ChunkedTrees != 2 {
		t.Fatalf("fsck verified %d chunked trees, want 2", r.ChunkedTrees)
	}
}

// A large tree sent cut otherwise than large trees are, or a tree too small to be cut sent as chunks,
// is refused.
func TestLargeTreesRefused(t *testing.T) {
	e := newCLEnv(t)
	b := newBuilder(t)
	f := b.file([]byte("x\n"))
	tree, body := largeTree(b, f, 6000, func(i int) string { return fmt.Sprintf("n%05d", i) })
	halves := func(body []byte) [][]byte { return [][]byte{body[:len(body)/2], body[len(body)/2:]} }
	if _, _, err := publishTrees(t, e.s, e.repo, b, map[encoding.ID][]byte{tree: body}, halves, nil); err == nil {
		t.Fatal("a large tree cut in halves was taken")
	}
	s := newBuilder(t)
	small := s.tree(fileEntry("a", s.file([]byte("a\n"))))
	sb := s.body(ObjectRef{Kind: encoding.KindTree, ID: small})
	if _, _, err := publishTrees(t, e.s, e.repo, s, map[encoding.ID][]byte{small: sb}, func(b []byte) [][]byte { return [][]byte{b} }, nil); err == nil {
		t.Fatal("a small tree sent as chunks was taken")
	}
}

// Garbage collection sweeps a large tree nothing reaches, and its chunks with it, and keeps one an
// operation log reaches, with its chunks.
func TestGCSweepsLargeTrees(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	f := b.file([]byte("kept\n"))
	kept, keptBody := largeTree(b, f, 5000, func(i int) string { return fmt.Sprintf("k%05d", i) })
	keep := b.commit(kept, "kept", encoding.RootCommitID)
	op := b.operation(b.view(keep), "snapshot", encoding.RootOperationID)
	if _, _, err := publishTrees(t, e.s, e.repo, b, map[encoding.ID][]byte{kept: keptBody}, canonicalCut(t), &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op}); err != nil {
		t.Fatal(err)
	}
	g := newBuilder(t)
	gf := g.file([]byte("gone\n"))
	gone, goneBody := largeTree(g, gf, 5000, func(i int) string { return fmt.Sprintf("g%05d", i) })
	g.commit(gone, "garbage", encoding.RootCommitID)
	if _, _, err := publishTrees(t, e.s, e.repo, g, map[encoding.ID][]byte{gone: goneBody}, canonicalCut(t), nil); err != nil {
		t.Fatal(err)
	}
	goneChunk := treeEntryOf(t, e.s, e.repo, gone).Chunks[1].ID
	keptChunk := treeEntryOf(t, e.s, e.repo, kept).Chunks[1].ID
	age(t, e.s, e.repo, 20)
	if _, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, entries, _ := e.s.contentEntries(ctx, repoScope(e.repo), encoding.KindTree, []encoding.ID{gone, kept}); len(entries) != 1 {
		t.Fatalf("after collection the index holds %d of the two trees, want the kept one", len(entries))
	}
	if !has(t, e.s, e.repo, "chunk", keptChunk) || has(t, e.s, e.repo, "chunk", goneChunk) {
		t.Fatal("collection kept the swept tree's chunk, or swept the kept tree's")
	}
	got, err := reopen(t, e.s).readTree(ctx, e.repo, kept)
	if err != nil || !bytes.Equal(encodeTreeBody(t, got), keptBody) {
		t.Fatalf("the kept tree reads back: %v", err)
	}
	clean(t, e.s, e.repo)
}

func encodeTreeBody(t *testing.T, tree *encoding.Tree) []byte {
	t.Helper()
	canonical, err := encoding.Encode(tree)
	if err != nil {
		t.Fatal(err)
	}
	return canonical[1:]
}

// A landing that splices a large tree holds it as its chunks, as a publish does, not as a row: main
// reads back with both CLs' edits, the tree is pinned with its chunks, and fsck verifies it. A
// candidate's splice is held so too.
func TestLandingSplicesLargeTrees(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	assets := map[string]string{}
	for i := range 6000 {
		assets[fmt.Sprintf("assets/a%05d.bin", i)] = "x\n"
	}
	e.landChange("assets", assets)
	base := e.head()
	edit := func(name, v string) map[string]string {
		files := map[string]string{}
		for k, c := range assets {
			files[k] = c
		}
		files[name] = v
		return files
	}
	a := e.open(e.commit(alice, newChange(), base, "a", edit("assets/a00010.bin", "a\n")))
	b := e.open(e.commit(alice, newChange(), base, "b", edit("assets/a05990.bin", "b\n")))
	c := e.open(e.commit(alice, newChange(), base, "c", edit("assets/a03000.bin", "c\n")))
	e.mustLand(LandItem{Number: a, Patchset: 1})
	e.mustLand(LandItem{Number: b, Patchset: 1})
	main := e.flatAt(e.head())
	if main["assets/a00010.bin"] != "a\n" || main["assets/a05990.bin"] != "b\n" || len(main) != 6000 {
		t.Fatalf("main after the splice: %d files", len(main))
	}
	dir := func(commit encoding.ID) encoding.ID {
		t.Helper()
		cm, err := e.s.readCommit(ctx, e.repo, commit)
		if err != nil {
			t.Fatal(err)
		}
		root, err := e.s.readTree(ctx, e.repo, cm.RootTree[0])
		if err != nil {
			t.Fatal(err)
		}
		for _, en := range root.Entries {
			if en.Name == "assets" {
				return en.Value.ID
			}
		}
		t.Fatal("no assets")
		return encoding.ID{}
	}
	spliced := dir(e.head())
	te := treeEntryOf(t, e.s, e.repo, spliced)
	if !te.Chunked || !te.Pinned {
		t.Fatalf("the spliced tree's entry: %+v", te)
	}
	_, ce, err := e.s.contentEntries(ctx, repoScope(e.repo), encoding.KindChunk, []encoding.ID{te.Chunks[1].ID})
	if err != nil || !ce[te.Chunks[1].ID].Pinned {
		t.Fatalf("the spliced tree's chunk: %+v, %v", ce, err)
	}
	if got, err := reopen(t, e.s).readTree(ctx, e.repo, spliced); err != nil || len(got.Entries) != 6000 {
		t.Fatalf("the spliced tree read back cold: %v", err)
	}

	cand, err := e.s.PrepareCandidate(ctx, e.repo, c, 1, e.head(), alice)
	if err != nil {
		t.Fatal(err)
	}
	ce2 := treeEntryOf(t, e.s, e.repo, dir(cand.Commit))
	if !ce2.Chunked || ce2.Pinned {
		t.Fatalf("the candidate's spliced tree's entry: %+v", ce2)
	}
	if r := clean(t, e.s, e.repo); r.ChunkedTrees < 2 {
		t.Fatalf("fsck verified %d chunked trees", r.ChunkedTrees)
	}
}

// wholeTrees reads trees whole, as a diff of trees not large does.
type wholeTrees struct{ ts treeStore }

func (w wholeTrees) readTree(id encoding.ID) (*encoding.Tree, error) { return w.ts.readTree(id) }
func (w wholeTrees) writeTree(t *encoding.Tree) (encoding.ID, error) { return w.ts.writeTree(t) }

// A diff of two large trees reads only the leaves each lacks of the other's and finds exactly what a
// diff of the whole trees finds: an entry changed, added, removed, and several at once.
func TestDiffLargeTrees(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	f := b.file([]byte("f\n"))
	g := b.file([]byte("g\n"))
	name := func(i int) string { return fmt.Sprintf("e%05d", i) }
	versions := map[string]func(int) (string, encoding.ID, bool){
		"base": func(i int) (string, encoding.ID, bool) { return name(i), f, true },
		"changed": func(i int) (string, encoding.ID, bool) {
			v := f
			if i == 4321 {
				v = g
			}
			return name(i), v, true
		},
		"added": func(i int) (string, encoding.ID, bool) {
			if i == 6000 {
				return "e02500a", g, true
			}
			return name(i), f, true
		},
		"removed": func(i int) (string, encoding.ID, bool) { return name(i), f, i != 777 },
		"several": func(i int) (string, encoding.ID, bool) {
			v := f
			if i%997 == 3 {
				v = g
			}
			return name(i), v, i != 5555
		},
	}
	large := map[encoding.ID][]byte{}
	ids := map[string]encoding.ID{}
	for k, rule := range versions {
		var entries []encoding.TreeEntry
		for i := range 6001 {
			if i == 6000 && k != "added" {
				break
			}
			n, v, ok := rule(i)
			if ok {
				entries = append(entries, fileEntry(n, v))
			}
		}
		slices.SortFunc(entries, func(a, b encoding.TreeEntry) int { return strings.Compare(a.Name, b.Name) })
		id := b.tree(entries...)
		ids[k] = id
		large[id] = b.body(ObjectRef{Kind: encoding.KindTree, ID: id})
	}
	if _, _, err := publishTrees(t, e.s, e.repo, b, large, canonicalCut(t), nil); err != nil {
		t.Fatal(err)
	}
	diff := func(ts treeStore, a, c encoding.ID) []string {
		t.Helper()
		var out []string
		if err := diffTrees(ts, a, c, "", func(ch leafChange) error {
			out = append(out, ch.Path)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, k := range []string{"changed", "added", "removed", "several"} {
		// On a replica with nothing cached: a large tree read whole would be kept in memory.
		cold := reopen(t, e.s)
		got := diff(cold.pendingTrees(ctx, e.repo), ids["base"], ids[k])
		for _, v := range []string{"base", k} {
			if _, whole := cold.objects.get(objectKey(e.repo, ObjectRef{Kind: encoding.KindTree, ID: ids[v]})); whole {
				t.Errorf("%s: the diff read %s whole", k, v)
			}
		}
		want := diff(wholeTrees{e.s.pendingTrees(ctx, e.repo)}, ids["base"], ids[k])
		if !slices.Equal(got, want) || len(want) == 0 {
			t.Errorf("%s: diffed by leaves %v, whole %v", k, got, want)
		}
	}
}

// A path in a large tree is found through its index — one leaf read, never the whole tree — whether
// it is the first entry, one in the middle, the last, or absent before, between, or after them.
func TestLargeTreeLookups(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	f := b.file([]byte("f\n"))
	tree, body := largeTree(b, f, 8000, func(i int) string { return fmt.Sprintf("m%05d", 2*i) })
	root := b.tree(treeEntry("dir", tree))
	b.commit(root, "large", encoding.RootCommitID)
	if _, _, err := publishTrees(t, e.s, e.repo, b, map[encoding.ID][]byte{tree: body}, canonicalCut(t), nil); err != nil {
		t.Fatal(err)
	}
	cold := reopen(t, e.s)
	tr := treeReader{ctx: ctx, s: cold, repo: e.repo}
	for p, want := range map[string]bool{
		"dir/m00000": true, "dir/m07998": true, "dir/m15998": true,
		"dir/a": false, "dir/m07999": false, "dir/z": false, "dir/m00000/x": false,
	} {
		v, err := lookupPath(tr, root, p)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		if (v != nil) != want || (v != nil && v.ID != f) {
			t.Errorf("%s: %+v, want found %v", p, v, want)
		}
	}
	if _, whole := cold.objects.get(objectKey(e.repo, ObjectRef{Kind: encoding.KindTree, ID: tree})); whole {
		t.Error("a lookup read the large tree whole")
	}
}
