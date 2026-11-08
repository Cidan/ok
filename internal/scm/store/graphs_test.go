package store

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/graph"
)

// graphOf reads main's graph's layers' records, in order, as a client joins them.
func graphOf(t *testing.T, s *Store, repo int64) (MainGraph, []encoding.GraphRecord) {
	t.Helper()
	ctx := context.Background()
	g, ok, err := s.mainGraph(ctx, repo)
	if err != nil || !ok {
		t.Fatalf("main's graph: %v, %v", ok, err)
	}
	var recs []encoding.GraphRecord
	for _, l := range g.Layers {
		b, err := reopen(t, s).GraphLayer(ctx, repo, l.ID)
		if err != nil {
			t.Fatal(err)
		}
		got, err := graph.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(got)) != l.Commits || int64(len(b)) != l.Size {
			t.Fatalf("layer %s holds %d commits in %d bytes; its reference says %d in %d", l.ID, len(got), len(b), l.Commits, l.Size)
		}
		recs = append(recs, got...)
	}
	return g, recs
}

// checkRecords holds records to the commits they name: each one's change ID, tree, and parents as
// its body has them, every parent before its child unless it is the root or one of known (of
// generations below any record's), and none twice.
func checkRecords(t *testing.T, s *Store, repo int64, recs []encoding.GraphRecord, known ...encoding.ID) {
	t.Helper()
	seen := map[encoding.ID]uint64{encoding.RootCommitID: 0}
	for _, k := range known {
		seen[k] = 0
	}
	for i, r := range recs {
		c, err := s.readCommit(context.Background(), repo, r.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(r.ChangeID, c.ChangeID[:]) || !slices.Equal(r.Tree, c.RootTree) || !slices.Equal(r.Parents, c.Parents) || r.Committed != c.Committer.Time {
			t.Fatalf("record %d of %s says %+v; its commit %+v", i, r.ID, r, c)
		}
		if _, dup := seen[r.ID]; dup {
			t.Fatalf("commit %s twice", r.ID)
		}
		for _, p := range r.Parents {
			g, ok := seen[p]
			if !ok {
				t.Fatalf("record %d's parent %s is not before it", i, p)
			}
			if g >= r.Generation {
				t.Fatalf("record %d's generation %d is not above its parent's %d", i, r.Generation, g)
			}
		}
		seen[r.ID] = r.Generation
	}
}

// Main's graph is derived as main moves: each landing's commits a layer on the last, the top layers
// merging until each is more than twice the one above it, the manifest naming them and the head
// they reach. Together the layers hold every commit of main once, in topological order, as their
// bodies say; deriving again with main where it was writes nothing.
func TestMainGraphLayers(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	if err := e.s.DeriveMainGraph(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := e.s.mainGraph(ctx, e.repo); err != nil || ok {
		t.Fatalf("an empty main's graph: %v, %v", ok, err)
	}
	var layers []int
	for i := range 40 {
		e.landChange(fmt.Sprintf("landing %d", i), map[string]string{fmt.Sprintf("f%02d", i): "x\n"})
		if err := e.s.DeriveMainGraph(ctx, e.repo); err != nil {
			t.Fatal(err)
		}
		g, recs := graphOf(t, e.s, e.repo)
		if g.Top != e.head() || len(recs) != i+1 || recs[len(recs)-1].ID != e.head() {
			t.Fatalf("after landing %d: top %s with %d commits, main at %s", i, g.Top, len(recs), e.head())
		}
		for k := 1; k < len(g.Layers); k++ {
			if g.Layers[k-1].Size <= 2*g.Layers[k].Size {
				t.Fatalf("after landing %d, layer %d (%d bytes) is not more than twice layer %d (%d)", i, k-1, g.Layers[k-1].Size, k, g.Layers[k].Size)
			}
		}
		layers = append(layers, len(g.Layers))
		checkRecords(t, e.s, e.repo, recs)
	}
	if slices.Max(layers) < 2 || slices.Max(layers) > graphMaxLayers {
		t.Errorf("layers as main grew: %v", layers)
	}
	before, _ := graphOf(t, e.s, e.repo)
	if err := e.s.DeriveMainGraph(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	if after, _ := graphOf(t, e.s, e.repo); !slices.Equal(after.Layers, before.Layers) {
		t.Error("deriving again changed main's graph")
	}
}

// readGraph runs ReadGraph, returning the header, the layers sent, and the records.
func readGraph(t *testing.T, s *Store, repo int64, heads, have, held []encoding.ID) (encoding.ID, []encoding.ID, map[encoding.ID][]byte, []encoding.GraphRecord) {
	t.Helper()
	var top encoding.ID
	var named []encoding.ID
	sent := map[encoding.ID][]byte{}
	var recs []encoding.GraphRecord
	err := s.ReadGraph(context.Background(), repo, heads, have, held, false, func(tp encoding.ID, layers []GraphLayerRef) error {
		top = tp
		for _, l := range layers {
			named = append(named, l.ID)
		}
		return nil
	}, func(id encoding.ID, data []byte, last bool) error {
		sent[id] = append(sent[id], data...)
		return nil
	}, func(data []byte) error {
		got, err := encoding.DecodeGraphRecords(data)
		recs = append(recs, got...)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return top, named, sent, recs
}

// ReadGraph sends main's layers a client lacks, then the records of the commits its heads reach
// that main's layers and its haves do not: a workspace's commits on main, and main's own since the
// head its layers reach while their derivation is behind. A head the repository does not hold is
// passed over; main's layers held are not sent; before main's graph is derived, all comes as records.
func TestReadGraph(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	for i := range 3 {
		e.landChange(fmt.Sprintf("landing %d", i), map[string]string{fmt.Sprintf("f%d", i): "x\n"})
	}
	main3 := e.head()
	// Before any derivation: everything as records.
	top, named, sent, recs := readGraph(t, e.s, e.repo, []encoding.ID{main3}, nil, nil)
	if top != (encoding.ID{}) || len(named) != 0 || len(sent) != 0 || len(recs) != 3 {
		t.Fatalf("underived: top %s, %d layers, %d sent, %d records", top, len(named), len(sent), len(recs))
	}
	checkRecords(t, e.s, e.repo, recs)
	if err := e.s.DeriveMainGraph(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	// A workspace's two commits on main, and a commit no one published.
	w1 := e.commit(alice, newChange(), main3, "work", map[string]string{"w": "1\n"})
	w2 := e.commit(alice, newChange(), w1, "more work", map[string]string{"w": "2\n"})
	unknown := encoding.Hash([]byte{byte(encoding.KindCommit), 1})
	top, named, sent, recs = readGraph(t, e.s, e.repo, []encoding.ID{w2, unknown}, nil, nil)
	if top != main3 || len(named) == 0 || len(sent) != len(named) {
		t.Fatalf("top %s (main at %s), %d layers named, %d sent", top, main3, len(named), len(sent))
	}
	if len(recs) != 2 || recs[0].ID != w1 || recs[1].ID != w2 {
		t.Fatalf("the workspace's records: %d", len(recs))
	}
	checkRecords(t, e.s, e.repo, recs, main3)
	// Holding the layers, and w1: only w2.
	_, _, sent, recs = readGraph(t, e.s, e.repo, []encoding.ID{w2}, []encoding.ID{w1}, named)
	if len(sent) != 0 || len(recs) != 1 || recs[0].ID != w2 {
		t.Fatalf("holding the layers and w1: %d sent, %d records", len(sent), len(recs))
	}
	// Main moves; until its graph is derived again, its new commit comes as a record.
	e.landChange("landing 3", map[string]string{"f3": "x\n"})
	top, _, _, recs = readGraph(t, e.s, e.repo, []encoding.ID{e.head()}, nil, named)
	if top != main3 || len(recs) != 1 || recs[0].ID != e.head() {
		t.Fatalf("main ahead of its graph: top %s, %d records", top, len(recs))
	}
}

// Layers main's graph no longer names are deleted once their grace has passed; those it names are
// kept, however old.
func TestPruneGraphs(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	for i := range 6 {
		e.landChange(fmt.Sprintf("landing %d", i), map[string]string{fmt.Sprintf("f%d", i): "x\n"})
		if err := e.s.DeriveMainGraph(ctx, e.repo); err != nil {
			t.Fatal(err)
		}
	}
	all, err := e.s.blobs.List(ctx, graphsPrefix(e.repo)+"l/")
	if err != nil {
		t.Fatal(err)
	}
	g, _ := graphOf(t, e.s, e.repo)
	if len(all) <= len(g.Layers) {
		t.Fatalf("%d layers in the bucket, %d named: nothing merged", len(all), len(g.Layers))
	}
	if n, err := e.s.pruneGraphs(ctx, e.repo, time.Now(), false); err != nil || n != 0 {
		t.Fatalf("within the grace: %d deleted, %v", n, err)
	}
	n, err := e.s.pruneGraphs(ctx, e.repo, time.Now().Add(2*graphLayerGrace), false)
	if err != nil || n != len(all)-len(g.Layers) {
		t.Fatalf("past the grace: %d deleted, %v; want %d", n, err, len(all)-len(g.Layers))
	}
	if _, recs := graphOf(t, e.s, e.repo); len(recs) != 6 {
		t.Fatalf("main's graph after pruning: %d commits", len(recs))
	}
}

// A manifest a later release wrote is as if there were none: reading the graph derives main's again
// as this release writes it, garbage collection prunes no layer meanwhile, and the next derivation
// writes this release's. A layer the manifest names that is gone has main's graph derived again,
// whole, before it is read.
func TestMainGraphOfAnotherRelease(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	for i := range 4 {
		e.landChange(fmt.Sprintf("landing %d", i), map[string]string{fmt.Sprintf("f%d", i): "x\n"})
		if err := e.s.DeriveMainGraph(ctx, e.repo); err != nil {
			t.Fatal(err)
		}
	}
	g, _ := graphOf(t, e.s, e.repo)
	newer := encodeMainGraph(g)
	newer[len(mainGraphMagic)] = mainGraphVersion + 1
	put := func(b []byte) {
		t.Helper()
		if err := e.s.blobs.Put(ctx, mainGraphKey(e.repo), bytes.NewReader(b), int64(len(b))); err != nil {
			t.Fatal(err)
		}
	}
	put(newer)
	if n, err := e.s.pruneGraphs(ctx, e.repo, time.Now().Add(2*graphLayerGrace), false); err != nil || n != 0 {
		t.Fatalf("pruning beside a later release's manifest: %d deleted, %v", n, err)
	}
	top, named, _, recs := readGraph(t, e.s, e.repo, []encoding.ID{e.head()}, nil, nil)
	if top != e.head() || len(named) != 1 || len(recs) != 0 {
		t.Fatalf("read beside a later release's manifest: top %s, %d layers, %d records", top, len(named), len(recs))
	}
	if _, recs := graphOf(t, e.s, e.repo); len(recs) != 4 {
		t.Fatalf("main's graph derived again holds %d commits", len(recs))
	}
	// Every layer gone but the manifest.
	all, err := e.s.blobs.List(ctx, graphsPrefix(e.repo)+"l/")
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range all {
		if err := e.s.blobs.Delete(ctx, o.Key); err != nil {
			t.Fatal(err)
		}
	}
	cold := reopen(t, e.s)
	top, named, sent, recs := readGraph(t, cold, e.repo, []encoding.ID{e.head()}, nil, nil)
	if top != e.head() || len(named) == 0 || len(sent) != len(named) || len(recs) != 0 {
		t.Fatalf("read with its layers gone: top %s, %d layers, %d sent, %d records", top, len(named), len(sent), len(recs))
	}
	e.landChange("landing 4", map[string]string{"f4": "x\n"})
	if err := cold.DeriveMainGraph(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	if _, recs := graphOf(t, cold, e.repo); len(recs) != 5 {
		t.Fatalf("main's graph after another landing holds %d commits", len(recs))
	}
}

// Each record of a commit of one parent and no conflict carries the filter of the paths it changed —
// each file and every directory above one — main's layers' and the records ReadGraph sends alike; a
// commit that changed more paths than a filter holds has the filter of every path. A manifest of
// version 1, whose layers' records have none, is derived again, whole. Records only, ReadGraph sends
// no layer.
func TestGraphPathFilters(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	// Each commit main's files and these.
	with := func(add map[string]string) map[string]string {
		files := map[string]string{}
		if e.head() != encoding.RootCommitID {
			files = e.flatAt(e.head())
		}
		for p, c := range add {
			files[p] = c
		}
		return files
	}
	e.landChange("one file", with(map[string]string{"src/a/one.go": "1\n"}))
	e.landChange("another", with(map[string]string{"docs/readme.md": "2\n"}))
	many := map[string]string{}
	for i := range encoding.PathFilterMaxPaths + 1 {
		many[fmt.Sprintf("big/f%04d", i)] = "x\n"
	}
	e.landChange("many", with(many))
	if err := e.s.DeriveMainGraph(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	g, recs := graphOf(t, e.s, e.repo)
	if g.Version != mainGraphVersion || len(recs) != 3 {
		t.Fatalf("version %d, %d records", g.Version, len(recs))
	}
	holds := func(r encoding.GraphRecord, p string) bool { return encoding.PathFilterHolds(r.Filter, p) }
	for _, r := range recs {
		if r.Filter == nil {
			t.Fatalf("commit %s has no filter", r.ID)
		}
	}
	if !holds(recs[0], "src/a/one.go") || !holds(recs[0], "src/a") || !holds(recs[0], "src") || holds(recs[0], "docs/readme.md") || holds(recs[0], "docs") {
		t.Errorf("the first commit's filter: %x", recs[0].Filter)
	}
	if !holds(recs[1], "docs/readme.md") || !holds(recs[1], "docs") || holds(recs[1], "src/a/one.go") || holds(recs[1], "src") {
		t.Errorf("the second commit's filter: %x", recs[1].Filter)
	}
	if !bytes.Equal(recs[2].Filter, []byte{0xff}) {
		t.Errorf("a commit of %d paths has the filter %x, not every path's", len(many)+1, recs[2].Filter)
	}

	// A workspace's commit beyond main, sent as a record, and one with two parents, which has none.
	w := e.commit(alice, newChange(), e.head(), "work", with(map[string]string{"src/a/two.go": "2\n"}))
	b := newBuilder(t)
	merge := b.commit(b.flat(map[string]string{"m": "m\n"}), "merge", e.head(), w)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	_, _, _, loose := readGraph(t, e.s, e.repo, []encoding.ID{merge}, nil, nil)
	if len(loose) != 2 || loose[0].ID != w || loose[1].ID != merge {
		t.Fatalf("%d records beyond main", len(loose))
	}
	if !holds(loose[0], "src/a/two.go") || holds(loose[0], "docs") || loose[1].Filter != nil {
		t.Errorf("filters beyond main: %x, %x", loose[0].Filter, loose[1].Filter)
	}

	// Records only: no layer, and every commit from main's first to w.
	top, named, sent, only := readGraphOnly(t, e.s, e.repo, []encoding.ID{w})
	if top != (encoding.ID{}) || len(named) != 0 || len(sent) != 0 || len(only) != 4 || only[3].ID != w {
		t.Fatalf("records only: top %s, %d layers, %d sent, %d records", top, len(named), len(sent), len(only))
	}

	// A version 1 manifest is derived again, whole, its records' filters with it.
	old := g
	old.Version = 1
	b1 := encodeMainGraph(old)
	if err := e.s.blobs.Put(ctx, mainGraphKey(e.repo), bytes.NewReader(b1), int64(len(b1))); err != nil {
		t.Fatal(err)
	}
	if err := e.s.DeriveMainGraph(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	if again, recs := graphOf(t, e.s, e.repo); again.Version != mainGraphVersion || len(again.Layers) != 1 || len(recs) != 3 || recs[0].Filter == nil {
		t.Fatalf("a version 1 manifest derived again: version %d, %d layers, %d records", again.Version, len(again.Layers), len(recs))
	}
}

// readGraphOnly runs ReadGraph for records alone.
func readGraphOnly(t *testing.T, s *Store, repo int64, heads []encoding.ID) (encoding.ID, []encoding.ID, map[encoding.ID][]byte, []encoding.GraphRecord) {
	t.Helper()
	var top encoding.ID
	var named []encoding.ID
	sent := map[encoding.ID][]byte{}
	var recs []encoding.GraphRecord
	err := s.ReadGraph(context.Background(), repo, heads, nil, nil, true, func(tp encoding.ID, layers []GraphLayerRef) error {
		top = tp
		for _, l := range layers {
			named = append(named, l.ID)
		}
		return nil
	}, func(id encoding.ID, data []byte, last bool) error {
		sent[id] = append(sent[id], data...)
		return nil
	}, func(data []byte) error {
		got, err := encoding.DecodeGraphRecords(data)
		recs = append(recs, got...)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return top, named, sent, recs
}
