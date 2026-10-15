package store

import (
	"context"
	"slices"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/serve/metrics"
)

func decodeSizes(t *testing.T, b []byte) []uint64 {
	t.Helper()
	s, err := encoding.DecodeTreeSizes(b)
	if err != nil {
		t.Fatal(err)
	}
	return s.Sizes
}

// A tree's sizes are its entries', in its order: files' lengths, symlinks' targets', 0 for a
// subtree. They are derived the first time, kept, and served from memory and then PostgreSQL;
// a tree the repository does not hold is left out, and the empty tree has none.
func TestTreeSizes(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	big := bytesOf(300_000, 9) // chunked: its size is the manifest's, not a chunk's
	sub := b.tree(fileEntry("x", b.file([]byte("x\n"))))
	root := b.tree(
		fileEntry("a.txt", b.file([]byte("hello\n"))),
		fileEntry("big.bin", b.file(big)),
		fileEntry("empty", b.file(nil)),
		encoding.TreeEntry{Name: "link", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: b.symlink("../target/path")}},
		treeEntry("sub", sub),
	)
	mustPublish(t, s, repo, "alice", b, nil)
	served := func(source string) float64 {
		return metrics.Default().Read("ok_scm_tree_sizes_total", source).Value
	}
	derived, kept, memory := served("derived"), served("kept"), served("memory")

	var missing encoding.ID
	missing[0] = 7
	got, err := s.TreeSizes(ctx, repo, []encoding.ID{root, sub, root, encoding.EmptyTreeID, missing})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got[missing]; ok || len(got) != 3 {
		t.Fatalf("answered %d trees: a tree not held is left out", len(got))
	}
	if want := []uint64{6, uint64(len(big)), 0, uint64(len("../target/path")), 0}; !slices.Equal(decodeSizes(t, got[root]), want) {
		t.Fatalf("root's sizes: %v, want %v", decodeSizes(t, got[root]), want)
	}
	if !slices.Equal(decodeSizes(t, got[sub]), []uint64{2}) || len(decodeSizes(t, got[encoding.EmptyTreeID])) != 0 {
		t.Fatal("sub's or the empty tree's sizes are wrong")
	}
	if served("derived")-derived != 2 {
		t.Fatalf("%v derived, want 2 (root and sub, once each)", served("derived")-derived)
	}
	var n int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM tree_sizes WHERE repository_id = $1`, repo).Scan(&n); err != nil || n != 2 {
		t.Fatalf("%d kept (%v), want 2", n, err)
	}

	// Again: from memory.
	if _, err := s.TreeSizes(ctx, repo, []encoding.ID{root}); err != nil {
		t.Fatal(err)
	}
	if served("memory")-memory != 1 {
		t.Fatal("the second read was not from memory")
	}
	// A replica that never derived them reads them kept.
	other := reopen(t, s)
	again, err := other.TreeSizes(ctx, repo, []encoding.ID{root, sub})
	if err != nil {
		t.Fatal(err)
	}
	if served("kept")-kept != 2 || string(again[root]) != string(got[root]) {
		t.Fatal("another replica did not read the kept records")
	}
	// Keeping them twice is no error: every replica derives the same record.
	if _, err := s.deriveTreeSizes(ctx, repo, []encoding.ID{root}); err != nil {
		t.Fatal(err)
	}
}

// A closure asked for sizes sends each tree's record after its level, under the tree's ID.
func TestReadTreeClosureWithSizes(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	sub := b.tree(fileEntry("x", b.file([]byte("xx\n"))))
	root := b.tree(fileEntry("a", b.file([]byte("a\n"))), treeEntry("sub", sub))
	mustPublish(t, s, repo, "alice", b, nil)
	var order []string
	sizes := map[encoding.ID][]uint64{}
	err := s.ReadTreeClosure(ctx, repo, root, nil, true, func(o EncodedObject) error {
		order = append(order, o.Kind.String()+" "+o.ID.String()[:8])
		if o.Kind == encoding.KindTreeSizes {
			sizes[o.ID] = decodeSizes(t, o.Body)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(sizes[root], []uint64{2, 0}) || !slices.Equal(sizes[sub], []uint64{3}) {
		t.Fatalf("sizes %v", sizes)
	}
	want := []string{
		"tree " + root.String()[:8], encoding.KindTreeSizes.String() + " " + root.String()[:8],
		"tree " + sub.String()[:8], encoding.KindTreeSizes.String() + " " + sub.String()[:8],
	}
	if !slices.Equal(order, want) {
		t.Fatalf("sent %v, want %v: each level's trees, then their sizes", order, want)
	}
}
