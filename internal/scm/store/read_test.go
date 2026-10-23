package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

func TestGetRelatedCopies(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	copyOf := func(path string, parents ...encoding.ID) encoding.ID {
		return b.add(&encoding.CopyHistory{CurrentPath: path, Parents: parents})
	}
	a := copyOf("a")
	bb := copyOf("b", a)
	c := copyOf("c", bb)
	d := copyOf("d", a)
	e := copyOf("unrelated")
	mustPublish(t, s, repo, "alice", b, nil)

	got, err := s.GetRelatedCopies(ctx, repo, c)
	if err != nil {
		t.Fatal(err)
	}
	pos := map[encoding.ID]int{}
	for i, o := range got {
		pos[o.ID] = i
		if !bytes.Equal(o.Body, b.body(ObjectRef{encoding.KindCopyHistory, o.ID})) {
			t.Errorf("copy %s: wrong body", o.ID)
		}
	}
	if len(got) != 4 || len(pos) != 4 {
		t.Fatalf("related copies = %d, want a, b, c, d", len(got))
	}
	if _, ok := pos[e]; ok {
		t.Error("an unrelated copy was returned")
	}
	for child, parent := range map[encoding.ID]encoding.ID{bb: a, c: bb, d: a} {
		if pos[child] > pos[parent] {
			t.Errorf("child %s after its parent %s", child, parent)
		}
	}
	again, _ := s.GetRelatedCopies(ctx, repo, c)
	if !reflect.DeepEqual(got, again) {
		t.Error("related copies are not deterministic")
	}
	if _, err := s.GetRelatedCopies(ctx, repo, encoding.ID{1}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown copy: %v", err)
	}
}

func TestPrefetchCommits(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	const n = 3000 // several generation windows deep
	chain := make([]encoding.ID, n)
	parent := encoding.RootCommitID
	for i := range chain {
		chain[i] = b.commit(encoding.EmptyTreeID, fmt.Sprintf("c%d\n", i), parent)
		parent = chain[i]
	}
	branch := b.commit(encoding.EmptyTreeID, "branch\n", chain[1000])
	v := b.view(chain[n-1], branch)
	o := b.operation(v, "ops", encoding.RootOperationID)
	mustPublish(t, s, repo, "alice", b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: o})

	fetch := func(want, have []encoding.ID, opLog bool, limit int) []encoding.ID {
		t.Helper()
		var out []encoding.ID
		if err := s.PrefetchCommits(ctx, repo, "alice", testWorkspace, want, have, opLog, limit, func(o EncodedObject) error {
			if o.Kind != encoding.KindCommit {
				t.Fatalf("prefetch sent a %s", o.Kind)
			}
			out = append(out, o.ID)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := fetch([]encoding.ID{chain[n-1]}, nil, false, 0); len(got) != n || got[0] != chain[n-1] || got[n-1] != chain[0] {
		t.Fatalf("full history: %d commits", len(got))
	}
	if got := fetch([]encoding.ID{chain[n-1]}, []encoding.ID{chain[999]}, false, 0); len(got) != n-1000 || got[len(got)-1] != chain[1000] {
		t.Fatalf("history above chain[999]: %d commits", len(got))
	}
	got := fetch([]encoding.ID{chain[n-1], branch}, []encoding.ID{chain[1500]}, false, 0)
	if len(got) != n-1501+1 {
		t.Fatalf("history above chain[1500] plus the branch: %d commits", len(got))
	}
	found := false
	for _, id := range got {
		found = found || id == branch
	}
	if !found {
		t.Fatal("the branch below have's generation was not sent")
	}
	if got := fetch([]encoding.ID{chain[n-1]}, nil, false, 1000); len(got) != 1000 || got[999] != chain[n-1000] {
		t.Fatalf("limited prefetch: %d commits", len(got))
	}
	if got := fetch(nil, nil, true, 0); len(got) != n+1 {
		t.Fatalf("op-log prefetch: %d commits, want %d", len(got), n+1)
	}
	if got := fetch([]encoding.ID{{0x99}}, nil, false, 0); len(got) != 0 {
		t.Fatalf("unknown want: %d commits", len(got))
	}
}

func TestReadTreeClosure(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	cp := b.add(&encoding.CopyHistory{CurrentPath: "a/b/c.txt"})
	link := b.symlink("target")
	fc := b.file([]byte("c\n"))
	bTree := b.tree(encoding.TreeEntry{Name: "c.txt", Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: fc, CopyID: &cp}})
	aTree := b.tree(treeEntry("b", bTree), fileEntry("d.txt", b.file([]byte("d\n"))),
		encoding.TreeEntry{Name: "l", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: link}})
	eTree := b.tree(fileEntry("f.txt", b.file([]byte("f\n"))))
	root := b.tree(treeEntry("a", aTree), treeEntry("e", eTree))
	mustPublish(t, s, repo, "alice", b, nil)

	closure := func(prefixes ...string) map[encoding.ID]bool {
		t.Helper()
		out := map[encoding.ID]bool{}
		if err := s.ReadTreeClosure(ctx, repo, root, prefixes, false, func(o EncodedObject) error {
			if out[o.ID] {
				t.Errorf("%s sent twice", o.ID)
			}
			out[o.ID] = true
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	all := closure()
	for name, id := range map[string]encoding.ID{"root": root, "a": aTree, "a/b": bTree, "e": eTree, "link": link, "copy": cp} {
		if !all[id] {
			t.Errorf("full closure lacks %s", name)
		}
	}
	if len(all) != 6 {
		t.Errorf("full closure has %d objects, want 6", len(all))
	}
	sparse := closure("a/b")
	if want := map[encoding.ID]bool{root: true, aTree: true, bTree: true, cp: true}; !reflect.DeepEqual(sparse, want) {
		t.Errorf("sparse closure = %d objects, want root, a, a/b, and the copy", len(sparse))
	}
	if err := s.ReadTreeClosure(ctx, repo, root, []string{"a/../e"}, false, func(EncodedObject) error { return nil }); !errors.Is(err, ErrInvalid) {
		t.Errorf("unclean prefix: %v", err)
	}
}

func TestGetCopyRecords(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	text := strings.Repeat("a line of the original file\n", 20)
	old := b.file([]byte(text))
	renamed := b.file([]byte(text + "an edit\n"))
	link := b.symlink("t")
	before := b.tree(fileEntry("old.txt", old), encoding.TreeEntry{Name: "l1", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: link}})
	after := b.tree(fileEntry("new.txt", renamed), encoding.TreeEntry{Name: "l2", Value: encoding.TreeValue{Kind: encoding.TreeValueSymlink, ID: link}})
	c1 := b.commit(before, "before\n", encoding.RootCommitID)
	c2 := b.commit(after, "after\n", c1)
	mustPublish(t, s, repo, "alice", b, nil)

	got, err := s.GetCopyRecords(ctx, repo, nil, c1, c2)
	if err != nil {
		t.Fatal(err)
	}
	want := []CopyRecord{{Target: "new.txt", TargetCommit: c2, Source: "old.txt", SourceFile: old, SourceCommit: c1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("copy records = %+v, want %+v (symlink renames are not records)", got, want)
	}
	if got, err := s.GetCopyRecords(ctx, repo, []string{"other"}, c1, c2); err != nil || len(got) != 0 {
		t.Fatalf("filtered to other paths: %+v, %v", got, err)
	}
	if got, err := s.GetCopyRecords(ctx, repo, nil, encoding.RootCommitID, c2); err != nil || len(got) != 0 {
		t.Fatalf("from the root: %+v, %v", got, err)
	}
}

// A read whose pack was retired between locating and fetching its chunks re-reads the
// locations once.
func TestReadFilesAfterRepack(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	contents := bytesOf(100_000, 4)
	f := b.file(contents)
	mustPublish(t, s, repo, "alice", b, nil)

	var oldPack []byte
	dbOf(s).scan(t, `SELECT id FROM pack WHERE network_id = @net`, map[string]any{"net": int64(netOf(t, s, repo))}, &oldPack)
	repack := func() {
		// Write every chunk of the file into a new pack, move them, delete the old one.
		spool, err := os.CreateTemp("", "repack-*")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(spool.Name())
		w := pack.NewWriter(spool)
		locs := map[encoding.ID]pack.Location{}
		// Reversed, so the new pack's bytes (and ID) differ from the old one's.
		for off := (len(contents) - 1) / testChunk * testChunk; off >= 0; off -= testChunk {
			c := contents[off:min(off+testChunk, len(contents))]
			if locs[pack.ChunkID(c)], err = w.Add(pack.ChunkID(c), c); err != nil {
				t.Fatal(err)
			}
		}
		id, size, err := w.Close()
		if err != nil {
			t.Fatal(err)
		}
		spool.Seek(0, 0)
		if err := s.blobs.Put(ctx, packKey(netOf(t, s, repo), id), spool, size); err != nil {
			t.Fatal(err)
		}
		// As compaction moves chunks.
		if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			num, err := tx.InsertPack(ctx, netOf(t, s, repo), id, size, nil)
			if err != nil {
				return err
			}
			for cid, loc := range locs {
				s.logIndex(tx, backend.NetworkScope(netOf(t, s, repo)), contentindex.Update{Op: contentindex.Move, ID: cid, Entry: contentindex.Entry{Kind: encoding.KindChunk,
					Loc: contentindex.Location{Pack: num, Offset: loc.Offset, Stored: loc.StoredLength, Raw: loc.RawLength, Codec: uint8(loc.Codec)}}})
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		old, err := toID(oldPack)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.blobs.Delete(ctx, packKey(netOf(t, s, repo), old)); err != nil {
			t.Fatal(err)
		}
	}
	var once bool
	s.afterLocate = func() {
		if !once {
			once = true
			repack()
		}
	}
	defer func() { s.afterLocate = nil }()
	var got []byte
	if err := s.ReadFiles(ctx, repo, []FileRange{{FileID: f}}, RawChunks, func(fr FileFrame) error {
		got = append(got, fr.Data...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, contents) {
		t.Fatal("contents differ after the re-read")
	}
}
