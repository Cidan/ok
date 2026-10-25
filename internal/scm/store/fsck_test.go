package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/testdb/testobjects"
)

func TestFsck(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	b := newBuilder(t)
	big := bytesOf(3*testChunk+17, 7)
	bigID := b.file(big)
	small := b.file([]byte("small\n"))
	cp := b.add(&encoding.CopyHistory{CurrentPath: "a.txt"})
	root := b.tree(encoding.TreeEntry{Name: "a.txt", Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: bigID, CopyID: &cp}},
		fileEntry("b.txt", small))
	c1 := b.commit(root, "one\n", encoding.RootCommitID)
	c2 := b.commit(root, "two\n", c1)
	o := b.operation(b.view(c2), "op", encoding.RootOperationID)
	mustPublish(t, s, repo, "alice", b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: o})

	fsck := func(repair bool) *FsckReport {
		t.Helper()
		r, err := s.Fsck(ctx, repo, repair)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	requireClean := func(when string) {
		t.Helper()
		r := fsck(false)
		if len(r.Problems) != 0 || len(r.OrphanPacks) != 0 || r.DeadBytes != 0 || len(r.Repaired) != 0 {
			t.Fatalf("%s: %s", when, r)
		}
	}
	wantProblem := func(r *FsckReport, parts ...string) {
		t.Helper()
		for _, p := range r.Problems {
			ok := true
			for _, part := range parts {
				ok = ok && strings.Contains(p, part)
			}
			if ok {
				return
			}
		}
		t.Fatalf("no problem mentions %q:\n%s", parts, r)
	}
	exec := func(sql string, args map[string]any) {
		t.Helper()
		if n := dbOf(s).exec(t, sql, args); n != 1 {
			t.Fatalf("%s: %d rows", sql, n)
		}
	}

	r := fsck(false)
	if len(r.Problems) != 0 || len(r.OrphanPacks) != 0 || r.DeadBytes != 0 {
		t.Fatalf("fresh repository: %s", r)
	}
	if r.Packs != 1 || r.Chunks != 5 || r.PackedTrees != 1 || r.Files != 2 || r.Objects != 1 || r.Commits != 2 || r.Operations != 1 || r.Views != 1 {
		t.Fatalf("fresh repository counts: %+v", r)
	}
	var packB []byte
	dbOf(s).scan(t, `SELECT id FROM pack WHERE network_id = @net`, map[string]any{"net": int64(netOf(t, s, repo))}, &packB)
	packID := encoding.ID(packB)
	var lost encoding.ID
	for _, m := range b.manifests() {
		if m.FileID == bigID {
			lost = m.Chunks[0].ID
		}
	}

	t.Run("missing chunk entry", func(t *testing.T) {
		dropFromIndex(t, s, repo, encoding.KindChunk, lost)
		r := fsck(false)
		wantProblem(r, lost.String(), "no location")
		r = fsck(true)
		if len(r.Problems) != 0 || len(r.Repaired) != 1 || !strings.Contains(r.Repaired[0], lost.String()) {
			t.Fatalf("repair: %s", r)
		}
		requireClean("after rebuilding the chunk entry")
		var got bytes.Buffer
		if err := reopen(t, s).ReadFiles(ctx, repo, []FileRange{{FileID: bigID, Length: uint64(len(big))}}, RawChunks, func(f FileFrame) error {
			got.Write(f.Data)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Bytes(), big) {
			t.Fatal("the file does not read back after repair")
		}
	})

	t.Run("orphan pack", func(t *testing.T) {
		key := packKey(netOf(t, s, repo), encoding.ID{0xee})
		if err := s.blobs.Put(ctx, key, strings.NewReader("x"), 1); err != nil {
			t.Fatal(err)
		}
		r := fsck(false)
		if len(r.Problems) != 0 || len(r.OrphanPacks) != 1 || r.OrphanPacks[0] != key {
			t.Fatalf("orphan: %s", r)
		}
		if err := s.blobs.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
		requireClean("after deleting the orphan")
	})

	t.Run("object body", func(t *testing.T) {
		other, err := encoding.Encode(&encoding.CopyHistory{CurrentPath: "z.txt"})
		if err != nil {
			t.Fatal(err)
		}
		exec(`UPDATE object SET body = @body WHERE repository_id = @repo AND id = @id`, map[string]any{"repo": repo, "id": cp[:], "body": other[1:]})
		wantProblem(fsck(false), cp.String())
		exec(`UPDATE object SET body = @body WHERE repository_id = @repo AND id = @id`,
			map[string]any{"repo": repo, "id": cp[:], "body": b.body(ObjectRef{Kind: encoding.KindCopyHistory, ID: cp})})
		requireClean("after restoring the copy history")
	})

	// A tree a publish stored is in its pack: one whose location the network lost is rebuilt from
	// the pack's record of it, which says it is a tree's.
	t.Run("missing tree location", func(t *testing.T) {
		if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			s.logIndex(tx, backend.NetworkScope(netOf(t, s, repo)), contentindex.Update{Op: contentindex.Drop, ID: root,
				Entry: contentindex.Entry{Kind: encoding.KindTree}})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		wantProblem(fsck(false), root.String(), "no location")
		r := fsck(true)
		if len(r.Problems) != 0 || len(r.Repaired) != 1 || !strings.Contains(r.Repaired[0], "tree "+root.String()) {
			t.Fatalf("repair: %s", r)
		}
		requireClean("after rebuilding the tree's location")
		if res, err := reopen(t, s).ReadObjects(ctx, repo, []ObjectRef{{Kind: encoding.KindTree, ID: root}}); err != nil || !res[0].Found {
			t.Fatalf("the tree after repair: %+v, %v", res, err)
		}
	})

	t.Run("parent index", func(t *testing.T) {
		exec(`DELETE FROM commit_parent WHERE repository_id = @repo AND commit_id = @commit`, map[string]any{"repo": repo, "commit": c2[:]})
		wantProblem(fsck(false), c2.String())
		exec(`INSERT INTO commit_parent (repository_id, commit_id, ord, parent_id) VALUES (@repo, @commit, 0, @parent)`,
			map[string]any{"repo": repo, "commit": c2[:], "parent": c1[:]})
		requireClean("after restoring the edge")
	})

	t.Run("pack bytes", func(t *testing.T) {
		key := packKey(netOf(t, s, repo), packID)
		var orig bytes.Buffer
		if _, err := s.blobs.Download(ctx, key, &orig); err != nil {
			t.Fatal(err)
		}
		bad := bytes.Clone(orig.Bytes())
		bad[len(bad)/2] ^= 0x01
		if err := s.blobs.Put(ctx, key, bytes.NewReader(bad), int64(len(bad))); err != nil {
			t.Fatal(err)
		}
		wantProblem(fsck(false), "pack "+packID.String())
		if err := s.blobs.Put(ctx, key, bytes.NewReader(orig.Bytes()), int64(orig.Len())); err != nil {
			t.Fatal(err)
		}
		requireClean("after restoring the pack")
	})

	if _, err := s.Fsck(ctx, 1<<62, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("fsck of a missing repository: %v", err)
	}
}

// fsck verifies a repository's files from its packs as it downloads each, once: however many files,
// it reads from the bucket only the chunks a file has in another pack than its first chunk's.
func TestFsckReadsEachPackOnce(t *testing.T) {
	ctx := context.Background()
	base := testStore(t)
	slow := testobjects.NewSlow(base.blobs, 0)
	s := mustOpen(t, dbOf(base), slow, Config{Signals: base.signals})
	repo := newRepo(t, s).ID
	// Two publishes, two packs of 300 files each; the second's last file begins with a chunk the
	// first stored, so its chunks are in both.
	shared := bytesOf(testChunk, 3)
	for p := range 2 {
		b := newBuilder(t)
		var entries []encoding.TreeEntry
		for i := range 300 {
			entries = append(entries, fileEntry(fmt.Sprintf("f%03d", i), b.file([]byte(fmt.Sprintf("pack %d file %d\n", p, i)))))
		}
		if p == 0 {
			entries = append(entries, fileEntry("shared", b.file(shared)))
		} else {
			entries = append(entries, fileEntry("both", b.file(append(bytes.Clone(shared), []byte("and more of its own\n")...))))
		}
		b.commit(b.tree(entries...), fmt.Sprint("pack ", p), encoding.RootCommitID)
		mustPublish(t, s, repo, alice, b, nil)
	}
	downloads, ranges := slow.Count("download"), slow.Count("get-range")
	r, err := s.Fsck(ctx, repo, false)
	if err != nil || len(r.Problems) != 0 {
		t.Fatalf("fsck: %v, %v", r, err)
	}
	if r.Files != 602 || r.Packs != 2 {
		t.Fatalf("fsck counted %d files in %d packs", r.Files, r.Packs)
	}
	if d, g := slow.Count("download")-downloads, slow.Count("get-range")-ranges; d != 2 || g > 2 {
		t.Errorf("fsck of 602 files in 2 packs downloaded %d times and read %d ranges", d, g)
	}
}
