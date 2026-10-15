package store

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
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
	exec := func(sql string, args ...any) {
		t.Helper()
		tag, err := s.pool.Exec(ctx, sql, args...)
		if err != nil {
			t.Fatal(err)
		}
		if tag.RowsAffected() != 1 {
			t.Fatalf("%s: %d rows", sql, tag.RowsAffected())
		}
	}

	r := fsck(false)
	if len(r.Problems) != 0 || len(r.OrphanPacks) != 0 || r.DeadBytes != 0 {
		t.Fatalf("fresh repository: %s", r)
	}
	if r.Packs != 1 || r.Chunks != 5 || r.Files != 2 || r.Objects != 2 || r.Commits != 2 || r.Operations != 1 || r.Views != 1 {
		t.Fatalf("fresh repository counts: %+v", r)
	}
	var packB []byte
	if err := s.pool.QueryRow(ctx, `SELECT id FROM pack WHERE repository_id = $1`, repo).Scan(&packB); err != nil {
		t.Fatal(err)
	}
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
		wantProblem(r, lost.String(), "not in the index")
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
		key := packKey(repo, encoding.ID{0xee})
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
		other, err := encoding.Encode(&encoding.Tree{Entries: []encoding.TreeEntry{fileEntry("z", small)}})
		if err != nil {
			t.Fatal(err)
		}
		exec(`UPDATE object SET body = $3 WHERE repository_id = $1 AND id = $2`, repo, root[:], other)
		wantProblem(fsck(false), root.String())
		exec(`UPDATE object SET body = $3 WHERE repository_id = $1 AND id = $2`, repo, root[:], b.body(ObjectRef{Kind: encoding.KindTree, ID: root}))
		requireClean("after restoring the tree")
	})

	t.Run("parent index", func(t *testing.T) {
		exec(`DELETE FROM commit_parent WHERE repository_id = $1 AND commit_id = $2`, repo, c2[:])
		wantProblem(fsck(false), c2.String())
		exec(`INSERT INTO commit_parent (repository_id, commit_id, ord, parent_id) VALUES ($1, $2, 0, $3)`, repo, c2[:], c1[:])
		requireClean("after restoring the edge")
	})

	t.Run("pack bytes", func(t *testing.T) {
		key := packKey(repo, packID)
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
