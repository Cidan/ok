package store

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// uploadFile publishes a commit holding one file and uploads it, so the file and its chunks are
// pinned, in a pack of their own; it returns the file's ID.
func uploadFile(t *testing.T, e *clEnv, name string, contents []byte) encoding.ID {
	t.Helper()
	b := newBuilder(t)
	f := b.file(contents)
	c := b.commit(b.tree(fileEntry(name, f)), "upload "+name, encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	e.mustUpload(alice, Ready, c)
	return f
}

// chunkPack is the pack a replica with nothing cached finds a file's first chunk in.
func chunkPack(t *testing.T, e *clEnv, f encoding.ID) uint64 {
	t.Helper()
	ctx := context.Background()
	s := reopen(t, e.s)
	_, files, err := s.contentEntries(ctx, repoScope(e.repo), encoding.KindFile, []encoding.ID{f})
	if err != nil || len(files) != 1 {
		t.Fatalf("file %s: %v, %v", f, files, err)
	}
	c := files[f].Chunks[0].ID
	_, chunks, err := s.contentEntries(ctx, backend.NetworkScope(netOf(t, e.s, e.repo)), encoding.KindChunk, []encoding.ID{c})
	if err != nil || len(chunks) != 1 {
		t.Fatalf("chunk %s: %v, %v", c, chunks, err)
	}
	return chunks[c].Loc.Pack
}

// indexLogOf is a content index's log since its last fold, as a replica with nothing cached reads it.
func indexLogOf(t *testing.T, s *Store, sc backend.Scope) *contentindex.Log {
	t.Helper()
	fresh := reopen(t, s)
	_, v, err := fresh.contentIndex(context.Background(), sc)
	if err != nil {
		t.Fatal(err)
	}
	return v.log
}

// logMoves counts the entries a content index's log moved.
func logMoves(t *testing.T, s *Store, sc backend.Scope) int {
	t.Helper()
	l := indexLogOf(t, s, sc)
	n := 0
	for _, id := range l.IDs() {
		for _, u := range l.Updates(id) {
			if u.Op == contentindex.Move {
				n++
				break
			}
		}
	}
	return n
}

// A fold writes the content index's pinned entries into a pinned run and its unpinned ones into the
// unpinned index, leaving no log; every reader of the index — reads, publishing what is held, a pin
// walk through what moved, fsck — finds them, on a replica with nothing cached.
func TestContentIndexFold(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	big := bytesOf(300_000, 7)
	f := uploadFile(t, e, "big", big)
	ub := newBuilder(t)
	loose := ub.file([]byte("loose\n"))
	ub.commit(ub.tree(fileEntry("loose", loose)), "unpinned", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, ub, nil)

	gen, unpinned, pinned, err := e.s.RebuildContentIndex(ctx, e.repo)
	if err != nil || gen == 0 || pinned < 4 || unpinned != 3 {
		t.Fatalf("a fold: generation %d, %d unpinned and %d pinned records, %v; want the unpinned file, chunk, and tree, and the pinned file, its chunks, and its tree", gen, unpinned, pinned, err)
	}
	if n := indexLogOf(t, e.s, repoScope(e.repo)).Len(); n != 0 {
		t.Fatalf("after the fold the log holds %d entries", n)
	}
	fresh := reopen(t, e.s)
	if got := readAll(t, fresh, e.repo, f); !bytes.Equal(got, big) {
		t.Fatalf("the pinned file read %d bytes, want %d", len(got), len(big))
	}
	if got := readAll(t, fresh, e.repo, loose); string(got) != "loose\n" {
		t.Fatalf("the unpinned file read %q", got)
	}

	// Publishing the file again asks for nothing: the index holds it, in a segment.
	again := newBuilder(t)
	again.file(big)
	ex, err := fresh.BeginPublish(ctx, e.repo, alice, testWorkspace, nil, again.manifests())
	if err != nil {
		t.Fatal(err)
	}
	if _, chunks := ex.Missing(); len(chunks) != 0 {
		t.Errorf("publishing a folded file again asks for %d chunks", len(chunks))
	}
	ex.Close()

	// A commit reusing the folded file, uploaded: the pin walk reaches it in the segment, pinned.
	b := newBuilder(t)
	b.files[f] = big
	c := b.commit(b.tree(fileEntry("again", f)), "reuse", encoding.RootCommitID)
	mustPublish(t, fresh, e.repo, alice, b, nil)
	e.mustUpload(alice, Ready, c)
	if fr, err := fresh.Fsck(ctx, e.repo, false); err != nil || len(fr.Problems) != 0 {
		t.Fatalf("fsck after the fold: %v, %v", fr.Problems, err)
	}
}

// A pinned chunk or tree repacked after its location was folded into a run of its network's chunk
// index is read at its new location — from the log's move, then, folded again, from the newer run,
// and after the runs merge — even once its old pack is gone.
func TestRepackFoldedChunks(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	a := uploadFile(t, e, "a", []byte("first pinned file\n"))
	b := uploadFile(t, e, "b", []byte("second pinned file\n"))
	net := backend.NetworkScope(netOf(t, e.s, e.repo))
	trees := map[encoding.ID]encoding.ID{a: newBuilder(t).tree(fileEntry("a", a)), b: newBuilder(t).tree(fileEntry("b", b))}
	if _, _, folded, err := e.s.RebuildContentIndex(ctx, e.repo); err != nil || folded != 6 {
		t.Fatalf("folding two files, their chunks, and their trees: %d, %v", folded, err)
	}
	if _, _, folded, err := e.s.foldIndex(ctx, net); err != nil || folded != 4 {
		t.Fatalf("folding the two chunks' locations and the two trees': %d, %v", folded, err)
	}
	before := chunkPack(t, e, a)

	r := &GCReport{}
	if err := e.s.compact(ctx, e.repo, GCOptions{}, r); err != nil || r.PacksWritten != 1 || r.PacksRetired != 2 {
		t.Fatalf("repacking two small packs: %s, %v", r, err)
	}
	if m := logMoves(t, e.s, net); m != 4 {
		t.Fatalf("the chunk index's log moved %d folded chunks and trees, want 4", m)
	}
	if after := chunkPack(t, e, a); after == before {
		t.Fatalf("the repacked chunk is still read from pack %d", before)
	}
	check := func(stage string) {
		t.Helper()
		fresh := reopen(t, e.s)
		for f, want := range map[encoding.ID]string{a: "first pinned file\n", b: "second pinned file\n"} {
			if got := readAll(t, fresh, e.repo, f); string(got) != want {
				t.Fatalf("%s: read %q, want %q", stage, got, want)
			}
			res, err := fresh.ReadObjects(ctx, e.repo, []ObjectRef{{Kind: encoding.KindTree, ID: trees[f]}})
			if err != nil || !res[0].Found {
				t.Fatalf("%s: the tree of %q: %+v, %v", stage, want, res, err)
			}
		}
	}
	check("moved in the log")
	if n, _, err := e.s.DeleteRetiredOf(ctx, e.repo, time.Now().Add(2*RetiredPackGrace)); err != nil || n != 2 {
		t.Fatalf("deleting the two retired packs: %d, %v", n, err)
	}
	check("moved in the log, the old packs gone")

	if _, _, folded, err := e.s.foldIndex(ctx, net); err != nil || folded != 4 {
		t.Fatalf("folding the four moves: %d, %v", folded, err)
	}
	if m := logMoves(t, e.s, net); m != 0 {
		t.Fatalf("%d moves left in the chunk index's log after the fold", m)
	}
	check("the moves folded into a newer run")

	// Two more folds, of two more files: four index segments at level 0, merged into one, which keeps
	// the newer of the repacked chunks' records.
	for i := range 2 {
		uploadFile(t, e, fmt.Sprint("more", i), []byte(fmt.Sprintf("more %d\n", i)))
		if _, _, _, err := e.s.foldIndex(ctx, net); err != nil {
			t.Fatal(err)
		}
	}
	segs, err := e.s.ops(ctx).Segments(ctx, net, segIndex)
	if err != nil || len(segs) != 4 {
		t.Fatalf("index segments before the merge: %d, %v", len(segs), err)
	}
	newest := segs[0].Age
	for i := 1; i < len(segs); i++ {
		if segs[i].Age >= segs[i-1].Age {
			t.Fatalf("segments not newest first by age: %+v", segs)
		}
	}
	if err := e.s.mergeSegments(ctx, net, networkKinds, &SegmentReport{}); err != nil {
		t.Fatal(err)
	}
	merged, err := e.s.ops(ctx).Segments(ctx, net, segIndex)
	if err != nil || len(merged) != 1 || merged[0].Level != 1 || merged[0].Age != newest {
		t.Fatalf("after the merge: %+v, %v; want one segment at level 1 as new as the newest merged", merged, err)
	}
	check("the index segments merged")
	if fr, err := reopen(t, e.s).Fsck(ctx, e.repo, false); err != nil || len(fr.Problems) != 0 {
		t.Fatalf("fsck: %v, %v", fr.Problems, err)
	}
}

// The whole index walks in ID order — the log's entries and the runs' together — from the start or
// from any ID: what dictionary training samples from.
func TestContentViewInOrder(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	for i := range 6 {
		uploadFile(t, e, fmt.Sprint("small", i), bytes.Repeat([]byte(fmt.Sprintf("sample text %d\n", i)), 200))
	}
	if _, _, folded, err := e.s.RebuildContentIndex(ctx, e.repo); err != nil || folded == 0 {
		t.Fatalf("a fold: %d, %v", folded, err)
	}
	ub := newBuilder(t)
	var loose []encoding.TreeEntry
	for i := range 3 {
		loose = append(loose, fileEntry(fmt.Sprint("u", i), ub.file([]byte(fmt.Sprintf("unpinned %d\n", i)))))
	}
	ub.commit(ub.tree(loose...), "unpinned", encoding.RootCommitID)
	if _, err := publish(ctx, e.s, e.repo, alice, ub, nil); err != nil {
		t.Fatal(err)
	}
	v, err := reopen(t, e.s).contentView(ctx, repoScope(e.repo))
	if err != nil {
		t.Fatal(err)
	}
	walk := func(from encoding.ID) []encoding.ID {
		t.Helper()
		var ids []encoding.ID
		if err := v.ascend(ctx, from, func(id encoding.ID, ent contentindex.Entry) bool {
			if ent.Kind == encoding.KindFile {
				ids = append(ids, id)
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
		return ids
	}
	all := walk(encoding.ID{})
	if len(all) != 9 {
		t.Fatalf("the whole index holds %d files, want 6 folded and 3 in the log", len(all))
	}
	for i := 1; i < len(all); i++ {
		if bytes.Compare(all[i-1][:], all[i][:]) >= 0 {
			t.Fatalf("not in ID order at %d: %v", i, all)
		}
	}
	if from := walk(all[4]); len(from) != 5 || from[0] != all[4] {
		t.Fatalf("from the fifth: %d files, first %s", len(from), from[0])
	}
}

// The unpinned index merges a day group's two newest runs while the older is at most four times the
// newer, and once the group's day has ended, the whole group into one; a merge that takes the oldest
// run drops its tombstones, which hide nothing older; every lookup is the same throughout.
func TestUnpinnedMerges(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	sc := repoScope(newRepo(t, s).ID)
	var bulk []contentindex.Update
	for n := range 40 {
		bulk = append(bulk, putChunk(chunkID(uint64(n))))
	}
	logAs(t, s, sc, false, bulk...)
	fold := func() {
		t.Helper()
		if _, _, _, err := s.foldIndex(ctx, sc); err != nil {
			t.Fatal(err)
		}
	}
	fold()
	// Small folds after a large one: they merge among themselves, never into it.
	for n := range 6 {
		logAs(t, s, sc, false, putChunk(chunkID(uint64(100+n))))
		fold()
	}
	logAs(t, s, sc, true, contentindex.Update{Op: contentindex.Drop, ID: chunkID(3), Entry: contentindex.Entry{Kind: encoding.KindChunk}})
	fold()
	runs := runsOf(t, s, sc, segIndexUnpinned)
	if len(runs) < 2 || runs[len(runs)-1].Records != 40 {
		t.Fatalf("today's runs: %+v; want the large one alone, and the small ones merged beside it", runs)
	}
	if runs[1].Records <= unpinnedRatio*runs[0].Records {
		t.Fatalf("the two newest runs left unmerged though the older is at most %d times the newer: %+v", unpinnedRatio, runs)
	}
	held := func(r *Store) map[encoding.ID]contentindex.Entry {
		t.Helper()
		return readWhole(t, r, sc)
	}
	before := held(reopen(t, s))
	if len(before) != 45 {
		t.Fatalf("%d chunks held, want 45", len(before))
	}
	// Today's day has ended: its runs merge into one, the tombstone gone with the chunk it hid.
	today := contentindex.DayOf(dbOf(s).today(t))
	if err := s.mergeUnpinned(ctx, sc, today+1); err != nil {
		t.Fatal(err)
	}
	runs = runsOf(t, s, sc, segIndexUnpinned)
	if len(runs) != 1 || runs[0].Records != 45 || contentindex.Day(runs[0].Day) != today {
		t.Fatalf("after the day ended: %+v; want one run of today's, of the 45 chunks held and no tombstone", runs)
	}
	after := held(reopen(t, s))
	if len(after) != len(before) {
		t.Fatalf("%d chunks held after the merge, %d before", len(after), len(before))
	}
	for id, e := range before {
		if !after[id].Equal(e) {
			t.Fatalf("chunk %x: %+v after the merge, %+v before", id[24:], after[id], e)
		}
	}
}
