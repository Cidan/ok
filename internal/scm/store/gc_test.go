package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
)

// age makes a repository's unpinned objects, and its operations and views, days old, as
// if nothing had touched them since.
func age(t *testing.T, s *Store, repo int64, days int) {
	t.Helper()
	ctx := context.Background()
	for _, tbl := range []string{"commit", "object"} {
		if _, err := poolOf(s).Exec(ctx, `UPDATE `+tbl+` SET touched_at = current_date - $2::int WHERE repository_id = $1 AND NOT pinned`, repo, days); err != nil {
			t.Fatal(err)
		}
	}
	for _, tbl := range []string{"operation", "view"} {
		if _, err := poolOf(s).Exec(ctx, `UPDATE `+tbl+` x SET touched_at = now() - make_interval(days => $2::int)
			FROM op_log l WHERE l.id = x.op_log_id AND l.repository_id = $1`, repo, days); err != nil {
			t.Fatal(err)
		}
	}
	ageIndex(t, s, repo, days)
}

// ageIndex makes a repository's unpinned files and chunks days old in its content index.
func ageIndex(t *testing.T, s *Store, repo int64, days int) {
	t.Helper()
	var today time.Time
	if err := poolOf(s).QueryRow(context.Background(), `SELECT current_date`).Scan(&today); err != nil {
		t.Fatal(err)
	}
	old := contentindex.DayOf(today) - contentindex.Day(days)
	rewriteIndex(t, s, repo, func(_ encoding.ID, e *contentindex.Entry) {
		if !e.Pinned {
			e.Day = old
		}
	})
}

// rewriteIndex writes a repository's content index as a new generation, each entry as f
// changes it, as a test that changes the index by hand needs.
func rewriteIndex(t *testing.T, s *Store, repo int64, f func(encoding.ID, *contentindex.Entry)) {
	t.Helper()
	ctx := context.Background()
	x := indexOf(t, s, repo)
	var entries []contentindex.Keyed
	for _, k := range x.Sorted() {
		if !k.Entry.Body {
			continue
		}
		f(k.ID, &k.Entry)
		entries = append(entries, k)
	}
	if err := s.inTx(ctx, func(tx *txn) error {
		gen, err := tx.LockContentIndex(ctx, repo)
		if err != nil {
			return err
		}
		if err := tx.DeleteIndexBlocks(ctx, repo, gen); err != nil {
			return err
		}
		logs, err := tx.ContentLogIDs(ctx, repo)
		if err != nil {
			return err
		}
		if err := tx.DeleteContentLogs(ctx, repo, logs); err != nil {
			return err
		}
		if err := tx.SetContentIndexGeneration(ctx, repo, gen+1); err != nil {
			return err
		}
		return writeBlocks(ctx, tx, repo, gen+1, entries)
	}); err != nil {
		t.Fatal(err)
	}
	s.indexChanged(repo)
}

// indexOf reads a repository's content index afresh.
func indexOf(t *testing.T, s *Store, repo int64) *contentindex.Index {
	t.Helper()
	x, found, err := s.LoadContentIndex(context.Background(), repo)
	if err != nil || !found {
		t.Fatalf("repository %d's content index: %v %v", repo, found, err)
	}
	return x
}

// dropFromIndex drops a file or chunk from a repository's content index, as a sweep does.
func dropFromIndex(t *testing.T, s *Store, repo int64, kind encoding.Kind, id encoding.ID) {
	t.Helper()
	if err := s.inTx(context.Background(), func(tx *txn) error {
		s.logIndex(tx, repo, contentindex.Update{Op: contentindex.Drop, ID: id, Entry: contentindex.Entry{Kind: kind}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// has reports whether a table holds a row for id in repo — for "file" and "chunk", whether
// the content index holds it.
func has(t *testing.T, s *Store, repo int64, tbl string, id encoding.ID) bool {
	t.Helper()
	if tbl == "file" || tbl == "chunk" {
		kind := encoding.KindChunk
		if tbl == "file" {
			kind = encoding.KindFile
		}
		_, entries, err := s.contentEntries(context.Background(), repo, kind, []encoding.ID{id})
		if err != nil {
			t.Fatal(err)
		}
		_, ok := entries[id]
		return ok
	}
	var ok bool
	if err := poolOf(s).QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM `+tbl+` WHERE repository_id = $1 AND id = $2)`, repo, id[:]).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

// dropChunk deletes a chunk as a sweep does — its pack's live bytes too — whatever its
// freshness, to put a publish where a sweep that raced it would.
func dropChunk(t *testing.T, s *Store, repo int64, id encoding.ID) {
	t.Helper()
	if n, _, err := s.sweepBatch(context.Background(), repo, tChunk, []encoding.ID{id}, time.Now().AddDate(1, 0, 0), false); err != nil || n != 1 {
		t.Fatalf("dropping chunk %s: %d, %v", id, n, err)
	}
}

func chunkIDs(contents []byte) []encoding.ID {
	var out []encoding.ID
	for off := 0; off < len(contents); off += testChunk {
		out = append(out, pack.ChunkID(contents[off:min(off+testChunk, len(contents))]))
	}
	return out
}

// clean requires fsck to find nothing wrong in repo.
func clean(t *testing.T, s *Store, repo int64) *FsckReport {
	t.Helper()
	r, err := s.Fsck(context.Background(), repo, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Problems) > 0 {
		t.Fatalf("fsck:\n%s", r)
	}
	return r
}

func readAll(t *testing.T, s *Store, repo int64, f encoding.ID) []byte {
	t.Helper()
	var got []byte
	if err := s.ReadFiles(context.Background(), repo, []FileRange{{FileID: f}}, RawChunks, func(fr FileFrame) error {
		got = append(got, fr.Data...)
		return nil
	}); err != nil {
		t.Fatalf("read file %s: %v", f, err)
	}
	return got
}

// liveBytes is the stored bytes of a repository's chunks, as its content index has them.
func liveBytes(t *testing.T, s *Store, repo int64) int64 {
	t.Helper()
	var n int64
	for _, k := range indexOf(t, s, repo).Sorted() {
		if k.Entry.Kind == encoding.KindChunk {
			n += int64(k.Entry.Loc.Stored)
		}
	}
	return n
}

func TestGCSweepsWhatNothingReaches(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)

	// Reachable: a commit in alice's operation log.
	b := newBuilder(t)
	keepContents := bytesOf(200_000, 1)
	keepFile := b.file(keepContents)
	keepTree := b.tree(fileEntry("keep", keepFile))
	keep := b.commit(keepTree, "kept", encoding.RootCommitID)
	op := b.operation(b.view(keep), "snapshot", encoding.RootOperationID)
	mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op})

	// Unreachable: a commit nothing references.
	g := newBuilder(t)
	goneContents := bytesOf(150_000, 2)
	goneFile := g.file(goneContents)
	goneTree := g.tree(fileEntry("gone", goneFile), fileEntry("also", g.file([]byte("small"))))
	gone := g.commit(goneTree, "garbage", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, g, nil)

	// Pinned: a landed CL. A candidate for another CL is a root.
	landed := e.commit(alice, newChange(), encoding.RootCommitID, "landed", map[string]string{"a": "landed"})
	n := e.open(landed)
	e.mustLand(LandItem{Number: n, Patchset: 1})
	other := e.commit(alice, newChange(), encoding.RootCommitID, "other", map[string]string{"b": "candidate"})
	m := e.open(other)
	cand, err := e.s.PrepareCandidate(ctx, e.repo, m, 1, e.head(), alice)
	if err != nil {
		t.Fatal(err)
	}
	if !has(t, e.s, e.repo, "commit", cand.Commit) || cand.Commit == other {
		t.Fatal("the candidate is not a spliced commit of its own")
	}

	// Both trees' sizes are kept: the swept tree's go with it.
	if _, err := e.s.TreeSizes(ctx, e.repo, []encoding.ID{keepTree, goneTree}); err != nil {
		t.Fatal(err)
	}
	keptSizes := func(tree encoding.ID) bool {
		t.Helper()
		var n int
		if err := poolOf(e.s).QueryRow(ctx, `SELECT count(*) FROM tree_sizes WHERE repository_id = $1 AND tree_id = $2`, e.repo, tree[:]).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}

	before := liveBytes(t, e.s, e.repo)
	age(t, e.s, e.repo, 20)

	dry, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if dry.CommitsSwept != 1 || dry.ObjectsSwept != 1 || dry.FilesSwept != 2 || dry.ChunksSwept != 4 {
		t.Fatalf("dry run: %s", dry)
	}
	if !has(t, e.s, e.repo, "commit", gone) || !has(t, e.s, e.repo, "chunk", chunkIDs(goneContents)[0]) {
		t.Fatal("a dry run deleted something")
	}

	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.CommitsSwept != dry.CommitsSwept || r.ObjectsSwept != dry.ObjectsSwept || r.FilesSwept != dry.FilesSwept ||
		r.ChunksSwept != dry.ChunksSwept || r.ChunkBytesSwept != dry.ChunkBytesSwept || r.ChunkBytesSwept == 0 {
		t.Fatalf("pass %s, dry run %s", r, dry)
	}
	for _, x := range []struct {
		tbl  string
		id   encoding.ID
		want bool
	}{
		{"commit", gone, false}, {"object", goneTree, false}, {"file", goneFile, false},
		{"commit", keep, true}, {"object", keepTree, true}, {"file", keepFile, true},
		{"commit", landed, true}, {"commit", other, true}, {"commit", cand.Commit, true},
	} {
		if got := has(t, e.s, e.repo, x.tbl, x.id); got != x.want {
			t.Errorf("%s %s present %v, want %v", x.tbl, x.id, got, x.want)
		}
	}
	for _, c := range chunkIDs(goneContents) {
		if has(t, e.s, e.repo, "chunk", c) {
			t.Errorf("chunk %s of the unreachable file survived", c)
		}
	}
	if keptSizes(goneTree) || !keptSizes(keepTree) {
		t.Errorf("tree sizes kept: swept tree %v, kept tree %v; want false, true", keptSizes(goneTree), keptSizes(keepTree))
	}
	if r.Marked == 0 {
		t.Error("the mark kept nothing, though the kept commit is stale and unpinned")
	}
	if !bytes.Equal(readAll(t, e.s, e.repo, keepFile), keepContents) {
		t.Fatal("the kept file reads back wrong")
	}
	if got := liveBytes(t, e.s, e.repo); got >= before {
		t.Errorf("live bytes %d, were %d before the sweep", got, before)
	}
	clean(t, e.s, e.repo)

	// A second pass has nothing left to sweep.
	again, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil || again.CommitsSwept+again.ObjectsSwept+again.FilesSwept+again.ChunksSwept != 0 {
		t.Fatalf("second pass: %s, %v", again, err)
	}
}

func TestGCKeepsWhatIsFresh(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	g := newBuilder(t)
	gone := g.commit(g.tree(fileEntry("f", g.file([]byte("fresh")))), "unreachable, but fresh", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, g, nil)
	for _, days := range []int{0, 14, 15} {
		age(t, e.s, e.repo, days)
		r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if r.CommitsSwept != 0 || !has(t, e.s, e.repo, "commit", gone) {
			t.Fatalf("an unreachable commit %d days old was swept: %s", days, r)
		}
	}
	age(t, e.s, e.repo, 16)
	if r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{}); err != nil || r.CommitsSwept != 1 {
		t.Fatalf("16 days old: %s, %v", r, err)
	}
}

// Collection runs first, so what only collected operations reached goes in the same pass.
func TestGCCollectsOperationsThenSweeps(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	b := newBuilder(t)
	abandoned := b.commit(b.tree(fileEntry("f", b.file([]byte("abandoned work")))), "abandoned", encoding.RootCommitID)
	v1 := b.view(abandoned)
	op1 := b.operation(v1, "one", encoding.RootOperationID)
	op2 := b.operation(v1, "two", op1)
	mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op2})
	// jj op abandon: a rewritten head that does not descend from the old one.
	b2 := newBuilder(t)
	kept := b2.commit(b2.tree(fileEntry("g", b2.file([]byte("kept work")))), "kept", encoding.RootCommitID)
	op3 := b2.operation(b2.view(kept), "rewritten", encoding.RootOperationID)
	mustPublish(t, e.s, e.repo, alice, b2, &OpHeadsUpdate{Old: []encoding.ID{op2}, New: op3})
	age(t, e.s, e.repo, 20)

	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.OperationsDeleted != 2 || r.ViewsDeleted != 1 {
		t.Fatalf("collection: %s", r)
	}
	if has(t, e.s, e.repo, "commit", abandoned) {
		t.Error("a commit only collected operations reached survived")
	}
	if !has(t, e.s, e.repo, "commit", kept) {
		t.Error("the current operation's commit was swept")
	}
	heads, err := e.s.GetOpHeads(ctx, e.repo, alice, testWorkspace)
	if err != nil || len(heads) != 1 || heads[0] != op3 {
		t.Fatalf("heads after collection: %v, %v", heads, err)
	}
	clean(t, e.s, e.repo)
}

// A publish that reuses a stale, unreachable subgraph keeps it: the publish touches it, and
// the sweep checks freshness again under each row's lock.
func TestGCReusedSubgraphSurvives(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	g := newBuilder(t)
	contents := bytesOf(150_000, 3)
	file := g.file(contents)
	tree := g.tree(fileEntry("reused", file))
	mustPublish(t, e.s, e.repo, alice, g, nil)
	age(t, e.s, e.repo, 20)

	// A new commit over the old tree, and a new file reusing one of the old file's chunks.
	b := newBuilder(t)
	b.files[file] = contents
	head := append(append([]byte(nil), contents[:testChunk]...), []byte("a different tail")...)
	newFile := b.file(head)
	c := b.commit(b.tree(treeEntry("old", tree), fileEntry("new", newFile)), "reuses the old tree", encoding.RootCommitID)
	b.tree(fileEntry("reused", file))
	op := b.operation(b.view(c), "snapshot", encoding.RootOperationID)
	mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op})

	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.CommitsSwept+r.ObjectsSwept+r.FilesSwept+r.ChunksSwept != 0 {
		t.Fatalf("swept what a publish reused: %s", r)
	}
	if !bytes.Equal(readAll(t, e.s, e.repo, file), contents) || !bytes.Equal(readAll(t, e.s, e.repo, newFile), head) {
		t.Fatal("reused contents read back wrong")
	}
	clean(t, e.s, e.repo)
}

// A new file must never point at a chunk a sweep took: a chunk that vanishes between the
// exchange and the commit refuses the publish, and a retry sends it.
func TestPublishNeverCommitsAVanishedChunk(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	g := newBuilder(t)
	old := bytesOf(100_000, 4)
	g.file(old)
	g.tree(fileEntry("old", g.file(old)))
	mustPublish(t, e.s, e.repo, alice, g, nil)
	shared := chunkIDs(old)[0]

	b := newBuilder(t)
	reuse := append(append([]byte(nil), old[:testChunk]...), []byte("new tail")...)
	newFile := b.file(reuse)
	b.tree(fileEntry("new", newFile))
	e.s.beforeCommit = func() {
		// A sweep that passed its checks just before the exchange touched the chunk.
		dropChunk(t, e.s, e.repo, shared)
	}
	_, err := publish(ctx, e.s, e.repo, alice, b, nil)
	e.s.beforeCommit = nil
	var rej *Rejection
	if !errors.As(err, &rej) || len(rej.Missing) != 1 || rej.Missing[0] != (ObjectRef{Kind: encoding.KindChunk, ID: shared}) {
		t.Fatalf("a publish over a vanished chunk: %v", err)
	}
	if has(t, e.s, e.repo, "file", newFile) {
		t.Fatal("the refused publish stored its file")
	}
	// The retry's exchange asks for the chunk, and the file is whole.
	mustPublish(t, e.s, e.repo, alice, b, nil)
	if !bytes.Equal(readAll(t, e.s, e.repo, newFile), reuse) {
		t.Fatal("the file reads back wrong")
	}
}

// A touch never commits a fresh object over a missing one: an exchange that finds a held
// object whose subgraph lost a row leaves it as stale as it was, and asks for it again.
func TestExchangeNeverFreshensOverAMissingObject(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	g := newBuilder(t)
	contents := bytesOf(90_000, 5)
	file := g.file(contents)
	tree := g.tree(fileEntry("f", file))
	mustPublish(t, e.s, e.repo, alice, g, nil)
	age(t, e.s, e.repo, 20)
	lost := chunkIDs(contents)[1]
	dropChunk(t, e.s, e.repo, lost)

	p, err := e.s.BeginPublish(ctx, e.repo, alice, testWorkspace, g.order, g.manifests())
	if err != nil {
		t.Fatal(err)
	}
	objs, chunks := p.Missing()
	p.Close()
	if len(objs) != 1 || objs[0] != (ObjectRef{Kind: encoding.KindTree, ID: tree}) || len(chunks) != 1 || chunks[0] != lost {
		t.Fatalf("missing %v, chunks %v", objs, chunks)
	}
	var fresh bool
	if err := poolOf(e.s).QueryRow(ctx, `SELECT touched_at >= current_date FROM object WHERE repository_id = $1 AND id = $2`, e.repo, tree[:]).Scan(&fresh); err != nil || fresh {
		t.Fatalf("the tree over the missing chunk was freshened (%v)", err)
	}
	mustPublish(t, e.s, e.repo, alice, g, nil)
	if !bytes.Equal(readAll(t, e.s, e.repo, file), contents) {
		t.Fatal("the repaired file reads back wrong")
	}
	clean(t, e.s, e.repo)
}

// GC races publishes that keep re-using stale, unreachable subgraphs: whatever the
// interleaving, a publish that commits references only objects that exist, and every
// object it reaches reads back. Each interleaving that matters is made, not hoped for: in a
// third of the rounds the sweep runs before the publish asks what is missing; in a third, between
// the publish's asking and its commit — the race the commit's check is for; and in the rest the
// two run at once.
func TestGCRacesPublishes(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	const rounds = 30
	var heldFirst, sweptFirst int
	for round := range rounds {
		mode := round % 3 // 0: the sweep first; 1: the sweep between Missing and Commit; 2: at once
		g := newBuilder(t)
		contents := bytesOf(120_000+round, byte(10+round))
		file := g.file(contents)
		tree := g.tree(fileEntry("f", file))
		mustPublish(t, e.s, e.repo, alice, g, nil)
		age(t, e.s, e.repo, 20)

		// A new commit over the old tree, published as the client does: the exchange,
		// then whatever it asks for, then the commit (retried once if refused).
		b := newBuilder(t)
		b.files[file] = contents
		b.tree(fileEntry("f", file))
		c := b.commit(b.tree(treeEntry("old", tree)), fmt.Sprintf("round %d", round), encoding.RootCommitID)
		op := b.operation(b.view(c), fmt.Sprintf("round %d", round), encoding.RootOperationID)

		var wg sync.WaitGroup
		var pubErr, gcErr error
		var treeWasHeld bool
		sweep := func() { _, gcErr = e.s.GC(ctx, e.repo, time.Now(), GCOptions{}) }
		if mode == 0 {
			sweep()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if mode == 2 {
				time.Sleep(time.Duration(round%6) * time.Millisecond)
			}
			for attempt := 0; attempt < 2; attempt++ {
				p, err := e.s.BeginPublish(ctx, e.repo, alice, testWorkspace, b.order, b.manifests())
				if err != nil {
					pubErr = err
					return
				}
				objs, chunks := p.Missing()
				if attempt == 0 {
					treeWasHeld = true
					for _, r := range objs {
						if r.ID == tree {
							treeWasHeld = false
						}
					}
					if mode == 1 {
						sweep()
					}
				}
				for _, r := range objs {
					if err := p.AddObject(EncodedObject{Kind: r.Kind, ID: r.ID, Body: b.body(r)}); err != nil {
						p.Close()
						pubErr = err
						return
					}
				}
				for _, id := range chunks {
					if err := p.AddChunk(id, b.chunk(id)); err != nil {
						p.Close()
						pubErr = err
						return
					}
				}
				_, pubErr = p.Commit(ctx, &OpHeadsUpdate{New: op})
				var rej *Rejection
				if !errors.As(pubErr, &rej) {
					return
				}
			}
		}()
		if mode == 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Duration((round/6)%6) * time.Millisecond)
				sweep()
			}()
		}
		wg.Wait()
		if pubErr != nil || gcErr != nil {
			t.Fatalf("round %d: publish %v, gc %v", round, pubErr, gcErr)
		}
		if treeWasHeld {
			heldFirst++
		} else {
			sweptFirst++
		}
		if !has(t, e.s, e.repo, "commit", c) || !has(t, e.s, e.repo, "object", tree) {
			t.Fatalf("round %d: the published commit lost its graph", round)
		}
		if !bytes.Equal(readAll(t, e.s, e.repo, file), contents) {
			t.Fatalf("round %d: the reused file reads back wrong", round)
		}
	}
	t.Logf("%d rounds: the publish found the old tree first in %d, the sweep took it first in %d", rounds, heldFirst, sweptFirst)
	if heldFirst < rounds/3 || sweptFirst < rounds/3 {
		t.Errorf("the made orders did not happen: the publish found the old tree first in %d of %d, the sweep took it first in %d", heldFirst, rounds, sweptFirst)
	}
	clean(t, e.s, e.repo)
}

// A landing or a candidate whose splice writes a tree an unreachable, stale one already
// is keeps it: the landing pins it, the candidate refreshes it.
func TestSpliceReusingGarbageKeepsIt(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	first := e.commit(alice, newChange(), encoding.RootCommitID, "first", map[string]string{"a": "1"})
	e.mustLand(LandItem{Number: e.open(first), Patchset: 1})
	main1 := e.head()
	// Two CLs on main1, touching different files.
	x := e.commit(alice, newChange(), main1, "x", map[string]string{"a": "1", "x": "x"})
	y := e.commit(alice, newChange(), main1, "y", map[string]string{"a": "1", "y": "y"})
	nx, ny := e.open(x), e.open(y)
	e.mustLand(LandItem{Number: nx, Patchset: 1})
	// The tree y lands as, on the new main, stored first as garbage.
	g := newBuilder(t)
	spliced := g.flat(map[string]string{"a": "1", "x": "x", "y": "y"})
	mustPublish(t, e.s, e.repo, bob, g, nil)
	age(t, e.s, e.repo, 20)

	cand, err := e.s.PrepareCandidate(ctx, e.repo, ny, 1, e.head(), alice)
	if err != nil {
		t.Fatal(err)
	}
	var fresh, pinned bool
	if err := poolOf(e.s).QueryRow(ctx, `SELECT touched_at >= current_date, pinned FROM object WHERE repository_id = $1 AND id = $2`, e.repo, spliced[:]).Scan(&fresh, &pinned); err != nil || !fresh {
		t.Fatalf("the candidate's reused tree was not refreshed (%v)", err)
	}
	if r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{}); err != nil || r.ObjectsSwept != 0 {
		t.Fatalf("GC beside a candidate: %s, %v", r, err)
	}
	landedAs := e.mustLand(LandItem{Number: ny, Patchset: 1, Candidate: &cand.Commit})
	if err := poolOf(e.s).QueryRow(ctx, `SELECT pinned FROM object WHERE repository_id = $1 AND id = $2`, e.repo, spliced[:]).Scan(&pinned); err != nil || !pinned {
		t.Fatalf("the landing did not pin its reused tree (%v)", err)
	}
	if landedAs[0].Commit != cand.Commit {
		t.Fatalf("landed as %s, the candidate was %s", landedAs[0].Commit, cand.Commit)
	}
	clean(t, e.s, e.repo)
}

func packsOf(t *testing.T, s *Store, repo int64) (live, retired int) {
	t.Helper()
	if err := poolOf(s).QueryRow(context.Background(), `SELECT count(*) FILTER (WHERE retired_at IS NULL), count(*) FILTER (WHERE retired_at IS NOT NULL)
		FROM pack WHERE repository_id = $1`, repo).Scan(&live, &retired); err != nil {
		t.Fatal(err)
	}
	return live, retired
}

func s3Bytes(t *testing.T, s *Store, repo int64) (int64, int) {
	t.Helper()
	objs, err := s.blobs.List(context.Background(), fmt.Sprintf("repos/%d/packs/", repo))
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, o := range objs {
		n += o.Size
	}
	return n, len(objs)
}

// Compaction merges small packs and drops dead chunks; retired packs leave S3 after their
// grace, and the bytes of what the sweep took are gone from S3.
func TestCompactionReclaimsBytes(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	contents := map[encoding.ID][]byte{}
	for i := range 6 {
		b := newBuilder(t)
		c := bytesOf(100_000, byte(40+i))
		f := b.file(c)
		contents[f] = c
		commit := b.commit(b.tree(treeEntry(fmt.Sprintf("dir%d", 5-i), b.tree(fileEntry("f", f)))), fmt.Sprintf("kept %d", i), encoding.RootCommitID)
		b.operation(b.view(commit), "snapshot", encoding.RootOperationID)
		mustPublish(t, e.s, e.repo, alice, b, nil)
	}
	// Make all six reachable from one operation.
	v := newBuilder(t)
	var heads []encoding.ID
	for f := range contents {
		heads = append(heads, v.commit(v.tree(fileEntry("f", f)), "head", encoding.RootCommitID))
	}
	// And all six on main, each at a path of its own, which orders the compacted pack.
	var entries []encoding.TreeEntry
	mainPath := map[encoding.ID]string{}
	n := 0
	for f := range contents {
		dir := fmt.Sprintf("d%d", 5-n)
		mainPath[f] = dir + "/f"
		entries = append(entries, treeEntry(dir, v.tree(fileEntry("f", f))))
		n++
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	onMain := v.commit(v.tree(entries...), "main", encoding.RootCommitID)
	heads = append(heads, onMain)
	op := v.operation(v.view(heads...), "all", encoding.RootOperationID)
	for f, c := range contents {
		v.files[f] = c
	}
	mustPublish(t, e.s, e.repo, alice, v, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op})
	if _, err := poolOf(e.s).Exec(ctx, `UPDATE repository SET head = $2 WHERE id = $1`, e.repo, onMain[:]); err != nil {
		t.Fatal(err)
	}
	// And one large unreachable file, whose pack the sweep leaves nearly empty.
	g := newBuilder(t)
	big := bytesOf(3_000_000, 99)
	g.commit(g.tree(fileEntry("big", g.file(big))), "garbage", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, g, nil)

	livePacks, _ := packsOf(t, e.s, e.repo)
	s3Before, _ := s3Bytes(t, e.s, e.repo)
	age(t, e.s, e.repo, 20)
	r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if r.ChunksSwept == 0 || r.PacksWritten != 1 || r.PacksRetired != livePacks {
		t.Fatalf("compaction of %d packs: %s", livePacks, r)
	}
	if lp, rp := packsOf(t, e.s, e.repo); lp != 1 || rp != livePacks {
		t.Fatalf("after compaction: %d live packs, %d retired", lp, rp)
	}
	for f, c := range contents {
		if !bytes.Equal(readAll(t, e.s, e.repo, f), c) {
			t.Fatalf("file %s reads back wrong after compaction", f)
		}
	}
	// The new pack holds the files' chunks in the order of their paths on main.
	x := indexOf(t, e.s, e.repo)
	type placed struct {
		path string
		off  int64
	}
	var files []placed
	for f, p := range mainPath {
		fe, _ := x.Get(f)
		first := int64(-1)
		for _, c := range fe.Chunks {
			if ce, ok := x.Get(c.ID); ok && (first < 0 || ce.Loc.Offset < first) {
				first = ce.Loc.Offset
			}
		}
		files = append(files, placed{p, first})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].off < files[j].off })
	var order []string
	for _, f := range files {
		order = append(order, f.path)
	}
	if len(order) != 6 || !sort.StringsAreSorted(order) {
		t.Errorf("files written in the order %v, not by their paths on main", order)
	}
	clean(t, e.s, e.repo)

	// Within the grace, retired packs stay; after it, they go.
	if n, err := e.s.DeleteRetiredPacks(ctx, time.Now()); err != nil || n != 0 {
		t.Fatalf("deleted %d retired packs within the grace (%v)", n, err)
	}
	if n, err := e.s.DeleteRetiredPacks(ctx, time.Now().Add(2*RetiredPackGrace)); err != nil || n != livePacks {
		t.Fatalf("deleted %d retired packs, want %d (%v)", n, livePacks, err)
	}
	s3After, objects := s3Bytes(t, e.s, e.repo)
	if objects != 1 || s3After >= s3Before-int64(len(big))/2 {
		t.Fatalf("S3 holds %d packs, %d bytes (was %d)", objects, s3After, s3Before)
	}
	for f, c := range contents {
		if !bytes.Equal(readAll(t, reopen(t, e.s), e.repo, f), c) {
			t.Fatalf("file %s reads back wrong with the old packs gone", f)
		}
	}
	clean(t, e.s, e.repo)
}

// A read that located its chunks in a pack compaction then retired still reads them.
func TestReadAcrossCompaction(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	var files []encoding.ID
	contents := map[encoding.ID][]byte{}
	for i := range 3 {
		b := newBuilder(t)
		c := bytesOf(80_000, byte(60+i))
		f := b.file(c)
		files = append(files, f)
		contents[f] = c
		commit := b.commit(b.tree(fileEntry("f", f)), "x", encoding.RootCommitID)
		b.operation(b.view(commit), "snapshot", encoding.RootOperationID)
		mustPublish(t, e.s, e.repo, alice, b, nil)
	}
	s := reopen(t, e.s)
	var once sync.Once
	s.afterLocate = func() {
		once.Do(func() {
			if r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{}); err != nil || r.PacksWritten != 1 {
				t.Errorf("compaction mid-read: %s, %v", r, err)
			}
			if _, err := e.s.DeleteRetiredPacks(ctx, time.Now().Add(2*RetiredPackGrace)); err != nil {
				t.Error(err)
			}
		})
	}
	for _, f := range files {
		if !bytes.Equal(readAll(t, s, e.repo, f), contents[f]) {
			t.Fatal("a read across compaction read the wrong bytes")
		}
	}
}

// Orphan packs — S3 objects no row names — go once no upload can still be committing them.
func TestOrphanPacks(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	var buf bytes.Buffer
	pw := pack.NewWriter(&buf)
	if _, err := pw.Add(pack.ChunkID([]byte("orphan")), []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	id, size, err := pw.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.s.blobs.Put(ctx, packKey(e.repo, id), &buf, size); err != nil {
		t.Fatal(err)
	}
	b := newBuilder(t)
	b.tree(fileEntry("f", b.file(bytesOf(10_000, 7))))
	mustPublish(t, e.s, e.repo, alice, b, nil)

	if r, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{}); err != nil || r.OrphanPacks != 0 {
		t.Fatalf("a fresh orphan: %s, %v", r, err)
	}
	later := time.Now().Add(OrphanPackGrace + time.Hour)
	if r, err := e.s.GC(ctx, e.repo, later, GCOptions{DryRun: true}); err != nil || r.OrphanPacks != 1 {
		t.Fatalf("dry run: %s, %v", r, err)
	}
	if _, n := s3Bytes(t, e.s, e.repo); n != 2 {
		t.Fatalf("a dry run deleted an orphan: %d packs", n)
	}
	if r, err := e.s.GC(ctx, e.repo, later, GCOptions{}); err != nil || r.OrphanPacks != 1 {
		t.Fatalf("an old orphan: %s, %v", r, err)
	}
	if _, n := s3Bytes(t, e.s, e.repo); n != 1 {
		t.Fatalf("%d packs left, want the published one", n)
	}
	clean(t, e.s, e.repo)
}

func TestGCScheduleAndLeader(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	other := newRepo(t, e.s).ID
	at := func(day, hour int) time.Time { return time.Date(2026, 9, day, hour, 0, 0, 0, time.UTC) }
	due := func(now time.Time) map[int64]bool {
		t.Helper()
		ids, err := e.s.DueForGC(ctx, now, 9)
		if err != nil {
			t.Fatal(err)
		}
		out := map[int64]bool{}
		for _, id := range ids {
			out[id] = true
		}
		return out
	}
	if d := due(at(27, 10)); !d[e.repo] || !d[other] {
		t.Fatalf("never collected: %v", d)
	}
	record := func(repo int64, started time.Time, err error) {
		t.Helper()
		if err := e.s.RecordGC(ctx, &GCReport{Repository: repo, Started: started, Finished: started.Add(time.Minute)}, err); err != nil {
			t.Fatal(err)
		}
	}
	record(e.repo, at(27, 9).Add(5*time.Minute), nil)
	record(other, at(27, 9).Add(5*time.Minute), errors.New("boom"))
	if d := due(at(27, 10).Add(10 * time.Minute)); d[e.repo] || !d[other] {
		t.Fatalf("after a pass, and a failure over an hour ago: %v", d)
	}
	if d := due(at(27, 9).Add(30 * time.Minute)); d[other] {
		t.Fatalf("a failure is retried within the hour: %v", d)
	}
	if d := due(at(28, 8)); d[e.repo] {
		t.Fatalf("due before the next scheduled hour: %v", d)
	}
	if d := due(at(28, 9)); !d[e.repo] {
		t.Fatalf("not due at the next scheduled hour: %v", d)
	}
	if err := e.s.RecordGC(ctx, &GCReport{Repository: e.repo, DryRun: true, Started: at(28, 9)}, nil); err != nil {
		t.Fatal(err)
	}
	if d := due(at(28, 10)); !d[e.repo] {
		t.Fatal("a dry run counted as a pass")
	}

	err := e.s.LeadGC(ctx, func(ctx context.Context) error {
		if err := e.s.LeadGC(ctx, func(context.Context) error { t.Error("a second leader"); return nil }); !errors.Is(err, lock.ErrNotAcquired) {
			t.Errorf("a second leader: %v, want lock.ErrNotAcquired", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("leader: %v", err)
	}
	led := false
	if err := e.s.LeadGC(ctx, func(context.Context) error { led = true; return nil }); err != nil || !led {
		t.Fatalf("leader after the first gave up: %v", err)
	}
}

// Compaction settles: a pack of one small chunk is overhead, not dead space, so it is
// merged with its peers, not rewritten alone, and a second pass has nothing to do.
func TestCompactionSettles(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	for i := range 2 {
		b := newBuilder(t)
		commit := b.commit(b.tree(fileEntry("f", b.file([]byte(fmt.Sprintf("tiny %d", i))))), "tiny", encoding.RootCommitID)
		op := b.operation(b.view(commit), "snapshot", encoding.RootOperationID)
		mustPublish(t, e.s, e.repo, alice, b, &OpHeadsUpdate{New: op})
	}
	first, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil || first.PacksWritten != 1 || first.PacksRetired != 2 {
		t.Fatalf("merging two tiny packs: %s, %v", first, err)
	}
	for range 2 {
		dry, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{DryRun: true})
		if err != nil || dry.PacksWritten != 0 || dry.PacksRetired != 0 {
			t.Fatalf("a lone tiny pack would be rewritten: %s, %v", dry, err)
		}
		again, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
		if err != nil || again.PacksWritten != 0 || again.PacksRetired != 0 {
			t.Fatalf("a settled repository compacted again: %s, %v", again, err)
		}
	}
	clean(t, e.s, e.repo)
}
