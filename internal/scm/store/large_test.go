package store

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/testdb/testobjects"
)

// largeCount is a count TestLargeRepository reads from the environment variable name, or def.
func largeCount(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("%s=%q is not a count", name, v)
	}
	return n
}

// largeFile is file i's contents at version v: size bytes, distinct for each.
func largeFile(i, v, size int) []byte {
	head := fmt.Sprintf("file %d version %d\n", i, v)
	if size <= len(head) {
		return []byte(head)
	}
	b := make([]byte, size)
	copy(b, head)
	r := rand.New(rand.NewPCG(uint64(i), uint64(v)))
	for j := len(head); j < size; j++ {
		b[j] = byte('a' + r.IntN(26))
	}
	return b
}

// largePath is file i's path: a hundred files to a directory, a hundred directories to a parent.
func largePath(i int) string {
	return fmt.Sprintf("d%03d/d%03d/f%05d.txt", i/10000, i/100%100, i)
}

// tableBytes is how many bytes a repository's rows take in each table that holds them — by
// repository_id; by owner_id, of its scope or its network's; or by network_id, its network's: a
// network of one is the repository's alone.
func tableBytes(t *testing.T, s *Store, repo int64) map[string]int64 {
	t.Helper()
	return dbOf(s).tableBytes(t, repo, int64(netOf(t, s, repo)))
}

// TestLargeRepository is phase 3's proof (docs/hosted.md, "The plan"): a repository of many files and
// a long history, landed CL by CL in stacks, then compacted and collected, keeps its immutable data
// out of the database — none of it left in the tail — reads back from a replica with nothing
// cached, and fsck verifies it. OK_TEST_LARGE_FILES, OK_TEST_LARGE_COMMITS, and OK_TEST_LARGE_BYTES
// (each file's size) size it: the plan's is 10^7 files and 10^6 commits, about 1 TB at 100 KB.
func TestLargeRepository(t *testing.T) {
	files := largeCount(t, "OK_TEST_LARGE_FILES", 3000)
	commits := largeCount(t, "OK_TEST_LARGE_COMMITS", 400)
	size := largeCount(t, "OK_TEST_LARGE_BYTES", 200)
	const stack, changed, publishBatch = 100, 3, 5000
	ctx := context.Background()
	defer func(was time.Duration) { eventsSettle = was }(eventsSettle)
	eventsSettle = 0 // every published event settled
	e := newCLEnv(t)
	start := time.Now()

	// The first commit holds every file, its objects published a batch of files at a time. Each
	// directory keeps its tree's ID, and a change recomputes only the trees above what it changed.
	version := make([]int, files)
	type dir struct {
		files map[string]encoding.ID
		subs  map[string]*dir
		tree  encoding.ID
		dirty bool
	}
	root := &dir{files: map[string]encoding.ID{}, subs: map[string]*dir{}}
	place := func(i int, id encoding.ID) {
		p := largePath(i)
		d := root
		d.dirty = true
		for _, part := range []string{p[:4], p[5:9]} {
			if d.subs[part] == nil {
				d.subs[part] = &dir{files: map[string]encoding.ID{}, subs: map[string]*dir{}}
			}
			d = d.subs[part]
			d.dirty = true
		}
		d.files[p[10:]] = id
	}
	// treeOf builds d's tree, its subtrees first, publishing them as jj does: in batches of about
	// 3 MiB (jj/store/publish.rs, BATCH_BYTES), a tree after the subtrees it names.
	treeBytes := 0
	var treeOf func(b *builder, d *dir) encoding.ID
	treeOf = func(b *builder, d *dir) encoding.ID {
		if !d.dirty {
			return d.tree
		}
		var entries []encoding.TreeEntry
		for name, id := range d.files {
			entries = append(entries, fileEntry(name, id))
		}
		for name, sub := range d.subs {
			entries = append(entries, treeEntry(name, treeOf(b, sub)))
		}
		d.tree, d.dirty = b.tree(entries...), false
		if treeBytes += len(b.body(ObjectRef{Kind: encoding.KindTree, ID: d.tree})); treeBytes >= 3<<20 {
			mustPublish(t, e.s, e.repo, alice, b, nil)
			*b, treeBytes = *newBuilder(t), 0
		}
		return d.tree
	}
	// commit adds a commit of a change of its own.
	seq := 0
	commit := func(b *builder, tree encoding.ID, desc string, parent encoding.ID) encoding.ID {
		seq++
		sig := encoding.Signature{Name: "t", Email: "t@example.com", Time: encoding.Timestamp{Millis: 1_790_000_000_000 + int64(seq)}}
		return b.add(&encoding.Commit{Parents: []encoding.ID{parent}, RootTree: encoding.Merge[encoding.ID]{tree},
			ConflictLabels: encoding.Merge[string]{""}, ChangeID: newChange(), Description: desc, Author: sig, Committer: sig})
	}
	// phase logs how long a phase took and, on a store with latency, the requests it made.
	slow, _ := e.s.blobs.(*testobjects.Slow)
	phase := func(what string, since time.Time) {
		t.Helper()
		if slow != nil {
			t.Logf("%s in %s; requests so far: %s", what, time.Since(since).Round(time.Millisecond), slow.Requests())
		} else {
			t.Logf("%s in %s", what, time.Since(since).Round(time.Millisecond))
		}
	}
	b := newBuilder(t)
	for i := range files {
		place(i, b.file(largeFile(i, 0, size)))
		if (i+1)%publishBatch == 0 {
			mustPublish(t, e.s, e.repo, alice, b, nil)
			b = newBuilder(t)
		}
	}
	first := commit(b, treeOf(b, root), "every file", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	e.mustLand(LandItem{Number: e.mustUpload(alice, Ready, first)[0].Number, Patchset: 1})

	// Then a long history: commits of a few changed files each, a stack of them uploaded and landed
	// at once.
	r := rand.New(rand.NewPCG(1, 2))
	for landed := 1; landed < commits; {
		n := min(stack, commits-landed)
		b := newBuilder(t)
		var stackCommits []encoding.ID
		parent := e.head()
		for range n {
			for range changed {
				i := r.IntN(files)
				version[i]++
				place(i, b.file(largeFile(i, version[i], size)))
			}
			parent = commit(b, treeOf(b, root), fmt.Sprint("commit ", landed+len(stackCommits)), parent)
			stackCommits = append(stackCommits, parent)
		}
		mustPublish(t, e.s, e.repo, alice, b, nil)
		res := e.mustUpload(alice, Ready, stackCommits...)
		items := make([]LandItem, len(res))
		for i, u := range res {
			items[i] = LandItem{Number: u.Number, Patchset: 1}
		}
		e.mustLand(items...)
		landed += n
	}
	phase(fmt.Sprintf("%d files, %d commits landed", files, commits), start)
	before := tableBytes(t, e.s, e.repo)

	// Garbage beside it, for the collection to take.
	g := newBuilder(t)
	garbage := g.commit(g.tree(fileEntry("garbage", g.file([]byte("garbage\n")))), "garbage", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, g, nil)
	age(t, e.s, e.repo, 20)

	// The relay publishes the events, which then may move.
	drain(t, e.s, &stream{})
	collected := time.Now()
	report, err := e.s.GC(ctx, e.repo, time.Now(), GCOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.CommitsSwept != 1 || has(t, e.s, e.repo, "commit", garbage) {
		t.Fatalf("the collection: %s", report)
	}
	phase("collected", collected)
	t.Logf("the collection: %s", report)
	compacted := time.Now()
	compactAll(t, e.s, e.repo)
	after := tableBytes(t, e.s, e.repo)
	phase("compacted", compacted)
	var total int64
	for tbl, n := range after {
		total += n
		t.Logf("  %-28s %12d bytes (%d before)", tbl, n, before[tbl])
	}
	t.Logf("  %-28s %12d bytes", "the repository's rows", total)
	for _, tbl := range []string{"object", "tree_sizes", "copy_parent", "commit", "commit_parent", "commit_path", "index_block", "content_log", "event",
		"changelist", "patchset", "head_log"} {
		if n := after[tbl]; n > 0 {
			t.Errorf("%s holds %d bytes after compaction: immutable data left in the tail", tbl, n)
		}
	}

	// Read back from a replica with nothing cached: the tree at main with each entry's last commit,
	// a directory's history, an annotation, and files' contents.
	fresh := reopen(t, e.s)
	head := e.head()
	reading := time.Now()
	if _, entries, err := fresh.ReadTree(ctx, e.repo, head, "d000/d000", true); err != nil || len(entries) == 0 || entries[0].LastCommit == nil {
		t.Fatalf("a directory at main with its last commits: %d entries, %v", len(entries), err)
	}
	if page, err := fresh.Log(ctx, e.repo, nil, []string{"d000"}, 20, 0, 0); err != nil || len(page.Commits) == 0 {
		t.Fatalf("a directory's history: %d commits, %v", len(page.Commits), err)
	}
	busiest := 0
	for i := range files {
		if version[i] > version[busiest] {
			busiest = i
		}
	}
	if ranges, err := fresh.Annotate(ctx, e.repo, head, largePath(busiest)); err != nil || len(ranges) == 0 {
		t.Fatalf("annotating %s: %v, %v", largePath(busiest), ranges, err)
	}
	mainTree, err := fresh.resolvedTree(ctx, e.repo, head)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range []int{0, files / 2, files - 1, busiest} {
		v, _, err := fresh.fileAt(ctx, e.repo, mainTree, largePath(i))
		if err != nil || !bytes.Equal(v, largeFile(i, version[i], size)) {
			t.Fatalf("file %d at main: %d bytes, %v; want version %d", i, len(v), err, version[i])
		}
	}
	phase("read back", reading)
	fscked := time.Now()
	fr, err := fresh.Fsck(ctx, e.repo, false)
	if err != nil || len(fr.Problems) != 0 {
		t.Fatalf("fsck: %v, %v", fr.Problems, err)
	}
	phase("fsck", fscked)
	t.Logf("fsck: %s", fr)

	// A fork costs the repository's segments, whatever its size: forking writes a reference to
	// each of its live segments and a few rows, and the fork reads its main from them.
	forking := time.Now()
	fork := newFork(t, e.s, e.repo)
	phase("forked", forking)
	refs, err := e.s.ops(ctx).AllSegments(ctx, repoScope(fork.ID))
	if err != nil {
		t.Fatal(err)
	}
	var forkRows int64
	for tbl, n := range tableBytes(t, e.s, fork.ID) {
		switch tbl {
		case "pack", "index_block", "content_log", "dictionary", "chunk_release", "segment":
			// The network's, shared; and the segment references, counted below.
		default:
			forkRows += n
		}
	}
	t.Logf("the fork: %d segment references, %d bytes of its own rows besides", len(refs), forkRows)
	if forkRows > 16<<10 {
		t.Errorf("forking wrote %d bytes of rows besides its segment references", forkRows)
	}
	forked := reopen(t, e.s)
	if v, _, err := forked.fileAt(ctx, fork.ID, mainTree, largePath(busiest)); err != nil || !bytes.Equal(v, largeFile(busiest, version[busiest], size)) {
		t.Fatalf("the fork's file %d at main: %d bytes, %v", busiest, len(v), err)
	}
	if page, err := forked.Log(ctx, fork.ID, nil, []string{"d000"}, 20, 0, 0); err != nil || len(page.Commits) == 0 {
		t.Fatalf("the fork's directory history: %d commits, %v", len(page.Commits), err)
	}
}
