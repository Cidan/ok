package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// forkView is everything a test reads of a repository's main: its head and place, its log whole
// and by path, an annotation, its root's entries with their last commits, and its files.
type forkView struct {
	Head        encoding.ID
	Seq         int64
	Log, ByPath []encoding.ID
	Annotated   []AnnotateRange
	Entries     []BrowseEntry
	Files       map[string]string
}

func viewOf(t *testing.T, s *Store, repo int64, path string) forkView {
	t.Helper()
	ctx := context.Background()
	r, err := s.GetRepository(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	v := forkView{Head: r.Head, Seq: r.HeadSeq}
	ids := func(paths []string) []encoding.ID {
		t.Helper()
		page, err := s.Log(ctx, repo, nil, paths, 1000, 0, 0)
		if err != nil {
			t.Fatalf("repository %d's log of %v: %v", repo, paths, err)
		}
		var out []encoding.ID
		for _, c := range page.Commits {
			out = append(out, c.ID)
		}
		return out
	}
	v.Log, v.ByPath = ids(nil), ids([]string{path})
	if v.Annotated, err = s.Annotate(ctx, repo, r.Head, path); err != nil {
		t.Fatalf("repository %d's annotation of %s: %v", repo, path, err)
	}
	if _, v.Entries, err = s.ReadTree(ctx, repo, r.Head, "", true); err != nil {
		t.Fatalf("repository %d's tree: %v", repo, err)
	}
	v.Files = (&clEnv{t: t, s: s, repo: repo}).flatAt(r.Head)
	return v
}

// newFork forks a repository for a test, with the test principals' workspaces.
func newFork(t *testing.T, s *Store, parent int64) Repository {
	t.Helper()
	ctx := context.Background()
	r, err := s.Fork(ctx, data.NewID(), parent, bob)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range testPrincipals {
		if _, err := s.CreateWorkspace(ctx, p, r.ID, testWorkspace); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

// fileID is the ID of a file of contents.
func fileID(t *testing.T, contents string) encoding.ID {
	return newBuilder(t).file([]byte(contents))
}

// readable reports whether a repository reads a commit and a file by ID.
func readable(t *testing.T, s *Store, repo int64, commit, file encoding.ID) (commitOK, fileOK bool) {
	t.Helper()
	ctx := context.Background()
	_, err := s.GetCommits(ctx, repo, []encoding.ID{commit})
	commitOK = err == nil
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("repository %d's commit %s: %v", repo, commit, err)
	}
	_, err = s.ReadFile(ctx, repo, file)
	fileOK = err == nil
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("repository %d's file %s: %v", repo, file, err)
	}
	return commitOK, fileOK
}

// TestForkNetwork is fork networks' proof (docs/hosted.md, "Fork networks, as built"): a fork
// costs its parent's segments, not its content; it reads its parent's main as of the fork; neither
// side reads what only the other holds, though their chunks share packs; a sync brings the parent's
// later work; and nothing the parent does — landing, compaction and merges, repacking, collection,
// fsck with repair — changes what the fork reads.
func TestForkNetwork(t *testing.T) {
	ctx := context.Background()
	p := newCLEnv(t)
	p.landChange("base", map[string]string{"a": "a0\n", "b": "b0\n"})
	p.landChange("second", map[string]string{"a": "a1\n", "b": "b0\n", "c": "c0\n"})
	// The parent's work published but never uploaded: not inherited.
	loose := p.commit(alice, newChange(), p.head(), "loose", map[string]string{"a": "a1\n", "b": "b0\n", "c": "c0\n", "loose": "unlanded\n"})
	before := viewOf(t, p.s, p.repo, "a")

	parentSegs := 0
	for _, kind := range inheritedKinds {
		live, err := p.s.ops(ctx).Segments(ctx, repoScope(p.repo), kind)
		if err != nil {
			t.Fatal(err)
		}
		parentSegs += len(live)
	}
	fr := newFork(t, p.s, p.repo)
	f := &clEnv{t: t, s: p.s, repo: fr.ID}
	if fr.Network != netOf(t, p.s, p.repo) || fr.Parent != p.repo || fr.Head != before.Head || fr.HeadSeq != before.Seq || fr.ParentSeq != before.Seq {
		t.Fatalf("the fork: %+v; want it in its parent's network, at its parent's main", fr)
	}

	// Forking wrote segment references and nothing per object: everything pinned was in segments.
	inherited, err := p.s.ops(ctx).AllSegments(ctx, repoScope(f.repo))
	if err != nil {
		t.Fatal(err)
	}
	segsNow := 0
	for _, kind := range inheritedKinds {
		live, err := p.s.ops(ctx).Segments(ctx, repoScope(p.repo), kind)
		if err != nil {
			t.Fatal(err)
		}
		segsNow += len(live)
	}
	if len(inherited) != segsNow || segsNow < parentSegs {
		t.Errorf("the fork took %d segments; its parent holds %d of the inherited kinds", len(inherited), segsNow)
	}
	for _, sg := range inherited {
		if !sg.Inherited || (boundedKinds[sg.Kind] != (sg.Bound == before.Seq)) {
			t.Errorf("inherited segment %+v", sg)
		}
	}
	for tbl, n := range tableBytes(t, p.s, f.repo) {
		switch tbl {
		case "segment", "content_index", "repository_event_counter", "event", "event_pending", "workspace", "op_log", "dictionary_grant",
			// When the fork's events were last sequenced, on Spanner: its own.
			"sequence_pass",
			// The network's, which the fork shares.
			"pack", "content_log", "dictionary", "chunk_release":
		default:
			t.Errorf("forking wrote %d bytes of %s", n, tbl)
		}
	}

	// The parent's unpinned index — what it holds of its workspaces — is its own: the fork takes none
	// of its runs.
	if len(runsOf(t, p.s, repoScope(p.repo), segIndexUnpinned)) == 0 {
		t.Fatal("the parent's unpinned entries were folded into no run")
	}
	if runs := runsOf(t, p.s, repoScope(f.repo), segIndexUnpinned); len(runs) != 0 {
		t.Fatalf("the fork took its parent's unpinned runs: %+v", runs)
	}

	// The fork reads its parent's main as of the fork, on a replica with nothing cached.
	if got := viewOf(t, reopen(t, p.s), f.repo, "a"); !reflect.DeepEqual(got, before) {
		t.Fatalf("the fork's main:\n%+v\nwant its parent's:\n%+v", got, before)
	}
	if c, file := readable(t, p.s, f.repo, loose, fileID(t, "unlanded\n")); c || file {
		t.Errorf("the fork reads its parent's work never uploaded (commit %v, file %v)", c, file)
	}

	// Both sides land; neither reads what only the other holds.
	pn := p.landChange("parent's", map[string]string{"a": "a2\n", "b": "b0\n", "c": "c0\n", "p": "parent only\n"})
	pLanded := p.cl(pn).LandedCommit
	fn := f.landChange("fork's", map[string]string{"a": "a1\n", "b": "b1\n", "c": "c0\n", "f": "fork only\n"})
	fLanded := f.cl(fn).LandedCommit
	if c, file := readable(t, p.s, f.repo, *pLanded, fileID(t, "parent only\n")); c || file {
		t.Errorf("the fork reads its parent's later work (commit %v, file %v)", c, file)
	}
	if c, file := readable(t, p.s, p.repo, *fLanded, fileID(t, "fork only\n")); c || file {
		t.Errorf("the parent reads its fork's work (commit %v, file %v)", c, file)
	}
	forkView := viewOf(t, p.s, f.repo, "a")
	if forkView.Seq != before.Seq+1 || len(forkView.Log) != len(before.Log)+1 || forkView.Log[0] != *fLanded ||
		!reflect.DeepEqual(forkView.Log[1:], before.Log) || !reflect.DeepEqual(forkView.ByPath, before.ByPath) {
		t.Fatalf("the fork's history after its own landing: %+v; before, %+v", forkView, before)
	}
	if on, err := p.s.onMain(ctx, p.s.ops(ctx), f.repo, *pLanded); err != nil || on {
		t.Errorf("the parent's later landing is on the fork's main: %v, %v", on, err)
	}
	// The fork compacts and merges its own segments, never those it inherited.
	for i := range 6 {
		f.landChange("fork more", map[string]string{"a": "a1\n", "b": "b1\n", "c": "c0\n", "f": "fork only\n", "n": string(rune('0' + i))})
		compactAll(t, p.s, f.repo)
	}
	forkHead := f.head()
	fLanded = &forkHead

	// A publish into the fork asks for the chunks only its parent holds: no dedup oracle.
	b := newBuilder(t)
	b.commit(b.tree(fileEntry("guess", b.file([]byte("parent only\n")))), "guess", encoding.RootCommitID)
	pub, err := p.s.BeginPublish(ctx, f.repo, bob, testWorkspace, b.order, b.manifests())
	if err != nil {
		t.Fatal(err)
	}
	if _, chunks := pub.Missing(); len(chunks) != 1 || chunks[0] != pack.ChunkID([]byte("parent only\n")) {
		t.Errorf("a publish into the fork of the parent's chunk asks for %v", chunks)
	}
	pub.Close()

	// Nothing the parent does changes what the fork reads: more landings and compactions, merges,
	// a collection with repacking, retired packs and segments deleted, fsck with repair.
	forkBefore := viewOf(t, p.s, f.repo, "a")
	for i := range 6 {
		p.landChange("more", map[string]string{"a": "a2\n", "b": "b0\n", "c": "c0\n", "p": "parent only\n", "m": string(rune('0' + i))})
		compactAll(t, p.s, p.repo)
	}
	if r, err := p.s.GC(ctx, p.repo, time.Now(), GCOptions{}); err != nil || r.PacksRetired == 0 {
		t.Fatalf("the parent's collection: %s, %v; want packs repacked", r, err)
	}
	if _, _, err := p.s.DeleteRetiredOf(ctx, p.repo, time.Now().Add(2*RetiredSegmentGrace)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.s.Fsck(ctx, p.repo, true); err != nil {
		t.Fatal(err)
	}
	clean(t, p.s, p.repo)
	if got := viewOf(t, reopen(t, p.s), f.repo, "a"); !reflect.DeepEqual(got, forkBefore) {
		t.Fatalf("after the parent's compaction, collection, and fsck, the fork reads\n%+v\nwant\n%+v", got, forkBefore)
	}
	clean(t, f.s, f.repo)

	// Chunks share packs: the network's repacking mixed both sides' chunks, and still neither reads
	// the other's file.
	locs := locationsView(t, p.s, p.repo)
	pc, fc := locs[pack.ChunkID([]byte("parent only\n"))], locs[pack.ChunkID([]byte("fork only\n"))]
	if pc.Pack == 0 || pc.Pack != fc.Pack {
		t.Errorf("the parent's chunk is in pack %d and the fork's in %d; want one pack", pc.Pack, fc.Pack)
	}
	if _, file := readable(t, p.s, p.repo, *fLanded, fileID(t, "fork only\n")); file {
		t.Error("the parent reads its fork's file from the shared pack")
	}

	// A sync brings the parent's later work, readable by ID; the fork's main keeps its own landing.
	r, err := p.s.Sync(ctx, f.repo, p.repo, bob)
	if err != nil || r.Moved || r.Head != *fLanded {
		t.Fatalf("syncing a fork with a landing of its own: %+v, %v; want its main kept", r, err)
	}
	if c, file := readable(t, p.s, f.repo, *pLanded, fileID(t, "parent only\n")); !c || !file {
		t.Errorf("after a sync the fork does not read its parent's later work (commit %v, file %v)", c, file)
	}
	if got := viewOf(t, p.s, f.repo, "a"); !reflect.DeepEqual(got, forkBefore) {
		t.Fatalf("a sync changed the fork's history:\n%+v\nwant\n%+v", got, forkBefore)
	}
	clean(t, f.s, f.repo)
}

// A fork that has landed nothing of its own follows its parent's main on a sync; until then it
// reads its parent's main as it forked it.
func TestForkSyncFollowsMain(t *testing.T) {
	ctx := context.Background()
	p := newCLEnv(t)
	p.landChange("base", map[string]string{"a": "a0\n"})
	fr := newFork(t, p.s, p.repo)
	p.landChange("later", map[string]string{"a": "a1\n", "later": "x\n"})
	parent := viewOf(t, p.s, p.repo, "a")
	if got := viewOf(t, p.s, fr.ID, "a"); got.Head == parent.Head || got.Files["later"] != "" {
		t.Fatalf("the fork reads its parent's later main before a sync: %+v", got)
	}
	r, err := p.s.Sync(ctx, fr.ID, p.repo, bob)
	if err != nil || !r.Moved || r.Head != parent.Head || r.Seq != parent.Seq {
		t.Fatalf("sync: %+v, %v; want main moved to %s", r, err, parent.Head)
	}
	if got := viewOf(t, reopen(t, p.s), fr.ID, "a"); !reflect.DeepEqual(got, parent) {
		t.Fatalf("after a sync the fork reads\n%+v\nwant its parent's\n%+v", got, parent)
	}
	// It lands on what it synced, and its history continues from there.
	f := &clEnv{t: t, s: p.s, repo: fr.ID}
	f.landChange("fork's", map[string]string{"a": "a2\n", "later": "x\n"})
	if got := viewOf(t, p.s, fr.ID, "a"); got.Seq != parent.Seq+1 || !reflect.DeepEqual(got.Log[1:], parent.Log) {
		t.Fatalf("the fork's history after landing on its sync: %+v", got)
	}
	clean(t, p.s, fr.ID)
	if _, err := p.s.Sync(ctx, p.repo, 0, bob); !errors.Is(err, ErrPrecondition) {
		t.Errorf("syncing a repository that is no fork: %v", err)
	}
}

// A fork whose parent is deleted takes a new upstream from GitHub's rules — a sibling fork — and
// syncs from it, if the sibling's main came to the fork's base at the fork's place: it follows the
// sibling's main, reading its work. One whose base the sibling never came to is refused.
func TestForkNewUpstream(t *testing.T) {
	ctx := context.Background()
	p := newCLEnv(t)
	p.landChange("base", map[string]string{"a": "a0\n"})
	promoted, sibling := newFork(t, p.s, p.repo), newFork(t, p.s, p.repo)
	p.landChange("later", map[string]string{"a": "a1\n"})
	late := newFork(t, p.s, p.repo)
	up := &clEnv{t: t, s: p.s, repo: promoted.ID}
	up.landChange("upstream's own", map[string]string{"a": "a0\n", "own": "o\n"})
	if err := p.s.DeleteRepository(ctx, p.repo); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{sibling.ID, late.ID} {
		if r := mustRepo(t, p.s, id); r.Parent != 0 || r.ParentSeq == 0 {
			t.Fatalf("a fork of the deleted parent: parent %d, its place %d; want none, and its place kept", r.Parent, r.ParentSeq)
		}
	}
	r, err := p.s.Sync(ctx, sibling.ID, promoted.ID, bob)
	want := viewOf(t, p.s, promoted.ID, "a")
	if err != nil || !r.Moved || r.Head != want.Head {
		t.Fatalf("syncing from the new upstream: %+v, %v; want main moved to %s", r, err, want.Head)
	}
	if got := viewOf(t, reopen(t, p.s), sibling.ID, "a"); !reflect.DeepEqual(got, want) {
		t.Fatalf("after syncing from its new upstream the fork reads\n%+v\nwant\n%+v", got, want)
	}
	if rr := mustRepo(t, p.s, sibling.ID); rr.Parent != promoted.ID {
		t.Errorf("the fork's upstream after the sync: %d", rr.Parent)
	}
	before := viewOf(t, p.s, late.ID, "a")
	if _, err := p.s.Sync(ctx, late.ID, promoted.ID, bob); !errors.Is(err, ErrPrecondition) {
		t.Errorf("syncing from an upstream that never came to the fork's base: %v", err)
	}
	if got := viewOf(t, p.s, late.ID, "a"); !reflect.DeepEqual(got, before) || mustRepo(t, p.s, late.ID).Parent != 0 {
		t.Errorf("a refused sync changed the fork: %+v", got)
	}
	for _, id := range []int64{promoted.ID, sibling.ID, late.ID} {
		clean(t, p.s, id)
	}
}

// A chunk or a tree two repositories of a network hold goes once neither does: the first's sweep
// releases it and its location stays; the second's sweep releases it too, and the network's
// collection drops it.
func TestForkReleasesChunks(t *testing.T) {
	ctx := context.Background()
	p := newCLEnv(t)
	p.landChange("base", map[string]string{"a": "a0\n"})
	fr := newFork(t, p.s, p.repo)
	shared := bytesOf(40_000, 5)
	chunks := chunkIDs(shared)
	var tree encoding.ID
	for _, repo := range []int64{p.repo, fr.ID} {
		b := newBuilder(t)
		tree = b.tree(fileEntry("shared", b.file(shared)))
		b.commit(tree, "unreachable", encoding.RootCommitID)
		mustPublish(t, p.s, repo, alice, b, nil)
		age(t, p.s, repo, 20)
	}
	r, err := p.s.GC(ctx, p.repo, time.Now(), GCOptions{})
	if err != nil || r.ChunksReleased != int64(len(chunks)) || r.TreesReleased != 1 || r.LocationsDropped != 0 {
		t.Fatalf("the parent's sweep: %s, %v; want its chunks and tree released, their locations kept", r, err)
	}
	if res, err := reopen(t, p.s).ReadObjects(ctx, fr.ID, []ObjectRef{{Kind: encoding.KindTree, ID: tree}}); err != nil || !res[0].Found {
		t.Fatalf("the fork lost the tree its parent swept: %v", err)
	}
	if got := readAll(t, reopen(t, p.s), fr.ID, fileID(t, string(shared))); string(got) != string(shared) {
		t.Fatal("the fork lost the chunks its parent swept")
	}
	if r, err = p.s.GC(ctx, fr.ID, time.Now(), GCOptions{}); err != nil || r.ChunksReleased != int64(len(chunks)) || r.TreesReleased != 1 ||
		r.LocationsDropped != int64(len(chunks))+1 {
		t.Fatalf("the fork's sweep: %s, %v; want its chunks and tree released and their locations dropped", r, err)
	}
	locs := locationsView(t, p.s, p.repo)
	if _, ok := locs[tree]; ok {
		t.Error("the tree still has a location once no repository holds it")
	}
	for _, c := range chunks {
		if _, ok := locs[c]; ok {
			t.Errorf("chunk %s still has a location once no repository holds it", c)
		}
	}
	clean(t, p.s, p.repo)
	clean(t, p.s, fr.ID)
}

// A fork reads the dictionaries its parent could read when it forked, and none trained after: a
// chunk compressed with a newer one reaches it raw, and the dictionary not at all. Only the
// network's first repository trains.
func TestForkDictionaries(t *testing.T) {
	ctx := context.Background()
	p := newCLEnv(t)
	p.s.dictionaryMinSamples = 50
	landFiles := func(e *clEnv, from, n int) {
		files := map[string]string{}
		for i := from; i < from+n; i++ {
			files[string(rune('a'+i%26))+string(rune('a'+i/26))+".go"] = string(goFile(i))
		}
		e.landChange("files", files)
	}
	landFiles(p, 0, 120)
	r, err := p.s.GC(ctx, p.repo, time.Now(), GCOptions{})
	if err != nil || r.DictionaryTrained == 0 {
		t.Fatalf("the parent's first dictionary: %s, %v", r, err)
	}
	first := r.DictionaryTrained
	fr := newFork(t, p.s, p.repo)
	if _, err := p.s.Dictionary(ctx, fr.ID, first); err != nil {
		t.Errorf("the fork cannot read its parent's dictionary from before the fork: %v", err)
	}
	if r, err := p.s.GC(ctx, fr.ID, time.Now().Add(2*DictionaryRetrain), GCOptions{}); err != nil || r.DictionaryTrained != 0 {
		t.Errorf("the fork trained a dictionary: %s, %v", r, err)
	}
	landFiles(p, 120, 120)
	r, err = p.s.GC(ctx, p.repo, time.Now().Add(2*DictionaryRetrain), GCOptions{})
	if err != nil || r.DictionaryTrained == 0 || r.ChunksRecompressed == 0 {
		t.Fatalf("the parent's second dictionary: %s, %v", r, err)
	}
	second := r.DictionaryTrained
	if _, err := p.s.Dictionary(ctx, fr.ID, second); !errors.Is(err, ErrNotFound) {
		t.Errorf("the fork reads its parent's dictionary from after the fork: %v", err)
	}
	if _, err := p.s.Dictionary(ctx, p.repo, second); err != nil {
		t.Errorf("the parent cannot read its own dictionary: %v", err)
	}
	// The fork lands files the parent landed after the fork: it holds their chunks, which the
	// network stored first for the parent and compressed with the newer dictionary. The parent reads
	// them so; the fork, raw.
	f := &clEnv{t: t, s: p.s, repo: fr.ID}
	same := map[string]string{}
	var files []FileRange
	for i := 120; i < 140; i++ {
		same[fmt.Sprintf("copy%d.go", i)] = string(goFile(i))
		files = append(files, FileRange{FileID: fileID(t, string(goFile(i)))})
	}
	f.landChange("copies", same)
	withDict := func(repo int64) map[uint32]int {
		t.Helper()
		// A replica with nothing cached: one that read the chunks before they were repacked keeps
		// them as they were stored then.
		out := map[uint32]int{}
		err := reopen(t, p.s).ReadFiles(ctx, repo, files, StoredChunksWithDictionaries, func(frame FileFrame) error {
			if frame.Codec != pack.ZstdDict {
				return nil
			}
			id, err := pack.DictionaryID(frame.Data)
			out[id]++
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := withDict(p.repo); got[second] == 0 {
		t.Fatalf("the parent reads none of the fork's files' chunks compressed with its newer dictionary: %v", got)
	}
	if got := withDict(fr.ID); got[second] != 0 {
		t.Errorf("%d chunks reached the fork compressed with a dictionary it may not read", got[second])
	}
	clean(t, p.s, fr.ID)
}

// locationsView is a repository's network's chunk index, whole: each chunk's and tree's location.
func locationsView(t *testing.T, s *Store, repo int64) map[encoding.ID]contentindex.Location {
	t.Helper()
	ctx := context.Background()
	v, err := reopen(t, s).contentView(ctx, backend.NetworkScope(netOf(t, s, repo)))
	if err != nil {
		t.Fatal(err)
	}
	out := map[encoding.ID]contentindex.Location{}
	if err := v.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
		if contentindex.Located(e.Kind) {
			out[id] = e.Loc
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return out
}
