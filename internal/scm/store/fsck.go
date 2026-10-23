package store

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// FsckReport is what Fsck found in one repository.
type FsckReport struct {
	Packs, Chunks, Files, Objects, Commits, Operations, Views int
	// IndexEntries are the content index's files and chunks.
	IndexEntries int
	// DeadBytes are pack bytes no chunk row points at (reclaimed by compaction).
	DeadBytes int64
	// OrphanPacks are S3 keys with no pack row (a failed publish's upload).
	OrphanPacks []string
	// Problems are inconsistencies: anything here is corruption or a bug.
	Problems []string
	// Repaired lists what repair fixed.
	Repaired []string
}

func (r *FsckReport) problem(format string, args ...any) {
	r.Problems = append(r.Problems, fmt.Sprintf(format, args...))
}

// Fsck verifies one repository against its own invariants: every pack against its bytes
// and the content index's chunks in it, every file's chunks against its ID, every object,
// commit, operation, and view against its ID and canonical encoding, and the parent and copy
// indexes against the bodies they index. With repair, it rebuilds the index's entries for
// chunks files use that it lacks, from the packs that hold them (packs are self-describing).
func (s *Store) Fsck(ctx context.Context, repo int64, repair bool) (*FsckReport, error) {
	if err := s.repositoryExists(ctx, s.ops(ctx), repo); err != nil {
		return nil, err
	}
	r := &FsckReport{}
	if err := s.fsckPacks(ctx, repo, repair, r); err != nil {
		return nil, err
	}
	if err := s.fsckObjects(ctx, repo, r); err != nil {
		return nil, err
	}
	if err := s.fsckCommits(ctx, repo, r); err != nil {
		return nil, err
	}
	if err := s.fsckOperations(ctx, repo, r); err != nil {
		return nil, err
	}
	return r, nil
}

type packRow struct {
	id   encoding.ID
	num  uint64
	size int64
}

// fsckFile is a file fsck verifies: its ID, size, and chunks.
type fsckFile struct {
	id     encoding.ID
	size   uint64
	chunks []contentindex.FileChunk
}

// orphan is a pack's record no content index entry points at.
type orphan struct {
	pack pack.Record
	pr   packRow
}

// fsckPacks verifies every live pack, a download of each, and every file from the pack its first
// chunk is in while that pack's copy is on local disk — reading only chunks other packs hold — so
// verifying a repository reads its packs once and asks the bucket for little more. One scan of the
// content index finds every chunk's and file's entry; one more, only if some pack holds records no
// entry points at, finds which of those a file uses.
func (s *Store) fsckPacks(ctx context.Context, repo int64, repair bool, r *FsckReport) error {
	net, netScope, err := s.networkOf(ctx, repo)
	if err != nil {
		return err
	}
	live, err := s.ops(ctx).LivePacks(ctx, net)
	if err != nil {
		return err
	}
	packs := make([]packRow, len(live))
	for i, p := range live {
		packs[i] = packRow{id: p.ID, num: p.Num, size: p.Size}
	}
	sort.Slice(packs, func(i, j int) bool { return string(packs[i].id[:]) < string(packs[j].id[:]) })
	// Retired packs are known too: repacking deletes them from object storage an hour after
	// retiring.
	known := map[string]bool{}
	retired, err := s.ops(ctx).RetiredPackIDs(ctx, net)
	if err != nil {
		return err
	}
	for _, id := range retired {
		known[packKey(net, id)] = true
	}
	// The network's chunk index says where each chunk is; the repository's content index what
	// it holds.
	locs, err := s.contentView(ctx, netScope)
	if err != nil {
		return err
	}
	v, err := s.contentView(ctx, repoScope(repo))
	if err != nil {
		return err
	}
	for _, view := range []*contentView{v, locs} {
		for _, k := range view.x.Sorted() {
			if !k.Entry.Body {
				r.problem("content index: %s %s has only a touch or a pin", k.Entry.Kind, k.ID)
				r.IndexEntries++
			}
		}
	}
	byPack := map[uint64][]contentindex.Keyed{}
	packOf := map[encoding.ID]uint64{}
	err = locs.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
		if e.Kind == encoding.KindChunk {
			byPack[e.Loc.Pack] = append(byPack[e.Loc.Pack], contentindex.Keyed{ID: id, Entry: e})
			packOf[id] = e.Loc.Pack
		}
		return true
	})
	if err != nil {
		return err
	}
	var files []fsckFile
	var unlocated []encoding.ID
	heldChunks := map[encoding.ID]bool{}
	err = v.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
		r.IndexEntries++
		switch e.Kind {
		case encoding.KindChunk:
			heldChunks[id] = true
			if _, ok := packOf[id]; !ok {
				unlocated = append(unlocated, id)
			}
		case encoding.KindFile:
			files = append(files, fsckFile{id: id, size: e.Size, chunks: e.Chunks})
		}
		return true
	})
	if err != nil {
		return err
	}
	// A repository that holds a file holds its chunks.
	var unheld []encoding.ID
	seen := map[encoding.ID]bool{}
	for _, f := range files {
		for _, c := range f.chunks {
			if !heldChunks[c.ID] && !seen[c.ID] {
				seen[c.ID] = true
				unheld = append(unheld, c.ID)
			}
		}
	}

	// A file a chunk of which the index lacks waits for repair, which may rebuild its entry from a
	// pack, then reads its chunks from the bucket.
	filesByPack := map[uint64][]fsckFile{}
	var afterRepair []fsckFile
	for _, f := range files {
		r.Files++
		if len(f.chunks) == 0 {
			s.fsckFile(ctx, repo, f, nil, r)
			continue
		}
		indexed := true
		for _, c := range f.chunks {
			if _, ok := packOf[c.ID]; !ok {
				indexed = false
				break
			}
		}
		if !indexed {
			afterRepair = append(afterRepair, f)
			continue
		}
		num := packOf[f.chunks[0].ID]
		filesByPack[num] = append(filesByPack[num], f)
	}
	files, packOf = nil, nil
	var orphans []orphan
	for _, pr := range packs {
		known[packKey(net, pr.id)] = true
		r.Packs++
		o, err := s.fsckPack(ctx, repo, net, byPack[pr.num], filesByPack[pr.num], pr, r)
		if err != nil {
			return err
		}
		orphans = append(orphans, o...)
		delete(byPack, pr.num)
		delete(filesByPack, pr.num)
	}
	for num, fs := range filesByPack {
		for _, f := range fs {
			r.problem("file %s: chunk %s is in pack %d, which is not live", f.id, f.chunks[0].ID, num)
		}
	}
	recorded, err := s.fsckOrphans(ctx, repo, netScope, v, orphans, repair, r)
	if err != nil {
		return err
	}
	// After the orphans: rebuilding a chunk's location records it held too.
	if err := s.fsckUnheld(ctx, repo, unheld, repair, r); err != nil {
		return err
	}
	// A chunk the repository holds that no live pack has a record of is gone.
	for _, id := range unlocated {
		if !recorded[id] {
			r.problem("chunk %s: held, but in none of its network's live packs", id)
		}
	}
	for _, f := range afterRepair {
		s.fsckFile(ctx, repo, f, nil, r)
	}
	objs, err := s.blobs.List(ctx, packPrefix(net))
	if err != nil {
		return err
	}
	for _, o := range objs {
		if !known[o.Key] {
			r.OrphanPacks = append(r.OrphanPacks, o.Key)
		}
	}
	return nil
}

// fsckPack verifies a pack against the content index's chunks in it (here), and the files whose
// first chunk it holds, and returns its records no entry points at.
func (s *Store) fsckPack(ctx context.Context, repo int64, net backend.Network, here []contentindex.Keyed, files []fsckFile, pr packRow, r *FsckReport) ([]orphan, error) {
	f, err := os.CreateTemp("", "ok scm-fsck-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	size, err := s.blobs.Download(ctx, packKey(net, pr.id), f)
	if err != nil {
		r.problem("pack %s: %v", pr.id, err)
		return nil, nil
	}
	if size != pr.size {
		r.problem("pack %s: %d bytes stored, row says %d", pr.id, size, pr.size)
	}
	records, err := pack.Verify(f, size, pr.id, s.dicts.of(net))
	if err != nil {
		r.problem("pack %s: %v", pr.id, err)
		return nil, nil
	}
	byID := map[encoding.ID]pack.Record{}
	for _, rec := range records {
		byID[rec.ID] = rec
	}
	// The index's chunks in this pack must agree with it.
	pointed := map[encoding.ID]bool{}
	for _, k := range here {
		id, e := k.ID, k.Entry
		pointed[id] = true
		r.Chunks++
		rec, ok := byID[id]
		want := pack.Location{Offset: e.Loc.Offset, Codec: pack.Codec(e.Loc.Codec), RawLength: e.Loc.Raw, StoredLength: e.Loc.Stored}
		switch {
		case !ok:
			r.problem("chunk %s: the index has it in pack %s, which does not hold it", id, pr.id)
		case rec.Location != want:
			r.problem("chunk %s: the index's %+v disagrees with pack %s record %+v", id, want, pr.id, rec.Location)
		}
	}
	// A chunk the index has here is read from the local copy, whose records Verify hashed.
	local := func(id encoding.ID) ([]byte, bool, error) {
		rec, ok := byID[id]
		if !ok || !pointed[id] {
			return nil, false, nil
		}
		stored := make([]byte, rec.StoredLength)
		if _, err := f.ReadAt(stored, rec.DataOffset()); err != nil {
			return nil, true, err
		}
		raw, err := pack.Decompress(s.dicts.of(net), id, rec.Codec, rec.RawLength, stored)
		return raw, true, err
	}
	for _, file := range files {
		s.fsckFile(ctx, repo, file, local, r)
	}
	var orphans []orphan
	for _, rec := range records {
		if !pointed[rec.ID] {
			orphans = append(orphans, orphan{pack: rec, pr: pr})
		}
	}
	return orphans, nil
}

// fsckWindow bounds how many of a file's chunks fsck fetches from the bucket at once.
const fsckWindow = 64

// fsckFile verifies a file: its chunks, in order, have the lengths its entry says and hash, as one,
// to its ID. local reads a chunk the pack being verified holds; every other is fetched, a window at
// a time.
func (s *Store) fsckFile(ctx context.Context, repo int64, f fsckFile, local func(encoding.ID) ([]byte, bool, error), r *FsckReport) {
	h := encoding.NewHasher(encoding.KindFile)
	var total uint64
	for start := 0; start < len(f.chunks); start += fsckWindow {
		window := f.chunks[start:min(start+fsckWindow, len(f.chunks))]
		data := make(map[encoding.ID][]byte, len(window))
		var remote []encoding.ID
		for _, c := range window {
			if local != nil {
				b, ok, err := local(c.ID)
				if err != nil {
					r.problem("file %s: chunk %s: %v", f.id, c.ID, err)
					return
				}
				if ok {
					data[c.ID] = b
					continue
				}
			}
			remote = append(remote, c.ID)
		}
		if len(remote) > 0 {
			got, err := s.chunkBytes(ctx, repo, remote)
			if err != nil {
				r.problem("file %s: %v", f.id, err)
				return
			}
			for id, b := range got {
				data[id] = b
			}
		}
		for _, c := range window {
			if uint64(len(data[c.ID])) != uint64(c.Length) {
				r.problem("file %s: chunk %s at %d (%d bytes) breaks the file's layout", f.id, c.ID, total, len(data[c.ID]))
				return
			}
			total += uint64(c.Length)
			h.Write(data[c.ID])
		}
	}
	if total != f.size {
		r.problem("file %s: chunks total %d bytes, the index says %d", f.id, total, f.size)
	} else if got := h.Sum(); got != f.id {
		r.problem("file %s: contents hash to %s", f.id, got)
	}
}

// fsckUnheld reports the chunks of files the repository holds that it does not hold itself, and
// with repair records them held.
func (s *Store) fsckUnheld(ctx context.Context, repo int64, ids []encoding.ID, repair bool, r *FsckReport) error {
	if len(ids) == 0 {
		return nil
	}
	sortIDs(ids)
	if !repair {
		for _, id := range ids {
			r.problem("chunk %s: of a file the repository holds, but not held", id)
		}
		return nil
	}
	return s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		held, err := s.txIndex(ctx, tx, repoScope(repo))
		if err != nil {
			return err
		}
		for _, id := range ids {
			if _, ok, err := held.has(ctx, encoding.KindChunk, id); err != nil {
				return err
			} else if !ok {
				s.logIndex(tx, repoScope(repo), contentindex.Update{Op: contentindex.Put, ID: id, Entry: contentindex.Entry{
					Kind: encoding.KindChunk, Day: held.today,
				}})
				r.Repaired = append(r.Repaired, fmt.Sprintf("recorded chunk %s held", id))
			}
		}
		return nil
	})
}

// fsckOrphans classifies packs' records no location in the network's chunk index points at: dead
// bytes, unless a file the repository holds uses the chunk and the network has no location of
// it, which repair rebuilds from the record — with the repository's entry of the chunk, if that is
// missing too. One scan of the repository's index finds the files that use them. It returns the
// chunks the records are of.
func (s *Store) fsckOrphans(ctx context.Context, repo int64, netScope backend.Scope, v *contentView, orphans []orphan, repair bool, r *FsckReport) (map[encoding.ID]bool, error) {
	recorded := make(map[encoding.ID]bool, len(orphans))
	if len(orphans) == 0 {
		return recorded, nil
	}
	ids := make([]encoding.ID, len(orphans))
	for i, o := range orphans {
		ids[i] = o.pack.ID
		recorded[o.pack.ID] = true
	}
	_, located, err := s.contentEntries(ctx, netScope, encoding.KindChunk, ids)
	if err != nil {
		return nil, err
	}
	used := map[encoding.ID]bool{}
	err = v.each(ctx, func(_ encoding.ID, e contentindex.Entry) bool {
		for _, c := range e.Chunks {
			if recorded[c.ID] {
				used[c.ID] = true
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	rebuilt := map[encoding.ID]bool{}
	for _, o := range orphans {
		id, rec, pr := o.pack.ID, o.pack, o.pr
		if _, ok := located[id]; ok || !used[id] || rebuilt[id] {
			r.DeadBytes += int64(rec.StoredLength) + pack.RecordHeaderLen
			continue
		}
		if !repair {
			r.problem("chunk %s: used by a file but not in the index (pack %s holds it)", id, pr.id)
			continue
		}
		inserted := false
		err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			inserted = false
			held, err := s.txIndex(ctx, tx, repoScope(repo))
			if err != nil {
				return err
			}
			locs, err := s.txIndex(ctx, tx, netScope)
			if err != nil {
				return err
			}
			if _, ok, err := locs.has(ctx, encoding.KindChunk, id); err != nil || ok {
				return err
			}
			inserted = true
			s.logIndex(tx, netScope, contentindex.Update{Op: contentindex.Put, ID: id, Entry: contentindex.Entry{
				Kind: encoding.KindChunk, Day: locs.today,
				Loc: contentindex.Location{Pack: pr.num, Offset: rec.Offset, Stored: rec.StoredLength, Raw: rec.RawLength, Codec: uint8(rec.Codec)},
			}})
			if _, ok, err := held.has(ctx, encoding.KindChunk, id); err != nil {
				return err
			} else if !ok {
				s.logIndex(tx, repoScope(repo), contentindex.Update{Op: contentindex.Put, ID: id, Entry: contentindex.Entry{
					Kind: encoding.KindChunk, Day: held.today,
				}})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if !inserted {
			// A concurrent publish stored it first, into its own pack.
			r.DeadBytes += int64(rec.StoredLength) + pack.RecordHeaderLen
			continue
		}
		rebuilt[id] = true
		r.Chunks++
		r.Repaired = append(r.Repaired, fmt.Sprintf("rebuilt chunk %s from pack %s", id, pr.id))
	}
	return recorded, nil
}

// verifyBody checks a stored body decodes canonically and hashes to its ID.
func verifyBody(kind encoding.Kind, id encoding.ID, body []byte) (encoding.Object, error) {
	canonical := append([]byte{byte(kind)}, body...)
	o, err := encoding.Decode(canonical)
	if err != nil {
		return nil, err
	}
	if got := encoding.Hash(canonical); got != id {
		return nil, fmt.Errorf("body hashes to %s", got)
	}
	return o, nil
}

func (s *Store) fsckObjects(ctx context.Context, repo int64, r *FsckReport) error {
	objects, err := s.ops(ctx).AllObjects(ctx, repo)
	if err != nil {
		return err
	}
	// Then the segments': an object moved there since the tail was read is counted once.
	inTail := make(map[encoding.ID]bool, len(objects))
	for _, o := range objects {
		inTail[o.ID] = true
	}
	err = s.segmentRange(ctx, repoScope(repo), segObjects, nil, nil, func(key, value []byte) error {
		id := encoding.ID(key)
		if inTail[id] {
			return nil
		}
		if len(value) == 0 {
			r.problem("object %s in a segment has no kind", id)
			return nil
		}
		objects = append(objects, backend.Object{ID: id, Kind: encoding.Kind(value[0]), Body: bytes.Clone(value[1:])})
		return nil
	})
	if err != nil {
		return err
	}
	copyParents := map[encoding.ID][]encoding.ID{}
	for _, row := range objects {
		r.Objects++
		o, err := verifyBody(row.Kind, row.ID, row.Body)
		if err != nil {
			r.problem("%s %s: %v", row.Kind, row.ID, err)
			continue
		}
		if c, ok := o.(*encoding.CopyHistory); ok {
			copyParents[row.ID] = c.Parents
		}
	}
	indexed := map[encoding.ID]map[encoding.ID]bool{}
	edges, err := s.ops(ctx).AllCopyParents(ctx, repo)
	if err != nil {
		return err
	}
	err = s.segmentRange(ctx, repoScope(repo), segCopyEdges, nil, nil, func(key, _ []byte) error {
		if len(key) != 2*encoding.IDLen {
			r.problem("a copy edge in a segment is %d bytes", len(key))
			return nil
		}
		edges = append(edges, [2]encoding.ID{encoding.ID(key[encoding.IDLen:]), encoding.ID(key[:encoding.IDLen])})
		return nil
	})
	if err != nil {
		return err
	}
	for _, e := range edges {
		c, p := e[0], e[1]
		if indexed[c] == nil {
			indexed[c] = map[encoding.ID]bool{}
		}
		indexed[c][p] = true
	}
	for c, parents := range copyParents {
		want := map[encoding.ID]bool{}
		for _, p := range parents {
			want[p] = true
		}
		if !sameSet(want, indexed[c]) {
			r.problem("copy history %s: copy_parent rows disagree with its parents", c)
		}
	}
	return nil
}

func sameSet(a, b map[encoding.ID]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func (s *Store) fsckCommits(ctx context.Context, repo int64, r *FsckReport) error {
	stored, err := s.ops(ctx).AllCommits(ctx, repo)
	if err != nil {
		return err
	}
	// Then the segments': bodies, and the commit graph's generations and parents, which stand for
	// commit_parent's rows. A commit moved there since the tail was read is counted once.
	inTail := make(map[encoding.ID]bool, len(stored))
	for _, c := range stored {
		inTail[c.ID] = true
	}
	moved := map[encoding.ID]backend.GraphCommit{}
	err = s.segmentRange(ctx, repoScope(repo), segCommitGraph, nil, nil, func(key, value []byte) error {
		g, err := decodeGraph(encoding.ID(key), value)
		if err != nil {
			r.problem("%v", err)
			return nil
		}
		if !inTail[g.ID] {
			moved[g.ID] = g
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = s.segmentRange(ctx, repoScope(repo), segCommitBodies, nil, nil, func(key, value []byte) error {
		id := encoding.ID(key)
		if inTail[id] {
			return nil
		}
		g, ok := moved[id]
		if !ok {
			r.problem("commit %s: a body in a segment, not in the commit graph", id)
			return nil
		}
		stored = append(stored, backend.StoredCommit{ID: id, Body: bytes.Clone(value), Generation: g.Generation})
		delete(moved, id)
		return nil
	})
	if err != nil {
		return err
	}
	for id := range moved {
		r.problem("commit %s: in the commit graph's segments with no body", id)
	}
	type info struct {
		parents []encoding.ID
		gen     int64
	}
	commits := map[encoding.ID]info{}
	for _, row := range stored {
		r.Commits++
		o, err := verifyBody(encoding.KindCommit, row.ID, row.Body)
		if err != nil {
			r.problem("commit %s: %v", row.ID, err)
			continue
		}
		commits[row.ID] = info{parents: o.(*encoding.Commit).Parents, gen: row.Generation}
	}
	indexed := map[encoding.ID][]encoding.ID{}
	edges, err := s.ops(ctx).AllCommitParents(ctx, repo)
	if err != nil {
		return err
	}
	for _, e := range edges {
		indexed[e[0]] = append(indexed[e[0]], e[1])
	}
	err = s.segmentRange(ctx, repoScope(repo), segCommitGraph, nil, nil, func(key, value []byte) error {
		g, err := decodeGraph(encoding.ID(key), value)
		if err == nil && !inTail[g.ID] {
			indexed[g.ID] = g.Parents
		}
		return nil
	})
	if err != nil {
		return err
	}
	for id, c := range commits {
		if fmt.Sprint(indexed[id]) != fmt.Sprint(c.parents) {
			r.problem("commit %s: commit_parent rows disagree with its parents", id)
		}
		var want int64
		for _, p := range c.parents {
			pg := int64(0)
			if p != encoding.RootCommitID {
				parent, ok := commits[p]
				if !ok {
					r.problem("commit %s: parent %s is missing", id, p)
					continue
				}
				pg = parent.gen
			}
			want = max(want, pg+1)
		}
		if c.gen != want {
			r.problem("commit %s: generation %d, parents imply %d", id, c.gen, want)
		}
	}
	return nil
}

func (s *Store) fsckOperations(ctx context.Context, repo int64, r *FsckReport) error {
	for _, t := range []struct {
		kind  encoding.Kind
		count *int
	}{
		{encoding.KindOperation, &r.Operations},
		{encoding.KindView, &r.Views},
	} {
		rows, err := s.repositoryOps(ctx, repo, t.kind)
		if err != nil {
			return err
		}
		for _, row := range rows {
			*t.count++
			if _, err := verifyBody(t.kind, row.ID, row.Body); err != nil {
				r.problem("%s %s: %v", t.kind, row.ID, err)
			}
		}
	}
	return nil
}

// String renders the report for the fsck command.
func (r *FsckReport) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d packs, %d chunks, %d files, %d objects, %d commits, %d operations, %d views\n",
		r.Packs, r.Chunks, r.Files, r.Objects, r.Commits, r.Operations, r.Views)
	fmt.Fprintf(&b, "%d dead pack bytes, %d orphan packs\n", r.DeadBytes, len(r.OrphanPacks))
	fmt.Fprintf(&b, "%d content index entries\n", r.IndexEntries)
	for _, o := range r.OrphanPacks {
		fmt.Fprintf(&b, "orphan pack: %s\n", o)
	}
	for _, p := range r.Repaired {
		fmt.Fprintf(&b, "repaired: %s\n", p)
	}
	for _, p := range r.Problems {
		fmt.Fprintf(&b, "PROBLEM: %s\n", p)
	}
	return b.String()
}
