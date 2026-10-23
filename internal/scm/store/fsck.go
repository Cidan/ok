package store

import (
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
	if err := s.repositoryExists(ctx, s.b, repo); err != nil {
		return nil, err
	}
	r := &FsckReport{}
	if err := s.fsckPacks(ctx, repo, repair, r); err != nil {
		return nil, err
	}
	if err := s.fsckFiles(ctx, repo, r); err != nil {
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

func (s *Store) fsckPacks(ctx context.Context, repo int64, repair bool, r *FsckReport) error {
	live, err := s.b.LivePacks(ctx, repo)
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
	retired, err := s.b.RetiredPackIDs(ctx, repo)
	if err != nil {
		return err
	}
	for _, id := range retired {
		known[packKey(repo, id)] = true
	}
	for _, pr := range packs {
		known[packKey(repo, pr.id)] = true
		r.Packs++
		if err := s.fsckPack(ctx, repo, pr, repair, r); err != nil {
			return err
		}
	}
	objs, err := s.blobs.List(ctx, fmt.Sprintf("repos/%d/packs/", repo))
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

func (s *Store) fsckPack(ctx context.Context, repo int64, pr packRow, repair bool, r *FsckReport) error {
	f, err := os.CreateTemp("", "ok scm-fsck-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	size, err := s.blobs.Download(ctx, packKey(repo, pr.id), f)
	if err != nil {
		r.problem("pack %s: %v", pr.id, err)
		return nil
	}
	if size != pr.size {
		r.problem("pack %s: %d bytes stored, row says %d", pr.id, size, pr.size)
	}
	records, err := pack.Verify(f, size, pr.id, s.dicts)
	if err != nil {
		r.problem("pack %s: %v", pr.id, err)
		return nil
	}
	byID := map[encoding.ID]pack.Record{}
	for _, rec := range records {
		byID[rec.ID] = rec
	}
	// The index's chunks in this pack must agree with it.
	li := s.live(repo)
	if err := s.refreshIndex(ctx, repo, li); err != nil {
		return err
	}
	x := li.x.Load()
	pointed := map[encoding.ID]bool{}
	var here []contentindex.Keyed
	x.Range(func(id encoding.ID, e contentindex.Entry) bool {
		if e.Kind == encoding.KindChunk && e.Loc.Pack == pr.num {
			here = append(here, contentindex.Keyed{ID: id, Entry: e})
		}
		return true
	})
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
	// Records no entry points at are dead, unless a file uses the chunk and the index lacks
	// it, which repair rebuilds from this record.
	var orphaned []encoding.ID
	for _, rec := range records {
		if !pointed[rec.ID] {
			orphaned = append(orphaned, rec.ID)
		}
	}
	rowless := map[encoding.ID]bool{}
	if len(orphaned) > 0 {
		present, err := s.existing(ctx, s.b, repo, tChunk, 0, orphaned)
		if err != nil {
			return err
		}
		want := map[encoding.ID]bool{}
		for _, id := range orphaned {
			want[id] = true
		}
		used := map[encoding.ID]bool{}
		x.Range(func(_ encoding.ID, e contentindex.Entry) bool {
			for _, c := range e.Chunks {
				if want[c.ID] {
					used[c.ID] = true
				}
			}
			return true
		})
		for _, id := range orphaned {
			if !present[id] && used[id] {
				rowless[id] = true
			} else {
				r.DeadBytes += int64(byID[id].StoredLength) + pack.RecordHeaderLen
			}
		}
	}
	for id := range rowless {
		if !repair {
			r.problem("chunk %s: used by a file but not in the index (pack %s holds it)", id, pr.id)
			continue
		}
		rec := byID[id]
		inserted := false
		err := s.inTx(ctx, func(tx *txn) error {
			lg, err := s.txIndex(ctx, tx, repo)
			if err != nil {
				return err
			}
			if _, ok := lg.has(encoding.KindChunk, id); ok {
				return nil
			}
			inserted = true
			s.logIndex(tx, repo, contentindex.Update{Op: contentindex.Put, ID: id, Entry: contentindex.Entry{
				Kind: encoding.KindChunk, Day: lg.today,
				Loc: contentindex.Location{Pack: pr.num, Offset: rec.Offset, Stored: rec.StoredLength, Raw: rec.RawLength, Codec: uint8(rec.Codec)},
			}})
			return nil
		})
		if err != nil {
			return err
		}
		if !inserted {
			// A concurrent publish stored it first, into its own pack.
			r.DeadBytes += int64(rec.StoredLength) + pack.RecordHeaderLen
			continue
		}
		r.Chunks++
		r.Repaired = append(r.Repaired, fmt.Sprintf("rebuilt chunk %s from pack %s", id, pr.id))
	}
	return nil
}

// fsckWindow is how many chunks fsck reads at once (at most 16 MiB).
const fsckWindow = 64

func (s *Store) fsckFiles(ctx context.Context, repo int64, r *FsckReport) error {
	li := s.live(repo)
	if err := s.refreshIndex(ctx, repo, li); err != nil {
		return err
	}
	var files []encoding.ID
	for _, k := range li.x.Load().Sorted() {
		switch {
		case k.Entry.Kind == encoding.KindFile && k.Entry.Body:
			files = append(files, k.ID)
		case !k.Entry.Body:
			r.problem("content index: %s %s has only a touch or a pin", k.Entry.Kind, k.ID)
		}
	}
	r.IndexEntries = li.x.Load().Len()
	for _, id := range files {
		r.Files++
		size, chunks, _, err := s.fileChunks(ctx, repo, id)
		if err != nil {
			return err
		}
		var expect uint64
		h := encoding.NewHasher(encoding.KindFile)
		ok := true
		for start := 0; start < len(chunks) && ok; start += fsckWindow {
			window := chunks[start:min(start+fsckWindow, len(chunks))]
			members := make([]encoding.ID, len(window))
			for i, c := range window {
				members[i] = c.id
			}
			data, err := s.chunkBytes(ctx, repo, members)
			if err != nil {
				r.problem("file %s: %v", id, err)
				ok = false
				break
			}
			for _, c := range window {
				if c.offset != expect || uint64(len(data[c.id])) != c.length {
					r.problem("file %s: chunk %s at %d (%d bytes) breaks the file's layout", id, c.id, c.offset, len(data[c.id]))
					ok = false
					break
				}
				expect += c.length
				h.Write(data[c.id])
			}
		}
		if !ok {
			continue
		}
		if expect != size {
			r.problem("file %s: chunks total %d bytes, the index says %d", id, expect, size)
		} else if got := h.Sum(); got != id {
			r.problem("file %s: contents hash to %s", id, got)
		}
	}
	return nil
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
	objects, err := s.b.AllObjects(ctx, repo)
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
	edges, err := s.b.AllCopyParents(ctx, repo)
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
	stored, err := s.b.AllCommits(ctx, repo)
	if err != nil {
		return err
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
	edges, err := s.b.AllCommitParents(ctx, repo)
	if err != nil {
		return err
	}
	for _, e := range edges {
		indexed[e[0]] = append(indexed[e[0]], e[1])
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
		read  func(backend.Ops, context.Context, int64) ([]backend.Object, error)
		count *int
	}{
		{encoding.KindOperation, backend.Ops.RepositoryOperations, &r.Operations},
		{encoding.KindView, backend.Ops.RepositoryViews, &r.Views},
	} {
		rows, err := t.read(s.b, ctx, repo)
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
