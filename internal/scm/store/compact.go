package store

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// Compaction's parameters (docs/scm.md, "Garbage collection and compaction").
const (
	// smallPackBytes is the size below which packs are merged.
	smallPackBytes = 16 << 20
	// maxPackBytes bounds a pack compaction writes.
	maxPackBytes = 256 << 20
	// maxCompactionRead bounds the source pack bytes one pass reads in a repository; the
	// rest waits for the next pass.
	maxCompactionRead = 1 << 30
	// minLoose is how many records a publish's pack holds before compaction frames it, alone:
	// fewer wait to be merged with another's.
	minLoose = 16
	// frameBytes bounds the raw bytes of small files compaction puts in one frame. Measured on the
	// monorepo's sources, frames of 1 MiB at SpeedBestCompression store 27% less than each file
	// compressed alone at SpeedDefault, as publishes store them; frames of 4 MiB 2% less again, for
	// four times the bytes a read of one file decodes.
	frameBytes = 1 << 20
	// historyCommits bounds how far back along main one pass looks for the paths of files' older
	// versions, which frame them by path; what it does not reach is framed by ID.
	historyCommits = 4096
)

type compactPack struct {
	id   encoding.ID
	num  uint64
	size int64
	// live is its live records' footprints (pack.Location.Footprint); records, how many the index
	// locates in it.
	live    int64
	records int
	// old is a pack of records, versions 1 and 2, not frames: compaction rewrites every one.
	old bool
	// compacted is a pack compaction wrote, not a publish.
	compacted bool
}

// loose is a publish's pack of minLoose records or more: worth framing whatever else it is.
func (p compactPack) loose() bool { return !p.compacted && p.records >= minLoose }

// dead is what a pack holds that no location in the index points at: its size less its live
// records' footprints and its header and footer.
func (p compactPack) dead() int64 {
	return p.size - p.live - pack.PackOverhead
}

// The order compaction writes records in, by group: frames of many hold records of one group.
const (
	// groupHead is chunks of files on main's head, by path: what checkouts and builds read.
	groupHead = iota
	// groupHistory is chunks of main's files' older versions, by path, oldest first: versions of a
	// file compress against each other in a frame as one does against another.
	groupHistory
	// groupRest is every other chunk — work not landed, other repositories' — by ID.
	groupRest
	// groupTrees is trees, each read alone, as a walk reads them.
	groupTrees
)

// compactChunk is a record compaction copies: a chunk's, or a tree's (kind), with where it goes.
type compactChunk struct {
	id   encoding.ID
	kind encoding.Kind
	pack encoding.ID
	loc  pack.Location
	// group, and within it path, gen, fileID, and offset, order it: a file that holds it — its
	// path, the generation of main that changed it, its ID — and the chunk's offset in it.
	group  int
	path   string
	gen    int64
	fileID encoding.ID
	offset int64
	// small is the whole of a file the repository holds: framed with others of its group.
	small bool
}

// compact rewrites a network's packs (docs/scm.md, "Garbage collection and compaction"), from the
// content index: packs no chunk or tree points at are retired outright; packs of records — versions
// 1 and 2 — packs more than half dead, the small packs when there are two or more, and publishes'
// packs of minLoose records or more, are rewritten into packs of up to maxPackBytes holding only their live chunks and trees, in frames.
// The repository's small files go many to a frame of up to frameBytes, at SpeedBestCompression
// with the network's newest dictionary — files on main's head by path, then older versions of
// main's files by path, then the rest — and every other record, a large file's chunk or a tree, in
// a frame of its own at SpeedBestCompression. Each new pack is written to S3, read back, then one
// transaction moves its records to it, in the index; the source packs left with nothing live are
// retired once every new pack is written. Readers that located a chunk in a retired pack still read
// it: a retired pack stays in S3 for RetiredPackGrace.
func (s *Store) compact(ctx context.Context, repo int64, opts GCOptions, r *GCReport) error {
	net, netScope, err := s.networkOf(ctx, repo)
	if err != nil {
		return err
	}
	v, err := s.contentView(ctx, netScope)
	if err != nil {
		return err
	}
	type packStats struct {
		records int
		live    int64
		old     bool
	}
	stats := map[uint64]*packStats{}
	err = v.each(ctx, func(_ encoding.ID, e contentindex.Entry) bool {
		if !contentindex.Located(e.Kind) {
			return true
		}
		st := stats[e.Loc.Pack]
		if st == nil {
			st = &packStats{}
			stats[e.Loc.Pack] = st
		}
		st.records++
		st.live += packLocation(e.Loc).Footprint()
		st.old = st.old || !e.Loc.Framed
		return true
	})
	if err != nil {
		return err
	}
	live, err := s.ops(ctx).LivePacks(ctx, net)
	if err != nil {
		return err
	}
	var empty, old, sparse, small, loose []compactPack
	for _, row := range live {
		p := compactPack{id: row.ID, num: row.Num, size: row.Size, compacted: row.Compacted}
		if st := stats[p.num]; st != nil {
			p.records, p.live, p.old = st.records, st.live, st.old
		}
		switch {
		case p.records == 0:
			empty = append(empty, p)
		case p.old:
			old = append(old, p)
		case p.dead()*2 > p.size:
			sparse = append(sparse, p)
		case p.size < smallPackBytes:
			small = append(small, p)
		case p.loose():
			loose = append(loose, p)
		}
	}
	if len(empty) > 0 {
		ids := make([]encoding.ID, len(empty))
		for i, p := range empty {
			ids[i] = p.id
		}
		n, bytes, err := s.retireDrained(ctx, net, ids, opts.DryRun)
		if err != nil {
			return err
		}
		r.PacksRetired += n
		r.BytesReclaimed += bytes
	}
	// The sparsest packs first, then the small ones, oldest first — a lone small pack has nothing to
	// merge with, but a publish's to frame — then publishes', then packs of records.
	sort.Slice(sparse, func(i, j int) bool { return sparse[i].dead()*sparse[j].size > sparse[j].dead()*sparse[i].size })
	sources := sparse
	if len(small) >= 2 || (len(small) == 1 && small[0].loose()) {
		sources = append(sources, small...)
	}
	sources = append(sources, loose...)
	sources = append(sources, old...)
	var budget int64
	var chosen []compactPack
	for _, p := range sources {
		if len(chosen) > 0 && budget+p.size > maxCompactionRead {
			break
		}
		chosen = append(chosen, p)
		budget += p.size
	}
	if len(chosen) == 0 {
		return nil
	}

	chunks, err := s.compactChunks(ctx, repo, v, chosen)
	if err != nil {
		return err
	}
	// Outputs of up to maxPackBytes, in order, as their records stand now: frames only shrink them.
	var outputs [][]compactChunk
	var cur []compactChunk
	var curBytes int64
	for _, c := range chunks {
		n := c.loc.Footprint()
		if len(cur) > 0 && curBytes+n > maxPackBytes {
			outputs = append(outputs, cur)
			cur, curBytes = nil, 0
		}
		cur = append(cur, c)
		curBytes += n
	}
	if len(cur) > 0 {
		outputs = append(outputs, cur)
	}
	sourceIDs := make([]encoding.ID, len(chosen))
	sizes := make(map[encoding.ID]int64, len(chosen))
	for i, p := range chosen {
		sourceIDs[i] = p.id
		sizes[p.id] = p.size
		if p.old {
			r.PacksUpgraded++
		}
	}
	if opts.DryRun {
		var in, out int64
		for _, p := range chosen {
			in += p.size
		}
		for _, o := range outputs {
			out += pack.PackOverhead
			for i, c := range o {
				if i == 0 || c.pack != o[i-1].pack || c.loc.DataOffset() != o[i-1].loc.DataOffset() {
					r.BytesRead += int64(c.loc.StoredLength)
				}
				out += c.loc.Footprint()
			}
		}
		r.PacksWritten += len(outputs)
		r.PacksRetired += len(chosen)
		r.BytesReclaimed += in - out
		return nil
	}

	encs, err := s.compactEncoders(ctx, repo)
	if err != nil {
		return err
	}
	defer encs.close()
	var written int64
	for _, out := range outputs {
		size, err := s.writeCompacted(ctx, net, out, sizes, encs, r)
		if err != nil {
			return err
		}
		r.PacksWritten++
		written += size
	}
	retired, freed, err := s.retireDrained(ctx, net, sourceIDs, false)
	if err != nil {
		return err
	}
	r.PacksRetired += retired
	r.BytesReclaimed += freed - written
	r.Dictionary = encs.dictionary
	return nil
}

// compactChunks lists the live chunks and trees of the source packs, from the content index, in the
// order compaction writes them (the groups), and which are the whole of a small file of the
// repository's.
func (s *Store) compactChunks(ctx context.Context, repo int64, v *contentView, sources []compactPack) ([]compactChunk, error) {
	byNum := make(map[uint64]encoding.ID, len(sources))
	for _, p := range sources {
		byNum[p.num] = p.id
	}
	head, err := s.mainPaths(ctx, repo)
	if err != nil {
		return nil, err
	}
	files, err := s.contentView(ctx, repoScope(repo))
	if err != nil {
		return nil, err
	}
	// One scan of the network's chunk index for the records in the source packs, and one of the
	// repository's content index for its files, whose chunks are matched to them after.
	var out []compactChunk
	at := map[encoding.ID]int{}
	err = v.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
		if pid, ok := byNum[e.Loc.Pack]; ok && contentindex.Located(e.Kind) {
			group := groupRest
			if e.Kind == encoding.KindTree {
				group = groupTrees
			}
			at[id] = len(out)
			out = append(out, compactChunk{id: id, kind: e.Kind, pack: pid, loc: packLocation(e.Loc), group: group})
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	type heldFile struct {
		id     encoding.ID
		chunks []contentindex.FileChunk
	}
	label := func(f heldFile, group int, path string, gen int64) {
		var off int64
		for _, fc := range f.chunks {
			if i, ok := at[fc.ID]; ok && out[i].kind == encoding.KindChunk {
				c := &out[i]
				if group < c.group || (group == c.group && (path < c.path || (path == c.path && bytes.Compare(f.id[:], c.fileID[:]) < 0))) {
					c.group, c.path, c.gen, c.fileID, c.offset = group, path, gen, f.id, off
				}
			}
			off += int64(fc.Length)
		}
	}
	var older []heldFile
	err = files.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
		if e.Kind != encoding.KindFile {
			return true
		}
		f := heldFile{id: id, chunks: e.Chunks}
		if len(e.Chunks) == 1 {
			if i, ok := at[e.Chunks[0].ID]; ok && out[i].kind == encoding.KindChunk {
				out[i].small = true
			}
		}
		if path, ok := head[id]; ok {
			label(f, groupHead, path, 0)
			return true
		}
		for _, fc := range e.Chunks {
			if _, ok := at[fc.ID]; ok {
				older = append(older, f)
				break
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if len(older) > 0 {
		want := make(map[encoding.ID]bool, len(older))
		for _, f := range older {
			want[f.id] = true
		}
		hist, err := s.historyPaths(ctx, repo, want)
		if err != nil {
			return nil, err
		}
		for _, f := range older {
			if h, ok := hist[f.id]; ok {
				label(f, groupHistory, h.path, h.gen)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.group != b.group {
			return a.group < b.group
		}
		if a.path != b.path {
			return a.path < b.path
		}
		if a.gen != b.gen {
			return a.gen < b.gen
		}
		if a.fileID != b.fileID {
			return bytes.Compare(a.fileID[:], b.fileID[:]) < 0
		}
		if a.offset != b.offset {
			return a.offset < b.offset
		}
		return bytes.Compare(a.id[:], b.id[:]) < 0
	})
	return out, nil
}

// mainPaths maps each file on main to its first path there.
func (s *Store) mainPaths(ctx context.Context, repo int64) (map[encoding.ID]string, error) {
	r, err := s.GetRepository(ctx, repo)
	if err != nil {
		return nil, err
	}
	out := map[encoding.ID]string{}
	if r.Head == encoding.RootCommitID {
		return out, nil
	}
	err = s.Export(ctx, repo, r.Head, nil, false, func(e *ExportEntry, _ []byte) error {
		if e.Value.Kind == encoding.TreeValueFile {
			if _, seen := out[e.Value.ID]; !seen {
				out[e.Value.ID] = e.Path
			}
		}
		return nil
	})
	return out, err
}

// historyPath is where a file was on main: a path, and the generation of the commit that changed
// it there.
type historyPath struct {
	path string
	gen  int64
}

// historyCommitBatch is how many of main's commits historyPaths diffs at once, a level of their
// trees to one read.
const historyCommitBatch = 256

// historyPaths finds, of the files in want, where each was on main, walking back from main's head
// up to historyCommits commits, each diffed from its parent, and stopping once it has them all. A
// file in no diff it reached is absent.
func (s *Store) historyPaths(ctx context.Context, repo int64, want map[encoding.ID]bool) (map[encoding.ID]historyPath, error) {
	out := map[encoding.ID]historyPath{}
	top, err := s.mainGeneration(ctx, repo, nil)
	if err != nil || top == 0 {
		return out, err
	}
	lo := max(1, top-historyCommits+1)
	rows, err := s.mainLog(ctx, repo, max(1, lo-1), top, false, int(top-max(1, lo-1)+1))
	if err != nil {
		return nil, err
	}
	trees := map[int64]encoding.ID{0: encoding.EmptyTreeID}
	ts := treeReader{ctx: ctx, s: s, repo: repo}
	treeOf := func(gen int64, commit encoding.ID) error {
		if _, ok := trees[gen]; ok {
			return nil
		}
		t, err := s.rootTreeOf(ctx, repo, commit)
		trees[gen] = t
		return err
	}
	byGen := make(map[int64]encoding.ID, len(rows))
	for _, row := range rows {
		byGen[row.Generation] = row.Commit
	}
	for start := top; start >= lo && len(out) < len(want); start -= historyCommitBatch {
		end := max(lo, start-historyCommitBatch+1)
		var pairs [][2]encoding.ID
		var gens []int64
		for g := start; g >= end; g-- {
			c, ok := byGen[g]
			if !ok {
				continue
			}
			if err := treeOf(g, c); err != nil {
				return nil, err
			}
			if p, ok := byGen[g-1]; ok {
				if err := treeOf(g-1, p); err != nil {
					return nil, err
				}
			} else if g-1 != 0 {
				continue
			}
			pairs = append(pairs, [2]encoding.ID{trees[g-1], trees[g]})
			gens = append(gens, g)
		}
		read, err := prefetchDiffs(ts, pairs)
		if err != nil {
			return nil, err
		}
		pre := prefetched{treeStore: ts, trees: read}
		for i, p := range pairs {
			g := gens[i]
			err := walkDiff(pre, p[0], p[1], "", func(ch leafChange) error {
				for _, val := range []*encoding.TreeValue{ch.Before, ch.After} {
					if val == nil || val.Kind != encoding.TreeValueFile || !want[val.ID] {
						continue
					}
					if _, seen := out[val.ID]; !seen {
						out[val.ID] = historyPath{path: ch.Path, gen: g}
					}
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// compactEncoders are what compaction compresses frames with: frames of many at
// SpeedBestCompression with the network's newest dictionary, if it has one — which a record of the
// frame is sent alone with (sendable) — and records alone at SpeedBestCompression, a past of their
// own.
type compactEncoders struct {
	many, alone *zstd.Encoder
	dictionary  uint32
}

func (s *Store) compactEncoders(ctx context.Context, repo int64) (*compactEncoders, error) {
	alone, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	encs := &compactEncoders{many: alone, alone: alone}
	id, body, _, err := s.newestDictionary(ctx, repo)
	if err != nil {
		alone.Close()
		return nil, err
	}
	if body != nil {
		if encs.many, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression), zstd.WithEncoderDict(body),
			zstd.WithEncoderCRC(true), zstd.WithEncoderConcurrency(1)); err != nil {
			alone.Close()
			return nil, fmt.Errorf("dictionary %d: %w", id, err)
		}
		encs.dictionary = id
	}
	return encs, nil
}

func (e *compactEncoders) close() {
	if e.many != e.alone {
		e.many.Close()
	}
	e.alone.Close()
}

// compactWindow bounds the source bytes compaction holds at once: the records of an output it reads
// together, through the read path's range reads (readLimits).
const compactWindow = 32 << 20

// sourceFrames is how many decoded frames of source packs compaction keeps while it copies their
// records: they are read in order, so a frame's records are most often read one after another.
const sourceFrames = 16

// writeCompacted writes one output pack of chunks, read from their source packs (of sizes) a window
// at a time, to the bucket as it is written, reads it back, and records it.
func (s *Store) writeCompacted(ctx context.Context, net backend.Network, chunks []compactChunk, sizes map[encoding.ID]int64, encs *compactEncoders, r *GCReport) (int64, error) {
	st, err := s.blobs.Stage(ctx, staging(packPrefix(net)))
	if err != nil {
		return 0, err
	}
	defer st.Abort()
	dicts := s.dicts.of(net)
	written := sha256.New()
	pw := pack.NewWriter(io.MultiWriter(st, written))
	locs := make([]pack.Location, len(chunks))

	type frameKey struct {
		pack encoding.ID
		off  int64
	}
	frames := map[frameKey][]byte{}
	var order []frameKey
	raw := func(c compactChunk, stored []byte) ([]byte, error) {
		if c.loc.Whole() {
			return pack.DecodeObject(dicts, c.kind, c.id, c.loc.Codec, c.loc.RawLength, stored)
		}
		k := frameKey{c.pack, c.loc.DataOffset()}
		frame, ok := frames[k]
		if !ok {
			var err error
			if frame, err = pack.DecodeFrame(dicts, c.loc, stored); err != nil {
				return nil, err
			}
			if len(order) == sourceFrames {
				delete(frames, order[0])
				order = order[1:]
			}
			frames[k] = frame
			order = append(order, k)
		}
		b, err := pack.RecordOf(frame, c.loc)
		if err == nil {
			err = pack.Check(c.kind, c.id, b)
		}
		return b, err
	}

	// The frame of many being gathered: its records' places in chunks, and their raw bytes.
	var pending []int
	var members []pack.Member
	var pendingBytes int
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		got, err := pw.AddFrame(dicts, encs.many, members)
		if err != nil {
			return err
		}
		for j, i := range pending {
			locs[i] = got[j]
		}
		r.FramesWritten++
		r.RecordsFramed += int64(len(pending))
		pending, members, pendingBytes = nil, nil, 0
		return nil
	}
	for start := 0; start < len(chunks); {
		end, window := start, int64(0)
		for end < len(chunks) && (end == start || window+int64(chunks[end].loc.StoredLength) <= compactWindow) {
			window += int64(chunks[end].loc.StoredLength)
			end++
		}
		stored, read, err := s.readCompactSources(ctx, net, chunks[start:end], sizes)
		if err != nil {
			return 0, err
		}
		r.BytesRead += read
		for i := start; i < end; i++ {
			c := chunks[i]
			data := stored[i-start]
			// An incompressible record alone stays as it is stored.
			if !c.small && c.loc.Whole() && c.loc.Codec == pack.Stored {
				if err := flush(); err != nil {
					return 0, err
				}
				if locs[i], err = pw.AddStored(dicts, c.kind, c.id, c.loc.Codec, c.loc.RawLength, data); err != nil {
					return 0, fmt.Errorf("pack %s: %s %s: %w", c.pack, c.kind, c.id, err)
				}
				r.FramesWritten++
				continue
			}
			b, err := raw(c, data)
			if err != nil {
				return 0, fmt.Errorf("pack %s: %w", c.pack, err)
			}
			m := pack.Member{Kind: c.kind, ID: c.id, Raw: b}
			if c.small {
				if len(pending) > 0 && (chunks[pending[0]].group != c.group || pendingBytes+len(b) > frameBytes) {
					if err := flush(); err != nil {
						return 0, err
					}
				}
				pending, members, pendingBytes = append(pending, i), append(members, m), pendingBytes+len(b)
				continue
			}
			if err := flush(); err != nil {
				return 0, err
			}
			got, err := pw.AddFrame(dicts, encs.alone, []pack.Member{m})
			if err != nil {
				return 0, fmt.Errorf("pack %s: %s %s: %w", c.pack, c.kind, c.id, err)
			}
			locs[i] = got[0]
			r.FramesWritten++
		}
		start = end
	}
	if err := flush(); err != nil {
		return 0, err
	}
	packID, size, err := pw.Close()
	if err != nil {
		return 0, err
	}
	if err := st.Commit(ctx, packKey(net, packID)); err != nil {
		return 0, err
	}
	if err := s.checkStored(ctx, packKey(net, packID), written.Sum(nil), size); err != nil {
		return 0, err
	}

	li := s.live(backend.NetworkScope(net))
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		// No transaction changes the index while this one moves chunks in it, and the index
		// read after the lock holds every change before.
		if _, _, err := tx.lockIndex(ctx, backend.NetworkScope(net)); err != nil {
			return err
		}
		lg, err := s.txIndex(ctx, tx, backend.NetworkScope(net))
		if err != nil {
			return err
		}
		num, err := tx.InsertPack(ctx, net, packID, size)
		if err != nil {
			return err
		}
		ids := make([]encoding.ID, len(chunks))
		for i, c := range chunks {
			ids[i] = c.id
		}
		if err := lg.load(ctx, ids); err != nil {
			return err
		}
		var live int64
		for i, c := range chunks {
			e, ok, err := lg.has(ctx, c.kind, c.id)
			if err != nil {
				return err
			}
			// Still where it was copied from: the records of a frame share its offset, so the whole of
			// the location says.
			if !ok || packLocation(e.Loc) != c.loc {
				continue
			}
			if pid, err := s.packID(ctx, net, li, e.Loc.Pack); err != nil || pid != c.pack {
				if err != nil {
					return err
				}
				continue
			}
			s.logIndex(tx, backend.NetworkScope(net), contentindex.Update{Op: contentindex.Move, ID: c.id, Entry: contentindex.Entry{
				Kind: c.kind, Loc: indexLocation(num, locs[i]),
			}})
			live += locs[i].Footprint()
		}
		if live == 0 {
			// Every chunk it copied moved or went meanwhile: it holds nothing live.
			return tx.RetirePack(ctx, net, packID)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return size, nil
}

// retireDrained retires the packs among ids that no chunk or tree in the content index is in, and
// returns how many and their total size.
func (s *Store) retireDrained(ctx context.Context, net backend.Network, packIDs []encoding.ID, dryRun bool) (int, int64, error) {
	v, err := s.contentView(ctx, backend.NetworkScope(net))
	if err != nil {
		return 0, 0, err
	}
	used := map[uint64]bool{}
	err = v.each(ctx, func(_ encoding.ID, e contentindex.Entry) bool {
		if contentindex.Located(e.Kind) {
			used[e.Loc.Pack] = true
		}
		return true
	})
	if err != nil {
		return 0, 0, err
	}
	nums, err := s.ops(ctx).LivePackNumbers(ctx, net, packIDs)
	if err != nil {
		return 0, 0, err
	}
	var drained []encoding.ID
	for _, id := range packIDs {
		if num, ok := nums[id]; ok && !used[num] {
			drained = append(drained, id)
		}
	}
	if len(drained) == 0 {
		return 0, 0, nil
	}
	var n int
	var bytes int64
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		var err error
		n, bytes, err = tx.RetirePacks(ctx, net, drained, dryRun)
		return err
	})
	return n, bytes, err
}

// readCompactSources reads chunks' stored bytes — their frames', or their records' — from their
// source packs (of sizes), in their order: stored bytes close together in a source read with one
// range GET, as the read path reads them, and up to fetchesAtOnce GETs at once. It returns the bytes
// it fetched too.
func (s *Store) readCompactSources(ctx context.Context, net backend.Network, chunks []compactChunk, sizes map[encoding.ID]int64) ([][]byte, int64, error) {
	type item struct {
		i   int
		off int64
		n   int64
	}
	byPack := map[encoding.ID][]item{}
	for i, c := range chunks {
		byPack[c.pack] = append(byPack[c.pack], item{i, c.loc.DataOffset(), int64(c.loc.StoredLength)})
	}
	type rangeRead struct {
		pack  encoding.ID
		items []item
	}
	var reads []rangeRead
	for p, items := range byPack {
		slices.SortFunc(items, func(a, b item) int { return cmp.Compare(a.off, b.off) })
		for start := 0; start < len(items); {
			end := start + 1
			for end < len(items) {
				prev, next := items[end-1], items[end]
				if next.off-(prev.off+prev.n) > s.reads.mergeGap || next.off+next.n-items[start].off > s.reads.maxSpan {
					break
				}
				end++
			}
			reads = append(reads, rangeRead{p, items[start:end]})
			start = end
		}
	}
	out := make([][]byte, len(chunks))
	var read atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.reads.fetchesAtOnce)
	for _, rr := range reads {
		g.Go(func() error {
			first := rr.items[0]
			var end int64
			for _, it := range rr.items {
				end = max(end, it.off+it.n)
			}
			if size, ok := sizes[rr.pack]; ok && end > size {
				return fmt.Errorf("pack %s: a record at %d past its %d bytes", rr.pack, end, size)
			}
			data, err := s.blobs.GetRange(gctx, packKey(net, rr.pack), first.off, end-first.off)
			if err != nil {
				return fmt.Errorf("pack %s: %w", rr.pack, err)
			}
			read.Add(int64(len(data)))
			for _, it := range rr.items {
				out[it.i] = data[it.off-first.off : it.off-first.off+it.n]
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, 0, err
	}
	return out, read.Load(), nil
}

// checkStored reads back the object at key and holds it to the size bytes whose hash is sum: what
// compaction moves chunks onto replaces the only other copy, which is deleted an hour after.
func (s *Store) checkStored(ctx context.Context, key string, sum []byte, size int64) error {
	read := sha256.New()
	n, err := s.blobs.Download(ctx, key, read)
	if err != nil {
		return fmt.Errorf("reading back %s: %w", key, err)
	}
	if n != size || !bytes.Equal(read.Sum(nil), sum) {
		return fmt.Errorf("%s reads back as %d bytes other than the %d written", key, n, size)
	}
	return nil
}
