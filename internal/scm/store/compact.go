package store

import (
	"cmp"
	"context"
	"fmt"
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
	// minRecompress is how many small chunks a pack holds not yet compressed with the
	// repository's dictionary before compaction rewrites it for them.
	minRecompress = 16
)

type compactPack struct {
	id         encoding.ID
	num        uint64
	size, live int64
	chunks     int // chunk rows pointing at it
	// small is how many of its chunks are small and not compressed with a dictionary, counted
	// only while the repository's newest dictionary has not considered the pack.
	small int
}

// dead is what a pack holds that no chunk row points at: its size less its live chunks'
// stored bytes and what a pack of just those spends besides (pack.Overhead). A pack
// holding a single small chunk is mostly overhead, not dead.
func (p compactPack) dead() int64 {
	return p.size - p.live - pack.Overhead(p.chunks)
}

// compactChunk is a record compaction copies: a chunk's, or a tree's (kind).
type compactChunk struct {
	id     encoding.ID
	kind   encoding.Kind
	pack   encoding.ID
	loc    pack.Location
	hinted bool        // a file holds it:
	hint   string      // that file's path,
	fileID encoding.ID // ID,
	offset int64       // and the chunk's offset in it
}

// compact rewrites a repository's packs (docs/scm.md, "Garbage collection and
// compaction"), from the content index: packs no chunk or tree points at are retired outright;
// packs more than half dead, the small packs when there are two or more, and — once the
// repository has a dictionary — packs holding small chunks not yet compressed with it are
// rewritten into packs of up to maxPackBytes holding only their live chunks and trees, the
// chunks ordered by the path of a file that holds them on main, for read locality, the trees
// after them. Records are copied as stored, but for small chunks the dictionary shrinks, which
// are re-encoded with it. Each new pack is written to S3, then one transaction moves its records
// to it, in the index; the source packs left with nothing live are retired once every new pack
// is written. Readers that located a chunk in a retired pack still read it: a
// retired pack stays in S3 for RetiredPackGrace.
func (s *Store) compact(ctx context.Context, repo int64, opts GCOptions, r *GCReport) error {
	net, netScope, err := s.networkOf(ctx, repo)
	if err != nil {
		return err
	}
	enc, dict, err := s.dictionaryEncoder(ctx, repo)
	if err != nil {
		return err
	}
	if enc != nil {
		defer enc.Close()
	}
	// A pack's small chunks are counted only while the newest dictionary has not considered it
	// (no dictionary: none is counted).
	dictRow := int64(-1)
	if enc != nil {
		dictRow = int64(dict - dictionaryIDBase)
	}
	v, err := s.contentView(ctx, netScope)
	if err != nil {
		return err
	}
	type packStats struct {
		chunks, small int
		live          int64
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
		st.chunks++
		st.live += int64(e.Loc.Stored)
		// The dictionary is a file's: a tree is copied as stored.
		if e.Kind == encoding.KindChunk && e.Loc.Raw <= DictionaryChunkMax && pack.Codec(e.Loc.Codec) != pack.ZstdDict {
			st.small++
		}
		return true
	})
	if err != nil {
		return err
	}
	live, err := s.ops(ctx).LivePacks(ctx, net)
	if err != nil {
		return err
	}
	var packs []compactPack
	for _, row := range live {
		p := compactPack{id: row.ID, num: row.Num, size: row.Size}
		considered := row.Dictionary
		if st := stats[p.num]; st != nil {
			p.chunks, p.live = st.chunks, st.live
			// Small chunks are counted only while the newest dictionary has not considered it.
			if dictRow >= 0 && (considered == nil || *considered != dictRow) {
				p.small = st.small
			}
		}
		packs = append(packs, p)
	}
	var empty, sparse, small, recompress, considered []compactPack
	for _, p := range packs {
		switch {
		case p.chunks == 0:
			empty = append(empty, p)
		case p.dead()*2 > p.size:
			sparse = append(sparse, p)
		case p.size < smallPackBytes:
			small = append(small, p)
		case p.small >= minRecompress:
			recompress = append(recompress, p)
		case enc != nil:
			considered = append(considered, p)
		}
	}
	// A pack with too few small chunks to rewrite for is marked considered by the dictionary,
	// so no pass counts its chunks again until there is a newer one.
	if len(considered) > 0 && !opts.DryRun {
		marked := make([]encoding.ID, len(considered))
		for i, p := range considered {
			marked[i] = p.id
		}
		if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
			return tx.SetPacksDictionary(ctx, net, marked, dictRow)
		}); err != nil {
			return err
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
	// The sparsest packs first, then the small ones, oldest first; a lone small pack has
	// nothing to merge with.
	sort.Slice(sparse, func(i, j int) bool { return sparse[i].dead()*sparse[j].size > sparse[j].dead()*sparse[i].size })
	sources := sparse
	if len(small) >= 2 || (len(small) == 1 && small[0].small >= minRecompress) {
		sources = append(sources, small...)
	}
	sources = append(sources, recompress...)
	var budget int64
	var chosen []compactPack
	for _, p := range sources {
		if len(chosen) > 0 && budget+p.size > maxCompactionRead {
			break
		}
		chosen = append(chosen, p)
		budget += p.size
	}
	if len(chosen) == 0 || (len(chosen) == 1 && chosen[0].dead()*2 <= chosen[0].size && chosen[0].small < minRecompress) {
		return nil
	}

	chunks, err := s.compactChunks(ctx, repo, v, chosen)
	if err != nil {
		return err
	}
	// Outputs of up to maxPackBytes, in path order.
	var outputs [][]compactChunk
	var cur []compactChunk
	var curBytes int64
	for _, c := range chunks {
		n := int64(pack.RecordHeaderLen) + int64(c.loc.StoredLength)
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
	}
	if opts.DryRun {
		for _, o := range outputs {
			for _, c := range o {
				r.BytesRead += int64(c.loc.StoredLength)
			}
		}
		r.PacksWritten += len(outputs)
		r.PacksRetired += len(chosen)
		var in, out int64
		for _, p := range chosen {
			in += p.size
		}
		for _, o := range outputs {
			out += pack.Overhead(len(o))
			for _, c := range o {
				out += int64(c.loc.StoredLength)
			}
		}
		r.BytesReclaimed += in - out
		return nil
	}

	var written int64
	for _, out := range outputs {
		size, err := s.writeCompacted(ctx, net, out, sizes, enc, dictRow, r)
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
	if enc != nil {
		r.Dictionary = dict
	}
	return nil
}

// compactChunks lists the live chunks of the source packs, from the content index, in the order
// compaction writes them: by the path on main of a file that holds each (the first in path
// order), then its offset in that file; chunks no file on main holds go last, by ID.
func (s *Store) compactChunks(ctx context.Context, repo int64, v *contentView, sources []compactPack) ([]compactChunk, error) {
	byNum := make(map[uint64]encoding.ID, len(sources))
	for _, p := range sources {
		byNum[p.num] = p.id
	}
	paths, err := s.mainPaths(ctx, repo)
	if err != nil {
		return nil, err
	}
	files, err := s.contentView(ctx, repoScope(repo))
	if err != nil {
		return nil, err
	}
	// One scan of the network's chunk index for the chunks in the source packs, and one of the
	// repository's content index for the files on main, whose chunks are matched to them after.
	type onMain struct {
		id     encoding.ID
		path   string
		chunks []contentindex.FileChunk
	}
	var out []compactChunk
	var onMainFiles []onMain
	at := map[encoding.ID]int{}
	err = v.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
		if pid, ok := byNum[e.Loc.Pack]; ok && contentindex.Located(e.Kind) {
			at[id] = len(out)
			out = append(out, compactChunk{id: id, kind: e.Kind, pack: pid, loc: pack.Location{
				Offset: e.Loc.Offset, Codec: pack.Codec(e.Loc.Codec), RawLength: e.Loc.Raw, StoredLength: e.Loc.Stored}})
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	err = files.each(ctx, func(id encoding.ID, e contentindex.Entry) bool {
		if path, ok := paths[id]; ok && e.Kind == encoding.KindFile {
			onMainFiles = append(onMainFiles, onMain{id: id, path: path, chunks: e.Chunks})
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	for _, f := range onMainFiles {
		var off int64
		for _, fc := range f.chunks {
			if i, ok := at[fc.ID]; ok {
				c := &out[i]
				if !c.hinted || f.path < c.hint || (f.path == c.hint && string(f.id[:]) < string(c.fileID[:])) {
					c.hinted, c.hint, c.fileID, c.offset = true, f.path, f.id, off
				}
			}
			off += int64(fc.Length)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.hinted != b.hinted {
			return a.hinted
		}
		if a.hint != b.hint {
			return a.hint < b.hint
		}
		if a.fileID != b.fileID {
			return string(a.fileID[:]) < string(b.fileID[:])
		}
		if a.offset != b.offset {
			return a.offset < b.offset
		}
		return string(a.id[:]) < string(b.id[:])
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

// compactWindow bounds the source bytes compaction holds at once: the records of an output it reads
// together, through the read path's range reads (readLimits).
const compactWindow = 32 << 20

// writeCompacted writes one output pack of chunks, read from their source packs (of sizes) a window
// at a time, to the bucket as it is written, and records it.
func (s *Store) writeCompacted(ctx context.Context, net backend.Network, chunks []compactChunk, sizes map[encoding.ID]int64, enc *zstd.Encoder, dictRow int64, r *GCReport) (int64, error) {
	st, err := s.blobs.Stage(ctx, staging(packPrefix(net)))
	if err != nil {
		return 0, err
	}
	defer st.Abort()
	pw := pack.NewWriter(st)
	locs := make([]pack.Location, len(chunks))
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
			codec := c.loc.Codec
			if enc != nil && c.kind == encoding.KindChunk && codec != pack.ZstdDict && c.loc.RawLength <= DictionaryChunkMax {
				raw, err := pack.Decode(s.dicts.of(net), c.id, codec, c.loc.RawLength, data)
				if err != nil {
					return 0, fmt.Errorf("pack %s: %w", c.pack, err)
				}
				if z := enc.EncodeAll(raw, nil); len(z) < len(data) {
					r.ChunksRecompressed++
					r.BytesRecompressed += int64(len(data) - len(z))
					dictionaryChunks.With("compressed").Inc()
					codec, data = pack.ZstdDict, z
				} else {
					dictionaryChunks.With("not_smaller").Inc()
				}
			}
			loc, err := pw.AddStored(s.dicts.of(net), c.kind, c.id, codec, c.loc.RawLength, data)
			if err != nil {
				return 0, fmt.Errorf("pack %s: %s %s: %w", c.pack, c.kind, c.id, err)
			}
			locs[i] = loc
		}
		start = end
	}
	packID, size, err := pw.Close()
	if err != nil {
		return 0, err
	}
	if err := st.Commit(ctx, packKey(net, packID)); err != nil {
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
		var considered *int64
		if enc != nil {
			considered = &dictRow
		}
		num, err := tx.InsertPack(ctx, net, packID, size, considered)
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
			if !ok || e.Loc.Offset != c.loc.Offset {
				continue
			}
			if pid, err := s.packID(ctx, net, li, e.Loc.Pack); err != nil || pid != c.pack {
				if err != nil {
					return err
				}
				continue
			}
			loc := locs[i]
			s.logIndex(tx, backend.NetworkScope(net), contentindex.Update{Op: contentindex.Move, ID: c.id, Entry: contentindex.Entry{
				Kind: c.kind,
				Loc:  contentindex.Location{Pack: num, Offset: loc.Offset, Stored: loc.StoredLength, Raw: loc.RawLength, Codec: uint8(loc.Codec)},
			}})
			live += int64(loc.StoredLength)
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

// readCompactSources reads chunks' stored bytes from their source packs (of sizes), in their order:
// records close together in a source read with one range GET, as the read path reads them, and up
// to fetchesAtOnce GETs at once. It returns the bytes it fetched too.
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
			first, last := rr.items[0], rr.items[len(rr.items)-1]
			end := last.off + last.n
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
