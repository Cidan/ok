package store

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/klauspost/compress/zstd"

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

type compactChunk struct {
	id     encoding.ID
	pack   encoding.ID
	loc    pack.Location
	hinted bool        // a file holds it:
	hint   string      // that file's path,
	fileID encoding.ID // ID,
	offset int64       // and the chunk's offset in it
}

// compact rewrites a repository's packs (docs/scm.md, "Garbage collection and
// compaction"), from the content index: packs no chunk points at are retired outright; packs more than half
// dead, the small packs when there are two or more, and — once the repository has a
// dictionary — packs holding small chunks not yet compressed with it are rewritten into packs
// of up to maxPackBytes holding only their live chunks, ordered by the path of a file that
// holds them on main, for read locality. Chunks are copied as stored, but for small ones the
// dictionary shrinks, which are re-encoded with it. Each new pack is written to S3, then one
// transaction moves its chunks to it, in the index and in their rows; the source packs left
// with no chunk are retired once every new pack is written. Readers that located a chunk in a retired pack still read it: a
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
		if e.Kind != encoding.KindChunk {
			return true
		}
		st := stats[e.Loc.Pack]
		if st == nil {
			st = &packStats{}
			stats[e.Loc.Pack] = st
		}
		st.chunks++
		st.live += int64(e.Loc.Stored)
		if e.Loc.Raw <= DictionaryChunkMax && pack.Codec(e.Loc.Codec) != pack.ZstdDict {
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
	for i, p := range chosen {
		sourceIDs[i] = p.id
		r.BytesRead += p.size
	}
	if opts.DryRun {
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

	dir, err := os.MkdirTemp("", "ok scm-compact-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	sourceFiles := map[encoding.ID]*os.File{}
	defer func() {
		for _, f := range sourceFiles {
			f.Close()
		}
	}()
	for _, p := range chosen {
		f, err := os.Create(filepath.Join(dir, "source-"+p.id.String()))
		if err != nil {
			return err
		}
		sourceFiles[p.id] = f
		size, err := s.blobs.Download(ctx, packKey(net, p.id), f)
		if err != nil {
			return fmt.Errorf("pack %s: %w", p.id, err)
		}
		if size != p.size {
			return fmt.Errorf("pack %s: %d bytes stored, row says %d", p.id, size, p.size)
		}
	}
	var written int64
	for i, out := range outputs {
		size, err := s.writeCompacted(ctx, net, dir, i, out, sourceFiles, enc, dictRow, r)
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
		if pid, ok := byNum[e.Loc.Pack]; ok && e.Kind == encoding.KindChunk {
			at[id] = len(out)
			out = append(out, compactChunk{id: id, pack: pid, loc: pack.Location{
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

// writeCompacted writes one compacted pack, uploads it, and in one transaction moves the
// chunks it holds — each only if the index still has it where compaction read it, with its
// codec and stored length as written. A small chunk not yet
// compressed with the repository's dictionary is, with enc, if that shrinks it. A pack written
// with enc records dictRow, the dictionary that considered its chunks. It returns the pack's
// size.
func (s *Store) writeCompacted(ctx context.Context, net backend.Network, dir string, n int, chunks []compactChunk, sources map[encoding.ID]*os.File, enc *zstd.Encoder, dictRow int64, r *GCReport) (int64, error) {
	f, err := os.Create(filepath.Join(dir, fmt.Sprintf("out-%d", n)))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	pw := pack.NewWriter(f)
	locs := make([]pack.Location, len(chunks))
	for i, c := range chunks {
		stored := make([]byte, c.loc.StoredLength)
		if _, err := sources[c.pack].ReadAt(stored, c.loc.DataOffset()); err != nil {
			return 0, fmt.Errorf("pack %s: chunk %s: %w", c.pack, c.id, err)
		}
		codec := c.loc.Codec
		if enc != nil && codec != pack.ZstdDict && c.loc.RawLength <= DictionaryChunkMax {
			raw, err := pack.Decode(s.dicts.of(net), c.id, codec, c.loc.RawLength, stored)
			if err != nil {
				return 0, fmt.Errorf("pack %s: %w", c.pack, err)
			}
			if z := enc.EncodeAll(raw, nil); len(z) < len(stored) {
				r.ChunksRecompressed++
				r.BytesRecompressed += int64(len(stored) - len(z))
				dictionaryChunks.With("compressed").Inc()
				codec, stored = pack.ZstdDict, z
			} else {
				dictionaryChunks.With("not_smaller").Inc()
			}
		}
		loc, err := pw.AddStored(s.dicts.of(net), c.id, codec, c.loc.RawLength, stored)
		if err != nil {
			return 0, fmt.Errorf("pack %s: chunk %s: %w", c.pack, c.id, err)
		}
		locs[i] = loc
	}
	packID, size, err := pw.Close()
	if err != nil {
		return 0, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if err := s.blobs.Put(ctx, packKey(net, packID), f, size); err != nil {
		return 0, err
	}

	li := s.live(backend.NetworkScope(net))
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		// No transaction changes the index while this one moves chunks in it, and the index
		// read after the lock holds every change before.
		if _, err := tx.LockContentIndex(ctx, backend.NetworkScope(net)); err != nil {
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
		var live int64
		for i, c := range chunks {
			e, ok, err := lg.has(ctx, encoding.KindChunk, c.id)
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
				Kind: encoding.KindChunk,
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

// retireDrained retires the packs among ids that no chunk in the content index is in, and
// returns how many and their total size.
func (s *Store) retireDrained(ctx context.Context, net backend.Network, packIDs []encoding.ID, dryRun bool) (int, int64, error) {
	v, err := s.contentView(ctx, backend.NetworkScope(net))
	if err != nil {
		return 0, 0, err
	}
	used := map[uint64]bool{}
	err = v.each(ctx, func(_ encoding.ID, e contentindex.Entry) bool {
		if e.Kind == encoding.KindChunk {
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
