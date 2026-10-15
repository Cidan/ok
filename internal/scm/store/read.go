package store

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/copies"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/pack"
	"github.com/Cidan/ok/internal/serve/metrics"
)

// ObjectResult is one object read: its body, or not found.
type ObjectResult struct {
	Ref   ObjectRef
	Body  []byte
	Found bool
}

// emptyTreeBody is the canonical body of the virtual empty tree.
var emptyTreeBody = make([]byte, 8)

func objectKey(repo int64, r ObjectRef) string {
	b := binary.LittleEndian.AppendUint64(nil, uint64(repo))
	b = append(b, byte(r.Kind))
	return string(append(b, r.ID[:]...))
}

func chunkKey(repo int64, id encoding.ID) string {
	b := binary.LittleEndian.AppendUint64(nil, uint64(repo))
	b = append(b, byte(encoding.KindChunk))
	return string(append(b, id[:]...))
}

// ReadObjects reads symlinks, trees, copy histories, and commits as canonical bodies,
// from an in-memory cache before PostgreSQL. The empty tree is served virtually; the root
// commit is virtual on the client and reported absent.
func (s *Store) ReadObjects(ctx context.Context, repo int64, refs []ObjectRef) ([]ObjectResult, error) {
	out := make([]ObjectResult, len(refs))
	misses := map[encoding.Kind][]encoding.ID{}
	for i, r := range refs {
		switch r.Kind {
		case encoding.KindSymlink, encoding.KindTree, encoding.KindCopyHistory, encoding.KindCommit:
		default:
			return nil, invalid("ReadObjects reads symlinks, trees, copy histories, and commits, not a %s", r.Kind)
		}
		out[i].Ref = r
		if r.Kind == encoding.KindTree && r.ID == encoding.EmptyTreeID {
			out[i].Body, out[i].Found = emptyTreeBody, true
			continue
		}
		if body, ok := s.objects.get(objectKey(repo, r)); ok {
			out[i].Body, out[i].Found = body, true
			continue
		}
		misses[r.Kind] = append(misses[r.Kind], r.ID)
	}
	found := map[ObjectRef][]byte{}
	for kind, members := range misses {
		var rows interface {
			Next() bool
			Scan(...any) error
			Close()
			Err() error
		}
		var err error
		if kind == encoding.KindCommit {
			rows, err = s.pool.Query(ctx, `SELECT id, body FROM commit WHERE repository_id = $1 AND id = ANY($2)`, repo, ids(members))
		} else {
			rows, err = s.pool.Query(ctx, `SELECT id, body FROM object WHERE repository_id = $1 AND id = ANY($2) AND kind = $3`, repo, ids(members), int16(kind))
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var idb, body []byte
			if err := rows.Scan(&idb, &body); err != nil {
				rows.Close()
				return nil, err
			}
			id, err := toID(idb)
			if err != nil {
				rows.Close()
				return nil, err
			}
			r := ObjectRef{Kind: kind, ID: id}
			found[r] = body
			s.objects.put(objectKey(repo, r), body, len(body))
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	for i := range out {
		if out[i].Found {
			continue
		}
		if body, ok := found[out[i].Ref]; ok {
			out[i].Body, out[i].Found = body, true
		}
	}
	return out, nil
}

// readObject reads and decodes one object, which must exist.
func (s *Store) readObject(ctx context.Context, repo int64, r ObjectRef) (encoding.Object, error) {
	res, err := s.ReadObjects(ctx, repo, []ObjectRef{r})
	if err != nil {
		return nil, err
	}
	if !res[0].Found {
		return nil, notFound("%s", r)
	}
	return decodeStored(r.Kind, r.ID, res[0].Body)
}

// treeBatch is how many trees one query reads.
const treeBatch = 1000

// readTrees reads and decodes trees, each of which must exist, a batch to a query.
func (s *Store) readTrees(ctx context.Context, repo int64, want []encoding.ID) (map[encoding.ID]*encoding.Tree, error) {
	out := make(map[encoding.ID]*encoding.Tree, len(want))
	for start := 0; start < len(want); start += treeBatch {
		batch := want[start:min(start+treeBatch, len(want))]
		refs := make([]ObjectRef, len(batch))
		for i, id := range batch {
			refs[i] = ObjectRef{Kind: encoding.KindTree, ID: id}
		}
		res, err := s.ReadObjects(ctx, repo, refs)
		if err != nil {
			return nil, err
		}
		for _, r := range res {
			if !r.Found {
				return nil, notFound("%s", r.Ref)
			}
			o, err := decodeStored(encoding.KindTree, r.Ref.ID, r.Body)
			if err != nil {
				return nil, err
			}
			out[r.Ref.ID] = o.(*encoding.Tree)
		}
	}
	return out, nil
}

func (s *Store) readTree(ctx context.Context, repo int64, id encoding.ID) (*encoding.Tree, error) {
	o, err := s.readObject(ctx, repo, ObjectRef{Kind: encoding.KindTree, ID: id})
	if err != nil {
		return nil, err
	}
	return o.(*encoding.Tree), nil
}

func (s *Store) readCommit(ctx context.Context, repo int64, id encoding.ID) (*encoding.Commit, error) {
	o, err := s.readObject(ctx, repo, ObjectRef{Kind: encoding.KindCommit, ID: id})
	if err != nil {
		return nil, err
	}
	return o.(*encoding.Commit), nil
}

type chunkLoc struct {
	id   encoding.ID
	pack encoding.ID
	loc  pack.Location
}

// chunkLocations locates chunks through the content index; one it lacks is absent.
func (s *Store) chunkLocations(ctx context.Context, repo int64, members []encoding.ID) (map[encoding.ID]chunkLoc, error) {
	out := map[encoding.ID]chunkLoc{}
	if len(members) == 0 {
		return out, nil
	}
	li, entries, err := s.contentEntries(ctx, repo, encoding.KindChunk, members)
	if err != nil {
		return nil, err
	}
	for id, e := range entries {
		pid, err := s.packID(ctx, repo, li, e.Loc.Pack)
		if err != nil {
			return nil, err
		}
		out[id] = chunkLoc{id: id, pack: pid, loc: pack.Location{
			Offset: e.Loc.Offset, Codec: pack.Codec(e.Loc.Codec), RawLength: e.Loc.Raw, StoredLength: e.Loc.Stored}}
	}
	return out, nil
}

// storedChunk is a chunk as a pack stores it: its codec, its raw length, and its stored bytes.
type storedChunk struct {
	codec pack.Codec
	raw   uint32
	data  []byte
}

// chunkBytes returns the raw bytes of chunks (storedChunks, decompressed).
func (s *Store) chunkBytes(ctx context.Context, repo int64, members []encoding.ID) (map[encoding.ID][]byte, error) {
	stored, err := s.storedChunks(ctx, repo, members)
	if err != nil {
		return nil, err
	}
	out := make(map[encoding.ID][]byte, len(stored))
	for id, c := range stored {
		raw, err := pack.Decompress(s.dicts, id, c.codec, c.raw, c.data)
		if err != nil {
			return nil, err
		}
		out[id] = raw
	}
	return out, nil
}

// storedChunks returns chunks as their packs store them, from the chunk cache before S3. The
// cache holds chunks compressed, as stored, each verified against its ID when it was fetched.
// A read whose pack was retired by repacking re-reads the chunk locations once.
func (s *Store) storedChunks(ctx context.Context, repo int64, members []encoding.ID) (map[encoding.ID]storedChunk, error) {
	out := map[encoding.ID]storedChunk{}
	var misses []encoding.ID
	for _, id := range members {
		if _, done := out[id]; done {
			continue
		}
		if c, ok := s.chunks.get(chunkKey(repo, id)); ok {
			out[id] = c
			continue
		}
		out[id] = storedChunk{}
		misses = append(misses, id)
	}
	for attempt := 0; len(misses) > 0; attempt++ {
		if attempt > 0 {
			// The packs it read from were retired: the index has moved on since.
			if err := s.refreshIndex(ctx, repo, s.live(repo)); err != nil {
				return nil, err
			}
		}
		locs, err := s.chunkLocations(ctx, repo, misses)
		if err != nil {
			return nil, err
		}
		for _, id := range misses {
			if _, ok := locs[id]; !ok {
				return nil, notFound("chunk %s", id)
			}
		}
		if s.afterLocate != nil {
			s.afterLocate()
		}
		retry, err := s.fetchChunks(ctx, repo, locs, out)
		if err != nil {
			return nil, err
		}
		if len(retry) > 0 && attempt > 0 {
			return nil, fmt.Errorf("chunks %v: packs missing from storage", retry)
		}
		misses = retry
	}
	return out, nil
}

// span is one range read of a pack: chunks whose records lie close together in it, in order.
type span struct {
	pack encoding.ID
	locs []chunkLoc
}

// fetchChunks reads the chunks at locs into out, returning those whose pack is gone. Chunks
// close together in a pack — their data at most mergeGap apart, the whole at most maxSpan — are
// read with one range GET, the bytes between them discarded, and up to fetchesAtOnce GETs run
// at once: a pack's chunks lie in the order compaction wrote them (by path), so a directory's
// files are usually one read.
func (s *Store) fetchChunks(ctx context.Context, repo int64, locs map[encoding.ID]chunkLoc, out map[encoding.ID]storedChunk) ([]encoding.ID, error) {
	byPack := map[encoding.ID][]chunkLoc{}
	for _, l := range locs {
		byPack[l.pack] = append(byPack[l.pack], l)
	}
	var spans []span
	for packID, ls := range byPack {
		sort.Slice(ls, func(i, j int) bool { return ls[i].loc.Offset < ls[j].loc.Offset })
		for start := 0; start < len(ls); {
			end := start + 1
			for end < len(ls) {
				prev, next := ls[end-1].loc, ls[end].loc
				gap := next.DataOffset() - (prev.DataOffset() + int64(prev.StoredLength))
				if gap > s.reads.mergeGap || next.DataOffset()+int64(next.StoredLength)-ls[start].loc.DataOffset() > s.reads.maxSpan {
					break
				}
				end++
			}
			spans = append(spans, span{pack: packID, locs: ls[start:end]})
			start = end
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		gone     []encoding.ID
		firstErr error
	)
	sem := make(chan struct{}, s.reads.fetchesAtOnce)
	for _, sp := range spans {
		sem <- struct{}{}
		mu.Lock()
		failed := firstErr != nil
		mu.Unlock()
		if failed {
			<-sem
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			got, lost, err := s.fetchSpan(ctx, repo, sp)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				return
			}
			gone = append(gone, lost...)
			for id, c := range got {
				out[id] = c
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return gone, nil
}

// How reads of file contents batch: how many chunks each range read of a pack fetches and the bytes
// between them it discards, and how many files (or ranges of them) and windows each ReadFiles call
// covers.
var (
	packRangeChunks = metrics.NewHistogram("ok_scm_pack_range_chunks",
		"Chunks each range read of a pack fetched.", metrics.Count)
	packRangeUnused = metrics.NewCounter("ok_scm_pack_range_unused_bytes_total",
		"Bytes range reads of packs fetched and discarded: the gaps between the chunks they wanted.")
	readFilesRanges = metrics.NewHistogram("ok_scm_read_files_ranges",
		"Files, or ranges of them, each ReadFiles call read, by what it read: contents raw, as stored, or manifests.", metrics.Count, "form")
	readFilesWindows = metrics.NewHistogram("ok_scm_read_files_windows",
		"Windows of chunks each ReadFiles call read, located and fetched together.", metrics.Count, "form")
)

// fetchSpan reads one span's chunks, verifying each against its ID and caching it as stored; a
// span whose pack is gone returns its chunks as lost.
func (s *Store) fetchSpan(ctx context.Context, repo int64, sp span) (map[encoding.ID]storedChunk, []encoding.ID, error) {
	first, last := sp.locs[0].loc, sp.locs[len(sp.locs)-1].loc
	length := last.DataOffset() + int64(last.StoredLength) - first.DataOffset()
	b, err := s.blobs.GetRange(ctx, packKey(repo, sp.pack), first.DataOffset(), length)
	if errors.Is(err, blob.ErrNotFound) {
		lost := make([]encoding.ID, len(sp.locs))
		for i, l := range sp.locs {
			lost[i] = l.id
		}
		return nil, lost, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if int64(len(b)) != length {
		return nil, nil, fmt.Errorf("pack %s: read %d bytes at %d, asked for %d", sp.pack, len(b), first.DataOffset(), length)
	}
	packRangeChunks.Observe(float64(len(sp.locs)))
	used := int64(0)
	for _, l := range sp.locs {
		used += int64(l.loc.StoredLength)
	}
	packRangeUnused.Add(uint64(length - used))
	got := make(map[encoding.ID]storedChunk, len(sp.locs))
	for _, l := range sp.locs {
		at := l.loc.DataOffset() - first.DataOffset()
		data := b[at : at+int64(l.loc.StoredLength)]
		if _, err := pack.Decode(s.dicts, l.id, l.loc.Codec, l.loc.RawLength, data); err != nil {
			return nil, nil, err
		}
		// A copy: the span's other bytes, and the gaps between its chunks, are not kept.
		c := storedChunk{codec: l.loc.Codec, raw: l.loc.RawLength, data: bytes.Clone(data)}
		got[l.id] = c
		s.chunks.put(chunkKey(repo, l.id), c, len(c.data))
	}
	return got, nil, nil
}

type fileChunk struct {
	id     encoding.ID
	offset uint64
	length uint64
}

// manifest is a file's size and its chunks, in order.
type manifest struct {
	size   uint64
	chunks []fileChunk
}

// manifestBatch bounds the files one batch of manifests names: how many a caller reads, and
// verifies, at once.
const manifestBatch = 1000

// manifests reads files' manifests from the content index; an absent file has no entry.
func (s *Store) manifests(ctx context.Context, repo int64, files []encoding.ID) (map[encoding.ID]*manifest, error) {
	_, entries, err := s.contentEntries(ctx, repo, encoding.KindFile, files)
	if err != nil {
		return nil, err
	}
	out := make(map[encoding.ID]*manifest, len(entries))
	for id, e := range entries {
		m := &manifest{size: e.Size, chunks: make([]fileChunk, len(e.Chunks))}
		var off uint64
		for i, c := range e.Chunks {
			m.chunks[i] = fileChunk{id: c.ID, offset: off, length: uint64(c.Length)}
			off += uint64(c.Length)
		}
		out[id] = m
	}
	return out, nil
}

// fileChunks returns a file's size and chunks in order, or found=false.
func (s *Store) fileChunks(ctx context.Context, repo int64, id encoding.ID) (size uint64, chunks []fileChunk, found bool, err error) {
	ms, err := s.manifests(ctx, repo, []encoding.ID{id})
	if err != nil {
		return 0, nil, false, err
	}
	m := ms[id]
	if m == nil {
		return 0, nil, false, nil
	}
	return m.size, m.chunks, true, nil
}

// readFile reads a whole file's contents.
func (s *Store) readFile(ctx context.Context, repo int64, id encoding.ID) ([]byte, error) {
	ms, err := s.manifests(ctx, repo, []encoding.ID{id})
	if err != nil {
		return nil, err
	}
	if ms[id] == nil {
		return nil, notFound("file %s", id)
	}
	got, err := s.contents(ctx, repo, ms, []encoding.ID{id})
	if err != nil {
		return nil, err
	}
	return got[id], nil
}

// contents assembles whole files from their manifests in ms, which must hold each: their chunks
// located with one query and read together (fetchChunks). Callers bound how much they ask for.
func (s *Store) contents(ctx context.Context, repo int64, ms map[encoding.ID]*manifest, files []encoding.ID) (map[encoding.ID][]byte, error) {
	var members []encoding.ID
	for _, id := range files {
		for _, c := range ms[id].chunks {
			members = append(members, c.id)
		}
	}
	data, err := s.chunkBytes(ctx, repo, members)
	if err != nil {
		return nil, err
	}
	out := make(map[encoding.ID][]byte, len(files))
	for _, id := range files {
		if _, done := out[id]; done {
			continue
		}
		m := ms[id]
		b := make([]byte, 0, m.size)
		for _, c := range m.chunks {
			b = append(b, data[c.id]...)
		}
		out[id] = b
	}
	return out, nil
}

// FileRange asks for [Offset, Offset+Length) of a file; Length 0 reads to the end.
type FileRange struct {
	FileID encoding.ID
	Offset uint64
	Length uint64
}

// FileFrame is one frame of ReadFiles: a whole chunk and where it sits in the file.
// A file's first frame carries its size, its last sets Last, and an absent file is one
// frame with NotFound. Range is the index of the range the frame answers. Read stored, Data is
// the chunk as its pack stores it, encoded by Codec, and RawLength its raw length.
type FileFrame struct {
	Range     int
	FileID    encoding.ID
	Size      uint64
	ChunkID   encoding.ID
	Offset    uint64
	Data      []byte
	Codec     pack.Codec
	RawLength uint32
	Last      bool
	NotFound  bool
}

// ChunkForm is how ReadFiles sends chunks.
type ChunkForm int

const (
	// RawChunks sends chunks decompressed.
	RawChunks ChunkForm = iota
	// StoredChunks sends chunks as their packs store them, but one compressed with a dictionary,
	// which it sends raw.
	StoredChunks
	// StoredChunksWithDictionaries sends chunks as their packs store them.
	StoredChunksWithDictionaries
)

// ReadFiles streams the chunks covering each range, whole and in order, as form says. Every range's manifest is read first, a query per manifestBatch
// files; the chunks are then read a window at a time — at most windowBytes of them, raw,
// windowChunks located with one query — each window's packs read together (fetchChunks) before
// its frames are sent.
func (s *Store) ReadFiles(ctx context.Context, repo int64, ranges []FileRange, form ChunkForm, emit func(FileFrame) error) error {
	stored := form != RawChunks
	label := "raw"
	switch form {
	case StoredChunks:
		label = "stored"
	case StoredChunksWithDictionaries:
		label = "stored_dictionaries"
	}
	readFilesRanges.With(label).Observe(float64(len(ranges)))
	windows := 0
	defer func() { readFilesWindows.With(label).Observe(float64(windows)) }()
	files := make([]encoding.ID, len(ranges))
	for i, r := range ranges {
		files[i] = r.FileID
	}
	ms, err := s.manifests(ctx, repo, files)
	if err != nil {
		return err
	}
	// Every frame, in order; a frame that carries a chunk has its length.
	type planned struct {
		frame  FileFrame
		length uint64
		chunk  bool
	}
	var plan []planned
	for i, r := range ranges {
		m := ms[r.FileID]
		if m == nil {
			plan = append(plan, planned{frame: FileFrame{Range: i, FileID: r.FileID, Last: true, NotFound: true}})
			continue
		}
		end := m.size
		if r.Length != 0 {
			end = r.Offset + r.Length
		}
		if r.Offset > m.size || end > m.size || end < r.Offset {
			return invalid("file %s: range [%d, %d) outside its %d bytes", r.FileID, r.Offset, end, m.size)
		}
		var want []fileChunk
		for _, c := range m.chunks {
			if c.offset < end && c.offset+c.length > r.Offset {
				want = append(want, c)
			}
		}
		if len(want) == 0 {
			plan = append(plan, planned{frame: FileFrame{Range: i, FileID: r.FileID, Size: m.size, Last: true}})
			continue
		}
		for j, c := range want {
			f := FileFrame{Range: i, FileID: r.FileID, ChunkID: c.id, Offset: c.offset, Last: j == len(want)-1}
			if j == 0 {
				f.Size = m.size
			}
			plan = append(plan, planned{frame: f, length: c.length, chunk: true})
		}
	}
	for start := 0; start < len(plan); {
		end := start
		var bytes uint64
		var n int
		for end < len(plan) {
			if p := plan[end]; p.chunk {
				if n > 0 && (bytes+p.length > s.reads.windowBytes || n == s.reads.windowChunks) {
					break
				}
				bytes += p.length
				n++
			}
			end++
		}
		var members []encoding.ID
		for _, p := range plan[start:end] {
			if p.chunk {
				members = append(members, p.frame.ChunkID)
			}
		}
		if len(members) > 0 {
			windows++
		}
		var raw map[encoding.ID][]byte
		var as map[encoding.ID]storedChunk
		var err error
		if stored {
			as, err = s.storedChunks(ctx, repo, members)
		} else {
			raw, err = s.chunkBytes(ctx, repo, members)
		}
		if err != nil {
			return err
		}
		for _, p := range plan[start:end] {
			f := p.frame
			switch {
			case p.chunk && stored:
				c := as[f.ChunkID]
				f.Data, f.Codec, f.RawLength = c.data, c.codec, c.raw
				if c.codec == pack.ZstdDict && form != StoredChunksWithDictionaries {
					raw, err := pack.Decompress(s.dicts, f.ChunkID, c.codec, c.raw, c.data)
					if err != nil {
						return err
					}
					f.Data, f.Codec = raw, pack.Stored
				}
			case p.chunk:
				f.Data = raw[f.ChunkID]
			}
			if err := emit(f); err != nil {
				return err
			}
		}
		start = end
	}
	return nil
}

// ReadManifests streams each file's manifest in ReadFiles's framing, without data: a file's
// first frame carries its size, each frame a chunk's ID and offset, and its last sets Last; a
// file with no chunks (the empty file) is one frame, and an absent one a NotFound frame. Files
// are read in batches, a query per batch rather than per file.
func (s *Store) ReadManifests(ctx context.Context, repo int64, files []encoding.ID, emit func(FileFrame) error) error {
	readFilesRanges.With("manifests").Observe(float64(len(files)))
	for start := 0; start < len(files); start += manifestBatch {
		batch := files[start:min(start+manifestBatch, len(files))]
		ms, err := s.manifests(ctx, repo, batch)
		if err != nil {
			return err
		}
		for k, id := range batch {
			i := start + k
			m := ms[id]
			if m == nil {
				if err := emit(FileFrame{Range: i, FileID: id, Last: true, NotFound: true}); err != nil {
					return err
				}
				continue
			}
			if len(m.chunks) == 0 {
				if err := emit(FileFrame{Range: i, FileID: id, Size: m.size, Last: true}); err != nil {
					return err
				}
				continue
			}
			for j, c := range m.chunks {
				f := FileFrame{Range: i, FileID: id, ChunkID: c.id, Offset: c.offset, Last: j == len(m.chunks)-1}
				if j == 0 {
					f.Size = m.size
				}
				if err := emit(f); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// GetRelatedCopies returns the copy histories related to copyID — its ancestors, and
// every descendant of those — children before parents, ties broken by ID (jj's
// get_related_copies contract).
func (s *Store) GetRelatedCopies(ctx context.Context, repo int64, copyID encoding.ID) ([]EncodedObject, error) {
	// Each step is a fenced per-row index probe, as in loadEdges.
	rows, err := s.pool.Query(ctx, `WITH RECURSIVE
		anc(id) AS (
			SELECT $2::bytea
			UNION
			SELECT p.parent_id FROM anc, LATERAL (
				SELECT cp.parent_id FROM copy_parent cp WHERE cp.repository_id = $1 AND cp.copy_id = anc.id OFFSET 0) p
		),
		rel(id) AS (
			SELECT id FROM anc
			UNION
			SELECT c.copy_id FROM rel, LATERAL (
				SELECT cp.copy_id FROM copy_parent cp WHERE cp.repository_id = $1 AND cp.parent_id = rel.id OFFSET 0) c
		)
		SELECT o.id, o.body FROM rel JOIN object o ON o.repository_id = $1 AND o.id = rel.id AND o.kind = $3`,
		repo, copyID[:], int16(encoding.KindCopyHistory))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	bodies := map[encoding.ID][]byte{}
	parents := map[encoding.ID][]encoding.ID{}
	for rows.Next() {
		var idb, body []byte
		if err := rows.Scan(&idb, &body); err != nil {
			return nil, err
		}
		id, err := toID(idb)
		if err != nil {
			return nil, err
		}
		o, err := decodeStored(encoding.KindCopyHistory, id, body)
		if err != nil {
			return nil, err
		}
		bodies[id] = body
		parents[id] = o.(*encoding.CopyHistory).Parents
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if _, ok := bodies[copyID]; !ok {
		return nil, notFound("copy history %s", copyID)
	}
	children := map[encoding.ID]int{}
	for id, ps := range parents {
		for _, p := range ps {
			if _, in := bodies[p]; in && p != id {
				children[p]++
			}
		}
	}
	var ready []encoding.ID
	for id := range bodies {
		if children[id] == 0 {
			ready = append(ready, id)
		}
	}
	out := make([]EncodedObject, 0, len(bodies))
	for len(ready) > 0 {
		sortIDs(ready)
		id := ready[0]
		ready = ready[1:]
		out = append(out, EncodedObject{Kind: encoding.KindCopyHistory, ID: id, Body: bodies[id]})
		for _, p := range parents[id] {
			if _, in := bodies[p]; !in || p == id {
				continue
			}
			if children[p]--; children[p] == 0 {
				ready = append(ready, p)
			}
		}
	}
	if len(out) != len(bodies) {
		return nil, fmt.Errorf("copy histories related to %s form a cycle", copyID)
	}
	return out, nil
}

// prefetchWindow is how many generations one edge query loads below a node.
const prefetchWindow = 1024

type genNode struct {
	id  encoding.ID
	gen int64
}

type genHeap []genNode

func (h genHeap) Len() int { return len(h) }
func (h genHeap) Less(i, j int) bool {
	if h[i].gen != h[j].gen {
		return h[i].gen > h[j].gen
	}
	return bytes.Compare(h[i].id[:], h[j].id[:]) < 0
}
func (h genHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *genHeap) Push(x any)   { *h = append(*h, x.(genNode)) }
func (h *genHeap) Pop() any     { old := *h; n := old[len(old)-1]; *h = old[:len(old)-1]; return n }

const (
	markWant = 1 << iota
	markHave
)

// PrefetchCommits streams every commit reachable from want (plus, with opLog, every
// commit the caller's operation log references) that is not reachable from have,
// newest generation first, stopping after limit commits (0: no limit). It paints
// ancestry by generation, loading parent edges a window of generations per query.
func (s *Store) PrefetchCommits(ctx context.Context, repo int64, principal, workspace string, want, have []encoding.ID, opLog bool, limit int, emit func(EncodedObject) error) error {
	if opLog {
		logged, err := s.opLogCommits(ctx, repo, principal, workspace)
		if err != nil {
			return err
		}
		want = append(append([]encoding.ID(nil), want...), logged...)
	}
	gen := map[encoding.ID]int64{}
	edges := map[encoding.ID][]encoding.ID{}
	loaded := map[encoding.ID]bool{}
	if err := s.loadGenerations(ctx, repo, append(append([]encoding.ID(nil), want...), have...), gen); err != nil {
		return err
	}
	mark := map[encoding.ID]int{}
	queued := map[encoding.ID]bool{}
	done := map[encoding.ID]bool{}
	h := &genHeap{}
	wantOnly := 0
	push := func(id encoding.ID, bits int) {
		g, known := gen[id]
		if !known || done[id] {
			return
		}
		before := mark[id]
		mark[id] |= bits
		if queued[id] {
			if before == markWant && mark[id] != markWant {
				wantOnly--
			}
			return
		}
		queued[id] = true
		heap.Push(h, genNode{id: id, gen: g})
		if mark[id] == markWant {
			wantOnly++
		}
	}
	for _, id := range have {
		push(id, markHave)
	}
	for _, id := range want {
		push(id, markWant)
	}
	var batch []encoding.ID
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		refs := make([]ObjectRef, len(batch))
		for i, id := range batch {
			refs[i] = ObjectRef{Kind: encoding.KindCommit, ID: id}
		}
		res, err := s.ReadObjects(ctx, repo, refs)
		if err != nil {
			return err
		}
		for _, r := range res {
			if r.Found {
				if err := emit(EncodedObject{Kind: encoding.KindCommit, ID: r.Ref.ID, Body: r.Body}); err != nil {
					return err
				}
			}
		}
		batch = batch[:0]
		return nil
	}
	sent := 0
	for h.Len() > 0 && wantOnly > 0 {
		n := heap.Pop(h).(genNode)
		delete(queued, n.id)
		done[n.id] = true
		m := mark[n.id]
		if m == markWant {
			wantOnly--
		}
		if !loaded[n.id] {
			frontier := []encoding.ID{n.id}
			for _, q := range *h {
				if !loaded[q.id] {
					frontier = append(frontier, q.id)
				}
			}
			if err := s.loadEdges(ctx, repo, frontier, n.gen-prefetchWindow, gen, edges, loaded); err != nil {
				return err
			}
		}
		if m&markHave != 0 {
			for _, p := range edges[n.id] {
				push(p, markHave)
			}
			continue
		}
		batch = append(batch, n.id)
		sent++
		if len(batch) == 256 {
			if err := flush(); err != nil {
				return err
			}
		}
		if limit > 0 && sent >= limit {
			break
		}
		for _, p := range edges[n.id] {
			push(p, markWant)
		}
	}
	return flush()
}

func (s *Store) loadGenerations(ctx context.Context, repo int64, members []encoding.ID, gen map[encoding.ID]int64) error {
	if len(members) == 0 {
		return nil
	}
	rows, err := s.pool.Query(ctx, `SELECT id, generation FROM commit WHERE repository_id = $1 AND id = ANY($2)`, repo, ids(members))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		var g int64
		if err := rows.Scan(&b, &g); err != nil {
			return err
		}
		id, err := toID(b)
		if err != nil {
			return err
		}
		gen[id] = g
	}
	return rows.Err()
}

// parentsOf is the parent edges of one commit, with each parent's generation. The
// OFFSET 0 fence keeps it a subquery run per row — two primary-key probes — so a walk
// that visits a window of a repository's history never becomes a join over every edge
// in the repository.
const parentsOf = `SELECT cp.parent_id, cp.ord, pc.generation
	FROM commit_parent cp JOIN commit pc ON pc.repository_id = cp.repository_id AND pc.id = cp.parent_id
	WHERE cp.repository_id = $1 AND cp.commit_id = walk.id OFFSET 0`

// loadEdges loads the parent edges of from and of every ancestor down to generation
// minGen in one query. The virtual root has no commit row, so it is never a parent here.
func (s *Store) loadEdges(ctx context.Context, repo int64, from []encoding.ID, minGen int64, gen map[encoding.ID]int64, edges map[encoding.ID][]encoding.ID, loaded map[encoding.ID]bool) error {
	rows, err := s.pool.Query(ctx, `WITH RECURSIVE walk(id) AS (
			SELECT unnest($2::bytea[])
			UNION
			SELECT e.parent_id FROM walk, LATERAL (`+parentsOf+`) e WHERE e.generation >= $3
		)
		SELECT walk.id, e.parent_id, e.generation
		FROM walk LEFT JOIN LATERAL (`+parentsOf+`) e ON true
		ORDER BY walk.id, e.ord`, repo, ids(from), minGen)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var idb, parentb []byte
		var pgen *int64
		if err := rows.Scan(&idb, &parentb, &pgen); err != nil {
			return err
		}
		id, err := toID(idb)
		if err != nil {
			return err
		}
		loaded[id] = true
		if pgen == nil {
			continue
		}
		parent, err := toID(parentb)
		if err != nil {
			return err
		}
		edges[id] = append(edges[id], parent)
		gen[parent] = *pgen
	}
	return rows.Err()
}

// opLogCommits lists every commit the caller's operation log references: each view's
// commits and each operation's commit_predecessors.
func (s *Store) opLogCommits(ctx context.Context, repo int64, principal, workspace string) ([]encoding.ID, error) {
	opLog, err := logOf(ctx, s.pool, repo, principal, workspace)
	if err != nil {
		return nil, err
	}
	seen := map[encoding.ID]bool{}
	var out []encoding.ID
	add := func(id encoding.ID) {
		if id != encoding.RootCommitID && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, t := range []struct {
		kind  encoding.Kind
		query string
	}{
		{encoding.KindView, `SELECT id, body FROM view WHERE op_log_id = $1`},
		{encoding.KindOperation, `SELECT id, body FROM operation WHERE op_log_id = $1`},
	} {
		rows, err := s.pool.Query(ctx, t.query, opLog)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var idb, body []byte
			if err := rows.Scan(&idb, &body); err != nil {
				rows.Close()
				return nil, err
			}
			id, err := toID(idb)
			if err != nil {
				rows.Close()
				return nil, err
			}
			o, err := decodeStored(t.kind, id, body)
			if err != nil {
				rows.Close()
				return nil, err
			}
			switch o := o.(type) {
			case *encoding.View:
				for _, c := range viewCommits(o) {
					add(c)
				}
			case *encoding.Operation:
				for c, preds := range o.CommitPredecessors {
					add(c)
					for _, p := range preds {
						add(p)
					}
				}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// cleanPrefixes normalizes sparse path prefixes, rejecting malformed ones.
func cleanPrefixes(prefixes []string) ([]string, error) {
	var out []string
	for _, p := range prefixes {
		p = strings.Trim(p, "/")
		if p == "" {
			return nil, nil // the whole tree
		}
		if path.Clean(p) != p || strings.HasPrefix(p, "../") || p == ".." {
			return nil, invalid("path prefix %q is not a clean relative path", p)
		}
		out = append(out, p)
	}
	return out, nil
}

// ReadTreeClosure streams every tree, symlink, and copy history under a tree, limited
// to path prefixes (for sparse workspaces): the trees along each prefix, and everything
// within one. The empty tree is virtual and not sent. withSizes sends, after each level of
// trees, their sizes records (TreeSizes), as encoding.KindTreeSizes under each tree's ID.
func (s *Store) ReadTreeClosure(ctx context.Context, repo int64, treeID encoding.ID, prefixes []string, withSizes bool, emit func(EncodedObject) error) error {
	prefixes, err := cleanPrefixes(prefixes)
	if err != nil {
		return err
	}
	inside := func(p string) bool {
		if len(prefixes) == 0 {
			return true
		}
		for _, pre := range prefixes {
			if p == pre || strings.HasPrefix(p, pre+"/") {
				return true
			}
		}
		return false
	}
	along := func(p string) bool {
		for _, pre := range prefixes {
			if strings.HasPrefix(pre, p+"/") {
				return true
			}
		}
		return false
	}
	type node struct {
		id   encoding.ID
		path string
	}
	sent := map[ObjectRef]bool{}
	send := func(refs []ObjectRef) error {
		var fresh []ObjectRef
		for _, r := range refs {
			if !sent[r] {
				sent[r] = true
				fresh = append(fresh, r)
			}
		}
		res, err := s.ReadObjects(ctx, repo, fresh)
		if err != nil {
			return err
		}
		for _, r := range res {
			if !r.Found {
				return notFound("%s", r.Ref)
			}
			if err := emit(EncodedObject{Kind: r.Ref.Kind, ID: r.Ref.ID, Body: r.Body}); err != nil {
				return err
			}
		}
		return nil
	}
	level := []node{{id: treeID}}
	if treeID == encoding.EmptyTreeID {
		level = nil
	}
	for len(level) > 0 {
		var trees []ObjectRef
		for _, n := range level {
			trees = append(trees, ObjectRef{Kind: encoding.KindTree, ID: n.id})
		}
		if err := send(trees); err != nil {
			return err
		}
		if withSizes {
			ids := make([]encoding.ID, len(level))
			for i, n := range level {
				ids[i] = n.id
			}
			sizes, err := s.TreeSizes(ctx, repo, ids)
			if err != nil {
				return err
			}
			for _, id := range ids {
				b, ok := sizes[id]
				if !ok {
					return notFound("tree %s", id)
				}
				if err := emit(EncodedObject{Kind: encoding.KindTreeSizes, ID: id, Body: b}); err != nil {
					return err
				}
			}
		}
		var next []node
		var leaves []ObjectRef
		for _, n := range level {
			t, err := s.readTree(ctx, repo, n.id)
			if err != nil {
				return err
			}
			for _, e := range t.Entries {
				p := e.Name
				if n.path != "" {
					p = n.path + "/" + e.Name
				}
				switch e.Value.Kind {
				case encoding.TreeValueTree:
					if e.Value.ID != encoding.EmptyTreeID && (inside(p) || along(p)) {
						next = append(next, node{id: e.Value.ID, path: p})
					}
				case encoding.TreeValueSymlink:
					if inside(p) {
						leaves = append(leaves, ObjectRef{Kind: encoding.KindSymlink, ID: e.Value.ID})
					}
				case encoding.TreeValueFile:
					if inside(p) && e.Value.CopyID != nil {
						leaves = append(leaves, ObjectRef{Kind: encoding.KindCopyHistory, ID: *e.Value.CopyID})
					}
				}
			}
		}
		if err := send(leaves); err != nil {
			return err
		}
		level = next
	}
	return nil
}

// CopyRecord is one copy or rename between two commits (jj's CopyRecord).
type CopyRecord struct {
	Target       string
	TargetCommit encoding.ID
	Source       string
	SourceFile   encoding.ID
	SourceCommit encoding.ID
}

// commitTree is the tree copy detection diffs for a commit: its root tree, or the first
// term of a conflicted one. The root commit's tree is the empty tree.
func (s *Store) commitTree(ctx context.Context, repo int64, id encoding.ID) (encoding.ID, error) {
	if id == encoding.RootCommitID {
		return encoding.EmptyTreeID, nil
	}
	c, err := s.readCommit(ctx, repo, id)
	if err != nil {
		return encoding.ID{}, err
	}
	return c.RootTree[0], nil
}

// GetCopyRecords returns the renames and copies from root to head, as jj's git backend
// computes them (see internal/scm/copies): each record's target_commit is
// head and source_commit is root. paths, when set, keeps only records targeting them.
func (s *Store) GetCopyRecords(ctx context.Context, repo int64, paths []string, root, head encoding.ID) ([]CopyRecord, error) {
	rootTree, err := s.commitTree(ctx, repo, root)
	if err != nil {
		return nil, err
	}
	headTree, err := s.commitTree(ctx, repo, head)
	if err != nil {
		return nil, err
	}
	trees := func(ctx context.Context, id encoding.ID) (*encoding.Tree, error) { return s.readTree(ctx, repo, id) }
	files := func(ctx context.Context, id encoding.ID) ([]byte, error) { return s.readFile(ctx, repo, id) }
	changes, err := copies.Changes(ctx, trees, rootTree, headTree)
	if err != nil {
		return nil, err
	}
	matches, err := copies.Track(ctx, changes, files)
	if err != nil {
		return nil, err
	}
	var keep map[string]bool
	if paths != nil {
		keep = map[string]bool{}
		for _, p := range paths {
			keep[p] = true
		}
	}
	var out []CopyRecord
	for _, m := range matches {
		// A CopyRecord names a source file, so renamed symlinks are not reported.
		if m.Dest.Symlink || m.Source.Symlink || (keep != nil && !keep[m.Dest.Path]) {
			continue
		}
		out = append(out, CopyRecord{Target: m.Dest.Path, TargetCommit: head, Source: m.Source.Path, SourceFile: m.Source.ID, SourceCommit: root})
	}
	return out, nil
}
