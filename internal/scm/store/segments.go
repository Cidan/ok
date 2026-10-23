package store

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/work"
)

// Segments (docs/hosted.md, "Segments, as built") hold what a repository holds pinned, in
// immutable sorted files in the bucket; the tail — the tables' rows — holds what is not pinned,
// and what was pinned since compaction last ran. A read looks in the tail, then in the segments.

// The kinds of segment.
const (
	// segObjects holds trees, symlinks, and copy histories by ID: each its kind's byte, then its
	// canonical body.
	segObjects segment.Kind = 1
	// segTreeSizes holds trees' sizes records, by the tree's ID.
	segTreeSizes segment.Kind = 2
	// segCopyEdges holds copy histories' parent edges reversed — a parent's ID, then the copy's,
	// with no value — for walking a copy's descendants.
	segCopyEdges segment.Kind = 3
	// segCommitGraph holds the commit graph, by commit ID: each commit's generation (uvarint) and
	// parents (a uvarint count, then their IDs) — small records, many to a block, so a walk over
	// history reads few blocks and, cached, none.
	segCommitGraph segment.Kind = 4
	// segCommitBodies holds commits' canonical bodies, by ID.
	segCommitBodies segment.Kind = 5
	// segPaths holds main's path history: a path, a NUL — which no path holds — the generation of a
	// commit of main that changed it (big-endian), and the commit's ID, with no value.
	segPaths segment.Kind = 6
	// segEvents holds a repository's events the relay has published, by place in its log
	// (big-endian): each one's body.
	segEvents segment.Kind = 7
	// segIndex holds the content index's pinned entries, by file or chunk ID: each entry
	// (contentindex.EncodeEntry) as it was when its segment was written. A newer segment's record
	// supersedes an older's, and the tail's moves (contentindex.Index.Moved) both: repacking moves
	// a pinned chunk (cindexsegments.go).
	segIndex segment.Kind = 8
	// segOps holds operation logs' settled operations and views: an operation log's ID (big-endian),
	// the kind, and the ID; each a live record or a tombstone collection wrote (opsegments.go).
	segOps segment.Kind = 9
	// segChangeLists holds landed CLs, each with its patchsets and landing, and the keys that find
	// them: by change ID, by a patchset's commit, by author, and by the CL a patchset was stacked
	// on (clsegments.go).
	segChangeLists segment.Kind = 10
	// segMainLog holds main's moves by place, and the place of each commit main moved to
	// (clsegments.go).
	segMainLog segment.Kind = 11
)

// segmentKinds are every kind of segment, in the order compaction writes them.
var segmentKinds = []segment.Kind{segObjects, segTreeSizes, segCopyEdges, segCommitGraph, segCommitBodies, segPaths, segEvents, segIndex, segOps,
	segChangeLists, segMainLog}

// segmentPrefixOf is, for each kind read by prefix, the part of its keys a read names: a parent's
// copy edges' parent, a path's history's path. Its segments' filters hold the prefixes too.
var segmentPrefixOf = map[segment.Kind]segment.Prefix{
	segCopyEdges:   func(k []byte) []byte { return k[:encoding.IDLen] },
	segPaths:       func(k []byte) []byte { return k[:bytes.IndexByte(k, 0)+1] },
	segOps:         func(k []byte) []byte { return k[:9] },
	segChangeLists: clPrefix,
	segMainLog:     mainLogPrefix,
}

// encodeGraph is a commit's record in the commit graph.
func encodeGraph(gen int64, parents []encoding.ID) []byte {
	b := binary.AppendUvarint(nil, uint64(gen))
	b = binary.AppendUvarint(b, uint64(len(parents)))
	for _, p := range parents {
		b = append(b, p[:]...)
	}
	return b
}

// decodeGraph reads a commit's record in the commit graph.
func decodeGraph(id encoding.ID, b []byte) (backend.GraphCommit, error) {
	g := backend.GraphCommit{ID: id}
	gen, n := binary.Uvarint(b)
	if n <= 0 {
		return g, fmt.Errorf("commit %s's record in the commit graph: %w", id, segment.ErrCorrupt)
	}
	b = b[n:]
	count, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b)-n) != count*encoding.IDLen {
		return g, fmt.Errorf("commit %s's record in the commit graph: %w", id, segment.ErrCorrupt)
	}
	b = b[n:]
	g.Generation = int64(gen)
	for i := uint64(0); i < count; i++ {
		g.Parents = append(g.Parents, encoding.ID(b[i*encoding.IDLen:(i+1)*encoding.IDLen]))
	}
	return g, nil
}

// SegmentWork is a repository's compaction, as keyed work (internal/work): marked when anything
// is pinned, it moves the tail's pinned rows into segments once enough wait, and merges segments.
const SegmentWork = "segments"

// RetiredSegmentGrace is how long a segment merged into another stays in the bucket, for readers
// whose manifest still names it.
const RetiredSegmentGrace = 24 * time.Hour

// OrphanSegmentGrace is how old a segment in the bucket with no row must be before it is deleted:
// a compaction writes it, then records it.
const OrphanSegmentGrace = 24 * time.Hour

// segmentChannel is the channel a change to a repository's segments notifies, with its ID.
const segmentChannel = "ok_scm_segments"

// mergeAt is how many segments of a kind at one level are merged into one at the next.
const mergeAt = 4

// segmentKey is a segment's object in the bucket, under its network's keys.
func segmentKey(net backend.Network, kind segment.Kind, id encoding.ID) string {
	return fmt.Sprintf("%s%d/%x", segmentPrefix(net), kind, id[:])
}

// segmentPrefix begins the key of every segment of a network: its chunk index's, and its
// repositories', which any manifest in the network may name (a fork names its parent's).
func segmentPrefix(net backend.Network) string { return fmt.Sprintf("nets/%d/segments/", net) }

// repoScope is a repository's scope.
func repoScope(repo int64) backend.Scope { return backend.RepositoryScope(repo) }

// manifestKey names a scope's manifest of one kind.
type manifestKey struct {
	scope backend.Scope
	kind  segment.Kind
}

// liveManifest is a repository's live segments of one kind as this replica holds them, newest
// first, marked stale when a change to them is announced.
type liveManifest struct {
	segs  atomic.Pointer[[]backend.Segment]
	stale atomic.Bool
}

// segmentState is the segments this replica holds: manifests, opened readers, and decoded blocks.
type segmentState struct {
	mu        sync.Mutex
	manifests map[manifestKey]*liveManifest
	readers   *cache[*segment.Reader]
	blocks    *cache[*segment.Block]
	// batch bounds the records one compaction moves of each kind, and min is the fewest a
	// compaction waits for (Config.SegmentBatch, SegmentMin).
	batch, min int
}

func (s *Store) liveManifest(sc backend.Scope, kind segment.Kind) *liveManifest {
	s.segs.mu.Lock()
	defer s.segs.mu.Unlock()
	if s.segs.manifests == nil {
		s.segs.manifests = map[manifestKey]*liveManifest{}
	}
	k := manifestKey{sc, kind}
	m := s.segs.manifests[k]
	if m == nil {
		m = &liveManifest{}
		s.segs.manifests[k] = m
	}
	return m
}

// segmentsChanged marks a repository's manifests stale.
func (s *Store) segmentsChanged(sc backend.Scope) {
	s.segs.mu.Lock()
	defer s.segs.mu.Unlock()
	for k, m := range s.segs.manifests {
		if k.scope == sc {
			m.stale.Store(true)
		}
	}
}

// allSegmentsChanged marks every manifest stale: announcements may have been missed.
func (s *Store) allSegmentsChanged() {
	s.segs.mu.Lock()
	defer s.segs.mu.Unlock()
	for _, m := range s.segs.manifests {
		m.stale.Store(true)
	}
}

// manifest is a repository's live segments of kind, newest first: within a transaction, read
// through it; otherwise the replica's, read again if stale or fresh is set.
func (s *Store) manifest(ctx context.Context, sc backend.Scope, kind segment.Kind, fresh bool) ([]backend.Segment, error) {
	if inTxContext(ctx) {
		return s.ops(ctx).Segments(ctx, sc, kind)
	}
	s.startListening()
	m := s.liveManifest(sc, kind)
	if p := m.segs.Load(); p != nil && !m.stale.Load() && !fresh {
		return *p, nil
	}
	// Cleared first, so an announcement during the read marks it again.
	m.stale.Store(false)
	segs, err := s.b.Segments(ctx, sc, kind)
	if err != nil {
		m.stale.Store(true)
		return nil, err
	}
	m.segs.Store(&segs)
	return segs, nil
}

// blockCache keeps segments' decoded blocks in the store's cache.
type blockCache struct{ c *cache[*segment.Block] }

func blockKey(id encoding.ID, block int) string {
	return string(id[:]) + strconv.Itoa(block)
}

func (b blockCache) Get(id encoding.ID, block int) (*segment.Block, bool) {
	return b.c.get(blockKey(id, block))
}

func (b blockCache) Put(id encoding.ID, block int, blk *segment.Block) {
	b.c.put(blockKey(id, block), blk, blk.Bytes())
}

// segmentReader opens a segment, or finds it opened.
func (s *Store) segmentReader(ctx context.Context, sg backend.Segment) (*segment.Reader, error) {
	r, err := s.openSegment(ctx, sg)
	if err != nil || sg.Bound == backend.NoBound {
		return r, err
	}
	return r.Bounded(boundFilter(sg.Kind, sg.Bound)), nil
}

// openSegment opens a segment whole, or finds it opened.
func (s *Store) openSegment(ctx context.Context, sg backend.Segment) (*segment.Reader, error) {
	key := string(sg.ID[:])
	if r, ok := s.segs.readers.get(key); ok {
		return r, nil
	}
	bkey := segmentKey(sg.Net, sg.Kind, sg.ID)
	r, err := segment.Open(ctx, sg.ID, sg.Size, func(ctx context.Context, off, n int64) ([]byte, error) {
		return s.blobs.GetRange(ctx, bkey, off, n)
	}, blockCache{s.segs.blocks})
	if err != nil {
		return nil, fmt.Errorf("segment %s: %w", bkey, err)
	}
	s.segs.readers.put(key, r, r.Bytes())
	return r, nil
}

// boundFilter keeps, of an inherited segment of a kind read along main, the records up to bound —
// a place in main's history, and the generation of main's commit there: a fork's history follows
// its parent's only as far as it forked or synced, though the segments it took may hold more.
func boundFilter(kind segment.Kind, bound int64) func(key, value []byte) bool {
	switch kind {
	case segPaths:
		// A path, a NUL, the generation (big-endian), and the commit's ID.
		return func(k, _ []byte) bool {
			at := len(k) - encoding.IDLen - 8
			return at >= 0 && int64(binary.BigEndian.Uint64(k[at:])) <= bound
		}
	case segMainLog:
		return func(k, v []byte) bool {
			switch {
			case len(k) == 9 && k[0] == logMove:
				return int64(binary.BigEndian.Uint64(k[1:])) <= bound
			case k[0] == logByHead:
				seq, n := binary.Uvarint(v)
				return n > 0 && int64(seq) <= bound
			}
			return false
		}
	}
	return func(_, _ []byte) bool { return true }
}

// scopeNetwork is the network a scope's segments are under.
func (s *Store) scopeNetwork(ctx context.Context, sc backend.Scope) (backend.Network, error) {
	if sc.Network {
		return backend.Network(sc.ID), nil
	}
	return s.network(ctx, sc.ID)
}

// segmentReaders opens a repository's live segments of kind, newest first.
func (s *Store) segmentReaders(ctx context.Context, sc backend.Scope, kind segment.Kind, fresh bool) ([]*segment.Reader, error) {
	segs, err := s.manifest(ctx, sc, kind, fresh)
	if err != nil {
		return nil, err
	}
	out := make([]*segment.Reader, len(segs))
	for i, sg := range segs {
		if out[i], err = s.segmentReader(ctx, sg); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// segmentGet reads keys of a repository's kind from its segments, newest first: the value of each
// found. A key none has is looked for again after the manifest is read anew — compaction may have
// moved it from the tail since the caller read the tail — as is every key when a segment the
// manifest named is gone from the bucket (merged long since).
func (s *Store) segmentGet(ctx context.Context, sc backend.Scope, kind segment.Kind, keys [][]byte) (map[string][]byte, error) {
	out := map[string][]byte{}
	for attempt := 0; attempt < 2 && len(out) < len(keys); attempt++ {
		readers, err := s.segmentReaders(ctx, sc, kind, attempt > 0)
		if errors.Is(err, blob.ErrNotFound) && attempt == 0 {
			continue
		}
		if err != nil {
			return nil, err
		}
		// Newest first: a key a newer segment holds is not looked for in an older.
		gone := false
		for _, r := range readers {
			var want [][]byte
			for _, k := range keys {
				if _, ok := out[string(k)]; !ok {
					want = append(want, k)
				}
			}
			if len(want) == 0 {
				break
			}
			got, err := r.GetMany(ctx, want, s.reads.fetchesAtOnce)
			if errors.Is(err, blob.ErrNotFound) && attempt == 0 {
				gone = true
				break
			}
			if err != nil {
				return nil, err
			}
			for k, v := range got {
				out[k] = v
			}
		}
		if inTxContext(ctx) && !gone {
			break // the transaction read the manifest itself
		}
	}
	return out, nil
}

// segmentBodies reads objects of kind — a commit's, or another's — from a repository's segments:
// each found's canonical body.
func (s *Store) segmentBodies(ctx context.Context, repo int64, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID][]byte, error) {
	if kind != encoding.KindCommit {
		return s.segmentObjects(ctx, repo, kind, ids)
	}
	keys := make([][]byte, len(ids))
	for i := range ids {
		keys[i] = ids[i][:]
	}
	got, err := s.segmentGet(ctx, repoScope(repo), segCommitBodies, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[encoding.ID][]byte, len(got))
	for k, v := range got {
		out[encoding.ID([]byte(k))] = v
	}
	return out, nil
}

// segmentObjects reads objects of kind from a repository's segments: each found's canonical body.
func (s *Store) segmentObjects(ctx context.Context, repo int64, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID][]byte, error) {
	keys := make([][]byte, len(ids))
	for i := range ids {
		keys[i] = ids[i][:]
	}
	got, err := s.segmentGet(ctx, repoScope(repo), segObjects, keys)
	if err != nil {
		return nil, err
	}
	out := make(map[encoding.ID][]byte, len(got))
	for _, id := range ids {
		if v, ok := got[string(id[:])]; ok && len(v) > 0 && encoding.Kind(v[0]) == kind {
			out[id] = v[1:]
		}
	}
	return out, nil
}

// segmentRange calls fn with each record of a repository's kind whose key is at least from and
// before to (nil: the end), across its segments, in key order, each key once. The manifest is
// read anew, after whatever of the tail the caller read.
func (s *Store) segmentRange(ctx context.Context, sc backend.Scope, kind segment.Kind, from, to []byte, fn func(key, value []byte) error) error {
	readers, err := s.segmentReaders(ctx, sc, kind, true)
	if err != nil {
		return err
	}
	var ferr error
	err = scanSegments(ctx, readers, nil, from, to, false, func(k, v []byte) bool {
		ferr = fn(k, v)
		return ferr == nil
	})
	return errors.Join(err, ferr)
}

// segmentPrefixes calls fn with each record of a repository's kind whose key begins with one of
// prefixes — each what the kind's Prefix gives — across its segments, each key once: the manifest
// read anew, once, after whatever of the tail the caller read.
func (s *Store) segmentPrefixes(ctx context.Context, sc backend.Scope, kind segment.Kind, prefixes [][]byte, fn func(key, value []byte) error) error {
	readers, err := s.segmentReaders(ctx, sc, kind, true)
	if err != nil || len(readers) == 0 {
		return err
	}
	for _, p := range prefixes {
		var ferr error
		err := scanSegments(ctx, readers, p, p, prefixEnd(p), false, func(k, v []byte) bool {
			ferr = fn(k, v)
			return ferr == nil
		})
		if err := errors.Join(err, ferr); err != nil {
			return err
		}
	}
	return nil
}

// scanSegments walks readers' records whose keys are at least from and before to (nil: the end),
// ascending or descending, as one run — each key once, the newest segment's record — skipping
// readers that hold no key beginning with prefix (nil: none skipped), until fn returns false.
func scanSegments(ctx context.Context, readers []*segment.Reader, prefix, from, to []byte, desc bool, fn func(key, value []byte) bool) error {
	var its []segment.Iterator
	for _, r := range readers {
		if prefix != nil && !r.MayContainPrefix(prefix) {
			continue
		}
		if desc {
			its = append(its, r.IterDesc(ctx, from, to))
		} else {
			its = append(its, r.Iter(ctx, from, to))
		}
	}
	if len(its) == 0 {
		return nil
	}
	merge := segment.Merge
	if desc {
		merge = segment.MergeDesc
	}
	m := merge(its...)
	for m.Next() {
		if !fn(m.Key(), m.Value()) {
			return nil
		}
	}
	return m.Err()
}

// prefixEnd is the first key after every key beginning with p.
func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// writeSegment writes it as a segment of a repository's kind at level, to the bucket, and says
// what it wrote; nothing is written for no records.
func (s *Store) writeSegment(ctx context.Context, sc backend.Scope, kind segment.Kind, level int16, it segment.Iterator) (backend.Segment, bool, error) {
	f, err := os.CreateTemp("", "ok-segment-*")
	if err != nil {
		return backend.Segment{}, false, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	w := segment.NewWriter(f, kind, segmentPrefixOf[kind])
	n := 0
	for it.Next() {
		if err := w.Add(it.Key(), it.Value()); err != nil {
			return backend.Segment{}, false, err
		}
		n++
	}
	if err := it.Err(); err != nil {
		return backend.Segment{}, false, err
	}
	if n == 0 {
		return backend.Segment{}, false, nil
	}
	meta, err := w.Finish()
	if err != nil {
		return backend.Segment{}, false, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return backend.Segment{}, false, err
	}
	net, err := s.scopeNetwork(ctx, sc)
	if err != nil {
		return backend.Segment{}, false, err
	}
	if err := s.blobs.Put(ctx, segmentKey(net, kind, meta.ID), f, meta.Size, blob.IfAbsent()); err != nil {
		return backend.Segment{}, false, fmt.Errorf("writing a segment of %s: %w", sc.Name(), err)
	}
	return backend.Segment{Scope: sc, Net: net, Kind: kind, ID: meta.ID, Level: level, Records: meta.Records, Size: meta.Size,
		Bound: backend.NoBound}, true, nil
}

// SegmentReport is what one compaction of a repository's tail did.
type SegmentReport struct {
	// Moved counts the tail's records moved into segments, by kind: objects, trees' sizes records,
	// copies' edges, commits, rows of main's path history, events, the content index's pinned
	// entries and moves, operation logs' operations, views, and tombstones, and landed CLs (each
	// with its patchsets and its move of main).
	Objects, TreeSizes, CopyEdges, Commits, Paths, Events, Index, Ops, ChangeLists int
	// Written and Merged count the segments written from the tail, and merged into others.
	Written, Merged int
	// More says compaction stopped at its batch, with more of the tail pinned.
	More bool
}

// CompactSegments moves the tail's pinned rows of a repository into segments, if at least min of
// them wait — up to a batch of each kind — and folds its content index, if at least min of its
// entries and moves would move (RebuildContentIndex); then it merges the repository's segments
// where a level holds mergeAt of a kind. The rows it moves are deleted from the tail in the
// transaction that records their segments, each only if still pinned, which nothing undoes; a
// reader that misses a row it moved finds it in the segment, its manifest read anew.
func (s *Store) CompactSegments(ctx context.Context, repo int64, min int) (*SegmentReport, error) {
	_, netScope, err := s.networkOf(ctx, repo)
	if err != nil {
		return nil, err
	}
	// A landing's path history it left, if it stopped short of writing it, before the tail moves.
	if err := s.finishPathIndexes(ctx, repo); err != nil {
		return nil, fmt.Errorf("indexing landings' paths: %w", err)
	}
	r := &SegmentReport{}
	if err := s.compactTail(ctx, repo, min, r); err != nil {
		return nil, err
	}
	// The repository's content index, and its network's chunk index.
	for _, sc := range []backend.Scope{repoScope(repo), netScope} {
		li := s.live(sc)
		if err := s.refreshIndex(ctx, sc, li); err != nil {
			return nil, err
		}
		if n := foldable(li.x.Load()); n > 0 && n >= min {
			_, _, folded, err := s.foldIndex(ctx, sc)
			if err != nil {
				return nil, fmt.Errorf("folding %s's content index: %w", sc.Name(), err)
			}
			r.Index += folded
			if folded > 0 {
				r.Written++
			}
		}
	}
	if err := s.mergeSegments(ctx, repoScope(repo), segmentKinds, r); err != nil {
		return nil, err
	}
	return r, s.mergeSegments(ctx, netScope, networkKinds, r)
}

// networkKinds are the kinds of segment a network holds: its chunk index's folds.
var networkKinds = []segment.Kind{segIndex}

// compactTail moves the tail's pinned rows into segments, if at least min of them wait.
func (s *Store) compactTail(ctx context.Context, repo int64, min int, r *SegmentReport) error {
	objs, err := s.ops(ctx).PinnedObjects(ctx, repo, s.segs.batch)
	if err != nil {
		return err
	}
	sizes, err := s.ops(ctx).PinnedTreeSizes(ctx, repo, s.segs.batch)
	if err != nil {
		return err
	}
	edges, err := s.ops(ctx).PinnedCopyEdges(ctx, repo, s.segs.batch)
	if err != nil {
		return err
	}
	commits, err := s.ops(ctx).PinnedCommits(ctx, repo, s.segs.batch)
	if err != nil {
		return err
	}
	paths, err := s.ops(ctx).PinnedPaths(ctx, repo, s.segs.batch)
	if err != nil {
		return err
	}
	events, err := s.ops(ctx).SettledEvents(ctx, repo, s.segs.batch)
	if err != nil {
		return err
	}
	opsBefore := time.Now().Add(-opSettled)
	ops, err := s.ops(ctx).SettledOps(ctx, repo, opsBefore, s.segs.batch)
	if err != nil {
		return err
	}
	tombstones, err := s.ops(ctx).OpTombstones(ctx, repo, s.segs.batch)
	if err != nil {
		return err
	}
	settled, err := s.ops(ctx).SettledChangeLists(ctx, repo, s.segs.batch)
	if err != nil {
		return err
	}
	var nOps, nViews int
	for _, o := range ops {
		if o.Kind == encoding.KindOperation {
			nOps++
		} else {
			nViews++
		}
	}
	counts := []int{len(objs), len(sizes), len(edges), len(commits), len(paths), len(events), nOps, nViews, len(tombstones), len(settled)}
	total := 0
	for _, n := range counts {
		total += n
		r.More = r.More || n == s.segs.batch
	}
	if total == 0 || total < min {
		r.More = false
		return nil
	}
	var objRecs, sizeRecs, edgeRecs, graphRecs, bodyRecs, pathRecs, eventRecs, opRecs, clRecs, logRecs []segment.Record
	var c backend.Compacted
	for _, o := range objs {
		objRecs = append(objRecs, segment.Record{Key: o.ID[:], Value: append([]byte{byte(o.Kind)}, o.Body...)})
		c.Objects = append(c.Objects, o.ID)
	}
	for _, o := range sizes {
		sizeRecs = append(sizeRecs, segment.Record{Key: o.ID[:], Value: o.Body})
		c.TreeSizes = append(c.TreeSizes, o.ID)
	}
	for _, e := range edges {
		edgeRecs = append(edgeRecs, segment.Record{Key: append(append([]byte{}, e[1][:]...), e[0][:]...)})
		c.CopyEdges = append(c.CopyEdges, e)
	}
	for _, cm := range commits {
		graphRecs = append(graphRecs, segment.Record{Key: cm.ID[:], Value: encodeGraph(cm.Generation, cm.Parents)})
		bodyRecs = append(bodyRecs, segment.Record{Key: cm.ID[:], Value: cm.Body})
		c.Commits = append(c.Commits, cm.ID)
	}
	for _, p := range paths {
		if strings.IndexByte(p.Path, 0) >= 0 {
			return fmt.Errorf("repository %d's path history holds a path with a NUL: %q", repo, p.Path)
		}
		pathRecs = append(pathRecs, segment.Record{Key: pathKey(p)})
		c.Paths = append(c.Paths, p)
	}
	for _, e := range events {
		eventRecs = append(eventRecs, segment.Record{Key: eventKey(e.RepositorySeq), Value: e.Body})
		c.Events = append(c.Events, e.RepositorySeq)
	}
	for _, o := range ops {
		opRecs = append(opRecs, segment.Record{Key: opKey(o.Log, o.Kind, o.ID), Value: encodeOpRecord(o)})
	}
	for _, k := range tombstones {
		opRecs = append(opRecs, segment.Record{Key: opKey(k.Log, k.Kind, k.ID), Value: []byte{opTombstone}})
	}
	slices.SortFunc(opRecs, func(a, b segment.Record) int { return bytes.Compare(a.Key, b.Key) })
	for _, sc := range settled {
		cls, log, err := settledRecords(sc)
		if err != nil {
			return err
		}
		clRecs, logRecs = append(clRecs, cls...), append(logRecs, log...)
		c.ChangeLists = append(c.ChangeLists, sc.Number)
		c.MainLog = append(c.MainLog, sc.Landing.Seq)
	}
	for _, recs := range [][]segment.Record{clRecs, logRecs} {
		slices.SortFunc(recs, func(a, b segment.Record) int { return bytes.Compare(a.Key, b.Key) })
	}
	c.Ops, c.OpsBefore, c.OpTombstones = ops, opsBefore, tombstones
	var written []backend.Segment
	for _, k := range []struct {
		kind segment.Kind
		recs []segment.Record
	}{{segObjects, objRecs}, {segTreeSizes, sizeRecs}, {segCopyEdges, edgeRecs}, {segCommitGraph, graphRecs}, {segCommitBodies, bodyRecs},
		{segPaths, pathRecs}, {segEvents, eventRecs}, {segOps, opRecs}, {segChangeLists, clRecs}, {segMainLog, logRecs}} {
		sg, ok, err := s.writeSegment(ctx, repoScope(repo), k.kind, 0, segment.Records(k.recs))
		if err != nil {
			return err
		}
		if ok {
			written = append(written, sg)
		}
	}
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		if err := tx.InsertSegments(ctx, written); err != nil {
			return err
		}
		if err := tx.DeleteCompacted(ctx, repo, c); err != nil {
			return err
		}
		return tx.Notify(ctx, segmentChannel, repoScope(repo).String())
	})
	if err != nil {
		// The segments are orphans, deleted once a day old (deleteSegments).
		return err
	}
	s.segmentsChanged(repoScope(repo))
	r.Objects, r.TreeSizes, r.CopyEdges, r.Commits, r.Paths, r.Events, r.Ops, r.ChangeLists, r.Written =
		len(objs), len(sizes), len(edges), len(commits), len(paths), len(events), len(ops)+len(tombstones), len(settled), len(written)
	return nil
}

// compactSegmentsNow compacts a repository's tail however little of it is pinned, holding its
// compaction's claim (SegmentWork) so it never runs beside the compaction that pins mark; it
// returns nil when that one is running.
func (s *Store) compactSegmentsNow(ctx context.Context, repo int64) (*SegmentReport, error) {
	held, release, ok, err := work.Claim(ctx, s.Work(), SegmentWork, repo, s.holder, time.Minute)
	if err != nil || !ok {
		return nil, err
	}
	defer release()
	return s.CompactSegments(held, repo, 1)
}

// mergeSegments merges each kind's segments where one level holds mergeAt of them: into one at
// the next level, retiring them. Records are immutable, so a key two hold has one value.
func (s *Store) mergeSegments(ctx context.Context, sc backend.Scope, kinds []segment.Kind, r *SegmentReport) error {
	for _, kind := range kinds {
		for {
			segs, err := s.ops(ctx).Segments(ctx, sc, kind)
			if err != nil {
				return err
			}
			// Inherited segments are never merged: a fork shares its parent's for as long as it
			// lives, rather than copying them into its own.
			byLevel := map[int16][]backend.Segment{}
			lowest := int16(-1)
			for _, sg := range segs {
				if sg.Inherited {
					continue
				}
				byLevel[sg.Level] = append(byLevel[sg.Level], sg)
				if len(byLevel[sg.Level]) == mergeAt && (lowest < 0 || sg.Level < lowest) {
					lowest = sg.Level
				}
			}
			if lowest < 0 {
				break
			}
			oldest := true
			for l := range byLevel {
				oldest = oldest && l <= lowest
			}
			if err := s.mergeLevel(ctx, sc, kind, lowest, byLevel[lowest], oldest); err != nil {
				return err
			}
			r.Merged += len(byLevel[lowest])
		}
	}
	return nil
}

// mergeLevel merges segs, of one level, into one at the next.
// mergeLevel merges segs, every segment of a kind at one level, into one at the next — none, when
// nothing of them is left to keep — and retires them. oldest says no segment of the kind is older:
// none at a higher level.
func (s *Store) mergeLevel(ctx context.Context, sc backend.Scope, kind segment.Kind, level int16, segs []backend.Segment, oldest bool) error {
	// Newest first, so the merge keeps the newest of a key's records; the merged segment is as new
	// as the newest of them.
	slices.SortFunc(segs, func(a, b backend.Segment) int { return cmp.Compare(b.Age, a.Age) })
	its := make([]segment.Iterator, len(segs))
	ids := make([]encoding.ID, len(segs))
	for i, sg := range segs {
		rd, err := s.segmentReader(ctx, sg)
		if err != nil {
			return err
		}
		its[i], ids[i] = rd.Iter(ctx, nil, nil), sg.ID
	}
	var it segment.Iterator = segment.Merge(its...)
	if kind == segOps {
		keep, err := s.opMergeFilter(ctx, sc.ID, oldest)
		if err != nil {
			return err
		}
		it = segment.Filter(it, keep)
	}
	merged, ok, err := s.writeSegment(ctx, sc, kind, level+1, it)
	if err != nil {
		return err
	}
	merged.Age = segs[0].Age
	return s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		retired, err := tx.RetireSegments(ctx, sc, kind, ids)
		if err != nil {
			return err
		}
		if retired != int64(len(ids)) {
			// The merged segment is an orphan, deleted once a day old.
			return fmt.Errorf("another merge took %s's segments of kind %d first", sc.Name(), kind)
		}
		if ok {
			if err := tx.InsertSegments(ctx, []backend.Segment{merged}); err != nil {
				return err
			}
		}
		return tx.Notify(ctx, segmentChannel, sc.String())
	})
}

// deleteSegments deletes the retired segments of a repository's network past their grace, and the
// segments in the bucket with no row past theirs (deleteNetworkSegments), returning how many of
// each. Every segment is unique (it carries a nonce), so no deleted key is written again.
func (s *Store) deleteSegments(ctx context.Context, repo int64, now time.Time, dryRun bool) (retired, orphans int, err error) {
	net, err := s.network(ctx, repo)
	if err != nil {
		return 0, 0, err
	}
	return s.deleteNetworkSegments(ctx, net, now, dryRun)
}

// deleteNetworkSegments deletes a network's segment rows retired more than RetiredSegmentGrace
// before now — of its chunk index and of every repository in it — and each one's object unless
// another row names it, live or retired within the grace: a fork names its parent's segments, and
// a merge's inputs stay for readers that located a key in one. Objects under the network's keys
// that no row names and older than OrphanSegmentGrace — written by a compaction that never
// recorded them — go too.
func (s *Store) deleteNetworkSegments(ctx context.Context, net backend.Network, now time.Time, dryRun bool) (retired, orphans int, err error) {
	grace := now.Add(-RetiredSegmentGrace)
	old, err := s.ops(ctx).RetiredSegmentsBefore(ctx, grace, &net)
	if err != nil {
		return 0, 0, err
	}
	for _, sg := range old {
		retired++
		if dryRun {
			continue
		}
		named, err := s.ops(ctx).SegmentNamed(ctx, net, sg.Scope, sg.Kind, sg.ID, grace)
		if err != nil {
			return retired, 0, err
		}
		if !named {
			if err := s.blobs.Delete(ctx, segmentKey(net, sg.Kind, sg.ID)); err != nil && !errors.Is(err, blob.ErrNotFound) {
				return retired, 0, err
			}
		}
		if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error { return tx.DeleteSegment(ctx, sg.Scope, sg.Kind, sg.ID) }); err != nil {
			return retired, 0, err
		}
	}
	objs, err := s.blobs.List(ctx, segmentPrefix(net))
	if err != nil {
		return retired, 0, err
	}
	all, err := s.ops(ctx).NetworkSegments(ctx, net)
	if err != nil {
		return retired, 0, err
	}
	known := make(map[string]bool, len(all))
	for _, sg := range all {
		known[segmentKey(net, sg.Kind, sg.ID)] = true
	}
	for _, o := range objs {
		if known[o.Key] || !o.Modified.Before(now.Add(-OrphanSegmentGrace)) {
			continue
		}
		orphans++
		if dryRun {
			continue
		}
		if err := s.blobs.Delete(ctx, o.Key); err != nil {
			return retired, orphans, err
		}
	}
	return retired, orphans, nil
}
