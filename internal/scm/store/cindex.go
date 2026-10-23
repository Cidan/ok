package store

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/work"
)

// The content index (docs/scm.md, "The content index") is ok scm's only record of
// files and chunks: every transaction that changes one logs the change, in one content_log row
// written as it commits; a transaction that relies on what the index holds reads it under the
// index's row lock (txIndex); and every garbage collection pass folds the log into the next
// generation's blocks (RebuildContentIndex).
//
// There are two kinds of content index (docs/hosted.md, "Fork networks, as built"), on one
// machinery. A repository's, by its scope, holds the files and chunks the repository holds: what
// it may read, with what garbage collection needs. Its network's holds where each chunk is — a
// chunk's entry with its location, pinned when a repository pins it — so a chunk's bytes are found
// there, and repacking moves them there alone. A transaction that writes both takes the
// repository's lock before the network's.

// contentChannel is the channel each logging transaction notifies, with its index's scope, as it
// commits.
const contentChannel = "ok_scm_content"

// txLog is what one transaction changed in a content index, and the index it decides by
// (txIndex).
type txLog struct {
	s       *Store
	scope   backend.Scope
	updates []contentindex.Update
	// index is the content index's tail as of when the transaction took its row lock, today the
	// database's day then; touched and pinned are what the transaction touched and pinned.
	index   *contentindex.Index
	today   contentindex.Day
	touched map[encoding.ID]bool
	pinned  map[encoding.ID]bool
	puts    map[encoding.ID]contentindex.Entry
	// readers are the index segments as the transaction read their manifest, once it needed them;
	// found are the entries it found there.
	readers *[]*segment.Reader
	found   map[encoding.ID]contentindex.Entry
}

// txLogOf is the transaction's log of a content index: one repository's and its network's at
// most.
func (s *Store) txLogOf(tx *txn, sc backend.Scope) *txLog {
	for _, lg := range tx.logs {
		if lg.scope == sc {
			return lg
		}
	}
	for _, lg := range tx.logs {
		if lg.scope.Network == sc.Network {
			panic(fmt.Sprintf("store: one transaction changes the content indexes of %s and %s", lg.scope.Name(), sc.Name()))
		}
	}
	lg := &txLog{s: s, scope: sc, touched: map[encoding.ID]bool{}, pinned: map[encoding.ID]bool{},
		puts: map[encoding.ID]contentindex.Entry{}, found: map[encoding.ID]contentindex.Entry{}}
	tx.logs = append(tx.logs, lg)
	return lg
}

// logIndex records, for the transaction tx, changes to a content index.
func (s *Store) logIndex(tx *txn, sc backend.Scope, updates ...contentindex.Update) {
	if len(updates) == 0 {
		return
	}
	lg := s.txLogOf(tx, sc)
	for _, u := range updates {
		switch u.Op {
		case contentindex.Touch:
			lg.touched[u.ID] = true
		case contentindex.Pin:
			lg.pinned[u.ID] = true
		case contentindex.Put:
			e := u.Entry
			e.Body = true
			lg.puts[u.ID] = e
		}
	}
	lg.updates = append(lg.updates, updates...)
}

// txIndex is the content index a transaction that writes it decides by: read once the
// transaction holds the index's row lock in share mode, so no sweep or compaction changes it
// before the transaction commits — what it finds stays — while other writers, whose puts,
// touches, and pins commute with its own, run beside it. It returns the database's day too.
func (s *Store) txIndex(ctx context.Context, tx *txn, sc backend.Scope) (*txLog, error) {
	lg := s.txLogOf(tx, sc)
	if lg.index != nil {
		return lg, nil
	}
	today, ok, err := tx.ShareContentIndex(ctx, sc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%s has no content index", sc.Name())
	}
	li := s.live(sc)
	if err := s.refreshIndex(ctx, sc, li); err != nil {
		return nil, err
	}
	lg.index, lg.today = li.x.Load(), dayOf(today)
	return lg, nil
}

// has reports whether the transaction's index holds id as kind — its own puts, the tail, or the
// index segments — and its entry. ctx is the transaction's.
func (lg *txLog) has(ctx context.Context, kind encoding.Kind, id encoding.ID) (contentindex.Entry, bool, error) {
	if e, ok := lg.puts[id]; ok {
		return e, e.Kind == kind, nil
	}
	if e, ok := lg.index.Get(id); ok {
		return e, e.Kind == kind, nil
	}
	e, ok, err := lg.segmentEntry(ctx, id)
	return e, ok && e.Kind == kind, err
}

// flushIndexLog writes tx's logged changes to each index it changed as content_log rows — one,
// unless they are more than logRowBytes — and notifies; it returns the scopes it wrote.
func (s *Store) flushIndexLog(ctx context.Context, tx *txn) ([]backend.Scope, error) {
	logs := tx.logs
	tx.logs = nil
	var wrote []backend.Scope
	for _, lg := range logs {
		if len(lg.updates) == 0 {
			continue
		}
		for _, row := range contentindex.EncodeLogs(lg.updates, logRowBytes) {
			if err := tx.InsertContentLog(ctx, lg.scope, row); err != nil {
				return nil, err
			}
		}
		if err := tx.Notify(ctx, contentChannel, lg.scope.String()); err != nil {
			return nil, err
		}
		wrote = append(wrote, lg.scope)
	}
	return wrote, nil
}

// logRowBytes bounds a content log row's body: a transaction's updates go in as many rows as it
// takes, in order. Spanner refuses a value of more than 10 MiB.
const logRowBytes = 4 << 20

func dayOf(t time.Time) contentindex.Day { return contentindex.DayOf(t) }

func touchUpdate(kind encoding.Kind, id encoding.ID, day time.Time) contentindex.Update {
	return contentindex.Update{Op: contentindex.Touch, ID: id, Entry: contentindex.Entry{Kind: kind, Day: dayOf(day)}}
}

func pinUpdate(kind encoding.Kind, id encoding.ID) contentindex.Update {
	return contentindex.Update{Op: contentindex.Pin, ID: id, Entry: contentindex.Entry{Kind: kind}}
}

// LoadContentIndex reads a repository's whole content index: its generation's blocks and every
// log row since. found is false while the repository has none (its backfill has not run).
func (s *Store) LoadContentIndex(ctx context.Context, repo int64) (x *contentindex.Index, found bool, err error) {
	err = s.snapshot(ctx, func(tx backend.Ops) error {
		x, _, _, found, err = s.loadContentIndex(ctx, tx, repoScope(repo))
		return err
	})
	return x, found, err
}

// loadContentIndex is LoadContentIndex in tx, a snapshot: with the log's place it read to, and
// how many of its rows it applied — the tail a fold folds.
func (s *Store) loadContentIndex(ctx context.Context, tx backend.Ops, sc backend.Scope) (x *contentindex.Index, place backend.LogPlace,
	rows int, found bool, err error) {
	err = func() error {
		gen, folded, ok, err := tx.ContentIndexGeneration(ctx, sc)
		if err != nil || !ok {
			return err
		}
		found = true
		x = contentindex.New(gen)
		blocks, err := tx.IndexBlocks(ctx, sc, gen)
		if err != nil {
			return err
		}
		for _, body := range blocks {
			if err := x.ApplyBlock(body); err != nil {
				return fmt.Errorf("%s's content index: %w", sc.Name(), err)
			}
		}
		place, rows, err = s.applyLogs(ctx, tx, sc, x, folded)
		return err
	}()
	return x, place, rows, found, err
}

// applyLogs applies, in the order they apply, the log rows of a content index that committed
// after since — all of them after the generation's fold, for since its place — and returns the
// place it read to and how many it applied.
func (s *Store) applyLogs(ctx context.Context, q backend.Ops, sc backend.Scope, x *contentindex.Index, since backend.LogPlace) (backend.LogPlace, int, error) {
	logs, place, err := q.ContentLogSince(ctx, sc, since)
	if err != nil {
		return nil, 0, err
	}
	for i, l := range logs {
		if err := x.ApplyLog(l.Body); err != nil {
			return nil, i, fmt.Errorf("%s's content log: %w", sc.Name(), err)
		}
	}
	return place, len(logs), nil
}

// RebuildContentIndex folds a repository's content index: the blocks and log rows a snapshot sees
// become a new generation, whose blocks hold the unpinned entries; the pinned ones, the moves
// logged of chunks in index segments — their entries there with their new locations — and a
// tombstone of each drop logged of what an index segment holds, go to a new index segment, the
// newest; entries with only a touch or a pin are dropped. The log rows written
// since stay, to apply over the new generation. One transaction makes the generation and records
// the segment, so a reader that finds an entry gone from the tail finds it in the segment, its
// manifest read anew. It returns the new generation and the entries its blocks hold, and how many
// it moved into the segment; a fold that finds another finished first changes nothing and returns
// generation 0.
func (s *Store) RebuildContentIndex(ctx context.Context, repo int64) (gen int64, entries, moved int, err error) {
	return s.foldIndex(ctx, repoScope(repo))
}

// foldIndex is RebuildContentIndex of a content index of either scope.
func (s *Store) foldIndex(ctx context.Context, sc backend.Scope) (gen int64, entries, moved int, err error) {
	// One fold of an index at a time, across replicas: a fold writes its next generation's blocks
	// before the transaction that makes it the generation, and two would write the same keys. A fold
	// that finds another running changes nothing, as one another finished first does; what it writes
	// it writes only while it holds the claim (data.Fence).
	held, release, ok, err := work.Claim(ctx, s.Work(), foldWork(sc), sc.ID, s.holder, time.Minute)
	if err != nil || !ok {
		return 0, 0, 0, err
	}
	defer release()
	ctx = held
	var snap struct {
		tail       []contentindex.Keyed
		pinned     []segment.Record
		generation int64
		// place is where in the log the snapshot read to: the next generation holds every row
		// committed by then.
		place backend.LogPlace
	}
	err = s.b.Snapshot(ctx, func(tx backend.Ops) error {
		x, place, _, ok, err := s.loadContentIndex(ctx, tx, sc)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%s has no content index", sc.Name())
		}
		snap.generation, snap.place = x.Generation(), place
		held := map[encoding.ID]bool{}
		for _, k := range x.Sorted() {
			switch {
			case !k.Entry.Body:
			case k.Entry.Pinned:
				held[k.ID] = true
				snap.pinned = append(snap.pinned, segment.Record{Key: bytes.Clone(k.ID[:]), Value: contentindex.EncodeEntry(k.Entry)})
			default:
				snap.tail = append(snap.tail, k)
			}
		}
		movedIDs, dropped := x.MovedIDs(), x.DroppedIDs()
		if len(movedIDs) > 0 || len(dropped) > 0 {
			// The index segments as the snapshot sees them: the ones this generation's folds wrote.
			segs, err := tx.Segments(ctx, sc, segIndex)
			if err != nil {
				return err
			}
			readers := make([]*segment.Reader, len(segs))
			for i, sg := range segs {
				if readers[i], err = s.segmentReader(ctx, sg); err != nil {
					return err
				}
			}
			// newest is the newest record of id in the segments, if any holds one.
			newest := func(id encoding.ID) ([]byte, bool, error) {
				for _, r := range readers {
					if !r.MayContain(id[:]) {
						continue
					}
					v, ok, err := r.Get(ctx, id[:])
					if err != nil || ok {
						return v, ok, err
					}
				}
				return nil, false, nil
			}
			for _, id := range movedIDs {
				if held[id] {
					continue // stored anew since: the tail's entry stands
				}
				v, ok, err := newest(id)
				if err != nil {
					return err
				}
				if !ok {
					continue // in no segment: dropped before it moved, and gone
				}
				e, err := indexEntry(x, id, v)
				if err != nil {
					return err
				}
				snap.pinned = append(snap.pinned, segment.Record{Key: bytes.Clone(id[:]), Value: contentindex.EncodeEntry(e)})
			}
			// A drop of what a segment holds becomes a tombstone, which hides the older records.
			for _, k := range dropped {
				v, ok, err := newest(k.ID)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				if e, err := contentindex.DecodeEntry(v); err != nil || !e.Body {
					if err != nil {
						return fmt.Errorf("content index entry %s in a segment: %w", k.ID, err)
					}
					continue // a tombstone already
				}
				snap.pinned = append(snap.pinned, segment.Record{Key: bytes.Clone(k.ID[:]), Value: contentindex.EncodeEntry(k.Entry)})
			}
			slices.SortFunc(snap.pinned, func(a, b segment.Record) int { return bytes.Compare(a.Key, b.Key) })
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	// Written before the transaction that records it: one whose fold another beat is an orphan,
	// deleted once a day old.
	sg, wrote, err := s.writeSegment(ctx, sc, segIndex, 0, segment.Records(snap.pinned))
	if err != nil {
		return 0, 0, 0, err
	}
	// The next generation's blocks, before the transaction that makes it the generation, a
	// transaction at a time — a repository's unpinned files are more than a Spanner transaction
	// holds (114 bytes of blocks a file) — none read until then; first, what a fold that stopped
	// short left: other generations' blocks.
	if err := s.deleteTail(ctx, func(ctx context.Context, tx *txn, limit int) (int64, error) {
		return tx.DeleteOtherIndexBlocks(ctx, sc, snap.generation, limit)
	}); err != nil {
		return 0, 0, 0, err
	}
	next := snap.generation + 1
	if err := s.writeBlocksBatched(ctx, sc, next, snap.tail); err != nil {
		return 0, 0, 0, err
	}
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		gen = 0
		current, err := tx.LockContentIndex(ctx, sc)
		if err != nil {
			return err
		}
		if current != snap.generation {
			return nil
		}
		// The generation holds the log rows the snapshot read, which go after it commits: a
		// reader of it reads only the rows committed after its place.
		if err := tx.SetContentIndexGeneration(ctx, sc, next, snap.place); err != nil {
			return err
		}
		if wrote {
			if err := tx.InsertSegments(ctx, []backend.Segment{sg}); err != nil {
				return err
			}
			if err := tx.Notify(ctx, segmentChannel, sc.String()); err != nil {
				return err
			}
		}
		gen = next
		return nil
	})
	if err != nil || gen == 0 {
		return 0, 0, 0, err
	}
	s.indexChanged(sc)
	if wrote {
		s.segmentsChanged(sc)
	}
	// The generation it replaced, which nothing reads now — a reader of it reads in a snapshot from
	// before — and the log rows it folded, which a reader of the new one skips. The next fold
	// deletes what this does not.
	if err := s.deleteTail(ctx, func(ctx context.Context, tx *txn, limit int) (int64, error) {
		return tx.DeleteOtherIndexBlocks(ctx, sc, next, limit)
	}); err != nil {
		slog.Warn("scm: deleting a content index's replaced blocks", slog.String("index", sc.Name()), slog.Any("err", err))
	}
	if err := s.deleteTail(ctx, func(ctx context.Context, tx *txn, limit int) (int64, error) {
		return tx.DeleteFoldedContentLogs(ctx, sc, snap.place, limit)
	}); err != nil {
		slog.Warn("scm: deleting a content index's folded log", slog.String("index", sc.Name()), slog.Any("err", err))
	}
	return gen, len(snap.tail), len(snap.pinned), nil
}

// foldable is how many of the content index's tail a fold would move into a segment: its pinned
// entries, and its moves of chunks in segments.
func foldable(x *contentindex.Index) int {
	n := len(x.MovedIDs())
	x.Range(func(_ encoding.ID, e contentindex.Entry) bool {
		if e.Pinned {
			n++
		}
		return true
	})
	return n
}

// FoldWork and FoldNetworkWork are a repository's content index's folds and a network's chunk
// index's, as keyed work (internal/work), claimed for a fold's length.
const (
	FoldWork        = "fold"
	FoldNetworkWork = "fold-network"
)

func foldWork(sc backend.Scope) string {
	if sc.Network {
		return FoldNetworkWork
	}
	return FoldWork
}

// writeBlocksBatched writes entries, in ID order, as a generation's blocks, Config.FoldBatch
// blocks to a transaction.
func (s *Store) writeBlocksBatched(ctx context.Context, sc backend.Scope, gen int64, entries []contentindex.Keyed) error {
	per := s.foldBlocks * contentindex.BlockEntries
	for start := 0; start < len(entries); start += per {
		batch := entries[start:min(start+per, len(entries))]
		if err := s.inTx(ctx, func(ctx context.Context, tx *txn) error { return writeBlocks(ctx, tx, sc, gen, batch) }); err != nil {
			return err
		}
	}
	return nil
}

// writeBlocks writes entries, in ID order, as a generation's blocks.
func writeBlocks(ctx context.Context, tx backend.Ops, sc backend.Scope, gen int64, entries []contentindex.Keyed) error {
	blocks := contentindex.Blocks(entries)
	firsts := make([]encoding.ID, len(blocks))
	bodies := make([][]byte, len(blocks))
	for i, b := range blocks {
		firsts[i] = b[0].ID
		bodies[i] = contentindex.EncodeBlock(b)
	}
	for start := 0; start < len(blocks); start += 500 {
		end := min(start+500, len(blocks))
		if err := tx.InsertIndexBlocks(ctx, sc, gen, firsts[start:end], bodies[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// liveIndex is a content index as this replica holds it, for reads.
type liveIndex struct {
	mu sync.Mutex // serializes loading and refreshing
	x  atomic.Pointer[contentindex.Index]
	// place is where in the log x was read to, tail how many log rows it applied over its blocks,
	// and foldMarked whether this replica asked for its fold: under mu.
	place      backend.LogPlace
	tail       int
	foldMarked bool
	packs      atomic.Pointer[map[uint64]encoding.ID] // a network's pack numbers to IDs
	// stale is set when another replica's commit was announced, or this one committed: the next
	// read refreshes first.
	stale atomic.Bool
}

// liveIndexes are the content indexes this replica holds, by scope.
type liveIndexes struct {
	mu      sync.Mutex
	byScope map[backend.Scope]*liveIndex
}

func (s *Store) live(sc backend.Scope) *liveIndex {
	s.indexes.mu.Lock()
	defer s.indexes.mu.Unlock()
	if s.indexes.byScope == nil {
		s.indexes.byScope = map[backend.Scope]*liveIndex{}
	}
	li := s.indexes.byScope[sc]
	if li == nil {
		li = &liveIndex{}
		s.indexes.byScope[sc] = li
	}
	return li
}

// indexChanged marks a live index stale, if this replica holds it.
func (s *Store) indexChanged(sc backend.Scope) {
	s.indexes.mu.Lock()
	li := s.indexes.byScope[sc]
	s.indexes.mu.Unlock()
	if li != nil {
		li.stale.Store(true)
	}
}

// allIndexesChanged marks every live index stale: announcements may have been missed.
func (s *Store) allIndexesChanged() {
	s.indexes.mu.Lock()
	defer s.indexes.mu.Unlock()
	for _, li := range s.indexes.byScope {
		li.stale.Store(true)
	}
}

// contentIndex returns a live index, loading it on first use and refreshing it when it is stale.
func (s *Store) contentIndex(ctx context.Context, sc backend.Scope) (*liveIndex, *contentindex.Index, error) {
	s.startListening()
	li := s.live(sc)
	if x := li.x.Load(); x != nil && !li.stale.Load() {
		return li, x, nil
	}
	if err := s.refreshIndex(ctx, sc, li); err != nil {
		return nil, nil, err
	}
	return li, li.x.Load(), nil
}

// refreshIndex brings a live index up to date, from one view of the database (snapshot): the log
// rows it lacks, or the whole of a new generation. Within a transaction, that is the transaction,
// which has written none of the index's tables yet — its log row goes last, as it commits — and
// whose share of the index's row lock holds the generation and its blocks as it reads them.
//
// The view's connection is held before the index's mutex is taken: a refresh that held the
// mutex while it waited for a connection, beside transactions that held every connection while
// they waited for the mutex, would wait forever.
func (s *Store) refreshIndex(ctx context.Context, sc backend.Scope, li *liveIndex) error {
	markFold := false
	err := s.snapshot(ctx, func(tx backend.Ops) error {
		li.mu.Lock()
		defer li.mu.Unlock()
		// Cleared first, so an announcement during the refresh marks it again.
		li.stale.Store(false)
		gen, _, ok, err := tx.ContentIndexGeneration(ctx, sc)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%s has no content index yet", sc.Name())
		}
		if x := li.x.Load(); x != nil && x.Generation() == gen {
			place, rows, err := s.applyLogs(ctx, tx, sc, x, li.place)
			if err != nil {
				// What it applied of the rows is not where its place says: load it anew.
				li.x.Store(nil)
				return err
			}
			li.place, li.tail = place, li.tail+rows
			markFold = s.foldDue(li, x)
			return nil
		}
		x, place, rows, _, err := s.loadContentIndex(ctx, tx, sc)
		if err != nil {
			return err
		}
		li.place, li.tail, li.foldMarked = place, rows, false
		markFold = s.foldDue(li, x)
		if sc.Network {
			packs, err := tx.PackNumbers(ctx, backend.Network(sc.ID))
			if err != nil {
				return err
			}
			li.packs.Store(&packs)
		}
		li.x.Store(x)
		return nil
	})
	if err != nil {
		li.stale.Store(true)
		return err
	}
	if markFold {
		// Marked once its snapshot is done: on PostgreSQL, another connection while one is held
		// could wait on a pool every other reader holds so.
		if err := s.Work().Mark(ctx, foldDueWork(sc), sc.ID); err != nil {
			li.mu.Lock()
			li.foldMarked = false
			li.mu.Unlock()
			slog.Warn("scm: asking for a content index's fold", slog.String("index", sc.Name()), slog.Any("err", err))
		}
	}
	return nil
}

// A content index's tail is folded once it is as long as an eighth of the index's entries, and
// foldTailMin rows at least: a fold rewrites the index at most eight times for what was written to
// it, and the tail a replica loads stays a fraction of the index.
var foldTailMin = 2000

const foldTailShare = 8

// foldDue reports whether a live index's tail is long enough to fold and this replica has not asked
// for its fold yet, marking that it now has; under li.mu.
func (s *Store) foldDue(li *liveIndex, x *contentindex.Index) bool {
	if li.foldMarked || li.tail < max(foldTailMin, x.Len()/foldTailShare) {
		return false
	}
	li.foldMarked = true
	return true
}

// FoldDueWork and FoldNetworkDueWork are a repository's content index and a network's chunk index
// whose tails grew long, as keyed work (internal/work): a replica that finds one so marks it, and
// one replica folds it (FoldDue).
const (
	FoldDueWork        = "fold-due"
	FoldNetworkDueWork = "fold-network-due"
)

func foldDueWork(sc backend.Scope) string {
	if sc.Network {
		return FoldNetworkDueWork
	}
	return FoldDueWork
}

// FoldDue folds a content index whose tail grew long: a repository's (kind FoldDueWork) or a
// network's (FoldNetworkDueWork). A fold that finds another running leaves it to that one.
func (s *Store) FoldDue(ctx context.Context, kind string, id int64) error {
	sc := repoScope(id)
	if kind == FoldNetworkDueWork {
		sc = backend.NetworkScope(backend.Network(id))
	}
	gen, entries, _, err := s.foldIndex(ctx, sc)
	if err == nil && gen > 0 {
		slog.Info("scm: folded a content index", slog.String("index", sc.Name()), slog.Int64("generation", gen), slog.Int("entries", entries))
	}
	return err
}

// packID is the ID of a pack a network's chunk index names by number, reading the pack table
// again for one it does not know (compaction wrote it since).
func (s *Store) packID(ctx context.Context, net backend.Network, li *liveIndex, num uint64) (encoding.ID, error) {
	if m := li.packs.Load(); m != nil {
		if id, ok := (*m)[num]; ok {
			return id, nil
		}
	}
	m, err := s.ops(ctx).PackNumbers(ctx, net)
	if err != nil {
		return encoding.ID{}, err
	}
	li.packs.Store(&m)
	id, ok := m[num]
	if !ok {
		return encoding.ID{}, fmt.Errorf("network %d's chunk index names pack %d, which it lacks", net, num)
	}
	return id, nil
}

// contentEntries returns the entries of the files or chunks among ids the index holds, of
// kind. One missing is looked for again after a refresh — it may have been committed since —
// and absent after that.
func (s *Store) contentEntries(ctx context.Context, sc backend.Scope, kind encoding.Kind, ids []encoding.ID) (*liveIndex, map[encoding.ID]contentindex.Entry, error) {
	li, x, err := s.contentIndex(ctx, sc)
	if err != nil {
		return nil, nil, err
	}
	out := make(map[encoding.ID]contentindex.Entry, len(ids))
	// lookup finds what out lacks: in the tail, then the index segments.
	lookup := func() error {
		var rest []encoding.ID
		for _, id := range ids {
			if _, done := out[id]; done {
				continue
			}
			if e, ok := x.Get(id); ok && e.Kind == kind {
				out[id] = e
			} else {
				rest = append(rest, id)
			}
		}
		moved, err := s.indexSegmentEntries(ctx, sc, x, kind, rest)
		for id, e := range moved {
			out[id] = e
		}
		return err
	}
	if err := lookup(); err != nil || len(out) == len(ids) {
		return li, out, err
	}
	if err := s.refreshIndex(ctx, sc, li); err != nil {
		return nil, nil, err
	}
	x = li.x.Load()
	return li, out, lookup()
}
