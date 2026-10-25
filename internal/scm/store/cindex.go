package store

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/work"
)

// The content index (docs/scm.md, "The content index") is ok scm's only record of
// files and chunks: every transaction that changes one logs the change, in one content_log row
// written as it commits; a transaction that relies on what the index holds reads it under the
// index's row lock (txIndex); and a fold writes what the log changed since the last into runs —
// segments in the bucket — each entry whole, as it now is (foldIndex). A replica holds the log
// since the last fold (contentindex.Log) and reads the runs through, newest first
// (cindexsegments.go): its memory is that log, not the index.
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
	// view is the content index as of when the transaction took its row lock, today the
	// database's day then; touched, pinned, and puts are what the transaction touched, pinned,
	// and put.
	view    *indexView
	today   contentindex.Day
	touched map[encoding.ID]bool
	pinned  map[encoding.ID]bool
	puts    map[encoding.ID]contentindex.Entry
	// found are the entries it resolved in the view, by ID: those the index holds, and absent
	// the rest (load).
	found map[encoding.ID]heldEntry
}

// heldEntry is an entry as a resolution found it: held, or not.
type heldEntry struct {
	e    contentindex.Entry
	held bool
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
		puts: map[encoding.ID]contentindex.Entry{}, found: map[encoding.ID]heldEntry{}}
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
// transaction holds the index's row lock in share mode, so no sweep, compaction, or fold changes
// it before the transaction commits — what it finds stays — while other writers, whose puts,
// touches, and pins commute with its own, run beside it. It returns the database's day too.
func (s *Store) txIndex(ctx context.Context, tx *txn, sc backend.Scope) (*txLog, error) {
	lg := s.txLogOf(tx, sc)
	if lg.view != nil {
		return lg, nil
	}
	today, gen, ok, err := tx.ShareContentIndex(ctx, sc)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%s has no content index", sc.Name())
	}
	// What was committed before the transaction held the index's row is what it must see: no fold
	// commits meanwhile, so a refresh begun since serves it, whoever's it is — and the generation
	// the share read is the index's while it is held.
	li := s.live(sc)
	if err := s.refreshIndexAt(ctx, sc, li, time.Now(), gen); err != nil {
		return nil, err
	}
	lg.view, lg.today = li.view.Load(), dayOf(today)
	return lg, nil
}

// load resolves ids in the transaction's view, those it has not, all at once: what has finds
// after it without reading. ctx is the transaction's.
func (lg *txLog) load(ctx context.Context, ids []encoding.ID) error {
	var want []encoding.ID
	for _, id := range ids {
		if _, ok := lg.found[id]; !ok {
			if _, put := lg.puts[id]; !put {
				want = append(want, id)
			}
		}
	}
	if len(want) == 0 {
		return nil
	}
	got, err := lg.s.resolveIn(ctx, lg.scope, lg.view, want)
	if err != nil {
		return err
	}
	for _, id := range want {
		e, held := got[id]
		lg.found[id] = heldEntry{e, held}
	}
	return nil
}

// has reports whether the transaction's index holds id as kind — its own puts, or the view — and
// its entry. ctx is the transaction's.
func (lg *txLog) has(ctx context.Context, kind encoding.Kind, id encoding.ID) (contentindex.Entry, bool, error) {
	if e, ok := lg.puts[id]; ok {
		return e, e.Kind == kind, nil
	}
	if err := lg.load(ctx, []encoding.ID{id}); err != nil {
		return contentindex.Entry{}, false, err
	}
	h := lg.found[id]
	return h.e, h.held && h.e.Kind == kind, nil
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

// loadView reads, in tx — a snapshot — a content index of generation st: the log since its fold,
// and its runs. It returns the log's place it read to, and how many rows it applied.
func (s *Store) loadView(ctx context.Context, tx backend.Ops, sc backend.Scope, st backend.IndexState) (*indexView, backend.LogPlace, int, error) {
	runs, err := s.readRuns(ctx, tx, sc)
	if err != nil {
		return nil, nil, 0, err
	}
	v := &indexView{log: contentindex.NewLog(st.Generation)}
	v.runs.Store(runs)
	place, rows, err := s.applyLogs(ctx, tx, sc, v.log, st.Folded)
	return v, place, rows, err
}

// applyLogs applies, in the order they apply, the log rows of a content index that committed
// after since — all of them after the generation's fold, for since its place — and returns the
// place it read to and how many it applied.
func (s *Store) applyLogs(ctx context.Context, q backend.Ops, sc backend.Scope, l *contentindex.Log, since backend.LogPlace) (backend.LogPlace, int, error) {
	logs, place, err := q.ContentLogSince(ctx, sc, since)
	if err != nil {
		return nil, 0, err
	}
	for i, r := range logs {
		if err := l.Apply(r.Body); err != nil {
			return nil, i, fmt.Errorf("%s's content log: %w", sc.Name(), err)
		}
	}
	return place, len(logs), nil
}

// RebuildContentIndex folds a repository's content index (foldIndex). It returns the new
// generation, and the unpinned and pinned records its fold wrote; a fold that finds another
// running, or nothing to fold, changes nothing and returns generation 0.
func (s *Store) RebuildContentIndex(ctx context.Context, repo int64) (gen int64, unpinned, pinned int, err error) {
	return s.foldIndex(ctx, repoScope(repo))
}

// foldIndex folds a content index of either scope, holding its fold's claim (foldIndexHeld).
func (s *Store) foldIndex(ctx context.Context, sc backend.Scope) (gen int64, unpinned, pinned int, err error) {
	// One fold of an index at a time, across replicas: what a fold writes it writes only while it
	// holds the claim (data.Fence). A fold that finds another running changes nothing, as one
	// another finished first does.
	held, release, ok, err := work.Claim(ctx, s.Work(), foldWork(sc), sc.ID, s.holder, time.Minute)
	if err != nil || !ok {
		return 0, 0, 0, err
	}
	defer release()
	gen, unpinned, pinned, err = s.foldIndexHeld(held, sc)
	if errors.Is(err, errFoldRaced) {
		return 0, 0, 0, nil
	}
	return gen, unpinned, pinned, err
}

// claimFold claims a content index's fold, waiting while another holds it: for a caller that must
// fold after what it did, which a fold under way may not have read. It returns the claim's context
// and its release.
func (s *Store) claimFold(ctx context.Context, sc backend.Scope) (context.Context, func(), error) {
	for wait := 50 * time.Millisecond; ; wait = min(2*wait, 2*time.Second) {
		held, release, ok, err := work.Claim(ctx, s.Work(), foldWork(sc), sc.ID, s.holder, time.Minute)
		if err != nil || ok {
			return held, release, err
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(wait):
		}
	}
}

// errFoldRaced is a fold that found the index's generation moved since its snapshot: another fold
// committed meanwhile, which only one whose claim lapsed could.
var errFoldRaced = errors.New("another fold of the content index committed first")

// foldIndexHeld folds a content index under its fold's claim: for each entry the log changed since
// the last fold, as one snapshot sees it, it writes the entry as it now is — its updates applied to
// its newest record (contentindex.Resolve) — or, for one dropped that a run holds, a tombstone, into
// a run of each kind. A repository's pinned entries go to its pinned runs, which forks inherit, and
// its unpinned ones and their tombstones to its unpinned index, which they do not; a network's chunk
// index is one kind. One transaction, under the index's lock alone, records the runs and makes the
// next generation, which holds the log the snapshot read: a reader of it reads the rows committed
// after, over the runs. Then the folded rows go, and a repository's unpinned index merges (cindex
// segments.go). It returns the generation, and the unpinned and pinned records it wrote; with
// nothing to fold, generation 0.
func (s *Store) foldIndexHeld(ctx context.Context, sc backend.Scope) (gen int64, unpinned, pinned int, err error) {
	var snap struct {
		generation         int64
		place              backend.LogPlace
		pinned, unpinnedOf []segment.Record
	}
	err = s.b.Snapshot(ctx, func(tx backend.Ops) error {
		snap.pinned, snap.unpinnedOf = nil, nil
		st, ok, err := tx.ContentIndexGeneration(ctx, sc)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%s has no content index", sc.Name())
		}
		v, place, _, err := s.loadView(ctx, tx, sc, st)
		if err != nil {
			return err
		}
		snap.generation, snap.place = st.Generation, place
		ids := v.log.IDs()
		recs, err := s.records(ctx, sc, v.runs.Load(), ids)
		if err != nil {
			return err
		}
		for _, id := range ids {
			rec, found := recs[id]
			e, holds := contentindex.Resolve(rec.e, found, v.log.Updates(id))
			switch {
			case holds && (sc.Network || e.Pinned):
				snap.pinned = append(snap.pinned, segment.Record{Key: bytes.Clone(id[:]), Value: contentindex.EncodeEntry(e)})
			case holds:
				snap.unpinnedOf = append(snap.unpinnedOf, segment.Record{Key: bytes.Clone(id[:]), Value: contentindex.EncodeEntry(e)})
			case found && rec.e.Body:
				tomb := segment.Record{Key: bytes.Clone(id[:]), Value: contentindex.EncodeEntry(contentindex.Entry{Kind: rec.e.Kind})}
				switch {
				case sc.Network:
					snap.pinned = append(snap.pinned, tomb)
				case rec.pinned:
					// Pinned is permanent in a repository's index: its pinned runs, which forks take, are
					// never superseded.
					return fmt.Errorf("%s's content index: %s %s, pinned, was dropped", sc.Name(), rec.e.Kind, id)
				default:
					snap.unpinnedOf = append(snap.unpinnedOf, tomb)
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	if len(snap.pinned) == 0 && len(snap.unpinnedOf) == 0 {
		return 0, 0, 0, nil
	}
	// Written before the transaction that records them: one whose fold another beat is an orphan,
	// deleted once a day old.
	var runs []backend.Segment
	for _, w := range []struct {
		kind segment.Kind
		recs []segment.Record
	}{{segIndex, snap.pinned}, {segIndexUnpinned, snap.unpinnedOf}} {
		if len(w.recs) == 0 {
			continue
		}
		sg, wrote, err := s.writeSegment(ctx, sc, w.kind, 0, segment.Records(w.recs))
		if err != nil {
			return 0, 0, 0, err
		}
		if wrote {
			runs = append(runs, sg)
		}
	}
	next := snap.generation + 1
	var today time.Time
	err = s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		gen = 0
		current, t, err := tx.LockContentIndex(ctx, sc)
		if err != nil {
			return err
		}
		if current != snap.generation {
			return errFoldRaced
		}
		today = t
		if err := tx.SetContentIndexGeneration(ctx, sc, next, snap.place); err != nil {
			return err
		}
		for i := range runs {
			if runs[i].Kind == segIndexUnpinned {
				runs[i].Day = int64(dayOf(today))
			}
		}
		if err := tx.InsertSegments(ctx, runs); err != nil {
			return err
		}
		if err := tx.Notify(ctx, segmentChannel, sc.String()); err != nil {
			return err
		}
		gen = next
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	s.indexChanged(sc)
	s.segmentsChanged(sc)
	// The log rows it folded, which a reader of the new generation skips. The next fold deletes
	// what this does not.
	if err := s.deleteTail(ctx, func(ctx context.Context, tx *txn, limit int) (int64, error) {
		return tx.DeleteFoldedContentLogs(ctx, sc, snap.place, limit)
	}); err != nil {
		slog.Warn("scm: deleting a content index's folded log", slog.String("index", sc.Name()), slog.Any("err", err))
	}
	if !sc.Network {
		if err := s.mergeUnpinned(ctx, sc, dayOf(today)); err != nil {
			slog.Warn("scm: merging an unpinned index's runs", slog.String("index", sc.Name()), slog.Any("err", err))
		}
	}
	return gen, len(snap.unpinnedOf), len(snap.pinned), nil
}

// foldable is how many entries the content index's log changed: what a fold would write.
func foldable(v *indexView) int { return v.log.Len() }

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

// indexView is a content index as a replica read it: the log since its generation's fold, and its
// runs as the manifest read with the generation named them. A merge since names other runs that
// hold the same, and a retirement ones nothing needs (cindexsegments.go), so the runs are read
// again only when one is gone from the bucket; a fold makes another generation, read anew.
type indexView struct {
	log  *contentindex.Log
	runs atomic.Pointer[indexRuns]
}

// liveIndex is a content index as this replica holds it, for reads.
type liveIndex struct {
	mu   sync.Mutex // serializes loading and refreshing
	view atomic.Pointer[indexView]
	// place is where in the log the view was read to, tail how many log rows it applied, and
	// foldMarked whether this replica asked for its fold: under mu.
	place      backend.LogPlace
	tail       int
	foldMarked bool
	// packs are a network's pack numbers' IDs this replica has read (packIDs): a number is one
	// pack's for good, so they are kept across generations.
	packMu sync.Mutex
	packs  map[uint64]encoding.ID
	// stale is set when another replica's commit was announced, or this one committed: the next
	// read refreshes first.
	stale atomic.Bool
	// readFrom is when the last refresh that succeeded began reading — once it held mu, before its
	// view's first read — so it holds what was committed before then: a refresh asked for since a
	// moment it began after is done already. Under mu.
	readFrom time.Time
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

// contentIndex returns a live index's view, loading it on first use and refreshing it when it is
// stale.
func (s *Store) contentIndex(ctx context.Context, sc backend.Scope) (*liveIndex, *indexView, error) {
	s.startListening()
	li := s.live(sc)
	seen := time.Now()
	if v := li.view.Load(); v != nil && !li.stale.Load() {
		return li, v, nil
	}
	// Stale when seen: a refresh begun since serves this read.
	if err := s.refreshIndexAt(ctx, sc, li, seen, 0); err != nil {
		return nil, nil, err
	}
	return li, li.view.Load(), nil
}

// refreshIndex brings a live index up to date, from one view of the database (snapshot): the log
// rows it lacks, or a new generation — its runs and the log since its fold. Within a transaction,
// that is the transaction, which has written none of the index's tables yet — its log row goes
// last, as it commits — and whose share of the index's row lock holds the generation as it reads.
//
// The view's connection is held before the index's mutex is taken: a refresh that held the
// mutex while it waited for a connection, beside transactions that held every connection while
// they waited for the mutex, would wait forever.
func (s *Store) refreshIndex(ctx context.Context, sc backend.Scope, li *liveIndex) error {
	return s.refreshIndexAt(ctx, sc, li, time.Now(), 0)
}

// refreshIndexAt is refreshIndex for a caller that needs what was committed before need: a refresh
// that began since — another caller's, which this one waited for on the mutex — serves it, so
// callers that find the index stale together read the database once, not one after another (a
// repository's publishes, each staling every replica's index, would otherwise queue on its mutex a
// read each). A caller that knows the index's generation (known; 0 for none) — a transaction
// holding the index in share mode, which no fold moves meanwhile — has a view of that generation
// brought up to date by the log since its place alone, one read; the generation is read otherwise.
//
// A caller reading through a read-only snapshot (inSnapshot) refreshes through it, which sees what
// was committed before the snapshot began and nothing after its first read: it needs no more, and
// its refresh claims no more — its view is current as of the snapshot's beginning, not its own —
// leaves the index stale for the next reader that is not, and never replaces a view of a later
// generation than its snapshot sees. Otherwise a commit after the snapshot, which it cleared the
// index's staleness for and claimed to hold, was missing from the view every replica's readers took
// as current: an upload's pin then found the tree its publish stored missing.
func (s *Store) refreshIndexAt(ctx context.Context, sc backend.Scope, li *liveIndex, need time.Time, known int64) error {
	snap := snapshotOf(ctx)
	if !snap.IsZero() && need.After(snap) {
		need = snap
	}
	markFold := false
	asked := time.Now()
	result := "served" // by another's refresh
	err := s.snapshot(ctx, func(tx backend.Ops) error {
		li.mu.Lock()
		began := time.Now()
		indexWaits.Observe(began.Sub(asked).Seconds())
		read := began
		if !snap.IsZero() && snap.Before(read) {
			read = snap
		}
		defer func() {
			li.mu.Unlock()
			if result != "served" {
				indexHolds.With(result).Since(began)
			}
		}()
		if li.view.Load() != nil && !li.readFrom.Before(need) {
			return nil
		}
		result = "applied"
		if s.indexReads != nil {
			s.indexReads()
		}
		// Cleared first, so an announcement during the refresh marks it again.
		if snap.IsZero() {
			li.stale.Store(false)
		}
		if v := li.view.Load(); v != nil && known != 0 && v.log.Generation() == known {
			place, rows, err := s.applyLogs(ctx, tx, sc, v.log, li.place)
			if err != nil {
				li.view.Store(nil)
				return err
			}
			li.place, li.tail, li.readFrom = place, li.tail+rows, read
			markFold = s.foldDue(li, v)
			return nil
		}
		st, ok, err := tx.ContentIndexGeneration(ctx, sc)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%s has no content index yet", sc.Name())
		}
		if v := li.view.Load(); v != nil && v.log.Generation() == st.Generation {
			place, rows, err := s.applyLogs(ctx, tx, sc, v.log, li.place)
			if err != nil {
				// What it applied of the rows is not where its place says: load it anew.
				li.view.Store(nil)
				return err
			}
			li.place, li.tail, li.readFrom = place, li.tail+rows, read
			markFold = s.foldDue(li, v)
			return nil
		}
		if v := li.view.Load(); v != nil && !snap.IsZero() && st.Generation < v.log.Generation() {
			// The snapshot is older than the view, which holds all the snapshot can see.
			result = "served"
			return nil
		}
		result = "loaded"
		v, place, rows, err := s.loadView(ctx, tx, sc, st)
		if err != nil {
			return err
		}
		li.place, li.tail, li.foldMarked = place, rows, false
		markFold = s.foldDue(li, v)
		li.view.Store(v)
		li.readFrom = read
		return nil
	})
	indexRefreshes.With(result).Inc()
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

var (
	indexRefreshes = metrics.NewCounter("ok_scm_index_refreshes_total",
		"Refreshes of a replica's content indexes, by what each did: applied the log since its place, loaded a new generation — its runs and the log since its fold — or was served by another's begun since it was asked for.", "result")
	indexWaits = metrics.NewHistogram("ok_scm_index_refresh_wait_seconds",
		"How long a refresh of a content index waited for the index's mutex: its turn behind the refresh under way.", metrics.Seconds)
	indexHolds = metrics.NewHistogram("ok_scm_index_refresh_seconds",
		"How long a refresh held a content index's mutex, by what it did: applied the log, or loaded a generation.", metrics.Seconds, "result")
)

// foldLogEntries is how many entries a content index's log changes before it is folded: a
// replica's memory of the index is its log, and a fold writes a run of them. Garbage collection's
// pass folds every index daily besides.
var foldLogEntries = 1 << 16

// foldDue reports whether a live index's log is long enough to fold and this replica has not
// asked for its fold yet, marking that it now has; under li.mu.
func (s *Store) foldDue(li *liveIndex, v *indexView) bool {
	if li.foldMarked || v.log.Len() < foldLogEntries {
		return false
	}
	li.foldMarked = true
	return true
}

// FoldDueWork and FoldNetworkDueWork are a repository's content index and a network's chunk index
// whose logs grew long, as keyed work (internal/work): a replica that finds one so marks it, and
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

// FoldDue folds a content index whose log grew long: a repository's (kind FoldDueWork) or a
// network's (FoldNetworkDueWork). A fold that finds another running leaves it to that one.
func (s *Store) FoldDue(ctx context.Context, kind string, id int64) error {
	sc := repoScope(id)
	if kind == FoldNetworkDueWork {
		sc = backend.NetworkScope(backend.Network(id))
	}
	gen, unpinned, pinned, err := s.foldIndex(ctx, sc)
	if err == nil && gen > 0 {
		slog.Info("scm: folded a content index", slog.String("index", sc.Name()), slog.Int64("generation", gen),
			slog.Int("unpinned", unpinned), slog.Int("pinned", pinned))
	}
	return err
}

// packID is the ID of a pack a network's chunk index names by number (packIDs).
func (s *Store) packID(ctx context.Context, net backend.Network, li *liveIndex, num uint64) (encoding.ID, error) {
	ids, err := s.packIDs(ctx, net, li, []uint64{num})
	if err != nil {
		return encoding.ID{}, err
	}
	return ids[num], nil
}

// packIDs are the IDs of the packs a network's chunk index names by numbers: those this replica
// read before, and the rest read now, just those — a network writes a pack a publish, so it holds
// millions.
func (s *Store) packIDs(ctx context.Context, net backend.Network, li *liveIndex, nums []uint64) (map[uint64]encoding.ID, error) {
	out := make(map[uint64]encoding.ID, len(nums))
	var ask []uint64
	li.packMu.Lock()
	for _, n := range nums {
		if id, ok := li.packs[n]; ok {
			out[n] = id
		} else {
			ask = append(ask, n)
		}
	}
	li.packMu.Unlock()
	if len(ask) == 0 {
		return out, nil
	}
	slices.Sort(ask)
	ask = slices.Compact(ask)
	got, err := s.ops(ctx).NumberedPacks(ctx, net, ask)
	if err != nil {
		return nil, err
	}
	li.packMu.Lock()
	if li.packs == nil {
		li.packs = map[uint64]encoding.ID{}
	}
	for n, id := range got {
		li.packs[n] = id
		out[n] = id
	}
	li.packMu.Unlock()
	for _, n := range ask {
		if _, ok := got[n]; !ok {
			return nil, fmt.Errorf("network %d's chunk index names pack %d, which it lacks", net, n)
		}
	}
	return out, nil
}

// contentEntries returns the entries of the files or chunks among ids the index holds, of
// kind. One missing is looked for again after a refresh — it may have been committed since —
// and absent after that.
func (s *Store) contentEntries(ctx context.Context, sc backend.Scope, kind encoding.Kind, ids []encoding.ID) (*liveIndex, map[encoding.ID]contentindex.Entry, error) {
	return s.entries(ctx, sc, kind, ids, false)
}

// knownEntries is contentEntries for a caller an entry the index dropped or moved since does no
// harm: the replica's live index is read as it is, and refreshed only for what it lacks. So are a
// publish's first looks at what the repository holds, which its touch checks again under the
// index's lock (touchHeld), and files' manifests and sizes, which a file's ID fixes — not chunks'
// locations, which compaction moves. A replica of a busy repository hears of a write every few
// milliseconds: refreshing for each, every such caller would wait its turn on the index's mutex.
func (s *Store) knownEntries(ctx context.Context, sc backend.Scope, kind encoding.Kind, ids []encoding.ID) (*liveIndex, map[encoding.ID]contentindex.Entry, error) {
	return s.entries(ctx, sc, kind, ids, true)
}

// entries is contentEntries, or — asIs — knownEntries.
func (s *Store) entries(ctx context.Context, sc backend.Scope, kind encoding.Kind, ids []encoding.ID, asIs bool) (*liveIndex, map[encoding.ID]contentindex.Entry, error) {
	var li *liveIndex
	var v *indexView
	if asIs {
		s.startListening()
		li = s.live(sc)
		v = li.view.Load()
	}
	if v == nil {
		var err error
		if li, v, err = s.contentIndex(ctx, sc); err != nil {
			return nil, nil, err
		}
	}
	out := make(map[encoding.ID]contentindex.Entry, len(ids))
	// lookup resolves what out lacks.
	lookup := func() error {
		var rest []encoding.ID
		for _, id := range ids {
			if _, done := out[id]; !done {
				rest = append(rest, id)
			}
		}
		got, err := s.resolveIn(ctx, sc, v, rest)
		for id, e := range got {
			if e.Kind == kind {
				out[id] = e
			}
		}
		return err
	}
	if err := lookup(); err != nil || len(out) == len(ids) {
		return li, out, err
	}
	if err := s.refreshIndex(ctx, sc, li); err != nil {
		return nil, nil, err
	}
	v = li.view.Load()
	return li, out, lookup()
}
