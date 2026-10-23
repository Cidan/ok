package store

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// The content index (docs/scm.md, "The content index") is ok scm's only record of
// files and chunks: every transaction that changes one logs the change, in one content_log row
// written as it commits; a transaction that relies on what the index holds reads it under the
// index's row lock (txIndex); and every garbage collection pass folds the log into the next
// generation's blocks (RebuildContentIndex).

// contentChannel is the channel each logging transaction notifies, with the repository's ID,
// as it commits.
const contentChannel = "ok_scm_content"

// txLog is what one transaction changed in a repository's content index, and the index it
// decides by (txIndex).
type txLog struct {
	repo    int64
	updates []contentindex.Update
	// index is the content index as of when the transaction took its row lock, today the
	// database's day then; touched and pinned are what the transaction touched and pinned.
	index   *contentindex.Index
	today   contentindex.Day
	touched map[encoding.ID]bool
	pinned  map[encoding.ID]bool
	puts    map[encoding.ID]contentindex.Entry
}

func (s *Store) txLogOf(tx *txn, repo int64) *txLog {
	lg := tx.log
	if lg == nil {
		lg = &txLog{repo: repo, touched: map[encoding.ID]bool{}, pinned: map[encoding.ID]bool{}, puts: map[encoding.ID]contentindex.Entry{}}
		tx.log = lg
	}
	if lg.repo != repo {
		panic(fmt.Sprintf("store: one transaction changes content of repositories %d and %d", lg.repo, repo))
	}
	return lg
}

// logIndex records, for the transaction tx, changes to repo's content index.
func (s *Store) logIndex(tx *txn, repo int64, updates ...contentindex.Update) {
	if len(updates) == 0 {
		return
	}
	lg := s.txLogOf(tx, repo)
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
func (s *Store) txIndex(ctx context.Context, tx *txn, repo int64) (*txLog, error) {
	lg := s.txLogOf(tx, repo)
	if lg.index != nil {
		return lg, nil
	}
	today, ok, err := tx.ShareContentIndex(ctx, repo)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("repository %d has no content index", repo)
	}
	li := s.live(repo)
	if err := s.refreshIndex(ctx, repo, li); err != nil {
		return nil, err
	}
	lg.index, lg.today = li.x.Load(), dayOf(today)
	return lg, nil
}

// has reports whether the transaction's index holds id as kind, its own puts included.
func (lg *txLog) has(kind encoding.Kind, id encoding.ID) (contentindex.Entry, bool) {
	if e, ok := lg.puts[id]; ok {
		return e, e.Kind == kind
	}
	e, ok := lg.index.Get(id)
	return e, ok && e.Kind == kind
}

// loggedRow is a content_log row a transaction wrote.
type loggedRow struct {
	repo, id int64
	body     []byte
}

// flushIndexLog writes tx's logged changes, if any, as one content_log row, and notifies.
func (s *Store) flushIndexLog(ctx context.Context, tx *txn) (*loggedRow, error) {
	lg := tx.log
	tx.log = nil
	if lg == nil || len(lg.updates) == 0 {
		return nil, nil
	}
	row := &loggedRow{repo: lg.repo, body: contentindex.EncodeLog(lg.updates)}
	var err error
	if row.id, err = tx.InsertContentLog(ctx, lg.repo, row.body); err != nil {
		return nil, err
	}
	if err := tx.Notify(ctx, contentChannel, strconv.FormatInt(lg.repo, 10)); err != nil {
		return nil, err
	}
	return row, nil
}

func dayOf(t time.Time) contentindex.Day { return contentindex.DayOf(t) }

func touchUpdate(kind encoding.Kind, id encoding.ID, day time.Time) contentindex.Update {
	return contentindex.Update{Op: contentindex.Touch, ID: id, Entry: contentindex.Entry{Kind: kind, Day: dayOf(day)}}
}

func pinUpdate(kind encoding.Kind, id encoding.ID) contentindex.Update {
	return contentindex.Update{Op: contentindex.Pin, ID: id, Entry: contentindex.Entry{Kind: kind}}
}

// LoadContentIndex reads a repository's whole content index: its generation's blocks and every
// log row. found is false while the repository has none (its backfill has not run).
func (s *Store) LoadContentIndex(ctx context.Context, repo int64) (x *contentindex.Index, found bool, err error) {
	err = s.b.Snapshot(ctx, func(tx backend.Ops) error {
		x, found, err = s.loadContentIndex(ctx, tx, repo)
		return err
	})
	return x, found, err
}

// loadContentIndex is LoadContentIndex in tx.
func (s *Store) loadContentIndex(ctx context.Context, tx backend.Ops, repo int64) (x *contentindex.Index, found bool, err error) {
	err = func() error {
		gen, ok, err := tx.ContentIndexGeneration(ctx, repo)
		if err != nil || !ok {
			return err
		}
		found = true
		x = contentindex.New(gen)
		blocks, err := tx.IndexBlocks(ctx, repo, gen)
		if err != nil {
			return err
		}
		for _, body := range blocks {
			if err := x.ApplyBlock(body); err != nil {
				return fmt.Errorf("repository %d's content index: %w", repo, err)
			}
		}
		return s.applyLogs(ctx, tx, repo, x)
	}()
	return x, found, err
}

// applyLogs applies every log row of repo that x lacks.
func (s *Store) applyLogs(ctx context.Context, q backend.Ops, repo int64, x *contentindex.Index) error {
	all, err := q.ContentLogIDs(ctx, repo)
	if err != nil {
		return err
	}
	var want []int64
	for _, id := range all {
		if !x.HasLog(id) {
			want = append(want, id)
		}
	}
	if len(want) == 0 {
		return nil
	}
	// In ID order: a later row never needs an earlier one, but applying them as written keeps
	// a touch from preceding its entry's body more often than it must.
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	logs, err := q.ContentLogs(ctx, repo, want)
	if err != nil {
		return err
	}
	for _, l := range logs {
		if err := x.ApplyLog(l.ID, l.Body); err != nil {
			return fmt.Errorf("repository %d's content log row %d: %w", repo, l.ID, err)
		}
	}
	return nil
}

// RebuildContentIndex writes a repository's content index as a new generation in place of the
// last: its blocks and the log rows a snapshot sees, folded into new blocks, with entries that
// have only a touch or a pin dropped. The log rows written since stay, to apply over the new
// blocks. It returns the new generation and its entries; a rebuild that finds another finished
// first changes nothing and returns generation 0.
func (s *Store) RebuildContentIndex(ctx context.Context, repo int64) (gen int64, entries int, err error) {
	var snap struct {
		entries    []contentindex.Keyed
		generation int64
		logs       []int64
	}
	err = s.b.Snapshot(ctx, func(tx backend.Ops) error {
		gen, ok, err := tx.ContentIndexGeneration(ctx, repo)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("repository %d has no content index", repo)
		}
		snap.generation = gen
		x, _, err := s.loadContentIndex(ctx, tx, repo)
		if err != nil {
			return err
		}
		for _, k := range x.Sorted() {
			if k.Entry.Body {
				snap.entries = append(snap.entries, k)
			}
		}
		snap.logs, err = tx.ContentLogIDs(ctx, repo)
		return err
	})
	if err != nil {
		return 0, 0, err
	}
	err = s.inTx(ctx, func(tx *txn) error {
		gen = 0
		current, err := tx.LockContentIndex(ctx, repo)
		if err != nil {
			return err
		}
		if current != snap.generation {
			return nil
		}
		if err := tx.SetContentIndexGeneration(ctx, repo, current+1); err != nil {
			return err
		}
		if err := tx.DeleteIndexBlocks(ctx, repo, current); err != nil {
			return err
		}
		if err := tx.DeleteContentLogs(ctx, repo, snap.logs); err != nil {
			return err
		}
		gen = snap.generation + 1
		return writeBlocks(ctx, tx, repo, gen, snap.entries)
	})
	if err != nil || gen == 0 {
		return 0, 0, err
	}
	s.indexChanged(repo)
	return gen, len(snap.entries), nil
}

// writeBlocks writes entries, in ID order, as a generation's blocks.
func writeBlocks(ctx context.Context, tx backend.Ops, repo, gen int64, entries []contentindex.Keyed) error {
	blocks := contentindex.Blocks(entries)
	firsts := make([]encoding.ID, len(blocks))
	bodies := make([][]byte, len(blocks))
	for i, b := range blocks {
		firsts[i] = b[0].ID
		bodies[i] = contentindex.EncodeBlock(b)
	}
	for start := 0; start < len(blocks); start += 500 {
		end := min(start+500, len(blocks))
		if err := tx.InsertIndexBlocks(ctx, repo, gen, firsts[start:end], bodies[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// liveIndex is a repository's content index as this replica holds it, for reads.
type liveIndex struct {
	mu    sync.Mutex // serializes loading and refreshing
	x     atomic.Pointer[contentindex.Index]
	packs atomic.Pointer[map[uint64]encoding.ID] // pack numbers to IDs
	// stale is set when another replica's commit was announced, or this one committed: the next
	// read refreshes first.
	stale atomic.Bool
}

// liveIndexes are the content indexes this replica holds, by repository.
type liveIndexes struct {
	mu     sync.Mutex
	byRepo map[int64]*liveIndex
}

func (s *Store) live(repo int64) *liveIndex {
	s.indexes.mu.Lock()
	defer s.indexes.mu.Unlock()
	if s.indexes.byRepo == nil {
		s.indexes.byRepo = map[int64]*liveIndex{}
	}
	li := s.indexes.byRepo[repo]
	if li == nil {
		li = &liveIndex{}
		s.indexes.byRepo[repo] = li
	}
	return li
}

// indexChanged marks a repository's live index stale, if this replica holds it.
func (s *Store) indexChanged(repo int64) {
	s.indexes.mu.Lock()
	li := s.indexes.byRepo[repo]
	s.indexes.mu.Unlock()
	if li != nil {
		li.stale.Store(true)
	}
}

// allIndexesChanged marks every live index stale: announcements may have been missed.
func (s *Store) allIndexesChanged() {
	s.indexes.mu.Lock()
	defer s.indexes.mu.Unlock()
	for _, li := range s.indexes.byRepo {
		li.stale.Store(true)
	}
}

// contentIndex returns a repository's live index, loading it on first use and refreshing it
// when it is stale.
func (s *Store) contentIndex(ctx context.Context, repo int64) (*liveIndex, *contentindex.Index, error) {
	s.startListening()
	li := s.live(repo)
	if x := li.x.Load(); x != nil && !li.stale.Load() {
		return li, x, nil
	}
	if err := s.refreshIndex(ctx, repo, li); err != nil {
		return nil, nil, err
	}
	return li, li.x.Load(), nil
}

// refreshIndex brings a live index up to date, from one snapshot: the log rows it lacks, or the
// whole of a new generation.
func (s *Store) refreshIndex(ctx context.Context, repo int64, li *liveIndex) error {
	li.mu.Lock()
	defer li.mu.Unlock()
	// Cleared first, so an announcement during the refresh marks it again.
	li.stale.Store(false)
	err := s.b.Snapshot(ctx, func(tx backend.Ops) error {
		gen, ok, err := tx.ContentIndexGeneration(ctx, repo)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("repository %d has no content index yet", repo)
		}
		if x := li.x.Load(); x != nil && x.Generation() == gen {
			return s.applyLogs(ctx, tx, repo, x)
		}
		x, _, err := s.loadContentIndex(ctx, tx, repo)
		if err != nil {
			return err
		}
		packs, err := tx.PackNumbers(ctx, repo)
		if err != nil {
			return err
		}
		li.packs.Store(&packs)
		li.x.Store(x)
		return nil
	})
	if err != nil {
		li.stale.Store(true)
	}
	return err
}

// packID is the ID of a pack the index names by number, reading the pack table again for one
// it does not know (compaction wrote it since).
func (s *Store) packID(ctx context.Context, repo int64, li *liveIndex, num uint64) (encoding.ID, error) {
	if m := li.packs.Load(); m != nil {
		if id, ok := (*m)[num]; ok {
			return id, nil
		}
	}
	m, err := s.b.PackNumbers(ctx, repo)
	if err != nil {
		return encoding.ID{}, err
	}
	li.packs.Store(&m)
	id, ok := m[num]
	if !ok {
		return encoding.ID{}, fmt.Errorf("repository %d's content index names pack %d, which it lacks", repo, num)
	}
	return id, nil
}

// contentEntries returns the entries of the files or chunks among ids the index holds, of
// kind. One missing is looked for again after a refresh — it may have been committed since —
// and absent after that.
func (s *Store) contentEntries(ctx context.Context, repo int64, kind encoding.Kind, ids []encoding.ID) (*liveIndex, map[encoding.ID]contentindex.Entry, error) {
	li, x, err := s.contentIndex(ctx, repo)
	if err != nil {
		return nil, nil, err
	}
	out := make(map[encoding.ID]contentindex.Entry, len(ids))
	missing := false
	for _, id := range ids {
		if e, ok := x.Get(id); ok && e.Kind == kind {
			out[id] = e
		} else {
			missing = true
		}
	}
	if !missing {
		return li, out, nil
	}
	if err := s.refreshIndex(ctx, repo, li); err != nil {
		return nil, nil, err
	}
	x = li.x.Load()
	for _, id := range ids {
		if _, done := out[id]; done {
			continue
		}
		if e, ok := x.Get(id); ok && e.Kind == kind {
			out[id] = e
		}
	}
	return li, out, nil
}
