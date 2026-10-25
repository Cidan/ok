package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// A content index's runs (docs/scm.md, "The content index") are segments, each a fold's — what the
// log changed since the fold before, every entry whole as the fold found it, or a tombstone of one
// it dropped — or merged from runs next to each other in age, so a newer run's record supersedes
// an older's and a lookup stops at the newest it finds. A repository's index is two kinds of run:
// pinned (segIndex), which forks inherit, and unpinned (segIndexUnpinned), its unpinned entries and
// their tombstones, which they do not. Pinned is permanent in a repository's index, so a lookup
// takes the pinned kind's record when it has one, the unpinned kind's newest otherwise, and an
// entry's records in the two are never ordered against each other. A network's chunk index is one
// kind, segIndex, holding pinned and unpinned locations, moves, and tombstones together.

// indexRuns are a content index's runs as one read of its manifest named them, newest first.
type indexRuns struct {
	pinned, unpinned []backend.Segment
}

// readRuns reads a content index's runs in q, unlocked: in a snapshot, or a transaction that holds
// the index, they are its generation's.
func (s *Store) readRuns(ctx context.Context, q backend.Ops, sc backend.Scope) (*indexRuns, error) {
	pinned, err := q.SegmentsUnlocked(ctx, sc, segIndex)
	if err != nil {
		return nil, err
	}
	r := &indexRuns{pinned: pinned}
	if !sc.Network {
		if r.unpinned, err = q.SegmentsUnlocked(ctx, sc, segIndexUnpinned); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// runRecord is an entry's newest record in a content index's runs — a tombstone is bodiless — and
// whether it is a repository's pinned kind's.
type runRecord struct {
	e      contentindex.Entry
	pinned bool
}

// errRunGone is a run whose object is gone from the bucket: merged, or retired, long since — the
// runs are read again.
var errRunGone = errors.New("a content index's run is gone")

// records looks up ids' newest records in runs: a repository's pinned kind first, then its unpinned
// kind; a network's one kind. An ID no run holds is absent.
func (s *Store) records(ctx context.Context, sc backend.Scope, runs *indexRuns, ids []encoding.ID) (map[encoding.ID]runRecord, error) {
	out := make(map[encoding.ID]runRecord, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	keys := make([][]byte, len(ids))
	for i := range ids {
		keys[i] = ids[i][:]
	}
	got, err := s.newest(ctx, runs.pinned, keys)
	if err != nil {
		return nil, err
	}
	if err := decodeRecords(got, !sc.Network, out); err != nil {
		return nil, err
	}
	if sc.Network || len(out) == len(ids) {
		return out, nil
	}
	rest := keys[:0:0]
	for _, k := range keys {
		if _, ok := got[string(k)]; !ok {
			rest = append(rest, k)
		}
	}
	if got, err = s.newest(ctx, runs.unpinned, rest); err != nil {
		return nil, err
	}
	return out, decodeRecords(got, false, out)
}

// decodeRecords adds runs' records, by key, to out.
func decodeRecords(got map[string][]byte, pinned bool, out map[encoding.ID]runRecord) error {
	for k, v := range got {
		id := encoding.ID([]byte(k))
		e, err := contentindex.DecodeEntry(v)
		if err != nil {
			return fmt.Errorf("content index entry %s in a run: %w", id, err)
		}
		out[id] = runRecord{e: e, pinned: pinned}
	}
	return nil
}

// newest reads keys from runs, newest first: each found's value in the newest run that holds it,
// through each run's Bloom filter.
func (s *Store) newest(ctx context.Context, runs []backend.Segment, keys [][]byte) (map[string][]byte, error) {
	out := map[string][]byte{}
	for _, sg := range runs {
		var want [][]byte
		for _, k := range keys {
			if _, ok := out[string(k)]; !ok {
				want = append(want, k)
			}
		}
		if len(want) == 0 {
			break
		}
		r, err := s.segmentReader(ctx, sg)
		if err == nil {
			var got map[string][]byte
			if got, err = r.GetMany(ctx, want, s.reads.fetchesAtOnce); err == nil {
				for k, v := range got {
					out[k] = v
				}
				continue
			}
		}
		if errors.Is(err, blob.ErrNotFound) {
			return nil, fmt.Errorf("%w: %v", errRunGone, err)
		}
		return nil, err
	}
	return out, nil
}

// resolveIn resolves ids in a content index as v holds it: each one's updates in the log applied to
// its newest record in the runs (contentindex.Resolve). It returns the entries the index holds, of
// any kind. A run gone from the bucket has the runs read again — in ctx's transaction, if it is in
// one — and the lookup made once more.
func (s *Store) resolveIn(ctx context.Context, sc backend.Scope, v *indexView, ids []encoding.ID) (map[encoding.ID]contentindex.Entry, error) {
	out := make(map[encoding.ID]contentindex.Entry, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	ups := make(map[encoding.ID][]contentindex.Update, len(ids))
	var need []encoding.ID
	for _, id := range ids {
		u := v.log.Updates(id)
		ups[id] = u
		// A drop leaves nothing of what was before it: the runs' record does not count.
		if !slices.ContainsFunc(u, func(u contentindex.Update) bool { return u.Op == contentindex.Drop }) {
			need = append(need, id)
		}
	}
	recs, err := s.records(ctx, sc, v.runs.Load(), need)
	if errors.Is(err, errRunGone) {
		runs, rerr := s.readRuns(ctx, s.ops(ctx), sc)
		if rerr != nil {
			return nil, rerr
		}
		v.runs.Store(runs)
		recs, err = s.records(ctx, sc, runs, need)
	}
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		rec, found := recs[id]
		if e, held := contentindex.Resolve(rec.e, found, ups[id]); held {
			out[id] = e
		}
	}
	return out, nil
}

// contentView is a whole content index, for a scan of it: the view this replica holds, brought up
// to date first, and readers of its runs.
type contentView struct {
	scope              backend.Scope
	view               *indexView
	pinned, unpinned   []*segment.Reader
	pinnedS, unpinnedS []backend.Segment
}

// contentView reads a whole content index for a scan.
func (s *Store) contentView(ctx context.Context, sc backend.Scope) (*contentView, error) {
	li := s.live(sc)
	if err := s.refreshIndex(ctx, sc, li); err != nil {
		return nil, err
	}
	v := li.view.Load()
	runs := v.runs.Load()
	cv := &contentView{scope: sc, view: v, pinnedS: runs.pinned, unpinnedS: runs.unpinned}
	for _, w := range []struct {
		segs []backend.Segment
		into *[]*segment.Reader
	}{{runs.pinned, &cv.pinned}, {runs.unpinned, &cv.unpinned}} {
		for _, sg := range w.segs {
			r, err := s.segmentReader(ctx, sg)
			if err != nil {
				return nil, err
			}
			*w.into = append(*w.into, r)
		}
	}
	return cv, nil
}

// each calls f with every entry the index holds, until f returns false. f must not call into the
// view's index.
func (v *contentView) each(ctx context.Context, f func(id encoding.ID, e contentindex.Entry) bool) error {
	return v.ascend(ctx, encoding.ID{}, f)
}

// ascend calls f with every entry the index holds with an ID at least from, in ID order, until f
// returns false: the runs' newest records — the pinned kind's first — and the log's, merged by ID,
// each entry's updates applied to its record.
func (v *contentView) ascend(ctx context.Context, from encoding.ID, f func(id encoding.ID, e contentindex.Entry) bool) error {
	merged := func(rs []*segment.Reader) segment.Iterator {
		its := make([]segment.Iterator, len(rs))
		for i, r := range rs {
			its[i] = r.Iter(ctx, from[:], nil)
		}
		return segment.Merge(its...)
	}
	p, u := merged(v.pinned), merged(v.unpinned)
	pOK, uOK := p.Next(), u.Next()
	var logIDs []encoding.ID
	for _, id := range v.view.log.IDs() {
		if bytes.Compare(id[:], from[:]) >= 0 {
			logIDs = append(logIDs, id)
		}
	}
	li := 0
	for pOK || uOK || li < len(logIDs) {
		// The least key of the three.
		var key []byte
		for _, k := range [][]byte{keyIf(pOK, p), keyIf(uOK, u), logKey(logIDs, li)} {
			if k != nil && (key == nil || bytes.Compare(k, key) < 0) {
				key = k
			}
		}
		id := encoding.ID(key)
		var rec runRecord
		found := false
		if pOK && bytes.Equal(p.Key(), key) {
			e, err := contentindex.DecodeEntry(p.Value())
			if err != nil {
				return fmt.Errorf("content index entry %s in a run: %w", id, err)
			}
			rec, found = runRecord{e: e, pinned: !v.scope.Network}, true
			pOK = p.Next()
		}
		if uOK && bytes.Equal(u.Key(), key) {
			if !found {
				e, err := contentindex.DecodeEntry(u.Value())
				if err != nil {
					return fmt.Errorf("content index entry %s in a run: %w", id, err)
				}
				rec, found = runRecord{e: e}, true
			}
			uOK = u.Next()
		}
		var ups []contentindex.Update
		if li < len(logIDs) && logIDs[li] == id {
			ups = v.view.log.Updates(id)
			li++
		}
		if e, held := contentindex.Resolve(rec.e, found, ups); held && !f(id, e) {
			return nil
		}
	}
	return errors.Join(p.Err(), u.Err())
}

func keyIf(ok bool, it segment.Iterator) []byte {
	if !ok {
		return nil
	}
	return it.Key()
}

func logKey(ids []encoding.ID, i int) []byte {
	if i >= len(ids) {
		return nil
	}
	return ids[i][:]
}

// touchedOnly are the entries the view's log only touched or pinned — no put, no drop — that no run
// holds: what a log of transactions that each found what they touched never holds (fsck).
func (s *Store) touchedOnly(ctx context.Context, v *contentView) ([]contentindex.Keyed, error) {
	var ids []encoding.ID
	kinds := map[encoding.ID]encoding.Kind{}
	for _, id := range v.view.log.IDs() {
		ups := v.view.log.Updates(id)
		if !slices.ContainsFunc(ups, func(u contentindex.Update) bool { return u.Op == contentindex.Put || u.Op == contentindex.Drop }) {
			ids = append(ids, id)
			kinds[id] = ups[0].Entry.Kind
		}
	}
	recs, err := s.records(ctx, v.scope, &indexRuns{pinned: v.pinnedS, unpinned: v.unpinnedS}, ids)
	if err != nil {
		return nil, err
	}
	var out []contentindex.Keyed
	for _, id := range ids {
		if rec, ok := recs[id]; !ok || !rec.e.Body {
			out = append(out, contentindex.Keyed{ID: id, Entry: contentindex.Entry{Kind: kinds[id]}})
		}
	}
	return out, nil
}

// unpinnedRatio bounds a day group's runs: its two newest merge while the older holds at most this
// many times the newer's records.
const unpinnedRatio = 4

// mergeUnpinned merges a repository's unpinned index's runs within their day groups — the runs one
// day's folds wrote, a band of ages — so no merge puts an older record above a newer one: a group
// whose day ended before today into one run, and a group's two newest while the older holds at most
// unpinnedRatio times the newer's records. A merge that takes the index's oldest run drops its
// tombstones, which hide nothing older. Under the index's fold's claim.
func (s *Store) mergeUnpinned(ctx context.Context, sc backend.Scope, today contentindex.Day) error {
	for {
		segs, err := s.b.Segments(ctx, sc, segIndexUnpinned)
		if err != nil {
			return err
		}
		var pick []backend.Segment
		for start := 0; start < len(segs) && pick == nil; {
			end := start
			for end < len(segs) && segs[end].Day == segs[start].Day {
				end++
			}
			g := segs[start:end]
			switch {
			case contentindex.Day(g[0].Day) < today && len(g) > 1:
				pick = g
			case len(g) >= 2 && g[1].Records <= unpinnedRatio*g[0].Records:
				pick = g[:2]
			}
			start = end
		}
		if pick == nil {
			return nil
		}
		if err := s.mergeRuns(ctx, sc, pick, pick[len(pick)-1].ID == segs[len(segs)-1].ID); err != nil {
			return err
		}
	}
}

// mergeRuns merges segs, runs of a repository's unpinned index next to each other in age, newest
// first, into one as new as the newest, of their day, and retires them; oldest says no run is older,
// so their tombstones go.
func (s *Store) mergeRuns(ctx context.Context, sc backend.Scope, segs []backend.Segment, oldest bool) error {
	its := make([]segment.Iterator, len(segs))
	ids := make([]encoding.ID, len(segs))
	level := int16(0)
	for i, sg := range segs {
		r, err := s.segmentReader(ctx, sg)
		if err != nil {
			return err
		}
		its[i], ids[i] = r.Iter(ctx, nil, nil), sg.ID
		level = max(level, sg.Level+1)
	}
	var it segment.Iterator = segment.Merge(its...)
	if oldest {
		it = segment.Filter(it, func(_, v []byte) bool {
			e, err := contentindex.DecodeEntry(v)
			return err != nil || e.Body
		})
	}
	merged, ok, err := s.writeSegment(ctx, sc, segIndexUnpinned, level, it)
	if err != nil {
		return err
	}
	merged.Age, merged.Day = segs[0].Age, segs[0].Day
	return s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		retired, err := tx.RetireSegments(ctx, sc, segIndexUnpinned, ids)
		if err != nil {
			return err
		}
		if retired != int64(len(ids)) {
			// The merged run is an orphan, deleted once a day old.
			return fmt.Errorf("another merge took %s's unpinned runs first", sc.Name())
		}
		if ok {
			if err := tx.InsertSegments(ctx, []backend.Segment{merged}); err != nil {
				return err
			}
		}
		return tx.Notify(ctx, segmentChannel, sc.String())
	})
}
