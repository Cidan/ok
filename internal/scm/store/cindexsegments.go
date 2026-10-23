package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/Cidan/ok/internal/scm/contentindex"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// The content index's pinned entries leave its tail — the blocks of its generation and the log
// since — for index segments, when the index is folded (RebuildContentIndex): the tail keeps what
// is unpinned, and every replica's memory holds only the tail. A file or chunk the tail lacks is
// looked for in the index segments, newest first, and a chunk's entry there takes the location of
// the last move the tail logged of it: repacking moves pinned chunks, and the next fold writes their
// entries anew. Pinned is permanent, so nothing in a segment is ever dropped, touched, or pinned.

// indexEntry decodes an index segment's record of id, the tail's last move of it applied: bodiless
// — absent — when the tail logged a drop of it since, or the record is a tombstone a fold wrote of
// one.
func indexEntry(x *contentindex.Index, id encoding.ID, value []byte) (contentindex.Entry, error) {
	e, err := contentindex.DecodeEntry(value)
	if err != nil {
		return e, fmt.Errorf("content index entry %s in a segment: %w", id, err)
	}
	if x.Dropped(id) {
		return contentindex.Entry{Kind: e.Kind}, nil
	}
	if l, ok := x.Moved(id); ok && e.Kind == encoding.KindChunk {
		e.Loc = l
	}
	return e, nil
}

// indexSegmentEntries looks up, in a content index's segments, the entries of kind among ids,
// x's moves applied — the manifest read anew for any it misses.
func (s *Store) indexSegmentEntries(ctx context.Context, sc backend.Scope, x *contentindex.Index, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID]contentindex.Entry, error) {
	out := map[encoding.ID]contentindex.Entry{}
	if len(ids) == 0 {
		return out, nil
	}
	keys := make([][]byte, len(ids))
	for i := range ids {
		keys[i] = ids[i][:]
	}
	got, err := s.segmentGet(ctx, sc, segIndex, keys)
	if err != nil {
		return nil, err
	}
	for k, v := range got {
		id := encoding.ID([]byte(k))
		e, err := indexEntry(x, id, v)
		if err != nil {
			return nil, err
		}
		if e.Kind == kind && e.Body {
			out[id] = e
		}
	}
	return out, nil
}

// contentView is a whole content index, for a scan of it: the tail this replica
// holds, brought up to date first, and the index segments of the manifest read after it — so they
// hold whatever the tail's generation folded out of it — the tail's moves applied to theirs.
type contentView struct {
	x       *contentindex.Index
	readers []*segment.Reader
}

// contentView reads a whole content index for a scan.
func (s *Store) contentView(ctx context.Context, sc backend.Scope) (*contentView, error) {
	li := s.live(sc)
	if err := s.refreshIndex(ctx, sc, li); err != nil {
		return nil, err
	}
	readers, err := s.segmentReaders(ctx, sc, segIndex, true)
	if err != nil {
		return nil, err
	}
	return &contentView{x: li.x.Load(), readers: readers}, nil
}

// each calls f with every entry that has a body — the tail's, then the segments' the tail lacks —
// until f returns false. f must not call into the view's index.
func (v *contentView) each(ctx context.Context, f func(id encoding.ID, e contentindex.Entry) bool) error {
	held := map[encoding.ID]bool{}
	stopped := false
	v.x.Range(func(id encoding.ID, e contentindex.Entry) bool {
		held[id] = true
		stopped = !f(id, e)
		return !stopped
	})
	if stopped {
		return nil
	}
	var derr error
	err := scanSegments(ctx, v.readers, nil, nil, nil, false, func(k, val []byte) bool {
		id := encoding.ID(k)
		if held[id] {
			return true
		}
		e, err := indexEntry(v.x, id, val)
		if err != nil {
			derr = err
			return false
		}
		return !e.Body || f(id, e)
	})
	return errors.Join(err, derr)
}

// ascend calls f with every entry that has a body and an ID at least from, in ID order, until f
// returns false.
func (v *contentView) ascend(ctx context.Context, from encoding.ID, f func(id encoding.ID, e contentindex.Entry) bool) error {
	var tail []contentindex.Keyed
	for _, k := range v.x.Sorted() {
		if k.Entry.Body && bytes.Compare(k.ID[:], from[:]) >= 0 {
			tail = append(tail, k)
		}
	}
	i, stopped := 0, false
	// next yields the tail's entries before id (all of them, with id nil).
	next := func(id *encoding.ID) {
		for !stopped && i < len(tail) && (id == nil || bytes.Compare(tail[i].ID[:], id[:]) < 0) {
			stopped = !f(tail[i].ID, tail[i].Entry)
			i++
		}
	}
	var derr error
	err := scanSegments(ctx, v.readers, nil, from[:], nil, false, func(k, val []byte) bool {
		id := encoding.ID(k)
		next(&id)
		if stopped {
			return false
		}
		if i < len(tail) && tail[i].ID == id {
			return true // the tail's, yielded next
		}
		e, err := indexEntry(v.x, id, val)
		if err != nil {
			derr = err
			return false
		}
		if !e.Body {
			return true
		}
		stopped = !f(id, e)
		return !stopped
	})
	if err := errors.Join(err, derr); err != nil {
		return err
	}
	next(nil)
	return nil
}

// segmentEntry looks up id in the transaction's index segments — the manifest read through the
// transaction once, which its share of the index's row lock holds, since only a fold, under the
// lock exclusively, adds to it — the tail's moves applied.
func (lg *txLog) segmentEntry(ctx context.Context, id encoding.ID) (contentindex.Entry, bool, error) {
	if e, ok := lg.found[id]; ok {
		return e, true, nil
	}
	if lg.readers == nil {
		readers, err := lg.s.segmentReaders(ctx, lg.scope, segIndex, true)
		if err != nil {
			return contentindex.Entry{}, false, err
		}
		lg.readers = &readers
	}
	for _, r := range *lg.readers {
		if !r.MayContain(id[:]) {
			continue
		}
		v, ok, err := r.Get(ctx, id[:])
		if err != nil {
			return contentindex.Entry{}, false, err
		}
		if !ok {
			continue
		}
		e, err := indexEntry(lg.index, id, v)
		if err != nil || !e.Body {
			return contentindex.Entry{}, false, err
		}
		lg.found[id] = e
		return e, true, nil
	}
	return contentindex.Entry{}, false, nil
}
