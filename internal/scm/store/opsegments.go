package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// Operation logs' operations and views leave the tail for op segments (segOps) once settled —
// touched before collection's window, opSettled — and collection, finding one there unreachable,
// writes a tombstone (op_tombstone), which compaction moves into a segment too. A read takes the
// tail's row, then a tombstone in the tail, then the newest segment's record, live or a tombstone.
// Re-publishing one only a segment holds puts its row in the tail again (touchOps), so collection's
// check of the last touch, under the log's lock, still keeps it. Merging drops the records of
// operation logs since deleted, and at the oldest level the tombstones, what they bury dropped
// with them.

// opSettled is how long after its last touch an operation or view moves into a segment: past the
// collection that would delete it if nothing reaches it.
const opSettled = OperationRetention + 24*time.Hour

// An op segment's value: its state, then for a live record when it was last touched (Unix
// microseconds, big-endian), an operation's view, and the body.
const (
	opLive      byte = 1
	opTombstone byte = 2
)

// opPrefix is the prefix of a log's operations or views (kind) in an op segment: the log's ID,
// big-endian, and the kind.
func opPrefix(log int64, kind encoding.Kind) []byte {
	return append(binary.BigEndian.AppendUint64(nil, uint64(log)), byte(kind))
}

// opKey is an operation's or view's key in an op segment.
func opKey(log int64, kind encoding.Kind, id encoding.ID) []byte {
	return append(opPrefix(log, kind), id[:]...)
}

// opKeyOf decodes an op segment's key.
func opKeyOf(k []byte) (backend.OpKey, bool) {
	if len(k) != 9+encoding.IDLen {
		return backend.OpKey{}, false
	}
	return backend.OpKey{Log: int64(binary.BigEndian.Uint64(k)), Kind: encoding.Kind(k[8]), ID: encoding.ID(k[9:])}, true
}

func encodeOpRecord(o backend.OpRecord) []byte {
	b := append([]byte{opLive}, binary.BigEndian.AppendUint64(nil, uint64(o.TouchedAt.UnixMicro()))...)
	if o.Kind == encoding.KindOperation {
		b = append(b, o.View[:]...)
	}
	return append(b, o.Body...)
}

// decodeOpRecord decodes an op segment's record of k: live false for a tombstone.
func decodeOpRecord(k backend.OpKey, v []byte) (rec backend.OpRecord, live bool, err error) {
	rec.OpKey = k
	if len(v) == 1 && v[0] == opTombstone {
		return rec, false, nil
	}
	head := 9
	if k.Kind == encoding.KindOperation {
		head += encoding.IDLen
	}
	if len(v) < head || v[0] != opLive {
		return rec, false, fmt.Errorf("%s %s of operation log %d in a segment: %w", k.Kind, k.ID, k.Log, segment.ErrCorrupt)
	}
	rec.TouchedAt = time.UnixMicro(int64(binary.BigEndian.Uint64(v[1:9]))).UTC()
	if k.Kind == encoding.KindOperation {
		rec.View = encoding.ID(v[9:head])
	}
	rec.Body = bytes.Clone(v[head:])
	return rec, true, nil
}

// segmentOps looks up a log's operations or views (kind) among ids in a repository's op segments:
// the newest record of each found, live or not.
func (s *Store) segmentOps(ctx context.Context, repo, log int64, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID]backend.OpRecord, map[encoding.ID]bool, error) {
	live, dead := map[encoding.ID]backend.OpRecord{}, map[encoding.ID]bool{}
	if len(ids) == 0 {
		return live, dead, nil
	}
	keys := make([][]byte, len(ids))
	for i, id := range ids {
		keys[i] = opKey(log, kind, id)
	}
	got, err := s.segmentGet(ctx, repoScope(repo), segOps, keys)
	if err != nil {
		return nil, nil, err
	}
	for k, v := range got {
		key, ok := opKeyOf([]byte(k))
		if !ok {
			return nil, nil, fmt.Errorf("an op segment's key of %d bytes: %w", len(k), segment.ErrCorrupt)
		}
		rec, isLive, err := decodeOpRecord(key, v)
		if err != nil {
			return nil, nil, err
		}
		if isLive {
			live[key.ID] = rec
		} else {
			dead[key.ID] = true
		}
	}
	return live, dead, nil
}

// opRecords are a log's operations or views (kind) among ids that it holds — the tail's rows, then
// for the rest those it holds no tombstone of in the tail, the segments' live records. q is the
// pool's queries or a transaction's, as ctx is.
func (s *Store) opRecords(ctx context.Context, q backend.Ops, repo, log int64, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID][]byte, map[encoding.ID]backend.OpRecord, error) {
	bodies, err := q.OpBodies(ctx, log, kind, ids)
	if err != nil {
		return nil, nil, err
	}
	var rest []encoding.ID
	for _, id := range ids {
		if _, ok := bodies[id]; !ok {
			rest = append(rest, id)
		}
	}
	if len(rest) == 0 {
		return bodies, nil, nil
	}
	dead, err := q.TailOpTombstones(ctx, log, kind, rest)
	if err != nil {
		return nil, nil, err
	}
	rest = slices.DeleteFunc(rest, func(id encoding.ID) bool { return dead[id] })
	moved, _, err := s.segmentOps(ctx, repo, log, kind, rest)
	if err != nil {
		return nil, nil, err
	}
	for id, rec := range moved {
		bodies[id] = rec.Body
	}
	return bodies, moved, nil
}

// opBodies are the bodies of a log's operations or views (kind) among ids that it holds.
func (s *Store) opBodies(ctx context.Context, q backend.Ops, repo, log int64, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID][]byte, error) {
	bodies, _, err := s.opRecords(ctx, q, repo, log, kind, ids)
	return bodies, err
}

// logOp is an operation or view of a log, and whether only a segment holds it.
type logOp struct {
	backend.OpRecord
	inSegment bool
}

// logOps are every operation or view (kind) of a log: the tail's, then the segments' live records
// of those it holds no row or tombstone of, the manifest read after the tail.
func (s *Store) logOps(ctx context.Context, repo, log int64, kind encoding.Kind) ([]logOp, error) {
	var out []logOp
	held := map[encoding.ID]bool{}
	if kind == encoding.KindOperation {
		ops, err := s.ops(ctx).Operations(ctx, log, time.Time{})
		if err != nil {
			return nil, err
		}
		for _, o := range ops {
			out = append(out, logOp{OpRecord: backend.OpRecord{OpKey: backend.OpKey{Log: log, Kind: kind, ID: o.ID}, View: o.View, Body: o.Body}})
			held[o.ID] = true
		}
	} else {
		views, err := s.ops(ctx).Views(ctx, log)
		if err != nil {
			return nil, err
		}
		for _, v := range views {
			out = append(out, logOp{OpRecord: backend.OpRecord{OpKey: backend.OpKey{Log: log, Kind: kind, ID: v.ID}, Body: v.Body}})
			held[v.ID] = true
		}
	}
	readers, err := s.segmentReaders(ctx, repoScope(repo), segOps, true)
	if err != nil || len(readers) == 0 {
		return out, err
	}
	var moved []logOp
	prefix := opPrefix(log, kind)
	var derr error
	err = scanSegments(ctx, readers, prefix, prefix, prefixEnd(prefix), false, func(k, v []byte) bool {
		key, ok := opKeyOf(k)
		if !ok || held[key.ID] {
			return true
		}
		rec, live, err := decodeOpRecord(key, v)
		if err != nil {
			derr = err
			return false
		}
		if live {
			moved = append(moved, logOp{OpRecord: rec, inSegment: true})
		}
		return true
	})
	if err := errors.Join(err, derr); err != nil {
		return nil, err
	}
	if len(moved) == 0 {
		return out, nil
	}
	ids := make([]encoding.ID, len(moved))
	for i, m := range moved {
		ids[i] = m.ID
	}
	dead, err := s.ops(ctx).TailOpTombstones(ctx, log, kind, ids)
	if err != nil {
		return nil, err
	}
	for _, m := range moved {
		if !dead[m.ID] {
			out = append(out, m)
		}
	}
	return out, nil
}

// logOperations are every operation of a log, as Operations lists them: Old for those last touched
// before before.
func (s *Store) logOperations(ctx context.Context, repo, log int64, before time.Time) ([]backend.Operation, error) {
	tail, err := s.ops(ctx).Operations(ctx, log, before)
	if err != nil {
		return nil, err
	}
	all, err := s.logOps(ctx, repo, log, encoding.KindOperation)
	if err != nil {
		return nil, err
	}
	for _, o := range all {
		if o.inSegment {
			tail = append(tail, backend.Operation{ID: o.ID, View: o.View, Body: o.Body, Old: o.TouchedAt.Before(before)})
		}
	}
	return tail, nil
}

// logViews are every view of a log.
func (s *Store) logViews(ctx context.Context, repo, log int64) ([]backend.Object, error) {
	all, err := s.logOps(ctx, repo, log, encoding.KindView)
	if err != nil {
		return nil, err
	}
	out := make([]backend.Object, len(all))
	for i, v := range all {
		out[i] = backend.Object{ID: v.ID, Kind: encoding.KindView, Body: v.Body}
	}
	return out, nil
}

// olderThan reports whether a log holds an operation or view last touched before t: in the tail,
// or — settled before collection's window, so always — in a segment.
func (s *Store) olderThan(ctx context.Context, repo, log int64, t time.Time) (bool, error) {
	if old, err := s.ops(ctx).OlderThan(ctx, log, t); err != nil || old {
		return old, err
	}
	for _, kind := range []encoding.Kind{encoding.KindOperation, encoding.KindView} {
		all, err := s.logOps(ctx, repo, log, kind)
		if err != nil {
			return false, err
		}
		for _, o := range all {
			if o.inSegment && o.TouchedAt.Before(t) {
				return true, nil
			}
		}
	}
	return false, nil
}

// operationIDsFrom are up to limit of a log's operations with IDs from lo, and below hi unless it
// is nil, by ID: the tail's and the segments' it holds no tombstone of.
func (s *Store) operationIDsFrom(ctx context.Context, repo, log int64, lo, hi []byte, limit int) ([]encoding.ID, error) {
	tail, err := s.ops(ctx).OperationIDsFrom(ctx, log, lo, hi, limit)
	if err != nil {
		return nil, err
	}
	readers, err := s.segmentReaders(ctx, repoScope(repo), segOps, true)
	if err != nil || len(readers) == 0 {
		return tail, err
	}
	prefix := opPrefix(log, encoding.KindOperation)
	to := prefixEnd(prefix)
	if hi != nil {
		to = append(bytes.Clone(prefix), hi...)
	}
	seen := map[encoding.ID]bool{}
	for _, id := range tail {
		seen[id] = true
	}
	out := slices.Clone(tail)
	// A page of live records at a time, less those the tail holds a tombstone of, until limit.
	from := append(bytes.Clone(prefix), lo...)
	for {
		var page []encoding.ID
		var last []byte
		var derr error
		err := scanSegments(ctx, readers, prefix, from, to, false, func(k, v []byte) bool {
			last = bytes.Clone(k)
			key, ok := opKeyOf(k)
			if !ok {
				return true
			}
			_, live, err := decodeOpRecord(key, v)
			if err != nil {
				derr = err
				return false
			}
			if live && !seen[key.ID] {
				page = append(page, key.ID)
			}
			return len(page) < limit
		})
		if err := errors.Join(err, derr); err != nil {
			return nil, err
		}
		dead, err := s.ops(ctx).TailOpTombstones(ctx, log, encoding.KindOperation, page)
		if err != nil {
			return nil, err
		}
		for _, id := range page {
			if !dead[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
		if len(page) < limit || len(dead) == 0 {
			break
		}
		from = append(last, 0)
	}
	slices.SortFunc(out, compareIDs)
	return out[:min(limit, len(out))], nil
}

// repositoryOps are every operation or view (kind) of a repository's operation logs: the tail's,
// then the segments' live records of logs that still exist, that the tail holds no row or tombstone
// of.
func (s *Store) repositoryOps(ctx context.Context, repo int64, kind encoding.Kind) ([]backend.Object, error) {
	var tail []backend.OpRecord
	var err error
	if kind == encoding.KindOperation {
		tail, err = s.ops(ctx).RepositoryOperations(ctx, repo)
	} else {
		tail, err = s.ops(ctx).RepositoryViews(ctx, repo)
	}
	if err != nil {
		return nil, err
	}
	held := map[backend.OpKey]bool{}
	out := make([]backend.Object, 0, len(tail))
	for _, o := range tail {
		held[o.OpKey] = true
		out = append(out, backend.Object{ID: o.ID, Kind: kind, Body: o.Body})
	}
	readers, err := s.segmentReaders(ctx, repoScope(repo), segOps, true)
	if err != nil || len(readers) == 0 {
		return out, err
	}
	logs, err := s.ops(ctx).AllOpLogs(ctx, repo)
	if err != nil {
		return nil, err
	}
	dead, err := s.ops(ctx).RepositoryOpTombstones(ctx, repo)
	if err != nil {
		return nil, err
	}
	for _, k := range dead {
		held[k] = true
	}
	for _, log := range logs {
		prefix := opPrefix(log, kind)
		var derr error
		err := scanSegments(ctx, readers, prefix, prefix, prefixEnd(prefix), false, func(k, v []byte) bool {
			key, ok := opKeyOf(k)
			if !ok || held[key] {
				return true
			}
			rec, live, err := decodeOpRecord(key, v)
			if err != nil {
				derr = err
				return false
			}
			if live {
				out = append(out, backend.Object{ID: rec.ID, Kind: kind, Body: rec.Body})
			}
			return true
		})
		if err := errors.Join(err, derr); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// touchOps touches a log's operations or views (kind) among ids, now: the tail's rows, and for
// those only a segment holds, their rows put in the tail again.
func (s *Store) touchOps(ctx context.Context, tx *txn, repo, log int64, kind encoding.Kind, ids []encoding.ID) error {
	touched, err := tx.TouchOps(ctx, log, kind, ids)
	if err != nil {
		return err
	}
	done := map[encoding.ID]bool{}
	for _, id := range touched {
		done[id] = true
	}
	var rest []encoding.ID
	for _, id := range ids {
		if !done[id] {
			rest = append(rest, id)
		}
	}
	moved, _, err := s.segmentOps(ctx, repo, log, kind, rest)
	if err != nil || len(moved) == 0 {
		return err
	}
	if kind == encoding.KindOperation {
		var ops []backend.Operation
		for _, id := range rest {
			if rec, ok := moved[id]; ok {
				ops = append(ops, backend.Operation{ID: id, View: rec.View, Body: rec.Body})
			}
		}
		return tx.PutOperations(ctx, log, ops)
	}
	var views []backend.Object
	for _, id := range rest {
		if rec, ok := moved[id]; ok {
			views = append(views, backend.Object{ID: id, Kind: kind, Body: rec.Body})
		}
	}
	return tx.PutViews(ctx, log, views)
}

// opMergeFilter keeps, of a merge of op segments, the records of operation logs that still exist,
// and — merging into the oldest level, where nothing older is left for a tombstone to bury — the
// live ones only.
func (s *Store) opMergeFilter(ctx context.Context, repo int64, oldest bool) (func(key, value []byte) bool, error) {
	logs, err := s.ops(ctx).AllOpLogs(ctx, repo)
	if err != nil {
		return nil, err
	}
	live := make(map[int64]bool, len(logs))
	for _, l := range logs {
		live[l] = true
	}
	return func(key, value []byte) bool {
		k, ok := opKeyOf(key)
		if !ok || !live[k.Log] {
			return false
		}
		return !oldest || len(value) != 1 || value[0] != opTombstone
	}, nil
}
