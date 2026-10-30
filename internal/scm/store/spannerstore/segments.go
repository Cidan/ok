package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Segments ───

const segmentColumns = `scope, owner_id, network_id, kind, id, level, records, size, created_at, age, inherited, bound, day`

func (x queries) segments(ctx context.Context, sql string, params map[string]any) ([]backend.Segment, error) {
	var out []backend.Segment
	err := x.rows(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		s, err := segmentOf(row)
		out = append(out, s)
		return err
	})
	return out, err
}

// segmentOf is a row of segmentColumns.
func segmentOf(row *spanner.Row) (backend.Segment, error) {
	var s backend.Segment
	var idb []byte
	var scope, net, kind, level int64
	var bound, day spanner.NullInt64
	if err := row.Columns(&scope, &s.Scope.ID, &net, &kind, &idb, &level, &s.Records, &s.Size, &s.CreatedAt, &s.Age,
		&s.Inherited, &bound, &day); err != nil {
		return s, err
	}
	s.Day = day.Int64
	s.Scope.Network, s.Net, s.Kind, s.Level = scope == 1, backend.Network(net), segment.Kind(kind), int16(level)
	s.Bound = backend.NoBound
	if bound.Valid {
		s.Bound = bound.Int64
	}
	var err error
	s.ID, err = toID(idb)
	return s, err
}

// boundOf is a segment's bound as its column holds it: NULL when it is read whole.
func boundOf(s backend.Segment) spanner.NullInt64 {
	return spanner.NullInt64{Int64: s.Bound, Valid: s.Bound != backend.NoBound}
}

func (x queries) Segments(ctx context.Context, sc backend.Scope, kind segment.Kind) ([]backend.Segment, error) {
	p := scopeParams(sc)
	p["kind"] = int64(kind)
	return x.segments(ctx, liveSegments, p)
}

// liveSegments are a repository's or network's live segments of a kind, newest first.
const liveSegments = `SELECT ` + segmentColumns + ` FROM segment
	WHERE scope = @scope AND owner_id = @owner AND kind = @kind AND retired_at IS NULL ORDER BY age DESC`

// SegmentsUnlocked are Segments, read unlocked: in the operation's snapshot, or a read-only
// transaction of its own.
func (x queries) SegmentsUnlocked(ctx context.Context, sc backend.Scope, kind segment.Kind) ([]backend.Segment, error) {
	p := scopeParams(sc)
	p["kind"] = int64(kind)
	var out []backend.Segment
	err := x.unlocked(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, liveSegments, p, func(row *spanner.Row) error {
			s, err := segmentOf(row)
			out = append(out, s)
			return err
		})
	})
	return out, err
}

// segmentParams are a segment's columns as parameters.
func segmentParams(s backend.Segment) map[string]any {
	scope, owner := scopeOf(s.Scope)
	return map[string]any{"scope": scope, "owner": owner, "net": int64(s.Net), "kind": int64(s.Kind), "id": s.ID[:], "level": int64(s.Level),
		"records": s.Records, "size": s.Size, "age": s.Age, "inherited": s.Inherited, "bound": boundOf(s),
		"day": spanner.NullInt64{Int64: s.Day, Valid: s.Day != 0}}
}

const insertSegment = `INSERT INTO segment (scope, owner_id, network_id, kind, id, level, records, size, created_at, age, inherited, bound, day)
	VALUES (@scope, @owner, @net, @kind, @id, @level, @records, @size, ` + now + `, @age, @inherited, @bound, @day)`

// InsertSegments records segments live, one written from the tail (Age 0) under its network's
// counter's next age.
func (x queries) InsertSegments(ctx context.Context, segs []backend.Segment) error {
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for _, s := range segs {
			if s.Age == 0 {
				var err error
				if s.Age, err = x.next(ctx, ageCounter, int64(s.Net), 1); err != nil {
					return err
				}
			}
			if _, err := spannerdata.Update(ctx, tx, insertSegment, segmentParams(s)); err != nil {
				return err
			}
		}
		return nil
	})
}

// segmentKey is a segment's kind and ID, as a STRUCT parameter.
type segmentKey struct {
	K int64  `spanner:"k"`
	I []byte `spanner:"i"`
}

// SetInherited records segs inherited: one recorded is live again with its bound, as PostgreSQL's
// upsert has it, keeping the rest of its row.
func (x queries) SetInherited(ctx context.Context, repo int64, segs []backend.Segment) (int64, error) {
	var retired int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		keep := make([]segmentKey, 0, len(segs))
		for _, s := range segs {
			s.Scope, s.Inherited = backend.RepositoryScope(repo), true
			p := segmentParams(s)
			n, err := spannerdata.Update(ctx, tx, `UPDATE segment SET retired_at = NULL, bound = @bound
				WHERE scope = @scope AND owner_id = @owner AND kind = @kind AND id = @id`, p)
			if err != nil {
				return err
			}
			if n == 0 {
				if _, err := spannerdata.Update(ctx, tx, insertSegment, p); err != nil {
					return err
				}
			}
			keep = append(keep, segmentKey{int64(s.Kind), s.ID[:]})
		}
		var err error
		retired, err = spannerdata.Update(ctx, tx, `UPDATE segment SET retired_at = `+now+`
			WHERE scope = 0 AND owner_id = @repo AND inherited AND retired_at IS NULL
			  AND STRUCT<k INT64, i BYTES>(kind, id) NOT IN UNNEST(@keep)`, map[string]any{"repo": repo, "keep": keep})
		return err
	})
	return retired, err
}

func (x queries) RetireSegments(ctx context.Context, sc backend.Scope, kind segment.Kind, members []encoding.ID) (int64, error) {
	p := scopeParams(sc)
	p["kind"], p["ids"] = int64(kind), ids(members)
	return x.execIn(ctx, `UPDATE segment SET retired_at = `+now+`
		WHERE scope = @scope AND owner_id = @owner AND kind = @kind AND id IN UNNEST(@ids) AND retired_at IS NULL AND NOT inherited`, p,
		[]string{"ids"}, nil)
}

func (x queries) RetiredSegmentsBefore(ctx context.Context, t time.Time, net *backend.Network) ([]backend.Segment, error) {
	return x.segments(ctx, `SELECT `+segmentColumns+` FROM segment
		WHERE retired_at < @t AND (@net IS NULL OR network_id = @net) ORDER BY network_id, scope, owner_id, kind, id`,
		map[string]any{"t": t, "net": optionalNetwork(net)})
}

func (x queries) AllSegments(ctx context.Context, sc backend.Scope) ([]backend.Segment, error) {
	return x.segments(ctx, `SELECT `+segmentColumns+` FROM segment WHERE scope = @scope AND owner_id = @owner`, scopeParams(sc))
}

func (x queries) NetworkSegments(ctx context.Context, net backend.Network) ([]backend.Segment, error) {
	return x.segments(ctx, `SELECT `+segmentColumns+` FROM segment WHERE network_id = @net`, map[string]any{"net": int64(net)})
}

func (x queries) SegmentNamed(ctx context.Context, net backend.Network, sc backend.Scope, kind segment.Kind, id encoding.ID, since time.Time) (bool, error) {
	p := scopeParams(sc)
	p["net"], p["kind"], p["id"], p["since"] = int64(net), int64(kind), id[:], since
	var named bool
	_, err := x.one(ctx, `SELECT EXISTS (SELECT 1 FROM segment
		WHERE network_id = @net AND kind = @kind AND id = @id AND NOT (scope = @scope AND owner_id = @owner)
		  AND (retired_at IS NULL OR retired_at >= @since))`, p, &named)
	return named, err
}

func (x queries) DeleteSegment(ctx context.Context, sc backend.Scope, kind segment.Kind, id encoding.ID) error {
	p := scopeParams(sc)
	p["kind"], p["id"] = int64(kind), id[:]
	_, err := x.exec(ctx, `DELETE FROM segment WHERE scope = @scope AND owner_id = @owner AND kind = @kind AND id = @id AND retired_at IS NOT NULL`, p)
	return err
}

func (x queries) PinnedPaths(ctx context.Context, repo int64, limit int) ([]backend.PathRow, error) {
	var out []backend.PathRow
	err := x.rows(ctx, `SELECT path, generation, commit_id FROM commit_path WHERE repository_id = @repo
		ORDER BY path, generation, commit_id LIMIT @limit`, map[string]any{"repo": repo, "limit": int64(limit)}, func() { out = nil },
		func(row *spanner.Row) error {
			var p backend.PathRow
			var idb []byte
			if err := row.Columns(&p.Path, &p.Generation, &idb); err != nil {
				return err
			}
			var err error
			if p.Commit, err = toID(idb); err != nil {
				return err
			}
			out = append(out, p)
			return nil
		})
	return out, err
}

func (x queries) SettledEvents(ctx context.Context, repo int64, before time.Time, limit int) ([]backend.Event, error) {
	return x.events(ctx, `SELECT e.repository_id, e.repository_seq, e.body FROM event e
		WHERE e.repository_id = @repo AND e.event_at < @before AND NOT EXISTS (SELECT 1 FROM event_pending p
			WHERE p.repository_id = e.repository_id AND p.repository_seq = e.repository_seq)
		ORDER BY e.repository_seq LIMIT @limit`, map[string]any{"repo": repo, "before": before, "limit": int64(limit)})
}

func (x queries) PinnedCommits(ctx context.Context, repo int64, limit int) ([]backend.PinnedCommit, error) {
	var out []backend.PinnedCommit
	err := x.rows(ctx, `SELECT c.id, c.generation, `+commitParents+`, c.body FROM commit c
		WHERE c.repository_id = @repo AND c.pinned ORDER BY c.id LIMIT @limit`, map[string]any{"repo": repo, "limit": int64(limit)},
		func() { out = nil }, func(row *spanner.Row) error {
			var c backend.PinnedCommit
			g, err := scanGraphCommit(row, &c.Body)
			if err != nil {
				return err
			}
			c.GraphCommit = g
			out = append(out, c)
			return nil
		})
	return out, err
}

func (x queries) PinnedObjects(ctx context.Context, repo int64, limit int) ([]backend.Object, error) {
	return x.kindedObjects(ctx, `SELECT id, kind, body FROM object WHERE repository_id = @repo AND pinned ORDER BY id LIMIT @limit`,
		map[string]any{"repo": repo, "limit": int64(limit)})
}

// window reads a window of the tail's keys, each with whether its row is to be taken: sql selects
// them, a key's columns then a BOOL, in order.
func (x queries) window(ctx context.Context, sql string, params map[string]any, key func(*spanner.Row) ([]byte, error)) (keys [][]byte, take []bool, err error) {
	err = x.rows(ctx, sql, params, func() { keys, take = nil, nil }, func(row *spanner.Row) error {
		k, err := key(row)
		if err != nil {
			return err
		}
		var t spanner.NullBool
		if err := row.Column(row.Size()-1, &t); err != nil {
			return err
		}
		keys, take = append(keys, k), append(take, t.Valid && t.Bool)
		return nil
	})
	return keys, take, err
}

func oneKey(row *spanner.Row) ([]byte, error) {
	var k []byte
	return k, row.Column(0, &k)
}

// PinnedTreeSizes reads the window's keys, each with whether its tree is pinned, then the records
// it takes.
func (x queries) PinnedTreeSizes(ctx context.Context, repo int64, after []byte, scan, limit int) ([]backend.Object, []byte, bool, error) {
	p := map[string]any{"repo": repo, "scan": int64(scan)}
	window := `SELECT tree_id FROM tree_sizes WHERE repository_id = @repo ORDER BY tree_id LIMIT @scan`
	if after != nil {
		p["after"] = after
		window = `SELECT tree_id FROM tree_sizes WHERE repository_id = @repo AND tree_id > @after ORDER BY tree_id LIMIT @scan`
	}
	keys, take, err := x.window(ctx, `SELECT w.tree_id, (o.id IS NULL OR IFNULL(o.pinned, FALSE)) FROM (`+window+`) w
		LEFT JOIN object o ON o.repository_id = @repo AND o.id = w.tree_id ORDER BY w.tree_id`, p, oneKey)
	if err != nil {
		return nil, nil, false, err
	}
	taken, last, _, more := backend.Window(keys, take, scan, limit)
	if len(taken) == 0 {
		return nil, last, more, nil
	}
	objs, err := x.objects(ctx, `SELECT tree_id, body FROM tree_sizes WHERE repository_id = @repo AND tree_id IN UNNEST(@ids) ORDER BY tree_id`,
		map[string]any{"repo": repo, "ids": taken}, encoding.KindTree)
	return objs, last, more, err
}

// PinnedCopyEdges reads the window's edges — those of the copy after's after its parent, then those
// of later copies, each a seek — each with whether its copy is pinned.
func (x queries) PinnedCopyEdges(ctx context.Context, repo int64, after *[2]encoding.ID, scan, limit int) ([][2]encoding.ID, *[2]encoding.ID, bool, error) {
	p := map[string]any{"repo": repo, "scan": int64(scan)}
	window := `SELECT copy_id, parent_id FROM copy_parent WHERE repository_id = @repo ORDER BY copy_id, parent_id LIMIT @scan`
	if after != nil {
		p["copy"], p["parent"] = after[0][:], after[1][:]
		window = `SELECT copy_id, parent_id FROM (
			(SELECT copy_id, parent_id FROM copy_parent WHERE repository_id = @repo AND copy_id = @copy AND parent_id > @parent
				ORDER BY parent_id LIMIT @scan)
			UNION ALL
			(SELECT copy_id, parent_id FROM copy_parent WHERE repository_id = @repo AND copy_id > @copy ORDER BY copy_id, parent_id LIMIT @scan))
			ORDER BY copy_id, parent_id LIMIT @scan`
	}
	keys, take, err := x.window(ctx, `SELECT w.copy_id, w.parent_id, (o.id IS NULL OR IFNULL(o.pinned, FALSE)) FROM (`+window+`) w
		LEFT JOIN object o ON o.repository_id = @repo AND o.id = w.copy_id ORDER BY w.copy_id, w.parent_id`, p,
		func(row *spanner.Row) ([]byte, error) {
			var c, pa []byte
			if err := row.Columns(&c, &pa, new(spanner.NullBool)); err != nil {
				return nil, err
			}
			return append(c, pa...), nil
		})
	if err != nil || len(keys) == 0 {
		return nil, nil, false, err
	}
	taken, last, _, more := backend.Window(keys, take, scan, limit)
	edge := func(k []byte) (e [2]encoding.ID) {
		copy(e[0][:], k[:encoding.IDLen])
		copy(e[1][:], k[encoding.IDLen:])
		return e
	}
	out := make([][2]encoding.ID, len(taken))
	for i, k := range taken {
		out[i] = edge(k)
	}
	l := edge(last)
	return out, &l, more, nil
}

// pathKey is a row of main's path history's key, as a STRUCT parameter.
type pathKey struct {
	P string `spanner:"p"`
	G int64  `spanner:"g"`
	C []byte `spanner:"c"`
}

// opKey is an operation's or a view's key in its log, as a STRUCT parameter; opKindKey a
// tombstone's.
type opKey struct {
	L int64  `spanner:"l"`
	I []byte `spanner:"i"`
}

type opKindKey struct {
	L int64  `spanner:"l"`
	K int64  `spanner:"k"`
	I []byte `spanner:"i"`
}

// DeleteCompacted deletes what a compaction moved, each kind's keys a chunk at a time.
func (x queries) DeleteCompacted(ctx context.Context, repo int64, c backend.Compacted) error {
	type stmt struct {
		sql   string
		p     map[string]any
		array string
	}
	var stmts []stmt
	add := func(sql, array string, keys any) {
		stmts = append(stmts, stmt{sql, map[string]any{"repo": repo, array: keys}, array})
	}
	if len(c.Objects) > 0 {
		add(`DELETE FROM object WHERE repository_id = @repo AND id IN UNNEST(@ids) AND pinned`, "ids", ids(c.Objects))
	}
	if len(c.Commits) > 0 {
		// Its parent edges go with it (commit_parent's foreign key).
		add(`DELETE FROM commit WHERE repository_id = @repo AND id IN UNNEST(@ids) AND pinned`, "ids", ids(c.Commits))
	}
	if len(c.Paths) > 0 {
		keys := make([]pathKey, len(c.Paths))
		for i, p := range c.Paths {
			keys[i] = pathKey{p.Path, p.Generation, p.Commit[:]}
		}
		add(`DELETE FROM commit_path WHERE repository_id = @repo
			AND STRUCT<p STRING, g INT64, c BYTES>(path, generation, commit_id) IN UNNEST(@keys)`, "keys", keys)
	}
	if len(c.Events) > 0 {
		// Only what the relay has published: nothing makes an event pending again.
		add(`DELETE FROM event AS e WHERE e.repository_id = @repo AND e.repository_seq IN UNNEST(@seqs)
			AND NOT EXISTS (SELECT 1 FROM event_pending p WHERE p.repository_id = e.repository_id AND p.repository_seq = e.repository_seq)`,
			"seqs", c.Events)
	}
	for _, kind := range []encoding.Kind{encoding.KindOperation, encoding.KindView} {
		var keys []opKey
		for _, o := range c.Ops {
			if o.Kind == kind {
				keys = append(keys, opKey{o.Log, o.ID[:]})
			}
		}
		if len(keys) > 0 {
			// A row touched since compaction read it — re-published — stays: it supersedes the segment's.
			add(`DELETE FROM `+opTable(kind)+` WHERE STRUCT<l INT64, i BYTES>(op_log_id, id) IN UNNEST(@keys) AND touched_at < @before`, "keys", keys)
			stmts[len(stmts)-1].p["before"] = c.OpsBefore
		}
	}
	if len(c.OpTombstones) > 0 {
		keys := make([]opKindKey, len(c.OpTombstones))
		for i, k := range c.OpTombstones {
			keys[i] = opKindKey{k.Log, int64(k.Kind), k.ID[:]}
		}
		add(`DELETE FROM op_tombstone WHERE STRUCT<l INT64, k INT64, i BYTES>(op_log_id, kind, id) IN UNNEST(@keys)`, "keys", keys)
	}
	if len(c.TreeSizes) > 0 {
		add(`DELETE FROM tree_sizes WHERE repository_id = @repo AND tree_id IN UNNEST(@ids)`, "ids", ids(c.TreeSizes))
	}
	if len(c.CopyEdges) > 0 {
		edges := make([]idPair, len(c.CopyEdges))
		for i, e := range c.CopyEdges {
			edges[i] = idPair{e[0][:], e[1][:]}
		}
		add(`DELETE FROM copy_parent WHERE repository_id = @repo AND STRUCT<a BYTES, b BYTES>(copy_id, parent_id) IN UNNEST(@edges)`, "edges", edges)
	}
	// Landed CLs go by key, their patchsets with them (patchset's foreign key cascades), read nowhere
	// in the transaction: a landed CL changes no more, and only the compaction that read it settled
	// deletes it. Deleted by statements that found them by number and status, the transaction read
	// the repository's CLs, holding them — every upload, transition, and sequencing pass waited, up
	// to twenty seconds a compaction, for a scan that grew with every CL landed.
	var muts []*spanner.Mutation
	for _, id := range c.ChangeListIDs {
		muts = append(muts, spanner.Delete("changelist", spanner.Key{repo, id}))
	}
	if len(c.MainLog) > 0 {
		add(`DELETE FROM head_log WHERE repository_id = @repo AND seq IN UNNEST(@seqs)`, "seqs", c.MainLog)
	}
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for _, s := range stmts {
			if _, err := updateIn(ctx, tx, s.sql, s.p, []string{s.array}, nil); err != nil {
				return err
			}
		}
		if len(muts) == 0 {
			return nil
		}
		return tx.BufferWrite(muts)
	})
}
