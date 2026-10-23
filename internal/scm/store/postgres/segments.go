package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Segments ───

func segments(rows pgx.Rows, err error) ([]backend.Segment, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (backend.Segment, error) {
		var s backend.Segment
		var idb []byte
		var scope, kind int16
		var bound *int64
		if err := r.Scan(&scope, &s.Scope.ID, &s.Net, &kind, &idb, &s.Level, &s.Records, &s.Size, &s.CreatedAt, &s.Age,
			&s.Inherited, &bound); err != nil {
			return s, err
		}
		s.Scope.Network = scope == 1
		s.Kind = segment.Kind(kind)
		s.Bound = backend.NoBound
		if bound != nil {
			s.Bound = *bound
		}
		var err error
		s.ID, err = toID(idb)
		return s, err
	})
}

const segmentColumns = `scope, owner_id, network_id, kind, id, level, records, size, created_at, age, inherited, bound`

// boundOf is a segment's bound as its column holds it: null when it is read whole.
func boundOf(s backend.Segment) *int64 {
	if s.Bound == backend.NoBound {
		return nil
	}
	return &s.Bound
}

func (x queries) Segments(ctx context.Context, sc backend.Scope, kind segment.Kind) ([]backend.Segment, error) {
	scope, owner := scopeOf(sc)
	return segments(x.q.Query(ctx, `SELECT `+segmentColumns+` FROM segment
		WHERE scope = $1 AND owner_id = $2 AND kind = $3 AND retired_at IS NULL ORDER BY age DESC`, scope, owner, int16(kind)))
}

func (x queries) InsertSegments(ctx context.Context, segs []backend.Segment) error {
	for _, s := range segs {
		scope, owner := scopeOf(s.Scope)
		if _, err := x.q.Exec(ctx, `INSERT INTO segment (scope, owner_id, network_id, kind, id, level, records, size, age, inherited, bound)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CASE WHEN $9::bigint = 0 THEN nextval('segment_age') ELSE $9 END, $10, $11)`,
			scope, owner, s.Net, int16(s.Kind), s.ID[:], s.Level, s.Records, s.Size, s.Age, s.Inherited, boundOf(s)); err != nil {
			return err
		}
	}
	return nil
}

func (x queries) SetInherited(ctx context.Context, repo int64, segs []backend.Segment) (int64, error) {
	kinds := make([]int16, 0, len(segs))
	keep := make([][]byte, 0, len(segs))
	for _, s := range segs {
		if _, err := x.q.Exec(ctx, `INSERT INTO segment (scope, owner_id, network_id, kind, id, level, records, size, age, inherited, bound)
			VALUES (0, $1, $2, $3, $4, $5, $6, $7, $8, true, $9)
			ON CONFLICT (scope, owner_id, kind, id) DO UPDATE SET retired_at = NULL, bound = EXCLUDED.bound`,
			repo, s.Net, int16(s.Kind), s.ID[:], s.Level, s.Records, s.Size, s.Age, boundOf(s)); err != nil {
			return 0, err
		}
		kinds, keep = append(kinds, int16(s.Kind)), append(keep, s.ID[:])
	}
	tag, err := x.q.Exec(ctx, `UPDATE segment SET retired_at = now()
		WHERE scope = 0 AND owner_id = $1 AND inherited AND retired_at IS NULL
		  AND (kind, id) NOT IN (SELECT * FROM unnest($2::smallint[], $3::bytea[]))`, repo, kinds, keep)
	return tag.RowsAffected(), err
}

func (x queries) RetireSegments(ctx context.Context, sc backend.Scope, kind segment.Kind, members []encoding.ID) (int64, error) {
	scope, owner := scopeOf(sc)
	tag, err := x.q.Exec(ctx, `UPDATE segment SET retired_at = now()
		WHERE scope = $1 AND owner_id = $2 AND kind = $3 AND id = ANY($4) AND retired_at IS NULL AND NOT inherited`,
		scope, owner, int16(kind), ids(members))
	return tag.RowsAffected(), err
}

func (x queries) RetiredSegmentsBefore(ctx context.Context, t time.Time, net *backend.Network) ([]backend.Segment, error) {
	return segments(x.q.Query(ctx, `SELECT `+segmentColumns+` FROM segment
		WHERE retired_at < $1 AND ($2::bigint IS NULL OR network_id = $2) ORDER BY network_id, scope, owner_id, kind, id`, t, net))
}

func (x queries) AllSegments(ctx context.Context, sc backend.Scope) ([]backend.Segment, error) {
	scope, owner := scopeOf(sc)
	return segments(x.q.Query(ctx, `SELECT `+segmentColumns+` FROM segment WHERE scope = $1 AND owner_id = $2`, scope, owner))
}

func (x queries) NetworkSegments(ctx context.Context, net backend.Network) ([]backend.Segment, error) {
	return segments(x.q.Query(ctx, `SELECT `+segmentColumns+` FROM segment WHERE network_id = $1`, net))
}

func (x queries) SegmentNamed(ctx context.Context, net backend.Network, sc backend.Scope, kind segment.Kind, id encoding.ID, since time.Time) (bool, error) {
	scope, owner := scopeOf(sc)
	var named bool
	err := x.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM segment
		WHERE network_id = $1 AND kind = $4 AND id = $5 AND NOT (scope = $2 AND owner_id = $3)
		  AND (retired_at IS NULL OR retired_at >= $6))`, net, scope, owner, int16(kind), id[:], since).Scan(&named)
	return named, err
}

func (x queries) DeleteSegment(ctx context.Context, sc backend.Scope, kind segment.Kind, id encoding.ID) error {
	scope, owner := scopeOf(sc)
	_, err := x.q.Exec(ctx, `DELETE FROM segment WHERE scope = $1 AND owner_id = $2 AND kind = $3 AND id = $4 AND retired_at IS NOT NULL`,
		scope, owner, int16(kind), id[:])
	return err
}

func (x queries) PinnedPaths(ctx context.Context, repo int64, limit int) ([]backend.PathRow, error) {
	rows, err := x.q.Query(ctx, `SELECT path, generation, commit_id FROM commit_path WHERE repository_id = $1
		ORDER BY path, generation, commit_id LIMIT $2`, repo, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (backend.PathRow, error) {
		var p backend.PathRow
		var idb []byte
		if err := r.Scan(&p.Path, &p.Generation, &idb); err != nil {
			return p, err
		}
		var err error
		p.Commit, err = toID(idb)
		return p, err
	})
}

func (x queries) SettledEvents(ctx context.Context, repo int64, limit int) ([]backend.Event, error) {
	return events(x.q.Query(ctx, `SELECT e.repository_id, e.repository_seq, e.body FROM event e
		WHERE e.repository_id = $1 AND NOT EXISTS (SELECT 1 FROM event_pending p
			WHERE p.repository_id = e.repository_id AND p.repository_seq = e.repository_seq)
		ORDER BY e.repository_seq LIMIT $2`, repo, limit))
}

func (x queries) PinnedCommits(ctx context.Context, repo int64, limit int) ([]backend.PinnedCommit, error) {
	rows, err := x.q.Query(ctx, `SELECT c.id, c.generation, `+commitParents+`, c.body FROM commit c
		WHERE c.repository_id = $1 AND c.pinned ORDER BY c.id LIMIT $2`, repo, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (backend.PinnedCommit, error) {
		var c backend.PinnedCommit
		var idb []byte
		var parents [][]byte
		if err := r.Scan(&idb, &c.Generation, &parents, &c.Body); err != nil {
			return c, err
		}
		var err error
		if c.ID, err = toID(idb); err != nil {
			return c, err
		}
		for _, p := range parents {
			id, err := toID(p)
			if err != nil {
				return c, err
			}
			c.Parents = append(c.Parents, id)
		}
		return c, nil
	})
}

func (x queries) PinnedObjects(ctx context.Context, repo int64, limit int) ([]backend.Object, error) {
	return kindedObjects(x.q.Query(ctx, `SELECT id, kind, body FROM object WHERE repository_id = $1 AND pinned ORDER BY id LIMIT $2`, repo, limit))
}

func (x queries) PinnedTreeSizes(ctx context.Context, repo int64, limit int) ([]backend.Object, error) {
	return kindedObjects(x.q.Query(ctx, `SELECT ts.tree_id, $3::smallint, ts.body FROM tree_sizes ts
		LEFT JOIN object o ON o.repository_id = ts.repository_id AND o.id = ts.tree_id
		WHERE ts.repository_id = $1 AND (o.id IS NULL OR o.pinned) ORDER BY ts.tree_id LIMIT $2`,
		repo, limit, int16(encoding.KindTree)))
}

func (x queries) PinnedCopyEdges(ctx context.Context, repo int64, limit int) ([][2]encoding.ID, error) {
	return pairs(x.q.Query(ctx, `SELECT cp.copy_id, cp.parent_id FROM copy_parent cp
		LEFT JOIN object o ON o.repository_id = cp.repository_id AND o.id = cp.copy_id
		WHERE cp.repository_id = $1 AND (o.id IS NULL OR o.pinned) ORDER BY cp.parent_id, cp.copy_id LIMIT $2`, repo, limit))
}

func (x queries) DeleteCompacted(ctx context.Context, repo int64, c backend.Compacted) error {
	if len(c.Objects) > 0 {
		if _, err := x.q.Exec(ctx, `DELETE FROM object WHERE repository_id = $1 AND id = ANY($2) AND pinned`, repo, ids(c.Objects)); err != nil {
			return err
		}
	}
	if len(c.Commits) > 0 {
		// Its parent edges go with it (commit_parent's foreign key).
		if _, err := x.q.Exec(ctx, `DELETE FROM commit WHERE repository_id = $1 AND id = ANY($2) AND pinned`, repo, ids(c.Commits)); err != nil {
			return err
		}
	}
	if len(c.Paths) > 0 {
		paths := make([]string, len(c.Paths))
		gens := make([]int64, len(c.Paths))
		commits := make([]encoding.ID, len(c.Paths))
		for i, p := range c.Paths {
			paths[i], gens[i], commits[i] = p.Path, p.Generation, p.Commit
		}
		if _, err := x.q.Exec(ctx, `DELETE FROM commit_path cp USING unnest($2::text[], $3::bigint[], $4::bytea[]) AS u(p, g, c)
			WHERE cp.repository_id = $1 AND cp.path = u.p AND cp.generation = u.g AND cp.commit_id = u.c`,
			repo, paths, gens, ids(commits)); err != nil {
			return err
		}
	}
	if len(c.Events) > 0 {
		// Only what the relay has published: nothing makes an event pending again.
		if _, err := x.q.Exec(ctx, `DELETE FROM event e WHERE e.repository_id = $1 AND e.repository_seq = ANY($2)
			AND NOT EXISTS (SELECT 1 FROM event_pending p WHERE p.repository_id = e.repository_id AND p.repository_seq = e.repository_seq)`,
			repo, c.Events); err != nil {
			return err
		}
	}
	for _, kind := range []encoding.Kind{encoding.KindOperation, encoding.KindView} {
		var logs []int64
		var members []encoding.ID
		for _, o := range c.Ops {
			if o.Kind == kind {
				logs, members = append(logs, o.Log), append(members, o.ID)
			}
		}
		if len(members) == 0 {
			continue
		}
		// A row touched since compaction read it — re-published — stays: it supersedes the segment's.
		if _, err := x.q.Exec(ctx, `DELETE FROM `+opTable(kind)+` t USING unnest($1::bigint[], $2::bytea[]) AS u(l, i)
			WHERE t.op_log_id = u.l AND t.id = u.i AND t.touched_at < $3`, logs, ids(members), c.OpsBefore); err != nil {
			return err
		}
	}
	if len(c.OpTombstones) > 0 {
		logs := make([]int64, len(c.OpTombstones))
		kinds := make([]int16, len(c.OpTombstones))
		members := make([]encoding.ID, len(c.OpTombstones))
		for i, k := range c.OpTombstones {
			logs[i], kinds[i], members[i] = k.Log, int16(k.Kind), k.ID
		}
		if _, err := x.q.Exec(ctx, `DELETE FROM op_tombstone d USING unnest($1::bigint[], $2::smallint[], $3::bytea[]) AS u(l, k, i)
			WHERE d.op_log_id = u.l AND d.kind = u.k AND d.id = u.i`, logs, kinds, ids(members)); err != nil {
			return err
		}
	}
	if len(c.TreeSizes) > 0 {
		if _, err := x.q.Exec(ctx, `DELETE FROM tree_sizes WHERE repository_id = $1 AND tree_id = ANY($2)`, repo, ids(c.TreeSizes)); err != nil {
			return err
		}
	}
	if len(c.CopyEdges) > 0 {
		copies := make([]encoding.ID, len(c.CopyEdges))
		parents := make([]encoding.ID, len(c.CopyEdges))
		for i, e := range c.CopyEdges {
			copies[i], parents[i] = e[0], e[1]
		}
		if _, err := x.q.Exec(ctx, `DELETE FROM copy_parent cp USING unnest($2::bytea[], $3::bytea[]) AS u(c, p)
			WHERE cp.repository_id = $1 AND cp.copy_id = u.c AND cp.parent_id = u.p`, repo, ids(copies), ids(parents)); err != nil {
			return err
		}
	}
	if len(c.ChangeLists) > 0 {
		// Their patchsets go with them (ON DELETE CASCADE).
		if _, err := x.q.Exec(ctx, `DELETE FROM changelist WHERE repository_id = $1 AND number = ANY($2) AND status = $3`,
			repo, c.ChangeLists, settledStatus); err != nil {
			return err
		}
	}
	if len(c.MainLog) > 0 {
		if _, err := x.q.Exec(ctx, `DELETE FROM head_log WHERE repository_id = $1 AND seq = ANY($2)`, repo, c.MainLog); err != nil {
			return err
		}
	}
	return nil
}
