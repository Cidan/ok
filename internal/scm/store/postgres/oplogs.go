package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Operation logs ───

func (x queries) OpLogID(ctx context.Context, repo int64, principal, workspace string) (int64, bool, error) {
	var id int64
	err := x.q.QueryRow(ctx, `SELECT id FROM op_log WHERE repository_id = $1 AND principal = $2 AND workspace = $3`,
		repo, principal, workspace).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return id, err == nil, err
}

func (x queries) InsertOpLog(ctx context.Context, repo int64, principal, workspace string) (int64, bool, error) {
	var id int64
	err := x.q.QueryRow(ctx,
		`INSERT INTO op_log (id, repository_id, principal, workspace) VALUES ($4, $1, $2, $3) ON CONFLICT DO NOTHING RETURNING id`,
		repo, principal, workspace, data.NewID()).Scan(&id)
	switch {
	case err == nil:
		return id, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		// It exists; ON CONFLICT waited for the transaction making it to commit.
		id, ok, err := x.OpLogID(ctx, repo, principal, workspace)
		if err == nil && !ok {
			err = errors.New("operation log vanished")
		}
		return id, false, err
	}
	return 0, false, err
}

func (x queries) LockOpLog(ctx context.Context, log int64) error {
	_, err := x.q.Exec(ctx, `SELECT id FROM op_log WHERE id = $1 FOR UPDATE`, log)
	return err
}

func (x queries) OpHeads(ctx context.Context, log int64) ([]encoding.ID, error) {
	return idList(x.q.Query(ctx, `SELECT op_id FROM op_head WHERE op_log_id = $1 ORDER BY op_id`, log))
}

func (x queries) AddOpHead(ctx context.Context, log int64, id encoding.ID) error {
	_, err := x.q.Exec(ctx, `INSERT INTO op_head (op_log_id, op_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, log, id[:])
	return err
}

func (x queries) DeleteOpHeads(ctx context.Context, log int64, heads []encoding.ID) error {
	_, err := x.q.Exec(ctx, `DELETE FROM op_head WHERE op_log_id = $1 AND op_id = ANY($2)`, log, ids(heads))
	return err
}

func (x queries) OperationIDsFrom(ctx context.Context, log int64, lo, hi []byte, limit int) ([]encoding.ID, error) {
	if hi == nil {
		return idList(x.q.Query(ctx, `SELECT id FROM operation WHERE op_log_id = $1 AND id >= $2 ORDER BY id LIMIT $3`, log, lo, limit))
	}
	return idList(x.q.Query(ctx, `SELECT id FROM operation WHERE op_log_id = $1 AND id >= $2 AND id < $3 ORDER BY id LIMIT $4`, log, lo, hi, limit))
}

// opTable is where an operation log keeps operations or views.
func opTable(kind encoding.Kind) string {
	if kind == encoding.KindView {
		return "view"
	}
	return "operation"
}

func (x queries) OpBodies(ctx context.Context, log int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID][]byte, error) {
	return bodies(x.q.Query(ctx, `SELECT id, body FROM `+opTable(kind)+` WHERE op_log_id = $1 AND id = ANY($2)`, log, ids(members)))
}

func (x queries) Operations(ctx context.Context, log int64, before time.Time) ([]backend.Operation, error) {
	rows, err := x.q.Query(ctx, `SELECT id, view_id, body, touched_at < $2 FROM operation WHERE op_log_id = $1`, log, before)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Operation
	for rows.Next() {
		var idb, viewb []byte
		var o backend.Operation
		if err := rows.Scan(&idb, &viewb, &o.Body, &o.Old); err != nil {
			return nil, err
		}
		if o.ID, err = toID(idb); err != nil {
			return nil, err
		}
		if o.View, err = toID(viewb); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (x queries) Views(ctx context.Context, log int64) ([]backend.Object, error) {
	rows, err := x.q.Query(ctx, `SELECT id, body FROM view WHERE op_log_id = $1`, log)
	return objects(rows, err, encoding.KindView)
}

func (x queries) OlderThan(ctx context.Context, log int64, t time.Time) (bool, error) {
	var old bool
	err := x.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM operation WHERE op_log_id = $1 AND touched_at < $2)
		OR EXISTS (SELECT 1 FROM view WHERE op_log_id = $1 AND touched_at < $2)`, log, t).Scan(&old)
	return old, err
}

func (x queries) PutOperations(ctx context.Context, log int64, ops []backend.Operation) error {
	idCol := make([]encoding.ID, len(ops))
	viewCol := make([]encoding.ID, len(ops))
	bs := make([][]byte, len(ops))
	for i, o := range ops {
		idCol[i], viewCol[i], bs[i] = o.ID, o.View, o.Body
	}
	_, err := x.q.Exec(ctx, `WITH revived AS (DELETE FROM op_tombstone WHERE op_log_id = $1 AND kind = $5 AND id = ANY($2))
		INSERT INTO operation (op_log_id, id, view_id, body)
		SELECT $1, u.id, u.view, u.body FROM unnest($2::bytea[], $3::bytea[], $4::bytea[]) AS u(id, view, body)
		ORDER BY u.id ON CONFLICT (op_log_id, id) DO UPDATE SET touched_at = now()`,
		log, ids(idCol), ids(viewCol), bs, int16(encoding.KindOperation))
	return err
}

func (x queries) PutViews(ctx context.Context, log int64, views []backend.Object) error {
	idCol := make([]encoding.ID, len(views))
	bs := make([][]byte, len(views))
	for i, v := range views {
		idCol[i], bs[i] = v.ID, v.Body
	}
	_, err := x.q.Exec(ctx, `WITH revived AS (DELETE FROM op_tombstone WHERE op_log_id = $1 AND kind = $4 AND id = ANY($2))
		INSERT INTO view (op_log_id, id, body)
		SELECT $1, u.id, u.body FROM unnest($2::bytea[], $3::bytea[]) AS u(id, body)
		ORDER BY u.id ON CONFLICT (op_log_id, id) DO UPDATE SET touched_at = now()`,
		log, ids(idCol), bs, int16(encoding.KindView))
	return err
}

func (x queries) TouchOps(ctx context.Context, log int64, kind encoding.Kind, members []encoding.ID) ([]encoding.ID, error) {
	return idList(x.q.Query(ctx, `UPDATE `+opTable(kind)+` SET touched_at = now() WHERE op_log_id = $1 AND id = ANY($2) RETURNING id`,
		log, ids(members)))
}

func (x queries) CollectOps(ctx context.Context, log int64, doomed, keep []encoding.ID, before time.Time, dry bool) (int64, int64, error) {
	var ops, views int64
	if dry {
		if err := x.q.QueryRow(ctx, `SELECT count(*) FROM operation WHERE op_log_id = $1 AND id = ANY($2) AND touched_at < $3`,
			log, ids(doomed), before).Scan(&ops); err != nil {
			return 0, 0, err
		}
		err := x.q.QueryRow(ctx, `SELECT count(*) FROM view v WHERE v.op_log_id = $1 AND v.touched_at < $2 AND NOT v.id = ANY($4)
			AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = v.op_log_id AND o.view_id = v.id
			AND NOT (o.id = ANY($3) AND o.touched_at < $2))`, log, before, ids(doomed), ids(keep)).Scan(&views)
		return ops, views, err
	}
	tag, err := x.q.Exec(ctx, `DELETE FROM operation WHERE op_log_id = $1 AND id = ANY($2) AND touched_at < $3`,
		log, ids(doomed), before)
	if err != nil {
		return 0, 0, err
	}
	ops = tag.RowsAffected()
	tag, err = x.q.Exec(ctx,
		`DELETE FROM view v WHERE v.op_log_id = $1 AND v.touched_at < $2 AND NOT v.id = ANY($3)
		   AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = v.op_log_id AND o.view_id = v.id)`,
		log, before, ids(keep))
	if err != nil {
		return 0, 0, err
	}
	return ops, tag.RowsAffected(), nil
}

func (x queries) TombstoneOps(ctx context.Context, log int64, kind encoding.Kind, members []encoding.ID, dry bool) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	without := `FROM unnest($3::bytea[]) AS u(id)
		WHERE NOT EXISTS (SELECT 1 FROM ` + opTable(kind) + ` t WHERE t.op_log_id = $1 AND t.id = u.id)
		  AND NOT EXISTS (SELECT 1 FROM op_tombstone d WHERE d.op_log_id = $1 AND d.kind = $2 AND d.id = u.id)`
	if dry {
		var n int64
		err := x.q.QueryRow(ctx, `SELECT count(*) `+without, log, int16(kind), ids(members)).Scan(&n)
		return n, err
	}
	tag, err := x.q.Exec(ctx, `INSERT INTO op_tombstone (op_log_id, kind, id) SELECT $1, $2, u.id `+without, log, int16(kind), ids(members))
	return tag.RowsAffected(), err
}

func (x queries) TailOpTombstones(ctx context.Context, log int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID]bool, error) {
	return idSet(x.q.Query(ctx, `SELECT id FROM op_tombstone WHERE op_log_id = $1 AND kind = $2 AND id = ANY($3)`, log, int16(kind), ids(members)))
}

// opKeys reads rows of an operation log, a kind, and an ID.
func opKeys(rows pgx.Rows, err error) ([]backend.OpKey, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (backend.OpKey, error) {
		var k backend.OpKey
		var kind int16
		var idb []byte
		if err := r.Scan(&k.Log, &kind, &idb); err != nil {
			return k, err
		}
		k.Kind = encoding.Kind(kind)
		var err error
		k.ID, err = toID(idb)
		return k, err
	})
}

func (x queries) RepositoryOpTombstones(ctx context.Context, repo int64) ([]backend.OpKey, error) {
	return opKeys(x.q.Query(ctx, `SELECT d.op_log_id, d.kind, d.id FROM op_tombstone d JOIN op_log l ON l.id = d.op_log_id
		WHERE l.repository_id = $1`, repo))
}

func (x queries) TailOpViewRefs(ctx context.Context, log int64, doomed []encoding.ID, before time.Time) (map[encoding.ID]bool, error) {
	return idSet(x.q.Query(ctx, `SELECT DISTINCT view_id FROM operation WHERE op_log_id = $1 AND NOT (id = ANY($2) AND touched_at < $3)`,
		log, ids(doomed), before))
}

func (x queries) AllOpLogs(ctx context.Context, repo int64) ([]int64, error) {
	return int64s(x.q.Query(ctx, `SELECT id FROM op_log WHERE repository_id = $1 ORDER BY id`, repo))
}

// opRecords reads rows of an operation log, a kind, an ID, a view (null for a view), a body, and
// a time.
func opRecords(rows pgx.Rows, err error) ([]backend.OpRecord, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (backend.OpRecord, error) {
		var o backend.OpRecord
		var kind int16
		var idb, viewb []byte
		if err := r.Scan(&o.Log, &kind, &idb, &viewb, &o.Body, &o.TouchedAt); err != nil {
			return o, err
		}
		o.Kind = encoding.Kind(kind)
		var err error
		if o.ID, err = toID(idb); err != nil {
			return o, err
		}
		if viewb != nil {
			o.View, err = toID(viewb)
		}
		return o, err
	})
}

func (x queries) SettledOps(ctx context.Context, repo int64, before time.Time, limit int) ([]backend.OpRecord, error) {
	ops, err := opRecords(x.q.Query(ctx, `SELECT o.op_log_id, $4::smallint, o.id, o.view_id, o.body, o.touched_at
		FROM operation o JOIN op_log l ON l.id = o.op_log_id
		WHERE l.repository_id = $1 AND o.touched_at < $2 ORDER BY o.op_log_id, o.id LIMIT $3`,
		repo, before, limit, int16(encoding.KindOperation)))
	if err != nil {
		return nil, err
	}
	views, err := opRecords(x.q.Query(ctx, `SELECT v.op_log_id, $4::smallint, v.id, NULL::bytea, v.body, v.touched_at
		FROM view v JOIN op_log l ON l.id = v.op_log_id
		WHERE l.repository_id = $1 AND v.touched_at < $2 ORDER BY v.op_log_id, v.id LIMIT $3`,
		repo, before, limit, int16(encoding.KindView)))
	return append(ops, views...), err
}

func (x queries) OpTombstones(ctx context.Context, repo int64, limit int) ([]backend.OpKey, error) {
	return opKeys(x.q.Query(ctx, `SELECT d.op_log_id, d.kind, d.id FROM op_tombstone d JOIN op_log l ON l.id = d.op_log_id
		WHERE l.repository_id = $1 ORDER BY d.op_log_id, d.kind, d.id LIMIT $2`, repo, limit))
}

func (x queries) WorkspaceOpLogs(ctx context.Context, repo int64) ([]int64, error) {
	return int64s(x.q.Query(ctx, `SELECT id FROM op_log WHERE repository_id = $1 AND workspace <> '' ORDER BY id`, repo))
}

func (x queries) deleteOrCount(ctx context.Context, from, where string, repo int64, dry bool) (int64, error) {
	if dry {
		var n int64
		err := x.q.QueryRow(ctx, `SELECT count(*) FROM `+from+` WHERE `+where, repo).Scan(&n)
		return n, err
	}
	tag, err := x.q.Exec(ctx, `DELETE FROM `+from+` WHERE `+where, repo)
	return tag.RowsAffected(), err
}

func (x queries) DeleteOrphanOpLogs(ctx context.Context, repo int64, dry bool) (int64, error) {
	return x.deleteOrCount(ctx, "op_log", `repository_id = $1 AND workspace = ''`, repo, dry)
}

func (x queries) DeleteOrphanWorkspaces(ctx context.Context, repo int64, dry bool) (int64, error) {
	return x.deleteOrCount(ctx, "workspace w", `w.repository_id = $1 AND NOT EXISTS (SELECT 1 FROM op_log l
		WHERE l.repository_id = w.repository_id AND l.principal = w.principal AND l.workspace = w.name)`, repo, dry)
}

func (x queries) RepositoryOperations(ctx context.Context, repo int64) ([]backend.OpRecord, error) {
	return opRecords(x.q.Query(ctx, `SELECT o.op_log_id, $2::smallint, o.id, o.view_id, o.body, o.touched_at
		FROM operation o JOIN op_log l ON l.id = o.op_log_id WHERE l.repository_id = $1`, repo, int16(encoding.KindOperation)))
}

func (x queries) RepositoryViews(ctx context.Context, repo int64) ([]backend.OpRecord, error) {
	return opRecords(x.q.Query(ctx, `SELECT v.op_log_id, $2::smallint, v.id, NULL::bytea, v.body, v.touched_at
		FROM view v JOIN op_log l ON l.id = v.op_log_id WHERE l.repository_id = $1`, repo, int16(encoding.KindView)))
}

func (x queries) OpContentPinned(ctx context.Context, repo int64) (bool, error) {
	var pinned bool
	err := x.q.QueryRow(ctx, `SELECT op_content_pinned FROM repository WHERE id = $1`, repo).Scan(&pinned)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return pinned, err
}

func (x queries) SetOpContentPinned(ctx context.Context, repo int64) error {
	_, err := x.q.Exec(ctx, `UPDATE repository SET op_content_pinned = true WHERE id = $1`, repo)
	return err
}

func (x queries) TailOps(ctx context.Context, repo int64, kind encoding.Kind, after backend.OpKey, limit int) ([]backend.OpRecord, error) {
	view := "o.view_id"
	table := "operation"
	if kind == encoding.KindView {
		view, table = "NULL::bytea", "view"
	}
	return opRecords(x.q.Query(ctx, `SELECT o.op_log_id, $2::smallint, o.id, `+view+`, o.body, o.touched_at
		FROM `+table+` o JOIN op_log l ON l.id = o.op_log_id
		WHERE l.repository_id = $1 AND (o.op_log_id, o.id) > ($3, $4::bytea)
		ORDER BY o.op_log_id, o.id LIMIT $5`, repo, int16(kind), after.Log, after.ID[:], limit))
}

func (x queries) DivergeOpLog(ctx context.Context, opLog int64) error {
	_, err := x.q.Exec(ctx, `UPDATE op_log SET diverged_at = now() WHERE id = $1`, opLog)
	return err
}

func (x queries) OpLogsToCollect(ctx context.Context, repo int64) ([]backend.OpLogDivergence, error) {
	rows, err := x.q.Query(ctx, `SELECT id, diverged_at FROM op_log
		WHERE repository_id = $1 AND workspace <> '' AND diverged_at IS NOT NULL
		  AND (collected_through IS NULL OR collected_through < diverged_at) ORDER BY id`, repo)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (backend.OpLogDivergence, error) {
		var d backend.OpLogDivergence
		return d, r.Scan(&d.OpLog, &d.DivergedAt)
	})
}

func (x queries) SetOpLogCollected(ctx context.Context, opLog int64, through time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE op_log SET collected_through = greatest(coalesce(collected_through, $2), $2) WHERE id = $1`, opLog, through)
	return err
}
