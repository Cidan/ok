package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

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
		`INSERT INTO op_log (repository_id, principal, workspace) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING id`,
		repo, principal, workspace).Scan(&id)
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
	_, err := x.q.Exec(ctx, `INSERT INTO operation (op_log_id, id, view_id, body)
		SELECT $1, u.id, u.view, u.body FROM unnest($2::bytea[], $3::bytea[], $4::bytea[]) AS u(id, view, body)
		ORDER BY u.id ON CONFLICT (op_log_id, id) DO UPDATE SET touched_at = now()`,
		log, ids(idCol), ids(viewCol), bs)
	return err
}

func (x queries) PutViews(ctx context.Context, log int64, views []backend.Object) error {
	idCol := make([]encoding.ID, len(views))
	bs := make([][]byte, len(views))
	for i, v := range views {
		idCol[i], bs[i] = v.ID, v.Body
	}
	_, err := x.q.Exec(ctx, `INSERT INTO view (op_log_id, id, body)
		SELECT $1, u.id, u.body FROM unnest($2::bytea[], $3::bytea[]) AS u(id, body)
		ORDER BY u.id ON CONFLICT (op_log_id, id) DO UPDATE SET touched_at = now()`,
		log, ids(idCol), bs)
	return err
}

func (x queries) TouchOps(ctx context.Context, log int64, kind encoding.Kind, members []encoding.ID) error {
	_, err := x.q.Exec(ctx, `UPDATE `+opTable(kind)+` SET touched_at = now() WHERE op_log_id = $1 AND id = ANY($2)`, log, ids(members))
	return err
}

func (x queries) CollectOps(ctx context.Context, log int64, doomed []encoding.ID, before time.Time, dry bool) (int64, int64, error) {
	var ops, views int64
	if dry {
		if err := x.q.QueryRow(ctx, `SELECT count(*) FROM operation WHERE op_log_id = $1 AND id = ANY($2) AND touched_at < $3`,
			log, ids(doomed), before).Scan(&ops); err != nil {
			return 0, 0, err
		}
		err := x.q.QueryRow(ctx, `SELECT count(*) FROM view v WHERE v.op_log_id = $1 AND v.touched_at < $2
			AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = v.op_log_id AND o.view_id = v.id
			AND NOT (o.id = ANY($3) AND o.touched_at < $2))`, log, before, ids(doomed)).Scan(&views)
		return ops, views, err
	}
	tag, err := x.q.Exec(ctx, `DELETE FROM operation WHERE op_log_id = $1 AND id = ANY($2) AND touched_at < $3`,
		log, ids(doomed), before)
	if err != nil {
		return 0, 0, err
	}
	ops = tag.RowsAffected()
	tag, err = x.q.Exec(ctx,
		`DELETE FROM view v WHERE v.op_log_id = $1 AND v.touched_at < $2
		   AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = v.op_log_id AND o.view_id = v.id)`,
		log, before)
	if err != nil {
		return 0, 0, err
	}
	return ops, tag.RowsAffected(), nil
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

func (x queries) RepositoryOperations(ctx context.Context, repo int64) ([]backend.Object, error) {
	rows, err := x.q.Query(ctx, `SELECT o.id, o.body FROM operation o JOIN op_log l ON l.id = o.op_log_id WHERE l.repository_id = $1`, repo)
	return objects(rows, err, encoding.KindOperation)
}

func (x queries) RepositoryViews(ctx context.Context, repo int64) ([]backend.Object, error) {
	rows, err := x.q.Query(ctx, `SELECT v.id, v.body FROM view v JOIN op_log l ON l.id = v.op_log_id WHERE l.repository_id = $1`, repo)
	return objects(rows, err, encoding.KindView)
}
