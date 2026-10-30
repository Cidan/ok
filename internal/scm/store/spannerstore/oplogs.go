package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Operation logs ───

func (x queries) OpLogID(ctx context.Context, repo int64, principal, workspace string) (int64, bool, error) {
	var id int64
	ok, err := x.one(ctx, `SELECT op_log_id FROM op_log WHERE repository_id = @repo AND principal = @principal AND workspace = @workspace`,
		map[string]any{"repo": repo, "principal": principal, "workspace": workspace}, &id)
	return id, ok, err
}

// InsertOpLog reads the workspace's log, then makes it if it has none: two that make one at once
// both read its absence, and Spanner aborts one of them, which finds it when it runs again.
func (x queries) InsertOpLog(ctx context.Context, repo int64, principal, workspace string) (int64, bool, error) {
	var id int64
	var made bool
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"repo": repo, "principal": principal, "workspace": workspace}
		ok, err := spannerdata.First(ctx, tx, `SELECT op_log_id FROM op_log
			WHERE repository_id = @repo AND principal = @principal AND workspace = @workspace`, p, &id)
		if err != nil || ok {
			made = false
			return err
		}
		id, made = data.NewID(), true
		p["id"] = id
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO op_log (op_log_id, repository_id, principal, workspace, created_at)
			VALUES (@id, @repo, @principal, @workspace, `+now+`)`, p)
		return err
	})
	return id, made, err
}

func (x queries) LockOpLog(ctx context.Context, log int64) error {
	var id int64
	_, err := x.lockOne(ctx, `SELECT op_log_id FROM op_log WHERE op_log_id = @log FOR UPDATE`, map[string]any{"log": log}, &id)
	return err
}

func (x queries) OpHeads(ctx context.Context, log int64) ([]encoding.ID, error) {
	return x.idList(ctx, `SELECT op_id FROM op_head WHERE op_log_id = @log ORDER BY op_id`, map[string]any{"log": log})
}

func (x queries) AddOpHead(ctx context.Context, log int64, id encoding.ID) error {
	_, err := x.exec(ctx, `INSERT OR IGNORE INTO op_head (op_log_id, op_id) VALUES (@log, @id)`, map[string]any{"log": log, "id": id[:]})
	return err
}

func (x queries) DeleteOpHeads(ctx context.Context, log int64, heads []encoding.ID) error {
	_, err := x.execIn(ctx, `DELETE FROM op_head WHERE op_log_id = @log AND op_id IN UNNEST(@ids)`, map[string]any{"log": log, "ids": ids(heads)},
		[]string{"ids"}, nil)
	return err
}

func (x queries) OperationIDsFrom(ctx context.Context, log int64, lo, hi []byte, limit int) ([]encoding.ID, error) {
	p := map[string]any{"log": log, "lo": lo, "limit": int64(limit)}
	if hi == nil {
		return x.idList(ctx, `SELECT id FROM operation WHERE op_log_id = @log AND id >= @lo ORDER BY id LIMIT @limit`, p)
	}
	p["hi"] = hi
	return x.idList(ctx, `SELECT id FROM operation WHERE op_log_id = @log AND id >= @lo AND id < @hi ORDER BY id LIMIT @limit`, p)
}

// opTable is where an operation log keeps operations or views.
func opTable(kind encoding.Kind) string {
	if kind == encoding.KindView {
		return "view"
	}
	return "operation"
}

func (x queries) OpBodies(ctx context.Context, log int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID][]byte, error) {
	return x.bodies(ctx, `SELECT id, body FROM `+opTable(kind)+` WHERE op_log_id = @log AND id IN UNNEST(@ids)`,
		map[string]any{"log": log, "ids": ids(members)}, "ids")
}

func (x queries) Operations(ctx context.Context, log int64, before time.Time) ([]backend.Operation, error) {
	var out []backend.Operation
	err := x.rows(ctx, `SELECT id, view_id, body, touched_at < @before FROM operation WHERE op_log_id = @log`,
		map[string]any{"log": log, "before": before}, func() { out = nil }, func(row *spanner.Row) error {
			var idb, viewb []byte
			var o backend.Operation
			if err := row.Columns(&idb, &viewb, &o.Body, &o.Old); err != nil {
				return err
			}
			var err error
			if o.ID, err = toID(idb); err != nil {
				return err
			}
			if o.View, err = toID(viewb); err != nil {
				return err
			}
			out = append(out, o)
			return nil
		})
	return out, err
}

func (x queries) Views(ctx context.Context, log int64) ([]backend.Object, error) {
	return x.objects(ctx, `SELECT id, body FROM view WHERE op_log_id = @log`, map[string]any{"log": log}, encoding.KindView)
}

func (x queries) OlderThan(ctx context.Context, log int64, t time.Time) (bool, error) {
	var old bool
	_, err := x.one(ctx, `SELECT EXISTS (SELECT 1 FROM operation WHERE op_log_id = @log AND touched_at < @t)
		OR EXISTS (SELECT 1 FROM view WHERE op_log_id = @log AND touched_at < @t)`, map[string]any{"log": log, "t": t}, &old)
	return old, err
}

// PutOperations revives operations' tombstones and records them, touching those recorded: an
// operation's ID is its body's hash, so writing a recorded one again changes nothing but its time.
func (x queries) PutOperations(ctx context.Context, log int64, ops []backend.Operation) error {
	if len(ops) == 0 {
		return nil
	}
	seen := make(map[encoding.ID]bool, len(ops))
	var idCol, viewCol, bs [][]byte
	for _, o := range ops {
		if !seen[o.ID] {
			seen[o.ID] = true
			idCol, viewCol, bs = append(idCol, o.ID[:]), append(viewCol, o.View[:]), append(bs, o.Body)
		}
	}
	return x.putOps(ctx, log, encoding.KindOperation, idCol, `INSERT OR UPDATE INTO operation (op_log_id, id, view_id, body, touched_at)
		SELECT @log, id, @views[OFFSET(i)], @bodies[OFFSET(i)], `+now+` FROM UNNEST(@ids) AS id WITH OFFSET i`,
		map[string]any{"views": viewCol, "bodies": bs}, []string{"ids", "views", "bodies"}, bodySize(bs))
}

func (x queries) PutViews(ctx context.Context, log int64, views []backend.Object) error {
	if len(views) == 0 {
		return nil
	}
	seen := make(map[encoding.ID]bool, len(views))
	var idCol, bs [][]byte
	for _, v := range views {
		if !seen[v.ID] {
			seen[v.ID] = true
			idCol, bs = append(idCol, v.ID[:]), append(bs, v.Body)
		}
	}
	return x.putOps(ctx, log, encoding.KindView, idCol, `INSERT OR UPDATE INTO view (op_log_id, id, body, touched_at)
		SELECT @log, id, @bodies[OFFSET(i)], `+now+` FROM UNNEST(@ids) AS id WITH OFFSET i`, map[string]any{"bodies": bs},
		[]string{"ids", "bodies"}, bodySize(bs))
}

func (x queries) putOps(ctx context.Context, log int64, kind encoding.Kind, idCol [][]byte, insert string, p map[string]any, arrays []string,
	size func(int) int) error {
	p["log"], p["ids"], p["kind"] = log, idCol, int64(kind)
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := updateIn(ctx, tx, `DELETE FROM op_tombstone WHERE op_log_id = @log AND kind = @kind AND id IN UNNEST(@ids)`, p,
			[]string{"ids"}, nil); err != nil {
			return err
		}
		_, err := updateIn(ctx, tx, insert, p, arrays, size)
		return err
	})
}

func (x queries) TouchOps(ctx context.Context, log int64, kind encoding.Kind, members []encoding.ID) ([]encoding.ID, error) {
	return x.returningIDs(ctx, `UPDATE `+opTable(kind)+` SET touched_at = `+now+` WHERE op_log_id = @log AND id IN UNNEST(@ids)
		THEN RETURN id`, map[string]any{"log": log, "ids": ids(members)}, nil, "ids")
}

func (x queries) CollectOps(ctx context.Context, log int64, doomed, keep []encoding.ID, before time.Time, dry bool) (int64, int64, error) {
	p := map[string]any{"log": log, "doomed": ids(doomed), "keep": ids(keep), "before": before}
	if dry {
		var ops, views int64
		err := x.read(func(r spannerdata.Reader) error {
			if _, err := spannerdata.First(ctx, r, `SELECT COUNT(*) FROM operation
				WHERE op_log_id = @log AND id IN UNNEST(@doomed) AND touched_at < @before`, p, &ops); err != nil {
				return err
			}
			_, err := spannerdata.First(ctx, r, `SELECT COUNT(*) FROM view v
				WHERE v.op_log_id = @log AND v.touched_at < @before AND v.id NOT IN UNNEST(@keep)
				  AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = v.op_log_id AND o.view_id = v.id
					AND NOT (o.id IN UNNEST(@doomed) AND o.touched_at < @before))`, p, &views)
			return err
		})
		return ops, views, err
	}
	var ops, views int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		if ops, err = updateIn(ctx, tx, `DELETE FROM operation
			WHERE op_log_id = @log AND id IN UNNEST(@doomed) AND touched_at < @before`, p, []string{"doomed"}, nil); err != nil {
			return err
		}
		views, err = deleteAll(ctx, tx, "view", `op_log_id = @log AND touched_at < @before AND id NOT IN UNNEST(@keep)
			AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = d.op_log_id AND o.view_id = d.id)`, p)
		return err
	})
	return ops, views, err
}

func (x queries) TombstoneOps(ctx context.Context, log int64, kind encoding.Kind, members []encoding.ID, dry bool) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	p := map[string]any{"log": log, "kind": int64(kind), "ids": ids(distinct(members))}
	without := `FROM UNNEST(@ids) AS u
		WHERE NOT EXISTS (SELECT 1 FROM ` + opTable(kind) + ` t WHERE t.op_log_id = @log AND t.id = u)
		  AND NOT EXISTS (SELECT 1 FROM op_tombstone d WHERE d.op_log_id = @log AND d.kind = @kind AND d.id = u)`
	if dry {
		var n int64
		err := x.read(func(r spannerdata.Reader) error {
			n = 0
			return eachIn(ctx, r, `SELECT COUNT(*) `+without, p, []string{"ids"}, func(row *spanner.Row) error {
				var c int64
				err := row.Columns(&c)
				n += c
				return err
			})
		})
		return n, err
	}
	return x.execIn(ctx, `INSERT INTO op_tombstone (op_log_id, kind, id) SELECT @log, @kind, u `+without, p, []string{"ids"}, nil)
}

func (x queries) TailOpTombstones(ctx context.Context, log int64, kind encoding.Kind, members []encoding.ID) (map[encoding.ID]bool, error) {
	return x.idSet(ctx, `SELECT id FROM op_tombstone WHERE op_log_id = @log AND kind = @kind AND id IN UNNEST(@ids)`,
		map[string]any{"log": log, "kind": int64(kind), "ids": ids(members)}, "ids")
}

// opKeys reads rows of an operation log, a kind, and an ID.
func (x queries) opKeys(ctx context.Context, sql string, params map[string]any) ([]backend.OpKey, error) {
	var out []backend.OpKey
	err := x.rows(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		var k backend.OpKey
		var kind int64
		var idb []byte
		if err := row.Columns(&k.Log, &kind, &idb); err != nil {
			return err
		}
		k.Kind = encoding.Kind(kind)
		var err error
		if k.ID, err = toID(idb); err != nil {
			return err
		}
		out = append(out, k)
		return nil
	})
	return out, err
}

func (x queries) RepositoryOpTombstones(ctx context.Context, repo int64) ([]backend.OpKey, error) {
	return x.opKeys(ctx, `SELECT d.op_log_id, d.kind, d.id FROM op_tombstone d JOIN op_log l ON l.op_log_id = d.op_log_id
		WHERE l.repository_id = @repo`, map[string]any{"repo": repo})
}

func (x queries) TailOpViewRefs(ctx context.Context, log int64, doomed []encoding.ID, before time.Time) (map[encoding.ID]bool, error) {
	return x.idSet(ctx, `SELECT DISTINCT view_id FROM operation WHERE op_log_id = @log AND NOT (id IN UNNEST(@doomed) AND touched_at < @before)`,
		map[string]any{"log": log, "doomed": ids(doomed), "before": before})
}

func (x queries) AllOpLogs(ctx context.Context, repo int64) ([]int64, error) {
	return x.int64s(ctx, `SELECT op_log_id FROM op_log WHERE repository_id = @repo ORDER BY op_log_id`, map[string]any{"repo": repo})
}

// opRecords reads rows of an operation log, an ID, a view (NULL for a view), a body, and a time,
// each of kind.
func (x queries) opRecords(ctx context.Context, kind encoding.Kind, sql string, params map[string]any) ([]backend.OpRecord, error) {
	var out []backend.OpRecord
	err := x.rows(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		o := backend.OpRecord{OpKey: backend.OpKey{Kind: kind}}
		var idb, viewb []byte
		if err := row.Columns(&o.Log, &idb, &viewb, &o.Body, &o.TouchedAt); err != nil {
			return err
		}
		var err error
		if o.ID, err = toID(idb); err != nil {
			return err
		}
		if viewb != nil {
			if o.View, err = toID(viewb); err != nil {
				return err
			}
		}
		out = append(out, o)
		return nil
	})
	return out, err
}

// SettledOpsIn reads the window's IDs, each with whether it is settled, then the records it takes.
func (x queries) SettledOpsIn(ctx context.Context, log int64, kind encoding.Kind, after []byte, before time.Time, scan, limit int) ([]backend.OpRecord, []byte, int, bool, error) {
	table, record := "operation", `SELECT op_log_id, id, view_id, body, touched_at FROM operation`
	if kind == encoding.KindView {
		table, record = "view", `SELECT op_log_id, id, CAST(NULL AS BYTES), body, touched_at FROM view`
	}
	p := map[string]any{"log": log, "scan": int64(scan), "before": before}
	window := `SELECT id, touched_at < @before FROM ` + table + ` WHERE op_log_id = @log ORDER BY id LIMIT @scan`
	if after != nil {
		p["after"] = after
		window = `SELECT id, touched_at < @before FROM ` + table + ` WHERE op_log_id = @log AND id > @after ORDER BY id LIMIT @scan`
	}
	keys, take, err := x.window(ctx, window, p, oneKey)
	if err != nil {
		return nil, nil, 0, false, err
	}
	taken, last, looked, more := backend.Window(keys, take, scan, limit)
	if len(taken) == 0 {
		return nil, last, looked, more, nil
	}
	ops, err := x.opRecords(ctx, kind, record+` WHERE op_log_id = @log AND id IN UNNEST(@ids) ORDER BY id`,
		map[string]any{"log": log, "ids": taken})
	return ops, last, looked, more, err
}

func (x queries) OpTombstones(ctx context.Context, repo int64, limit int) ([]backend.OpKey, error) {
	return x.opKeys(ctx, `SELECT d.op_log_id, d.kind, d.id FROM op_tombstone d JOIN op_log l ON l.op_log_id = d.op_log_id
		WHERE l.repository_id = @repo ORDER BY d.op_log_id, d.kind, d.id LIMIT @limit`, map[string]any{"repo": repo, "limit": int64(limit)})
}

func (x queries) WorkspaceOpLogs(ctx context.Context, repo int64) ([]int64, error) {
	return x.int64s(ctx, `SELECT op_log_id FROM op_log WHERE repository_id = @repo AND workspace != '' ORDER BY op_log_id`,
		map[string]any{"repo": repo})
}

// opLogTables are an operation log's rows' tables, which go with it.
var opLogTables = []string{"operation", "view", "op_head", "op_tombstone"}

// DeleteOrphanOpLogs deletes a repository's operation logs of no workspace, and their rows first:
// a shared log may hold far more than one transaction deletes, so outside one it deletes them in
// several (deleteInTransactions). A log is no workspace's for good, so one half deleted is deleted
// the rest of the way by the next.
func (x queries) DeleteOrphanOpLogs(ctx context.Context, repo int64, dry bool) (int64, error) {
	const where = `repository_id = @repo AND workspace = ''`
	p := map[string]any{"repo": repo}
	if dry {
		return x.count(ctx, `SELECT COUNT(*) FROM op_log WHERE `+where, p)
	}
	var tables []tableRows
	for _, t := range opLogTables {
		tables = append(tables, tableRows{t, `op_log_id IN (SELECT op_log_id FROM op_log WHERE ` + where + `)`})
	}
	return x.deleteInTransactions(ctx, append(tables, tableRows{"op_log", where}), p)
}

// noLog is a row d — a workspace, its name in the column ws, or a checkpoint — having no operation
// log. (op_log has a column named workspace, which would shadow the table's name.)
func noLog(ws string) string {
	return ` NOT EXISTS (SELECT 1 FROM op_log l WHERE l.repository_id = @repo AND l.principal = d.principal AND l.workspace = d.` + ws + `)`
}

// DeleteOrphanWorkspaces deletes a repository's workspaces with no operation log, their
// checkpoints with them; a checkpoint of another workspace whose parent one was loses its parent,
// as PostgreSQL's foreign key sets it NULL.
func (x queries) DeleteOrphanWorkspaces(ctx context.Context, repo int64, dry bool) (int64, error) {
	p := map[string]any{"repo": repo}
	if dry {
		return x.count(ctx, `SELECT COUNT(*) FROM workspace AS d WHERE d.repository_id = @repo AND`+noLog("name"), p)
	}
	var doomed []int64
	if err := x.rows(ctx, `SELECT d.id FROM checkpoint AS d WHERE d.repository_id = @repo AND`+noLog("workspace"), p, func() { doomed = nil },
		func(row *spanner.Row) error {
			var id int64
			err := row.Columns(&id)
			doomed = append(doomed, id)
			return err
		}); err != nil {
		return 0, err
	}
	if err := x.deleteCheckpoints(ctx, repo, doomed); err != nil {
		return 0, err
	}
	return x.deleteInTransactions(ctx, []tableRows{{"workspace", `d.repository_id = @repo AND` + noLog("name")}}, p)
}

func (x queries) RepositoryOperations(ctx context.Context, repo int64) ([]backend.OpRecord, error) {
	return x.opRecords(ctx, encoding.KindOperation, `SELECT o.op_log_id, o.id, o.view_id, o.body, o.touched_at
		FROM operation o JOIN op_log l ON l.op_log_id = o.op_log_id WHERE l.repository_id = @repo`, map[string]any{"repo": repo})
}

func (x queries) RepositoryViews(ctx context.Context, repo int64) ([]backend.OpRecord, error) {
	return x.opRecords(ctx, encoding.KindView, `SELECT v.op_log_id, v.id, CAST(NULL AS BYTES), v.body, v.touched_at
		FROM view v JOIN op_log l ON l.op_log_id = v.op_log_id WHERE l.repository_id = @repo`, map[string]any{"repo": repo})
}

func (x queries) OpContentPinned(ctx context.Context, repo int64) (bool, error) {
	var pinned bool
	_, err := x.one(ctx, `SELECT op_content_pinned FROM repository WHERE repository_id = @repo`, map[string]any{"repo": repo}, &pinned)
	return pinned, err
}

func (x queries) SetOpContentPinned(ctx context.Context, repo int64) error {
	_, err := x.exec(ctx, `UPDATE repository SET op_content_pinned = TRUE WHERE repository_id = @repo`, map[string]any{"repo": repo})
	return err
}

func (x queries) TailOps(ctx context.Context, repo int64, kind encoding.Kind, after backend.OpKey, limit int) ([]backend.OpRecord, error) {
	view, table := "o.view_id", "operation"
	if kind == encoding.KindView {
		view, table = "CAST(NULL AS BYTES)", "view"
	}
	return x.opRecords(ctx, kind, `SELECT o.op_log_id, o.id, `+view+`, o.body, o.touched_at
		FROM `+table+` o JOIN op_log l ON l.op_log_id = o.op_log_id
		WHERE l.repository_id = @repo AND (o.op_log_id > @log OR (o.op_log_id = @log AND o.id > @id))
		ORDER BY o.op_log_id, o.id LIMIT @limit`,
		map[string]any{"repo": repo, "log": after.Log, "id": after.ID[:], "limit": int64(limit)})
}

func (x queries) DivergeOpLog(ctx context.Context, opLog int64) error {
	_, err := x.exec(ctx, `UPDATE op_log SET diverged_at = `+now+` WHERE op_log_id = @log`, map[string]any{"log": opLog})
	return err
}

func (x queries) OpLogsToCollect(ctx context.Context, repo int64) ([]backend.OpLogDivergence, error) {
	var out []backend.OpLogDivergence
	err := x.rows(ctx, `SELECT op_log_id, diverged_at FROM op_log
		WHERE repository_id = @repo AND workspace != '' AND diverged_at IS NOT NULL
		  AND (collected_through IS NULL OR collected_through < diverged_at) ORDER BY op_log_id`,
		map[string]any{"repo": repo}, func() { out = nil }, func(row *spanner.Row) error {
			var d backend.OpLogDivergence
			if err := row.Columns(&d.OpLog, &d.DivergedAt); err != nil {
				return err
			}
			out = append(out, d)
			return nil
		})
	return out, err
}

func (x queries) SetOpLogCollected(ctx context.Context, opLog int64, through time.Time) error {
	_, err := x.exec(ctx, `UPDATE op_log SET collected_through = GREATEST(IFNULL(collected_through, @through), @through) WHERE op_log_id = @log`,
		map[string]any{"log": opLog, "through": through})
	return err
}
