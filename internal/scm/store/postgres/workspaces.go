package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Dictionaries ───

func (x queries) NetworkDictionary(ctx context.Context, net backend.Network, id int64) ([]byte, int64, bool, error) {
	var body []byte
	var trainer int64
	err := x.q.QueryRow(ctx, `SELECT body, coalesce(trainer_id, 0) FROM dictionary WHERE network_id = $1 AND id = $2 AND length(body) > 0`,
		net, id).Scan(&body, &trainer)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, 0, false, nil
	}
	return body, trainer, err == nil, err
}

func (x queries) NetworkTrainer(ctx context.Context, net backend.Network) (int64, error) {
	var trainer int64
	err := x.q.QueryRow(ctx, `SELECT coalesce(trainer_id, 0) FROM network WHERE id = $1`, net).Scan(&trainer)
	return trainer, err
}

func (x queries) DictionaryGrants(ctx context.Context, repo int64) (map[int64]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT trainer_id, up_to FROM dictionary_grant WHERE repository_id = $1`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var trainer, upTo int64
		if err := rows.Scan(&trainer, &upTo); err != nil {
			return nil, err
		}
		out[trainer] = upTo
	}
	return out, rows.Err()
}

func (x queries) SetDictionaryGrants(ctx context.Context, repo int64, grants map[int64]int64) error {
	if _, err := x.q.Exec(ctx, `DELETE FROM dictionary_grant WHERE repository_id = $1`, repo); err != nil {
		return err
	}
	for trainer, upTo := range grants {
		if _, err := x.q.Exec(ctx, `INSERT INTO dictionary_grant (repository_id, trainer_id, up_to) VALUES ($1, $2, $3)`,
			repo, trainer, upTo); err != nil {
			return err
		}
	}
	return nil
}

func (x queries) NewestDictionaryOf(ctx context.Context, trainer int64) (int64, bool, error) {
	var id int64
	err := x.q.QueryRow(ctx, `SELECT id FROM dictionary WHERE trainer_id = $1 AND length(body) > 0 ORDER BY id DESC LIMIT 1`, trainer).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return id, err == nil, err
}

func (x queries) NewestDictionary(ctx context.Context, net backend.Network) (int64, []byte, time.Time, bool, error) {
	var id int64
	var body []byte
	var at time.Time
	err := x.q.QueryRow(ctx, `SELECT id, body, created_at FROM dictionary
		WHERE network_id = $1 AND length(body) > 0 ORDER BY id DESC LIMIT 1`, net).Scan(&id, &body, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, time.Time{}, false, nil
	}
	return id, body, at, err == nil, err
}

func (x queries) InsertDictionary(ctx context.Context, net backend.Network, trainer int64, samples int) (int64, error) {
	var id int64
	err := x.q.QueryRow(ctx, `INSERT INTO dictionary (network_id, trainer_id, body, samples) VALUES ($1, $2, '', $3) RETURNING id`,
		net, trainer, samples).Scan(&id)
	return id, err
}

func (x queries) SetDictionaryBody(ctx context.Context, net backend.Network, id int64, body []byte) error {
	_, err := x.q.Exec(ctx, `UPDATE dictionary SET body = $3 WHERE network_id = $1 AND id = $2`, net, id, body)
	return err
}

func (x queries) DeleteDictionary(ctx context.Context, net backend.Network, id int64) error {
	_, err := x.q.Exec(ctx, `DELETE FROM dictionary WHERE network_id = $1 AND id = $2`, net, id)
	return err
}

// ─── Garbage collection ───

func (x queries) DueForGC(ctx context.Context, scheduled, retry time.Time) ([]int64, error) {
	return int64s(x.q.Query(ctx, `SELECT r.id FROM repository r
		WHERE NOT EXISTS (SELECT 1 FROM gc_pass g WHERE g.repository_id = r.id AND g.started_at >= $1 AND g.error IS NULL)
		  AND NOT EXISTS (SELECT 1 FROM gc_pass g WHERE g.repository_id = r.id AND g.error IS NOT NULL AND g.started_at > $2)
		ORDER BY r.id`, scheduled, retry))
}

func (x queries) RepositoryDueForGC(ctx context.Context, repo int64, scheduled, retry time.Time) (bool, error) {
	var due bool
	err := x.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM repository r WHERE r.id = $1
		AND NOT EXISTS (SELECT 1 FROM gc_pass g WHERE g.repository_id = r.id AND g.started_at >= $2 AND g.error IS NULL)
		AND NOT EXISTS (SELECT 1 FROM gc_pass g WHERE g.repository_id = r.id AND g.error IS NOT NULL AND g.started_at > $3))`,
		repo, scheduled, retry).Scan(&due)
	return due, err
}

// ─── Keyed work ───

func (x queries) MarkWork(ctx context.Context, kind string, key int64) error {
	return pgdata.MarkWork(ctx, x.q, kind, key)
}

func (x queries) MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return pgdata.MarkWorkAt(ctx, x.q, kind, key, at)
}

func (x queries) TakeWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return pgdata.TakeWork(ctx, x.q, kind, key, holder, ttl)
}

func (x queries) RenewWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	return pgdata.RenewWork(ctx, x.q, kind, key, holder, ttl)
}

func (x queries) ReleaseWork(ctx context.Context, kind string, key int64, holder string) (bool, error) {
	return pgdata.ReleaseWork(ctx, x.q, kind, key, holder)
}

func (x queries) DueWork(ctx context.Context, kind string, limit int) ([]int64, error) {
	return pgdata.DueWork(ctx, x.q, kind, limit)
}

func (x queries) InsertGCPass(ctx context.Context, p backend.GCPass) error {
	_, err := x.q.Exec(ctx, `INSERT INTO gc_pass (repository_id, started_at, finished_at, operations_deleted, views_deleted,
			marked, commits_swept, objects_swept, files_swept, chunks_swept, chunk_bytes_swept,
			packs_written, packs_retired, bytes_read, bytes_reclaimed, packs_deleted, orphan_packs, error, checkpoints_deleted, op_logs_deleted, workspaces_deleted)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		ON CONFLICT DO NOTHING`,
		p.Repository, p.Started, p.Finished, p.OperationsDeleted, p.ViewsDeleted,
		p.Marked, p.CommitsSwept, p.ObjectsSwept, p.FilesSwept, p.ChunksSwept, p.ChunkBytesSwept,
		p.PacksWritten, p.PacksRetired, p.BytesRead, p.BytesReclaimed, p.PacksDeleted, p.OrphanPacks, p.Error,
		p.CheckpointsDeleted, p.OpLogsDeleted, p.WorkspacesDeleted)
	return err
}

// ─── Workspaces and checkpoints ───

func (x queries) InsertWorkspace(ctx context.Context, repo int64, principal, name string) (bool, error) {
	tag, err := x.q.Exec(ctx, `INSERT INTO workspace (repository_id, principal, name) VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`, repo, principal, name)
	return tag.RowsAffected() > 0, err
}

func (x queries) LockWorkspace(ctx context.Context, repo int64, principal, name string) (backend.WorkspaceLease, bool, error) {
	var l backend.WorkspaceLease
	var holder, takeover *string
	var expires *time.Time
	err := x.q.QueryRow(ctx, `SELECT holder, epoch, expires_at, takeover_by FROM workspace
		WHERE repository_id = $1 AND principal = $2 AND name = $3 FOR UPDATE`, repo, principal, name).
		Scan(&holder, &l.Epoch, &expires, &takeover)
	if errors.Is(err, pgx.ErrNoRows) {
		return l, false, nil
	}
	if err != nil {
		return l, false, err
	}
	if holder != nil {
		l.Holder = *holder
	}
	if takeover != nil {
		l.TakeoverBy = *takeover
	}
	l.ExpiresAt = timeOrZero(expires)
	return l, true, nil
}

func (x queries) SetTakeover(ctx context.Context, repo int64, principal, name, machine string) error {
	_, err := x.q.Exec(ctx, `UPDATE workspace SET takeover_by = $4
		WHERE repository_id = $1 AND principal = $2 AND name = $3`, repo, principal, name, machine)
	return err
}

func (x queries) GrantLease(ctx context.Context, repo int64, principal, name, machine string, epoch int64, expires time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE workspace SET holder = $4, epoch = $5, expires_at = $6,
			takeover_by = CASE WHEN takeover_by = $4 THEN NULL ELSE takeover_by END
		WHERE repository_id = $1 AND principal = $2 AND name = $3`,
		repo, principal, name, machine, epoch, expires)
	return err
}

func (x queries) ExtendLease(ctx context.Context, repo int64, principal, name string, expires time.Time) error {
	_, err := x.q.Exec(ctx, `UPDATE workspace SET expires_at = $4 WHERE repository_id = $1 AND principal = $2 AND name = $3`,
		repo, principal, name, expires)
	return err
}

func (x queries) ReleaseLease(ctx context.Context, repo int64, principal, name, machine string, epoch int64) error {
	_, err := x.q.Exec(ctx, `UPDATE workspace SET holder = NULL, expires_at = NULL
		WHERE repository_id = $1 AND principal = $2 AND name = $3 AND holder = $4 AND epoch = $5`,
		repo, principal, name, machine, epoch)
	return err
}

func (x queries) CheckpointOf(ctx context.Context, id, repo int64, principal, workspace string) (bool, error) {
	var ok bool
	err := x.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM checkpoint
		WHERE id = $1 AND repository_id = $2 AND principal = $3 AND workspace = $4)`,
		id, repo, principal, workspace).Scan(&ok)
	return ok, err
}

func (x queries) InsertCheckpoint(ctx context.Context, principal string, c backend.Checkpoint) (int64, time.Time, error) {
	var id int64
	var at time.Time
	err := x.q.QueryRow(ctx, `INSERT INTO checkpoint (id, repository_id, principal, workspace, machine, parent_id, tree_ids, base_commit_id, fork,
			created_at)
		VALUES ($9, $1, $2, $3, $4, $5, $6, $7, $8, clock_timestamp()) RETURNING id, created_at`,
		c.Repo, principal, c.Workspace, c.Machine, nullable(c.ParentID), ids(c.Trees), c.BaseCommit[:], c.Fork, data.NewID()).Scan(&id, &at)
	return id, at, err
}

const checkpointColumns = `id, repository_id, workspace, machine, coalesce(parent_id, 0), tree_ids, base_commit_id, fork, created_at`

func scanCheckpoint(row pgx.Row) (backend.Checkpoint, error) {
	var c backend.Checkpoint
	var trees [][]byte
	var base []byte
	if err := row.Scan(&c.ID, &c.Repo, &c.Workspace, &c.Machine, &c.ParentID, &trees, &base, &c.Fork, &c.CreatedAt); err != nil {
		return c, err
	}
	for _, t := range trees {
		id, err := toID(t)
		if err != nil {
			return c, err
		}
		c.Trees = append(c.Trees, id)
	}
	b, err := toID(base)
	if err != nil {
		return c, err
	}
	c.BaseCommit = b
	return c, nil
}

func oneCheckpoint(row pgx.Row) (backend.Checkpoint, bool, error) {
	c, err := scanCheckpoint(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return backend.Checkpoint{}, false, nil
	}
	return c, err == nil, err
}

func (x queries) LatestCheckpoint(ctx context.Context, repo int64, principal, workspace string) (backend.Checkpoint, bool, error) {
	return oneCheckpoint(x.q.QueryRow(ctx, `SELECT `+checkpointColumns+` FROM checkpoint
		WHERE repository_id = $1 AND principal = $2 AND workspace = $3 AND NOT fork ORDER BY created_at DESC, id DESC LIMIT 1`, repo, principal, workspace))
}

func (x queries) PrincipalCheckpoint(ctx context.Context, repo int64, principal string, id int64) (backend.Checkpoint, bool, error) {
	return oneCheckpoint(x.q.QueryRow(ctx, `SELECT `+checkpointColumns+` FROM checkpoint
		WHERE id = $1 AND repository_id = $2 AND principal = $3`, id, repo, principal))
}

func (x queries) Checkpoints(ctx context.Context, repo int64, principal, workspace string, before int64, limit int) ([]backend.Checkpoint, error) {
	rows, err := x.q.Query(ctx, `SELECT `+checkpointColumns+` FROM checkpoint
		WHERE repository_id = $1 AND principal = $2 AND workspace = $3
			AND ($4::bigint = 0 OR (created_at, id) < (SELECT created_at, id FROM checkpoint WHERE id = $4))
		ORDER BY created_at DESC, id DESC LIMIT $5`,
		repo, principal, workspace, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Checkpoint
	for rows.Next() {
		c, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (x queries) Workspaces(ctx context.Context, repo int64, principal string) ([]backend.Workspace, error) {
	rows, err := x.q.Query(ctx, `SELECT w.name, coalesce(w.holder, ''), w.epoch, w.expires_at, coalesce(w.takeover_by, ''),
			EXISTS (SELECT 1 FROM op_log l WHERE l.repository_id = w.repository_id AND l.principal = w.principal AND l.workspace = w.name)
		FROM workspace w WHERE w.repository_id = $1 AND w.principal = $2 ORDER BY w.name`, repo, principal)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Workspace
	for rows.Next() {
		var w backend.Workspace
		var expires *time.Time
		if err := rows.Scan(&w.Name, &w.Holder, &w.Epoch, &expires, &w.TakeoverBy, &w.OwnLog); err != nil {
			return nil, err
		}
		w.ExpiresAt = timeOrZero(expires)
		out = append(out, w)
	}
	return out, rows.Err()
}

func (x queries) WorkspaceNames(ctx context.Context, repo int64, after backend.WorkspaceKey, limit int) ([]backend.WorkspaceKey, error) {
	rows, err := x.q.Query(ctx, `SELECT principal, name FROM workspace WHERE repository_id = $1 AND (principal, name) > ($2, $3)
		ORDER BY principal, name LIMIT $4`, repo, after.Principal, after.Name, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.WorkspaceKey
	for rows.Next() {
		var k backend.WorkspaceKey
		if err := rows.Scan(&k.Principal, &k.Name); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (x queries) ThinCheckpoints(ctx context.Context, repo int64, every, hourly, daily time.Time, dry bool) (int64, error) {
	query := `WITH old AS (
			SELECT id, created_at,
				row_number() OVER (PARTITION BY principal, workspace, date_trunc('hour', created_at AT TIME ZONE 'UTC')
					ORDER BY created_at DESC, id DESC) AS in_hour,
				row_number() OVER (PARTITION BY principal, workspace, date_trunc('day', created_at AT TIME ZONE 'UTC')
					ORDER BY created_at DESC, id DESC) AS in_day
			FROM checkpoint WHERE repository_id = $1 AND created_at < $2
		), latest AS (
			SELECT DISTINCT ON (principal, workspace) id FROM checkpoint
			WHERE repository_id = $1 AND NOT fork ORDER BY principal, workspace, created_at DESC, id DESC
		), doomed AS (
			SELECT id FROM old WHERE id NOT IN (SELECT id FROM latest) AND (
				created_at < $4
				OR (created_at < $3 AND in_day > 1)
				OR (created_at >= $3 AND in_hour > 1))
		)`
	args := []any{repo, every, hourly, daily}
	if dry {
		var n int64
		err := x.q.QueryRow(ctx, query+` SELECT count(*) FROM doomed`, args...).Scan(&n)
		return n, err
	}
	tag, err := x.q.Exec(ctx, query+` DELETE FROM checkpoint WHERE id IN (SELECT id FROM doomed)`, args...)
	return tag.RowsAffected(), err
}

func (x queries) CheckpointTrees(ctx context.Context, repo int64) ([]encoding.ID, error) {
	return idList(x.q.Query(ctx, `SELECT DISTINCT t FROM checkpoint, unnest(tree_ids) AS t WHERE repository_id = $1 AND NOT trees_pinned`, repo))
}

func (x queries) CheckpointsToPin(ctx context.Context, repo int64, before time.Time, limit int) ([]backend.CheckpointTreeIDs, error) {
	rows, err := x.q.Query(ctx, `SELECT id, tree_ids FROM checkpoint
		WHERE repository_id = $1 AND NOT trees_pinned AND created_at < $2 ORDER BY created_at, id LIMIT $3`, repo, before, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (backend.CheckpointTreeIDs, error) {
		var c backend.CheckpointTreeIDs
		var trees [][]byte
		if err := r.Scan(&c.ID, &trees); err != nil {
			return c, err
		}
		for _, b := range trees {
			id, err := toID(b)
			if err != nil {
				return c, err
			}
			c.Trees = append(c.Trees, id)
		}
		return c, nil
	})
}

func (x queries) SetCheckpointsPinned(ctx context.Context, repo int64, ids []int64) error {
	_, err := x.q.Exec(ctx, `UPDATE checkpoint SET trees_pinned = true WHERE repository_id = $1 AND id = ANY($2)`, repo, ids)
	return err
}
