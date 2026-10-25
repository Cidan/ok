package spannerstore

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Dictionaries ───

func (x queries) NetworkDictionary(ctx context.Context, net backend.Network, id int64) ([]byte, int64, bool, error) {
	var body []byte
	var trainer int64
	ok, err := x.one(ctx, `SELECT body, IFNULL(trainer_id, 0) FROM dictionary WHERE network_id = @net AND id = @id AND BYTE_LENGTH(body) > 0`,
		map[string]any{"net": int64(net), "id": id}, &body, &trainer)
	return body, trainer, ok, err
}

func (x queries) NetworkTrainer(ctx context.Context, net backend.Network) (int64, error) {
	var trainer int64
	ok, err := x.one(ctx, `SELECT IFNULL(trainer_id, 0) FROM network WHERE network_id = @net`, map[string]any{"net": int64(net)}, &trainer)
	if err == nil && !ok {
		err = fmt.Errorf("network %d: %w", net, errNoRow)
	}
	return trainer, err
}

func (x queries) DictionaryGrants(ctx context.Context, repo int64) (map[int64]int64, error) {
	out := map[int64]int64{}
	err := x.rows(ctx, `SELECT trainer_id, up_to FROM dictionary_grant WHERE repository_id = @repo`, map[string]any{"repo": repo},
		func() { clear(out) }, func(row *spanner.Row) error {
			var trainer, upTo int64
			if err := row.Columns(&trainer, &upTo); err != nil {
				return err
			}
			out[trainer] = upTo
			return nil
		})
	return out, err
}

func (x queries) SetDictionaryGrants(ctx context.Context, repo int64, grants map[int64]int64) error {
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `DELETE FROM dictionary_grant WHERE repository_id = @repo`, map[string]any{"repo": repo}); err != nil {
			return err
		}
		for trainer, upTo := range grants {
			if _, err := spannerdata.Update(ctx, tx, `INSERT INTO dictionary_grant (repository_id, trainer_id, up_to) VALUES (@repo, @trainer, @upTo)`,
				map[string]any{"repo": repo, "trainer": trainer, "upTo": upTo}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (x queries) NewestDictionaryOf(ctx context.Context, trainer int64) (int64, bool, error) {
	var id int64
	ok, err := x.one(ctx, `SELECT id FROM dictionary WHERE trainer_id = @trainer AND BYTE_LENGTH(body) > 0 ORDER BY id DESC LIMIT 1`,
		map[string]any{"trainer": trainer}, &id)
	return id, ok, err
}

func (x queries) NewestDictionary(ctx context.Context, net backend.Network) (int64, []byte, time.Time, bool, error) {
	var id int64
	var body []byte
	var at time.Time
	ok, err := x.one(ctx, `SELECT id, body, created_at FROM dictionary
		WHERE network_id = @net AND BYTE_LENGTH(body) > 0 ORDER BY id DESC LIMIT 1`, map[string]any{"net": int64(net)}, &id, &body, &at)
	return id, body, at, ok, err
}

// InsertDictionary records a dictionary under its network's dictionaries' counter's next value: a
// frame names it by its ID, which must stay small, and which is its network's alone.
func (x queries) InsertDictionary(ctx context.Context, net backend.Network, trainer int64, samples int) (int64, error) {
	id, err := x.next(ctx, dictionaryCounter, int64(net), 1)
	if err != nil {
		return 0, err
	}
	_, err = x.exec(ctx, `INSERT INTO dictionary (id, network_id, trainer_id, body, samples, created_at)
		VALUES (@id, @net, @trainer, b'', @samples, `+now+`)`,
		map[string]any{"id": id, "net": int64(net), "trainer": trainer, "samples": int64(samples)})
	return id, err
}

func (x queries) SetDictionaryBody(ctx context.Context, net backend.Network, id int64, body []byte) error {
	_, err := x.exec(ctx, `UPDATE dictionary SET body = @body WHERE network_id = @net AND id = @id`,
		map[string]any{"net": int64(net), "id": id, "body": body})
	return err
}

func (x queries) DeleteDictionary(ctx context.Context, net backend.Network, id int64) error {
	_, err := x.exec(ctx, `DELETE FROM dictionary WHERE network_id = @net AND id = @id`, map[string]any{"net": int64(net), "id": id})
	return err
}

// ─── Garbage collection ───

const dueForGC = `NOT EXISTS (SELECT 1 FROM gc_pass g WHERE g.repository_id = r.repository_id AND g.started_at >= @scheduled AND g.error IS NULL)
	AND NOT EXISTS (SELECT 1 FROM gc_pass g WHERE g.repository_id = r.repository_id AND g.error IS NOT NULL AND g.started_at > @retry)`

func (x queries) DueForGC(ctx context.Context, scheduled, retry time.Time) ([]int64, error) {
	return x.int64s(ctx, `SELECT r.repository_id FROM repository r WHERE `+dueForGC+` ORDER BY r.repository_id`,
		map[string]any{"scheduled": scheduled, "retry": retry})
}

func (x queries) RepositoryDueForGC(ctx context.Context, repo int64, scheduled, retry time.Time) (bool, error) {
	var due bool
	_, err := x.one(ctx, `SELECT EXISTS (SELECT 1 FROM repository r WHERE r.repository_id = @repo AND `+dueForGC+`)`,
		map[string]any{"repo": repo, "scheduled": scheduled, "retry": retry}, &due)
	return due, err
}

// ─── Keyed work ───

func (x queries) MarkWork(ctx context.Context, kind string, key int64) error {
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.MarkWork(ctx, tx, kind, key) })
}

func (x queries) MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.MarkWorkAt(ctx, tx, kind, key, at) })
}

func (x queries) TakeWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	var ok bool
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		ok, err = spannerdata.TakeWork(ctx, tx, kind, key, holder, ttl)
		return err
	})
	return ok, err
}

func (x queries) RenewWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error) {
	var ok bool
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		ok, err = spannerdata.RenewWork(ctx, tx, kind, key, holder, ttl)
		return err
	})
	return ok, err
}

func (x queries) ReleaseWork(ctx context.Context, kind string, key int64, holder string) (bool, error) {
	var again bool
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		again, err = spannerdata.ReleaseWork(ctx, tx, kind, key, holder)
		return err
	})
	return again, err
}

func (x queries) LetWork(ctx context.Context, kind string, key int64, holder string) error {
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return spannerdata.LetWork(ctx, tx, kind, key, holder)
	})
}

func (x queries) DueWork(ctx context.Context, kind string, limit int) ([]int64, error) {
	var out []int64
	err := x.read(func(r spannerdata.Reader) error {
		var err error
		out, err = spannerdata.DueWork(ctx, r, kind, limit)
		return err
	})
	return out, err
}

func (x queries) InsertGCPass(ctx context.Context, p backend.GCPass) error {
	errText := spanner.NullString{}
	if p.Error != nil {
		errText = spanner.NullString{StringVal: *p.Error, Valid: true}
	}
	_, err := x.exec(ctx, `INSERT OR IGNORE INTO gc_pass (repository_id, started_at, finished_at, operations_deleted, views_deleted,
			marked, commits_swept, objects_swept, files_swept, chunks_swept, chunk_bytes_swept, packs_written, packs_retired, bytes_read,
			bytes_reclaimed, packs_deleted, orphan_packs, error, checkpoints_deleted, op_logs_deleted, workspaces_deleted)
		VALUES (@repo, @started, @finished, @opsDeleted, @viewsDeleted, @marked, @commitsSwept, @objectsSwept, @filesSwept, @chunksSwept,
			@chunkBytesSwept, @packsWritten, @packsRetired, @bytesRead, @bytesReclaimed, @packsDeleted, @orphanPacks, @error,
			@checkpointsDeleted, @opLogsDeleted, @workspacesDeleted)`,
		map[string]any{"repo": p.Repository, "started": micros(p.Started), "finished": micros(p.Finished), "opsDeleted": p.OperationsDeleted,
			"viewsDeleted": p.ViewsDeleted, "marked": int64(p.Marked), "commitsSwept": p.CommitsSwept, "objectsSwept": p.ObjectsSwept,
			"filesSwept": p.FilesSwept, "chunksSwept": p.ChunksSwept, "chunkBytesSwept": p.ChunkBytesSwept,
			"packsWritten": int64(p.PacksWritten), "packsRetired": int64(p.PacksRetired), "bytesRead": p.BytesRead,
			"bytesReclaimed": p.BytesReclaimed, "packsDeleted": int64(p.PacksDeleted), "orphanPacks": int64(p.OrphanPacks), "error": errText,
			"checkpointsDeleted": p.CheckpointsDeleted, "opLogsDeleted": p.OpLogsDeleted, "workspacesDeleted": p.WorkspacesDeleted})
	return err
}

// ─── Workspaces and checkpoints ───

func (x queries) InsertWorkspace(ctx context.Context, repo int64, principal, name string) (bool, error) {
	n, err := x.exec(ctx, `INSERT OR IGNORE INTO workspace (repository_id, principal, name, epoch, created_at)
		VALUES (@repo, @principal, @name, 0, `+now+`)`, map[string]any{"repo": repo, "principal": principal, "name": name})
	return n > 0, err
}

func workspaceParams(repo int64, principal, name string) map[string]any {
	return map[string]any{"repo": repo, "principal": principal, "name": name}
}

const workspaceKey = `repository_id = @repo AND principal = @principal AND name = @name`

func (x queries) LockWorkspace(ctx context.Context, repo int64, principal, name string) (backend.WorkspaceLease, bool, error) {
	var l backend.WorkspaceLease
	var holder, takeover spanner.NullString
	var expires spanner.NullTime
	ok, err := x.lockOne(ctx, `SELECT holder, epoch, expires_at, takeover_by FROM workspace WHERE `+workspaceKey+` FOR UPDATE`,
		workspaceParams(repo, principal, name), &holder, &l.Epoch, &expires, &takeover)
	if err != nil || !ok {
		return l, false, err
	}
	l.Holder, l.TakeoverBy, l.ExpiresAt = holder.StringVal, takeover.StringVal, expires.Time
	return l, true, nil
}

func (x queries) SetTakeover(ctx context.Context, repo int64, principal, name, machine string) error {
	p := workspaceParams(repo, principal, name)
	p["machine"] = machine
	_, err := x.exec(ctx, `UPDATE workspace SET takeover_by = @machine WHERE `+workspaceKey, p)
	return err
}

func (x queries) GrantLease(ctx context.Context, repo int64, principal, name, machine string, epoch int64, expires time.Time) error {
	p := workspaceParams(repo, principal, name)
	p["machine"], p["epoch"], p["expires"] = machine, epoch, micros(expires)
	_, err := x.exec(ctx, `UPDATE workspace SET holder = @machine, epoch = @epoch, expires_at = @expires,
			takeover_by = IF(takeover_by = @machine, NULL, takeover_by)
		WHERE `+workspaceKey, p)
	return err
}

func (x queries) ExtendLease(ctx context.Context, repo int64, principal, name string, expires time.Time) error {
	p := workspaceParams(repo, principal, name)
	p["expires"] = micros(expires)
	_, err := x.exec(ctx, `UPDATE workspace SET expires_at = @expires WHERE `+workspaceKey, p)
	return err
}

func (x queries) ReleaseLease(ctx context.Context, repo int64, principal, name, machine string, epoch int64) error {
	p := workspaceParams(repo, principal, name)
	p["machine"], p["epoch"] = machine, epoch
	_, err := x.exec(ctx, `UPDATE workspace SET holder = NULL, expires_at = NULL WHERE `+workspaceKey+` AND holder = @machine AND epoch = @epoch`, p)
	return err
}

func (x queries) CheckpointOf(ctx context.Context, id, repo int64, principal, workspace string) (bool, error) {
	var got int64
	return x.one(ctx, `SELECT id FROM checkpoint WHERE repository_id = @repo AND id = @id AND principal = @principal AND workspace = @workspace`,
		map[string]any{"id": id, "repo": repo, "principal": principal, "workspace": workspace}, &got)
}

func (x queries) InsertCheckpoint(ctx context.Context, principal string, c backend.Checkpoint) (int64, time.Time, error) {
	id := data.NewID()
	var at time.Time
	err := x.returning(ctx, `INSERT INTO checkpoint (repository_id, id, principal, workspace, machine, parent_id, tree_ids, base_commit_id, fork,
			created_at)
		VALUES (@repo, @id, @principal, @workspace, @machine, @parent, @trees, @base, @fork, `+now+`) THEN RETURN created_at`,
		map[string]any{"repo": c.Repo, "id": id, "principal": principal, "workspace": c.Workspace, "machine": c.Machine,
			"parent": nullable(c.ParentID), "trees": ids(c.Trees), "base": c.BaseCommit[:], "fork": c.Fork},
		func() {}, func(row *spanner.Row) error { return row.Columns(&at) }, nil)
	return id, at, err
}

const checkpointColumns = `id, repository_id, workspace, machine, IFNULL(parent_id, 0), tree_ids, base_commit_id, fork, created_at`

func (x queries) checkpoints(ctx context.Context, r spannerdata.Reader, sql string, params map[string]any) ([]backend.Checkpoint, error) {
	var out []backend.Checkpoint
	err := spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
		var c backend.Checkpoint
		var trees [][]byte
		var base []byte
		if err := row.Columns(&c.ID, &c.Repo, &c.Workspace, &c.Machine, &c.ParentID, &trees, &base, &c.Fork, &c.CreatedAt); err != nil {
			return err
		}
		for _, t := range trees {
			id, err := toID(t)
			if err != nil {
				return err
			}
			c.Trees = append(c.Trees, id)
		}
		var err error
		if c.BaseCommit, err = toID(base); err != nil {
			return err
		}
		out = append(out, c)
		return nil
	})
	return out, err
}

func (x queries) oneCheckpoint(ctx context.Context, sql string, params map[string]any) (backend.Checkpoint, bool, error) {
	var cs []backend.Checkpoint
	err := x.read(func(r spannerdata.Reader) error {
		var err error
		cs, err = x.checkpoints(ctx, r, sql, params)
		return err
	})
	if err != nil || len(cs) == 0 {
		return backend.Checkpoint{}, false, err
	}
	return cs[0], true, nil
}

func (x queries) LatestCheckpoint(ctx context.Context, repo int64, principal, workspace string) (backend.Checkpoint, bool, error) {
	return x.oneCheckpoint(ctx, `SELECT `+checkpointColumns+` FROM checkpoint@{FORCE_INDEX=checkpoint_newest}
		WHERE repository_id = @repo AND principal = @principal AND workspace = @workspace AND NOT fork ORDER BY created_at DESC, id DESC LIMIT 1`,
		map[string]any{"repo": repo, "principal": principal, "workspace": workspace})
}

func (x queries) PrincipalCheckpoint(ctx context.Context, repo int64, principal string, id int64) (backend.Checkpoint, bool, error) {
	return x.oneCheckpoint(ctx, `SELECT `+checkpointColumns+` FROM checkpoint WHERE repository_id = @repo AND id = @id AND principal = @principal`,
		map[string]any{"repo": repo, "principal": principal, "id": id})
}

// Checkpoints pages a workspace's checkpoints newest first, below the checkpoint before by time,
// then ID; none when before names none.
func (x queries) Checkpoints(ctx context.Context, repo int64, principal, workspace string, before int64, limit int) ([]backend.Checkpoint, error) {
	var out []backend.Checkpoint
	err := x.read(func(r spannerdata.Reader) error {
		out = nil
		p := map[string]any{"repo": repo, "principal": principal, "workspace": workspace, "before": before, "limit": int64(limit)}
		page := ``
		if before != 0 {
			var at time.Time
			ok, err := spannerdata.First(ctx, r, `SELECT created_at FROM checkpoint WHERE repository_id = @repo AND id = @before`, p, &at)
			if err != nil || !ok {
				return err
			}
			p["at"] = at
			page = ` AND (created_at < @at OR (created_at = @at AND id < @before))`
		}
		var err error
		out, err = x.checkpoints(ctx, r, `SELECT `+checkpointColumns+` FROM checkpoint@{FORCE_INDEX=checkpoint_newest}
			WHERE repository_id = @repo AND principal = @principal AND workspace = @workspace`+page+`
			ORDER BY created_at DESC, id DESC LIMIT @limit`, p)
		return err
	})
	return out, err
}

func (x queries) Workspaces(ctx context.Context, repo int64, principal string) ([]backend.Workspace, error) {
	var out []backend.Workspace
	err := x.rows(ctx, `SELECT w.name, IFNULL(w.holder, ''), w.epoch, w.expires_at, IFNULL(w.takeover_by, ''),
			EXISTS (SELECT 1 FROM op_log l WHERE l.repository_id = w.repository_id AND l.principal = w.principal AND l.workspace = w.name)
		FROM workspace w WHERE w.repository_id = @repo AND w.principal = @principal ORDER BY w.name`,
		map[string]any{"repo": repo, "principal": principal}, func() { out = nil }, func(row *spanner.Row) error {
			var w backend.Workspace
			var expires spanner.NullTime
			if err := row.Columns(&w.Name, &w.Holder, &w.Epoch, &expires, &w.TakeoverBy, &w.OwnLog); err != nil {
				return err
			}
			w.ExpiresAt = expires.Time
			out = append(out, w)
			return nil
		})
	return out, err
}

func (x queries) WorkspaceNames(ctx context.Context, repo int64, after backend.WorkspaceKey, limit int) ([]backend.WorkspaceKey, error) {
	var out []backend.WorkspaceKey
	err := x.rows(ctx, `SELECT principal, name FROM workspace WHERE repository_id = @repo
			AND (principal > @principal OR (principal = @principal AND name > @name)) ORDER BY principal, name LIMIT @limit`,
		map[string]any{"repo": repo, "principal": after.Principal, "name": after.Name, "limit": int64(limit)}, func() { out = nil }, func(row *spanner.Row) error {
			var k backend.WorkspaceKey
			if err := row.Columns(&k.Principal, &k.Name); err != nil {
				return err
			}
			out = append(out, k)
			return nil
		})
	return out, err
}

// thinned is a checkpoint as thinning ranks it.
type thinned struct {
	id                   int64
	principal, workspace string
	fork                 bool
	at                   time.Time
}

// ThinCheckpoints ranks a repository's checkpoints as PostgreSQL's window functions do — each
// workspace's of an hour, and of a day (UTC), newest first — and deletes, or counts, those it
// keeps no longer: GoogleSQL has no window functions. A checkpoint whose parent goes loses it, as
// PostgreSQL's foreign key sets it NULL.
func (x queries) ThinCheckpoints(ctx context.Context, repo int64, every, hourly, daily time.Time, dry bool) (int64, error) {
	var all []thinned
	if err := x.rows(ctx, `SELECT id, principal, workspace, fork, created_at FROM checkpoint WHERE repository_id = @repo`,
		map[string]any{"repo": repo}, func() { all = nil }, func(row *spanner.Row) error {
			var c thinned
			if err := row.Columns(&c.id, &c.principal, &c.workspace, &c.fork, &c.at); err != nil {
				return err
			}
			all = append(all, c)
			return nil
		}); err != nil {
		return 0, err
	}
	doomed := thin(all, every, hourly, daily)
	if dry {
		return int64(len(doomed)), nil
	}
	return int64(len(doomed)), x.deleteCheckpoints(ctx, repo, doomed)
}

// deleteCheckpoints deletes a repository's checkpoints among ids, first setting NULL the parent of
// each checkpoint whose parent goes, as PostgreSQL's foreign key does: in the operation's
// transaction, if it is in one; otherwise in transactions of its own of up to rowsPerTransaction
// of them. Nothing makes a checkpoint thinning or an orphan's deletion chose kept, so one deletion
// stopped short is no harm.
func (x queries) deleteCheckpoints(ctx context.Context, repo int64, doomed []int64) error {
	per := len(doomed)
	if x.tx == nil {
		per = rowsPerTransaction
	}
	for lo := 0; lo < len(doomed); lo += per {
		chunk := doomed[lo:min(lo+per, len(doomed))]
		if err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
			p := map[string]any{"repo": repo, "ids": chunk}
			if _, err := updateIn(ctx, tx, `UPDATE checkpoint SET parent_id = NULL WHERE repository_id = @repo AND parent_id IN UNNEST(@ids)`, p,
				[]string{"ids"}, nil); err != nil {
				return err
			}
			_, err := updateIn(ctx, tx, `DELETE FROM checkpoint WHERE repository_id = @repo AND id IN UNNEST(@ids)`, p, []string{"ids"}, nil)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// thin is the checkpoints thinning deletes: of those made before every, all but each workspace's
// latest that is not a fork — before daily, every one; before hourly, all but the newest of each
// day; otherwise all but the newest of each hour.
func thin(all []thinned, every, hourly, daily time.Time) []int64 {
	newest := func(a, b thinned) int {
		if c := b.at.Compare(a.at); c != 0 {
			return c
		}
		return cmp.Compare(b.id, a.id)
	}
	slices.SortFunc(all, newest)
	type ws struct{ principal, workspace string }
	latest := map[ws]int64{}
	for _, c := range all {
		if k := (ws{c.principal, c.workspace}); !c.fork {
			if _, ok := latest[k]; !ok {
				latest[k] = c.id
			}
		}
	}
	type bucket struct {
		ws
		start time.Time
	}
	inHour, inDay := map[bucket]int{}, map[bucket]int{}
	var doomed []int64
	for _, c := range all {
		if !c.at.Before(every) {
			continue
		}
		k := ws{c.principal, c.workspace}
		at := c.at.UTC()
		hour := bucket{k, at.Truncate(time.Hour)}
		day := bucket{k, time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, time.UTC)}
		inHour[hour]++
		inDay[day]++
		if latest[k] == c.id {
			continue
		}
		if c.at.Before(daily) || (c.at.Before(hourly) && inDay[day] > 1) || (!c.at.Before(hourly) && inHour[hour] > 1) {
			doomed = append(doomed, c.id)
		}
	}
	return doomed
}

func (x queries) CheckpointTrees(ctx context.Context, repo int64) ([]encoding.ID, error) {
	return x.idList(ctx, `SELECT DISTINCT t FROM checkpoint AS c, UNNEST(c.tree_ids) AS t WHERE c.repository_id = @repo AND NOT c.trees_pinned`,
		map[string]any{"repo": repo})
}

func (x queries) CheckpointsToPin(ctx context.Context, repo int64, before time.Time, limit int) ([]backend.CheckpointTreeIDs, error) {
	var out []backend.CheckpointTreeIDs
	err := x.rows(ctx, `SELECT id, tree_ids FROM checkpoint@{FORCE_INDEX=checkpoint_unpinned}
		WHERE repository_id = @repo AND NOT trees_pinned AND created_at < @before ORDER BY created_at, id LIMIT @limit`,
		map[string]any{"repo": repo, "before": before, "limit": int64(limit)}, func() { out = nil }, func(row *spanner.Row) error {
			var c backend.CheckpointTreeIDs
			var trees [][]byte
			if err := row.Columns(&c.ID, &trees); err != nil {
				return err
			}
			for _, b := range trees {
				id, err := toID(b)
				if err != nil {
					return err
				}
				c.Trees = append(c.Trees, id)
			}
			out = append(out, c)
			return nil
		})
	return out, err
}

func (x queries) SetCheckpointsPinned(ctx context.Context, repo int64, ids []int64) error {
	_, err := x.execIn(ctx, `UPDATE checkpoint SET trees_pinned = TRUE WHERE repository_id = @repo AND id IN UNNEST(@ids)`,
		map[string]any{"repo": repo, "ids": ids}, []string{"ids"}, nil)
	return err
}
