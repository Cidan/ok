package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// Workspaces: each of a principal's workspaces of a repository, keyed by (repository, principal,
// workspace name) — its own operation log (docs/scm.md, "Decisions", 3, made by
// CreateWorkspace), and okfs's attachment lease and checkpoints (docs/okfs.md,
// "Checkpoints" and "Attaching a workspace on another machine"). Every call names the principal
// it acts for, and reaches only that principal's workspaces.

// LeaseTTL is how long a lease lasts after it was granted or last renewed.
const LeaseTTL = 30 * time.Second

// Retention of checkpoints (docs/okfs.md, "Checkpoints"): every one for CheckpointsAll;
// then the last of each hour until CheckpointsHourly; then the last of each day until
// CheckpointsDaily; none after. A workspace's latest is always kept.
const (
	CheckpointsAll    = 7 * 24 * time.Hour
	CheckpointsHourly = 30 * 24 * time.Hour
	CheckpointsDaily  = 365 * 24 * time.Hour
)

// maxWorkspaceName bounds a workspace's name (jj's own names are short: host-directory).
const maxWorkspaceName = 255

// Lease is a machine's hold on a workspace, at the epoch it was granted.
type Lease struct {
	Repo      int64
	Workspace string
	Machine   string
	Epoch     int64
	ExpiresAt time.Time
}

// Checkpoint is what a workspace's working copy held at a moment, on a machine.
type Checkpoint struct {
	ID         int64
	Repo       int64
	Workspace  string
	Machine    string
	ParentID   int64 // 0: none
	Trees      []encoding.ID
	BaseCommit encoding.ID
	Fork       bool
	CreatedAt  time.Time
}

// Attachment is Attach's answer: the lease, when granted; otherwise who holds it.
type Attachment struct {
	Granted       bool
	Lease         Lease
	Holder        string
	HolderExpires time.Time
	Latest        *Checkpoint
}

// WorkspaceState is one of a principal's workspaces.
type WorkspaceState struct {
	Name       string
	Holder     string // "": not attached
	Epoch      int64
	ExpiresAt  time.Time
	TakeoverBy string
	Latest     *Checkpoint
	// OwnLog: it has an operation log of its own. One without predates that, and shares the
	// principal's legacy log.
	OwnLog bool
}

// CreateWorkspace makes a workspace of the caller's in repo, with an operation log of its own
// holding only the root operation. ErrAlreadyExists when the caller has a workspace of that name
// already — with a log of its own, or one okfs recorded before workspaces had their own.
func (s *Store) CreateWorkspace(ctx context.Context, principal string, repo int64, name string) (WorkspaceState, error) {
	if name == "" || len(name) > maxWorkspaceName {
		return WorkspaceState{}, invalid("a workspace name is 1 to %d bytes", maxWorkspaceName)
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := s.repositoryExists(ctx, tx, repo); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO workspace (repository_id, principal, name) VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`, repo, principal, name)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: you have a workspace %q in this repository already", ErrAlreadyExists, name)
		}
		_, made, err := ensureOpLog(ctx, tx, repo, principal, name)
		if err == nil && !made {
			err = fmt.Errorf("%w: workspace %q has an operation log already", ErrAlreadyExists, name)
		}
		return err
	})
	if err != nil {
		return WorkspaceState{}, err
	}
	return WorkspaceState{Name: name, OwnLog: true}, nil
}

func checkWorkspace(name, machine string) error {
	if name == "" || len(name) > maxWorkspaceName {
		return invalid("a workspace name is 1 to %d bytes", maxWorkspaceName)
	}
	if machine == "" || len(machine) > maxWorkspaceName {
		return invalid("a machine ID is 1 to %d bytes", maxWorkspaceName)
	}
	return nil
}

type workspaceRow struct {
	holder     *string
	epoch      int64
	expiresAt  *time.Time
	takeoverBy *string
}

func (w workspaceRow) heldBy(machine string) bool { return w.holder != nil && *w.holder == machine }

func (w workspaceRow) live(now time.Time) bool {
	return w.holder != nil && w.expiresAt != nil && w.expiresAt.After(now)
}

// lockWorkspace reads a workspace's row for update; found=false when it has none.
func lockWorkspace(ctx context.Context, tx pgx.Tx, principal string, repo int64, name string) (workspaceRow, bool, error) {
	var w workspaceRow
	err := tx.QueryRow(ctx, `SELECT holder, epoch, expires_at, takeover_by FROM workspace
		WHERE repository_id = $1 AND principal = $2 AND name = $3 FOR UPDATE`, repo, principal, name).
		Scan(&w.holder, &w.epoch, &w.expiresAt, &w.takeoverBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return w, false, nil
	}
	return w, err == nil, err
}

// Attach takes a workspace's lease for a machine: granted when no live lease is held, or the
// machine holds it already (re-attaching after a restart); a new grant bumps the epoch. Held
// live by another machine, it is not granted; with takeover, the holder is asked to hand over
// (it learns at its next Renew), and Attach is called again until the holder releases or its
// lease expires. The workspace must exist (CreateWorkspace): NotFound otherwise.
func (s *Store) Attach(ctx context.Context, principal string, repo int64, name, machine string, takeover bool, now time.Time) (Attachment, error) {
	if err := checkWorkspace(name, machine); err != nil {
		return Attachment{}, err
	}
	now = now.UTC()
	var out Attachment
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		out = Attachment{}
		if err := s.repositoryExists(ctx, tx, repo); err != nil {
			return err
		}
		w, found, err := lockWorkspace(ctx, tx, principal, repo, name)
		if err != nil {
			return err
		}
		if !found {
			return noWorkspace(name)
		}
		if w.live(now) && !w.heldBy(machine) {
			out.Holder, out.HolderExpires = *w.holder, *w.expiresAt
			if takeover {
				if _, err := tx.Exec(ctx, `UPDATE workspace SET takeover_by = $4
					WHERE repository_id = $1 AND principal = $2 AND name = $3`, repo, principal, name, machine); err != nil {
					return err
				}
			}
		} else {
			out.Granted = true
			out.Lease = Lease{Repo: repo, Workspace: name, Machine: machine, Epoch: w.epoch + 1, ExpiresAt: now.Add(LeaseTTL)}
			// A request to take over stands unless it was this machine's own: a holder
			// re-attaching still hands over to the machine that asked.
			if _, err := tx.Exec(ctx, `UPDATE workspace SET holder = $4, epoch = $5, expires_at = $6,
					takeover_by = CASE WHEN takeover_by = $4 THEN NULL ELSE takeover_by END
				WHERE repository_id = $1 AND principal = $2 AND name = $3`,
				repo, principal, name, machine, out.Lease.Epoch, out.Lease.ExpiresAt); err != nil {
				return err
			}
		}
		latest, err := latestCheckpoint(ctx, tx, principal, repo, name)
		out.Latest = latest
		return err
	})
	return out, err
}

// lostLease refuses a call made with a lease that is no longer the caller's.
func lostLease(l Lease) error {
	return precondition("the lease on workspace %q is no longer machine %s's (epoch %d)", l.Workspace, l.Machine, l.Epoch)
}

// Renew extends a lease, and says which machine, if any, has asked to take over. A lease
// renews while no other grant has been made since it — even after it expired — so a machine
// that was briefly offline keeps a workspace no one else took.
func (s *Store) Renew(ctx context.Context, principal string, l Lease, now time.Time) (Lease, string, error) {
	if err := checkWorkspace(l.Workspace, l.Machine); err != nil {
		return Lease{}, "", err
	}
	now = now.UTC()
	var out Lease
	var takeoverBy string
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		w, found, err := lockWorkspace(ctx, tx, principal, l.Repo, l.Workspace)
		if err != nil {
			return err
		}
		if !found || !w.heldBy(l.Machine) || w.epoch != l.Epoch {
			return lostLease(l)
		}
		out = l
		out.ExpiresAt = now.Add(LeaseTTL)
		if w.takeoverBy != nil {
			takeoverBy = *w.takeoverBy
		}
		_, err = tx.Exec(ctx, `UPDATE workspace SET expires_at = $4 WHERE repository_id = $1 AND principal = $2 AND name = $3`,
			l.Repo, principal, l.Workspace, out.ExpiresAt)
		return err
	})
	return out, takeoverBy, err
}

// Release gives a lease up. Releasing a lease that is no longer the caller's does nothing.
func (s *Store) Release(ctx context.Context, principal string, l Lease) error {
	if err := checkWorkspace(l.Workspace, l.Machine); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `UPDATE workspace SET holder = NULL, expires_at = NULL
		WHERE repository_id = $1 AND principal = $2 AND name = $3 AND holder = $4 AND epoch = $5`,
		l.Repo, principal, l.Workspace, l.Machine, l.Epoch)
	return err
}

// RecordCheckpoint records a checkpoint under a lease. Its trees must be in the repository —
// published first — and are touched, so garbage collection keeps them from now on. It
// becomes the workspace's latest when the lease is current (no grant since it) and parent is
// the latest (0 for a workspace with none); otherwise it is recorded as a fork.
func (s *Store) RecordCheckpoint(ctx context.Context, principal string, l Lease, parent int64, trees []encoding.ID, base encoding.ID) (Checkpoint, error) {
	if err := checkWorkspace(l.Workspace, l.Machine); err != nil {
		return Checkpoint{}, err
	}
	if len(trees) == 0 || len(trees)%2 == 0 {
		return Checkpoint{}, invalid("a checkpoint's tree is a merge's terms: an odd number of tree IDs, not %d", len(trees))
	}
	var out Checkpoint
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		w, found, err := lockWorkspace(ctx, tx, principal, l.Repo, l.Workspace)
		if err != nil {
			return err
		}
		if !found {
			return notFound("workspace %q", l.Workspace)
		}
		var roots []ref
		for _, t := range trees {
			if t != encoding.EmptyTreeID {
				roots = append(roots, ref{table: tObject, kind: encoding.KindTree, id: t})
			}
		}
		missing, err := s.touch(ctx, tx, l.Repo, roots)
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			return &Rejection{Missing: refObjects(missing), Message: "publish the checkpoint's trees first"}
		}
		latest, err := latestCheckpoint(ctx, tx, principal, l.Repo, l.Workspace)
		if err != nil {
			return err
		}
		var latestID int64
		if latest != nil {
			latestID = latest.ID
		}
		if parent != 0 {
			var ok bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM checkpoint
				WHERE id = $1 AND repository_id = $2 AND principal = $3 AND workspace = $4)`,
				parent, l.Repo, principal, l.Workspace).Scan(&ok); err != nil {
				return err
			}
			if !ok {
				return invalid("checkpoint %d is not one of workspace %q's", parent, l.Workspace)
			}
		}
		current := w.heldBy(l.Machine) && w.epoch == l.Epoch
		out = Checkpoint{Repo: l.Repo, Workspace: l.Workspace, Machine: l.Machine, ParentID: parent, Trees: trees,
			BaseCommit: base, Fork: !current || parent != latestID}
		var parentArg *int64
		if parent != 0 {
			parentArg = &parent
		}
		return tx.QueryRow(ctx, `INSERT INTO checkpoint (repository_id, principal, workspace, machine, parent_id, tree_ids, base_commit_id, fork)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id, created_at`,
			l.Repo, principal, l.Workspace, l.Machine, parentArg, ids(trees), base[:], out.Fork).Scan(&out.ID, &out.CreatedAt)
	})
	return out, err
}

// refObjects converts walk refs to the ObjectRefs a rejection names.
func refObjects(in []ref) []ObjectRef {
	out := make([]ObjectRef, len(in))
	for i, r := range in {
		out[i] = r.objectRef()
	}
	return out
}

const checkpointColumns = `id, repository_id, workspace, machine, coalesce(parent_id, 0), tree_ids, base_commit_id, fork, created_at`

func scanCheckpoint(row pgx.Row) (*Checkpoint, error) {
	var c Checkpoint
	var trees [][]byte
	var base []byte
	if err := row.Scan(&c.ID, &c.Repo, &c.Workspace, &c.Machine, &c.ParentID, &trees, &base, &c.Fork, &c.CreatedAt); err != nil {
		return nil, err
	}
	for _, t := range trees {
		id, err := toID(t)
		if err != nil {
			return nil, err
		}
		c.Trees = append(c.Trees, id)
	}
	b, err := toID(base)
	if err != nil {
		return nil, err
	}
	c.BaseCommit = b
	return &c, nil
}

// latestCheckpoint is a workspace's newest checkpoint that is not a fork, or nil.
func latestCheckpoint(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, principal string, repo int64, name string) (*Checkpoint, error) {
	c, err := scanCheckpoint(q.QueryRow(ctx, `SELECT `+checkpointColumns+` FROM checkpoint
		WHERE repository_id = $1 AND principal = $2 AND workspace = $3 AND NOT fork ORDER BY id DESC LIMIT 1`, repo, principal, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return c, err
}

// GetCheckpoint reads one of the principal's checkpoints.
func (s *Store) GetCheckpoint(ctx context.Context, principal string, repo, id int64) (*Checkpoint, error) {
	c, err := scanCheckpoint(s.pool.QueryRow(ctx, `SELECT `+checkpointColumns+` FROM checkpoint
		WHERE id = $1 AND repository_id = $2 AND principal = $3`, id, repo, principal))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("checkpoint %d", id)
	}
	return c, err
}

// ListCheckpoints lists a workspace's checkpoints, newest first, before a checkpoint (0: from
// the newest).
func (s *Store) ListCheckpoints(ctx context.Context, principal string, repo int64, name string, before int64, limit int) ([]Checkpoint, error) {
	if before == 0 {
		before = 1<<63 - 1
	}
	rows, err := s.pool.Query(ctx, `SELECT `+checkpointColumns+` FROM checkpoint
		WHERE repository_id = $1 AND principal = $2 AND workspace = $3 AND id < $4 ORDER BY id DESC LIMIT $5`,
		repo, principal, name, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Checkpoint
	for rows.Next() {
		c, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

// ListWorkspaces lists the principal's workspaces of a repository, by name.
func (s *Store) ListWorkspaces(ctx context.Context, principal string, repo int64) ([]WorkspaceState, error) {
	rows, err := s.pool.Query(ctx, `SELECT w.name, coalesce(w.holder, ''), w.epoch, w.expires_at, coalesce(w.takeover_by, ''),
			EXISTS (SELECT 1 FROM op_log l WHERE l.repository_id = w.repository_id AND l.principal = w.principal AND l.workspace = w.name)
		FROM workspace w WHERE w.repository_id = $1 AND w.principal = $2 ORDER BY w.name`, repo, principal)
	if err != nil {
		return nil, err
	}
	var out []WorkspaceState
	for rows.Next() {
		var w WorkspaceState
		var expires *time.Time
		if err := rows.Scan(&w.Name, &w.Holder, &w.Epoch, &expires, &w.TakeoverBy, &w.OwnLog); err != nil {
			rows.Close()
			return nil, err
		}
		if expires != nil {
			w.ExpiresAt = *expires
		}
		out = append(out, w)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].Latest, err = latestCheckpoint(ctx, s.pool, principal, repo, out[i].Name); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// thinCheckpoints deletes the checkpoints retention no longer keeps, as of now: past
// CheckpointsAll, all but the last of each hour; past CheckpointsHourly, all but the last of
// each day; past CheckpointsDaily, all. A workspace's latest is always kept.
func (s *Store) thinCheckpoints(ctx context.Context, repo int64, now time.Time, dryRun bool) (int64, error) {
	query := `WITH old AS (
			SELECT id, created_at,
				row_number() OVER (PARTITION BY principal, workspace, date_trunc('hour', created_at AT TIME ZONE 'UTC') ORDER BY id DESC) AS in_hour,
				row_number() OVER (PARTITION BY principal, workspace, date_trunc('day', created_at AT TIME ZONE 'UTC') ORDER BY id DESC) AS in_day
			FROM checkpoint WHERE repository_id = $1 AND created_at < $2
		), latest AS (
			SELECT DISTINCT ON (principal, workspace) id FROM checkpoint
			WHERE repository_id = $1 AND NOT fork ORDER BY principal, workspace, id DESC
		), doomed AS (
			SELECT id FROM old WHERE id NOT IN (SELECT id FROM latest) AND (
				created_at < $4
				OR (created_at < $3 AND in_day > 1)
				OR (created_at >= $3 AND in_hour > 1))
		)`
	args := []any{repo, now.Add(-CheckpointsAll), now.Add(-CheckpointsHourly), now.Add(-CheckpointsDaily)}
	var n int64
	if dryRun {
		err := s.pool.QueryRow(ctx, query+` SELECT count(*) FROM doomed`, args...).Scan(&n)
		return n, err
	}
	tag, err := s.pool.Exec(ctx, query+` DELETE FROM checkpoint WHERE id IN (SELECT id FROM doomed)`, args...)
	if err != nil {
		return 0, fmt.Errorf("thinning checkpoints: %w", err)
	}
	return tag.RowsAffected(), nil
}

// checkpointTrees are the trees of a repository's checkpoints, roots of garbage collection's
// mark.
func (s *Store) checkpointTrees(ctx context.Context, repo int64) ([]encoding.ID, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT t FROM checkpoint, unnest(tree_ids) AS t WHERE repository_id = $1`, repo)
	if err != nil {
		return nil, err
	}
	raw, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		return nil, err
	}
	out := make([]encoding.ID, 0, len(raw))
	for _, b := range raw {
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		if id != encoding.EmptyTreeID {
			out = append(out, id)
		}
	}
	return out, nil
}
