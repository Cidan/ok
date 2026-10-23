package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
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

// Checkpoint is what a workspace's working copy held at a moment, on a machine; ParentID is 0
// for none.
type Checkpoint = backend.Checkpoint

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
	err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		if err := s.repositoryExists(ctx, tx, repo); err != nil {
			return err
		}
		made, err := tx.InsertWorkspace(ctx, repo, principal, name)
		if err != nil {
			return err
		}
		if !made {
			return fmt.Errorf("%w: you have a workspace %q in this repository already", ErrAlreadyExists, name)
		}
		_, made, err = ensureOpLog(ctx, tx, repo, principal, name)
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

type workspaceRow struct{ backend.WorkspaceLease }

func (w workspaceRow) heldBy(machine string) bool { return w.Holder != "" && w.Holder == machine }

func (w workspaceRow) live(now time.Time) bool {
	return w.Holder != "" && !w.ExpiresAt.IsZero() && w.ExpiresAt.After(now)
}

// lockWorkspace reads a workspace's row for update; found=false when it has none.
func lockWorkspace(ctx context.Context, tx *txn, principal string, repo int64, name string) (workspaceRow, bool, error) {
	l, ok, err := tx.LockWorkspace(ctx, repo, principal, name)
	return workspaceRow{l}, ok, err
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
	err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
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
			out.Holder, out.HolderExpires = w.Holder, w.ExpiresAt
			if takeover {
				if err := tx.SetTakeover(ctx, repo, principal, name, machine); err != nil {
					return err
				}
			}
		} else {
			out.Granted = true
			out.Lease = Lease{Repo: repo, Workspace: name, Machine: machine, Epoch: w.Epoch + 1, ExpiresAt: now.Add(LeaseTTL)}
			// A request to take over stands unless it was this machine's own: a holder
			// re-attaching still hands over to the machine that asked.
			if err := tx.GrantLease(ctx, repo, principal, name, machine, out.Lease.Epoch, out.Lease.ExpiresAt); err != nil {
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
	err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		w, found, err := lockWorkspace(ctx, tx, principal, l.Repo, l.Workspace)
		if err != nil {
			return err
		}
		if !found || !w.heldBy(l.Machine) || w.Epoch != l.Epoch {
			return lostLease(l)
		}
		out = l
		out.ExpiresAt = now.Add(LeaseTTL)
		takeoverBy = w.TakeoverBy
		return tx.ExtendLease(ctx, l.Repo, principal, l.Workspace, out.ExpiresAt)
	})
	return out, takeoverBy, err
}

// Release gives a lease up. Releasing a lease that is no longer the caller's does nothing.
func (s *Store) Release(ctx context.Context, principal string, l Lease) error {
	if err := checkWorkspace(l.Workspace, l.Machine); err != nil {
		return err
	}
	return s.inTx(ctx, func(ctx context.Context, tx *txn) error {
		return tx.ReleaseLease(ctx, l.Repo, principal, l.Workspace, l.Machine, l.Epoch)
	})
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
	// A secret's value under //secret is refused; a tree not yet published is refused below.
	if err := s.checkSecretValues(ctx, l.Repo, s.pendingTrees(ctx, l.Repo), s.storedFiles(ctx, l.Repo), trees); err != nil && !errors.Is(err, ErrNotFound) {
		return Checkpoint{}, err
	}
	var out Checkpoint
	err := s.inTx(ctx, func(ctx context.Context, tx *txn) error {
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
			ok, err := tx.CheckpointOf(ctx, parent, l.Repo, principal, l.Workspace)
			if err != nil {
				return err
			}
			if !ok {
				return invalid("checkpoint %d is not one of workspace %q's", parent, l.Workspace)
			}
		}
		current := w.heldBy(l.Machine) && w.Epoch == l.Epoch
		out = Checkpoint{Repo: l.Repo, Workspace: l.Workspace, Machine: l.Machine, ParentID: parent, Trees: trees,
			BaseCommit: base, Fork: !current || parent != latestID}
		out.ID, out.CreatedAt, err = tx.InsertCheckpoint(ctx, principal, out)
		return err
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

// latestCheckpoint is a workspace's newest checkpoint that is not a fork, or nil.
func latestCheckpoint(ctx context.Context, q backend.Ops, principal string, repo int64, name string) (*Checkpoint, error) {
	c, ok, err := q.LatestCheckpoint(ctx, repo, principal, name)
	if err != nil || !ok {
		return nil, err
	}
	return &c, nil
}

// GetCheckpoint reads one of the principal's checkpoints.
func (s *Store) GetCheckpoint(ctx context.Context, principal string, repo, id int64) (*Checkpoint, error) {
	c, ok, err := s.ops(ctx).PrincipalCheckpoint(ctx, repo, principal, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, notFound("checkpoint %d", id)
	}
	return &c, nil
}

// ListCheckpoints lists a workspace's checkpoints, newest first, before a checkpoint (0: from
// the newest).
func (s *Store) ListCheckpoints(ctx context.Context, principal string, repo int64, name string, before int64, limit int) ([]Checkpoint, error) {
	return s.ops(ctx).Checkpoints(ctx, repo, principal, name, before, limit)
}

// ListWorkspaces lists the principal's workspaces of a repository, by name.
func (s *Store) ListWorkspaces(ctx context.Context, principal string, repo int64) ([]WorkspaceState, error) {
	rows, err := s.ops(ctx).Workspaces(ctx, repo, principal)
	if err != nil {
		return nil, err
	}
	out := make([]WorkspaceState, len(rows))
	for i, w := range rows {
		out[i] = WorkspaceState{Name: w.Name, Holder: w.Holder, Epoch: w.Epoch, ExpiresAt: w.ExpiresAt, TakeoverBy: w.TakeoverBy, OwnLog: w.OwnLog}
		if out[i].Latest, err = latestCheckpoint(ctx, s.ops(ctx), principal, repo, w.Name); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// thinCheckpoints deletes the checkpoints retention no longer keeps, as of now: past
// CheckpointsAll, all but the last of each hour; past CheckpointsHourly, all but the last of
// each day; past CheckpointsDaily, all. A workspace's latest is always kept.
func (s *Store) thinCheckpoints(ctx context.Context, repo int64, now time.Time, dryRun bool) (int64, error) {
	n, err := s.ops(ctx).ThinCheckpoints(ctx, repo, now.Add(-CheckpointsAll), now.Add(-CheckpointsHourly), now.Add(-CheckpointsDaily), dryRun)
	if err != nil {
		return 0, fmt.Errorf("thinning checkpoints: %w", err)
	}
	return n, nil
}

// checkpointTrees are the trees of a repository's checkpoints, roots of garbage collection's
// mark.
func (s *Store) checkpointTrees(ctx context.Context, repo int64) ([]encoding.ID, error) {
	trees, err := s.ops(ctx).CheckpointTrees(ctx, repo)
	if err != nil {
		return nil, err
	}
	out := make([]encoding.ID, 0, len(trees))
	for _, id := range trees {
		if id != encoding.EmptyTreeID {
			out = append(out, id)
		}
	}
	return out, nil
}
