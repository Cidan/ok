package store

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/Cidan/ok/internal/wait"
)

// txOps are a transaction's operations as the store hands them on: the backend's, and the
// deployments they changed, whose commits it says changed before it commits (wait.CommitSubject),
// for ok control's watchers of the CLs that landed as them.
type txOps struct {
	Ops
	touched map[int64]bool
}

// txn runs fn in a read-write transaction (Backend.Txn), saying which commits' deployments it
// changed.
func (s *Store) txn(ctx context.Context, fn func(tx Ops) error) error {
	return s.b.Txn(ctx, func(tx Ops) error {
		t := txOps{Ops: tx, touched: map[int64]bool{}}
		if err := fn(t); err != nil {
			return err
		}
		return t.changed(ctx)
	})
}

// changed pends the steps saying what the deployments the transaction touched changed: each
// deployment, each one's commit, and the services, once each.
func (t txOps) changed(ctx context.Context) error {
	if len(t.touched) == 0 {
		return nil
	}
	ds, err := t.Ops.Deployments(ctx, slices.Collect(maps.Keys(t.touched)))
	if err != nil {
		return err
	}
	said := map[string]bool{wait.ServicesSubject: true}
	subjects := []string{wait.ServicesSubject}
	for _, d := range ds {
		for _, s := range []string{wait.DeploymentSubject(d.ID), wait.CommitSubject(d.Repository, d.Commit)} {
			if !said[s] {
				said[s] = true
				subjects = append(subjects, s)
			}
		}
	}
	return pendChanged(ctx, t.Ops, subjects...)
}

// pendChanged pends, in tx, the steps saying subjects changed.
func pendChanged(ctx context.Context, tx Ops, subjects ...string) error {
	for _, s := range subjects {
		kind, key, body := wait.Changed(s)
		if err := tx.PendStep(ctx, kind, key, time.Time{}, body); err != nil {
			return err
		}
	}
	return nil
}

func (t txOps) InsertDeployment(ctx context.Context, d DeploymentRow) (int64, error) {
	id, err := t.Ops.InsertDeployment(ctx, d)
	if err == nil {
		t.touched[id] = true
	}
	return id, err
}

func (t txOps) SetGates(ctx context.Context, id int64, gates []Gate) error {
	t.touched[id] = true
	return t.Ops.SetGates(ctx, id, gates)
}

func (t txOps) CancelWaitingOf(ctx context.Context, repo int64, deploy, reason string) ([]Replaced, error) {
	rs, err := t.Ops.CancelWaitingOf(ctx, repo, deploy, reason)
	for _, r := range rs {
		t.touched[r.ID] = true
	}
	return rs, err
}

func (t txOps) CancelWaitingIn(ctx context.Context, plan, except int64, reason string) ([]int64, error) {
	ids, err := t.Ops.CancelWaitingIn(ctx, plan, except, reason)
	for _, id := range ids {
		t.touched[id] = true
	}
	return ids, err
}

func (t txOps) HaltRunningIn(ctx context.Context, plan, except int64, wave int, reason string) ([]*Deployment, error) {
	ds, err := t.Ops.HaltRunningIn(ctx, plan, except, wave, reason)
	for _, d := range ds {
		t.touched[d.ID] = true
	}
	return ds, err
}

func (t txOps) StartDeployment(ctx context.Context, id int64, driver string) (bool, error) {
	t.touched[id] = true
	return t.Ops.StartDeployment(ctx, id, driver)
}

func (t txOps) SetStage(ctx context.Context, id int64, stage Stage) error {
	t.touched[id] = true
	return t.Ops.SetStage(ctx, id, stage)
}

func (t txOps) SetRoll(ctx context.Context, id int64, roll []byte) error {
	t.touched[id] = true
	return t.Ops.SetRoll(ctx, id, roll)
}

func (t txOps) SetPushRun(ctx context.Context, id int64, container string, run int64) error {
	t.touched[id] = true
	return t.Ops.SetPushRun(ctx, id, container, run)
}

func (t txOps) SetPushed(ctx context.Context, id int64, container, reference string) error {
	t.touched[id] = true
	return t.Ops.SetPushed(ctx, id, container, reference)
}

func (t txOps) SetMigration(ctx context.Context, id int64, position int, status MigrationStatus, output string, report []byte, refused []string) error {
	t.touched[id] = true
	return t.Ops.SetMigration(ctx, id, position, status, output, report, refused)
}

func (t txOps) FinishDeployment(ctx context.Context, id int64, status Status, reason string) (bool, error) {
	t.touched[id] = true
	return t.Ops.FinishDeployment(ctx, id, status, reason)
}

func (t txOps) SetHaltBy(ctx context.Context, id int64, by string) error {
	t.touched[id] = true
	return t.Ops.SetHaltBy(ctx, id, by)
}

func (t txOps) AppendEvent(ctx context.Context, id int64, message string) error {
	t.touched[id] = true
	return t.Ops.AppendEvent(ctx, id, message)
}

func (t txOps) AllowMigration(ctx context.Context, id int64, position int, allowed []string, by string) error {
	t.touched[id] = true
	return t.Ops.AllowMigration(ctx, id, position, allowed, by)
}
