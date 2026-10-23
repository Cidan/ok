package store

import (
	"context"
	"fmt"
)

// StartTurns starts the waiting deployments whose turn has come — what is running admits them
// (occupancy.admits), and every deployment of their plan's earlier waves, and of the earlier waves
// of each place they took in an earlier plan, succeeded (or was replaced) — oldest first. It is one
// transaction, one at a time across every replica (LockStarts): what each running deployment holds
// is read, and what each started one holds claimed, at once, so no two deployments ever hold one
// deploy or workload, and one that runs alone runs alone. driver names the copy of the deployer
// that drives a self deployment it starts: this one. It returns those it started.
func (s *Store) StartTurns(ctx context.Context, driver string) ([]*Deployment, error) {
	var started []*Deployment
	err := s.b.Txn(ctx, func(tx Ops) error {
		started = nil
		if err := tx.LockStarts(ctx); err != nil {
			return err
		}
		ds, err := tx.OpenDeployments(ctx)
		if err != nil {
			return err
		}
		busy := occupancy{claims: map[string]bool{}}
		for _, d := range ds {
			if d.Status == Running {
				busy.take(d)
			}
		}
		for _, d := range ds {
			if d.Status != Waiting {
				continue
			}
			// One whose repository moves between cells, or moved, waits: it starts where the
			// repository is (docs/hosted.md, "Cells, as built").
			if state, err := tx.GateState(ctx, d.Repository); err != nil || state != "" {
				if err != nil {
					return err
				}
				continue
			}
			ok, err := mayStart(ctx, tx, d, busy)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			drv := ""
			if d.Self {
				drv = driver
			}
			ok, err = tx.StartDeployment(ctx, d.ID, drv)
			if err != nil {
				return err
			}
			if !ok {
				continue // canceled meanwhile: a person halted it
			}
			if err := tx.AppendEvent(ctx, d.ID, "started"); err != nil {
				return err
			}
			busy.take(d)
			d.Status, d.Driver = Running, drv
			started = append(started, d)
		}
		return nil
	})
	return started, err
}

// occupancy is what the running deployments hold.
type occupancy struct {
	claims  map[string]bool // their deploys and workloads
	running int
	alone   bool // one of them runs alone
}

// take records a deployment as running.
func (o *occupancy) take(d *Deployment) {
	for _, k := range claims(d) {
		o.claims[k] = true
	}
	o.running++
	o.alone = o.alone || d.Alone
}

// admits reports whether d may run beside what is running: nothing else deploys its deploy or
// workload, no deployment that runs alone is running, and — for one that runs alone — nothing
// is running at all.
func (o *occupancy) admits(d *Deployment) bool {
	if o.alone || (d.Alone && o.running > 0) {
		return false
	}
	for _, k := range claims(d) {
		if o.claims[k] {
			return false
		}
	}
	return true
}

// claims are what a running deployment holds: its deploy, and its workload.
func claims(d *Deployment) []string {
	out := []string{fmt.Sprintf("deploy %d %s", d.Repository, d.Deploy)}
	if d.Workload != nil {
		out = append(out, d.Workload.String())
	}
	return out
}

// mayStart reports whether a waiting deployment's turn has come: what is running admits it, and
// every deployment of its plan's earlier waves — and of the earlier waves of each place it took in
// an earlier plan — succeeded (or was replaced).
func mayStart(ctx context.Context, tx Ops, d *Deployment, busy occupancy) (bool, error) {
	if !busy.admits(d) {
		return false, nil
	}
	gates := append([]Gate(nil), d.Gates...)
	if d.PlanID != 0 {
		gates = append(gates, Gate{Plan: d.PlanID, Wave: d.Wave})
	}
	for _, g := range gates {
		waves, err := tx.PlanStatuses(ctx, g.Plan)
		if err != nil {
			return false, err
		}
		for w := 1; w < g.Wave; w++ {
			for _, st := range waves[w] {
				if st != Succeeded && st != Canceled {
					return false, nil
				}
			}
		}
	}
	return true, nil
}
