// Package mover runs the directory's moves of fork networks between cells (docs/hosted.md, "Cells,
// as built"): each move, phase by phase, under its network's lock, on whichever identity replica
// takes it, each step safe to repeat — so a replica that stops short leaves the move to the next.
package mover

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/serve/node"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

const (
	// every is how often the runner looks for moves due a step.
	every = 10 * time.Second
	// retry is how long after a failed step it is tried again.
	retry = 30 * time.Second
	// frozenLimit is how long a network stays frozen, its rows not yet copied, before its move is
	// abandoned and it thaws where it is.
	frozenLimit = time.Hour
)

// The roles a move copies, in the order the destination imports them and opens — the SCM first,
// which the others read when they open — and the order the source freezes them: the deployer
// first, which refuses while a deployment runs, before anything else is frozen.
var (
	importOrder = []string{cells.SCM, cells.Control, cells.Automation, cells.Deploy, cells.Secrets}
	freezeOrder = []string{cells.Deploy, cells.SCM, cells.Control, cells.Automation, cells.Secrets}
)

// Runner runs the directory's moves, a node.Application.
type Runner struct {
	Store *store.Store
	// Cells are the cells, and where each one's roles are reached.
	Cells []config.IdentityCell
	// Moves reaches a role's Moves service at an address, as ok identity.
	Moves func(addr string) (okpb.MovesClient, error)
	// Wake, when it receives, has the runner look at once: a move asked, or canceled.
	Wake <-chan struct{}
	Log  *slog.Logger
}

var _ node.Application = (*Runner)(nil)

func (*Runner) Name() string                 { return "ok/identity/mover" }
func (*Runner) Dependencies() []node.Node    { return nil }
func (*Runner) Setup(context.Context) error  { return nil }
func (*Runner) Healthy(context.Context) bool { return true }

func (r *Runner) Start(ctx context.Context) error {
	for {
		r.Pass(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-r.Wake:
		case <-time.After(every):
		}
	}
}

// Pass drives each move due a step as far as it goes now.
func (r *Runner) Pass(ctx context.Context) {
	due, err := r.Store.DueMoves(ctx, 100)
	if err != nil {
		if ctx.Err() == nil {
			r.Log.Warn("mover: finding moves due", slog.Any("err", err))
		}
		return
	}
	for _, network := range due {
		err := r.Store.LockMove(ctx, network, func(ctx context.Context) error { return r.drive(ctx, network) })
		if err != nil && !errors.Is(err, lock.ErrNotAcquired) && ctx.Err() == nil {
			r.Log.Warn("mover: driving a move", slog.Int64("network", network), slog.Any("err", err))
		}
	}
}

// drive takes a move through its phases until it finishes, a step fails — when it is tried again
// a while later — or its lock is lost.
func (r *Runner) drive(ctx context.Context, network int64) error {
	for ctx.Err() == nil {
		m, err := r.Store.GetMove(ctx, network)
		if err != nil {
			return err
		}
		if m.Phase.Finished() || m.NextAt.After(time.Now()) {
			return nil
		}
		repos, err := r.Store.NetworkRepositories(ctx, network)
		if err != nil {
			return err
		}
		sc := &okpb.MoveScope{NetworkId: network}
		for _, n := range repos {
			sc.Repositories = append(sc.Repositories, n.ID)
		}
		var next store.MovePhase
		switch m.Phase {
		case store.MoveCopying:
			next, err = store.MoveFrozen, r.copyThenFreeze(ctx, m, sc)
		case store.MoveFrozen:
			next, err = store.MoveImported, r.each(ctx, m.Destination, importOrder, sc, func(c okpb.MovesClient, sc *okpb.MoveScope) error {
				_, err := c.Import(ctx, &okpb.ImportRequest{Scope: sc, Source: m.Source, Stage: okpb.ImportRequest_STAGE_ALL})
				return err
			})
		case store.MoveImported:
			// Named, and recorded so, in one transaction.
			if err = r.Store.NameDestination(ctx, network); err == nil {
				r.Log.Info("mover: named the destination", slog.Int64("network", network), slog.String("cell", m.Destination))
				continue
			}
		case store.MoveNamed:
			next, err = store.MoveDone, r.openThenFinish(ctx, m, sc)
		case store.MoveAbandoning:
			next, err = store.MoveAbandoned, r.abandon(ctx, m, sc)
		}
		from := m.Phase
		if _, uerr := r.Store.UpdateMove(ctx, network, func(m *store.Move) error {
			if m.Phase != from {
				return nil // canceled meanwhile
			}
			now := time.Now()
			if err != nil {
				m.Error, m.NextAt = err.Error(), now.Add(retry)
				if m.Phase == store.MoveFrozen && now.Sub(m.FrozenAt) > frozenLimit {
					m.Phase, m.NextAt = store.MoveAbandoning, now
					m.Error = fmt.Sprintf("frozen over %s: %v", frozenLimit, err)
				}
				return nil
			}
			m.Phase, m.Error, m.NextAt = next, "", now
			if next == store.MoveFrozen {
				m.FrozenAt = now
			}
			return nil
		}); uerr != nil {
			return errors.Join(err, uerr)
		}
		if err != nil {
			r.Log.Warn("mover: a step failed; it is tried again", slog.Int64("network", network), slog.String("phase", string(from)), slog.Any("err", err))
			return nil
		}
		r.Log.Info("mover: a step done", slog.Int64("network", network), slog.String("phase", string(next)))
	}
	return ctx.Err()
}

// copyThenFreeze has the destination copy the network's objects while it is written, then freezes
// it at the source — thawing what froze if a role refuses, as the deployer does while a deployment
// runs, so that no role waits frozen for another.
func (r *Runner) copyThenFreeze(ctx context.Context, m store.Move, sc *okpb.MoveScope) error {
	if err := r.each(ctx, m.Destination, importOrder, sc, func(c okpb.MovesClient, sc *okpb.MoveScope) error {
		_, err := c.Import(ctx, &okpb.ImportRequest{Scope: sc, Source: m.Source, Stage: okpb.ImportRequest_STAGE_OBJECTS})
		return err
	}); err != nil {
		return err
	}
	type side struct {
		c  okpb.MovesClient
		sc *okpb.MoveScope
	}
	var frozen []side
	err := r.each(ctx, m.Source, freezeOrder, sc, func(c okpb.MovesClient, sc *okpb.MoveScope) error {
		if _, err := c.Freeze(ctx, sc); err != nil {
			return err
		}
		frozen = append(frozen, side{c, sc})
		return nil
	})
	if err != nil {
		for _, f := range frozen {
			if _, terr := f.c.Thaw(context.WithoutCancel(ctx), f.sc); terr != nil {
				err = errors.Join(err, fmt.Errorf("thawing: %w", terr))
			}
		}
	}
	return err
}

// openThenFinish opens the network at the destination, then has the source refuse it, naming the
// destination, and delete it a day later.
func (r *Runner) openThenFinish(ctx context.Context, m store.Move, sc *okpb.MoveScope) error {
	if err := r.each(ctx, m.Destination, importOrder, sc, func(c okpb.MovesClient, sc *okpb.MoveScope) error {
		_, err := c.Open(ctx, sc)
		return err
	}); err != nil {
		return err
	}
	return r.each(ctx, m.Source, importOrder, sc, func(c okpb.MovesClient, sc *okpb.MoveScope) error {
		_, err := c.Finish(ctx, &okpb.FinishRequest{Scope: sc, Destination: m.Destination})
		return err
	})
}

// abandon drops what the destination copied, then thaws the network at the source.
func (r *Runner) abandon(ctx context.Context, m store.Move, sc *okpb.MoveScope) error {
	if err := r.each(ctx, m.Destination, importOrder, sc, func(c okpb.MovesClient, sc *okpb.MoveScope) error {
		_, err := c.Drop(ctx, sc)
		return err
	}); err != nil {
		return err
	}
	return r.each(ctx, m.Source, importOrder, sc, func(c okpb.MovesClient, sc *okpb.MoveScope) error {
		_, err := c.Thaw(ctx, sc)
		return err
	})
}

// each calls fn with the Moves of each of a cell's roles, in order — those the source names, which
// the destination must name too — and the scope as that role's.
func (r *Runner) each(ctx context.Context, cell string, order []string, sc *okpb.MoveScope, fn func(okpb.MovesClient, *okpb.MoveScope) error) error {
	for _, role := range order {
		addr, err := r.addr(cell, role)
		if err != nil {
			return err
		}
		if addr == "" {
			continue
		}
		c, err := r.Moves(addr)
		if err != nil {
			return fmt.Errorf("reaching cell %s's %s: %w", cell, role, err)
		}
		if err := fn(c, &okpb.MoveScope{NetworkId: sc.GetNetworkId(), Repositories: sc.GetRepositories(), Role: role}); err != nil {
			return fmt.Errorf("cell %s's %s: %w", cell, role, err)
		}
	}
	return nil
}

// addr is where a cell's role is reached; "" for a role no cell names.
func (r *Runner) addr(cell, role string) (string, error) {
	named := false
	var addr string
	for _, c := range r.Cells {
		a := map[string]string{cells.SCM: c.SCM, cells.Control: c.Control, cells.Automation: c.Automation, cells.Deploy: c.Deploy,
			cells.Secrets: c.Secrets}[role]
		named = named || a != ""
		if c.Name == cell {
			addr = a
		}
	}
	if addr == "" && named {
		return "", fmt.Errorf("cell %s names no address for its %s, which other cells run", cell, role)
	}
	return addr, nil
}
