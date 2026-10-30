package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Moving a fork network between cells (docs/hosted.md, "Cells, as built"): the directory's record
// of each move, which its runner (internal/id/mover) drives phase by phase.

// MovePhase is where a move is.
type MovePhase string

const (
	MoveCopying    MovePhase = "copying"  // the destination copies its objects; it is written
	MoveFrozen     MovePhase = "frozen"   // the source refuses its writes; the destination copies its rows
	MoveImported   MovePhase = "imported" // the destination holds it all
	MoveNamed      MovePhase = "named"    // the directory names the destination; it opens there
	MoveDone       MovePhase = "done"
	MoveAbandoning MovePhase = "abandoning" // what the destination copied is dropped; the source thaws
	MoveAbandoned  MovePhase = "abandoned"
)

// Finished reports whether a move is over.
func (p MovePhase) Finished() bool { return p == MoveDone || p == MoveAbandoned }

// Move is a network's move between cells.
type Move struct {
	Network             int64
	Source, Destination string
	Phase               MovePhase
	AskedBy             string // the administrator, by ID
	AskedAt             time.Time
	FrozenAt            time.Time // zero until frozen
	UpdatedAt           time.Time
	NextAt              time.Time // when the runner next tries it
	Error               string    // why its last step failed; "" for none
}

var (
	// ErrMoving is a move asked of a network another move of which is unfinished.
	ErrMoving = errors.New("the network is moving already")
	// ErrNamed is a cancellation of a move the directory has named the destination of.
	ErrNamed = errors.New("the move named its destination: it finishes")
)

// AskMove records a move of the network of a repository to a cell, for an administrator (by ID):
// every repository in it, none still being made, from the cell holding them to another that is
// open to new repositories.
func (s *Store) AskMove(ctx context.Context, repo int64, destination, by string) (Move, error) {
	var m Move
	err := s.txn(ctx, func(tx Ops) error {
		r, err := tx.Repository(ctx, repo)
		if err != nil {
			return err
		}
		repos, err := tx.NetworkRepositories(ctx, r.Network)
		if err != nil {
			return err
		}
		source := s.CellOf(r)
		for _, n := range repos {
			if n.State == Creating {
				return invalid("repository %d of the network is still being made", n.ID)
			}
			if c := s.CellOf(n); c != source {
				return invalid("the network's repositories are in cells %s and %s", source, c)
			}
		}
		switch open, known := s.cellOpen(destination); {
		case !known:
			return invalid("no cell is named %s", destination)
		case destination == source:
			return invalid("the network is in cell %s already", source)
		case !open:
			return invalid("cell %s is closed to new repositories", destination)
		}
		switch had, err := tx.LockMove(ctx, r.Network); {
		case errors.Is(err, ErrNotFound):
		case err != nil:
			return err
		case !had.Phase.Finished():
			return fmt.Errorf("%w: to cell %s, %s", ErrMoving, had.Destination, had.Phase)
		}
		now := s.now()
		m = Move{Network: r.Network, Source: source, Destination: destination, Phase: MoveCopying, AskedBy: by, AskedAt: now, UpdatedAt: now,
			NextAt: now}
		return putMove(ctx, tx, m)
	})
	return m, err
}

// MoveWork is a network's move's next step, as keyed work (internal/work) keyed by the network:
// booked by every write of an unfinished move, for its NextAt, in that write's transaction.
const MoveWork = "move"

// putMove records a move in tx and books its next step, if it has one.
func putMove(ctx context.Context, tx Ops, m Move) error {
	if err := tx.PutMove(ctx, m); err != nil {
		return err
	}
	if m.Phase.Finished() {
		return nil
	}
	return tx.MarkWorkAt(ctx, MoveWork, m.Network, m.NextAt)
}

// cellOpen reports whether a cell is open to new repositories, and whether there is one so named.
func (s *Store) cellOpen(name string) (open, known bool) {
	for _, c := range s.cells {
		if c.Name == name {
			return c.Open, true
		}
	}
	return false, false
}

// GetMove is a network's latest move.
func (s *Store) GetMove(ctx context.Context, network int64) (Move, error) {
	return s.b.Move(ctx, network)
}

// ListMoves are the latest moves, newest first.
func (s *Store) ListMoves(ctx context.Context, limit int) ([]Move, error) {
	return s.b.Moves(ctx, limit)
}

// DueMoves are the unfinished moves due a step now, by network.
func (s *Store) DueMoves(ctx context.Context, limit int) ([]int64, error) {
	return s.b.DueMoves(ctx, s.now(), limit)
}

// UnfinishedMoves are the unfinished moves, due a step or not, by network: what a release before
// the moves' steps were booked left (mover.Runner).
func (s *Store) UnfinishedMoves(ctx context.Context, limit int) ([]Move, error) {
	ids, err := s.b.DueMoves(ctx, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), limit)
	if err != nil {
		return nil, err
	}
	out := make([]Move, 0, len(ids))
	for _, id := range ids {
		m, err := s.b.Move(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// NetworkRepositories are a network's repositories, every state, by ID.
func (s *Store) NetworkRepositories(ctx context.Context, network int64) ([]Repository, error) {
	return s.b.NetworkRepositories(ctx, network)
}

// UpdateMove changes a network's move as fn says, in one transaction: a step done, or failed.
func (s *Store) UpdateMove(ctx context.Context, network int64, fn func(m *Move) error) (Move, error) {
	var m Move
	err := s.txn(ctx, func(tx Ops) error {
		var err error
		if m, err = tx.LockMove(ctx, network); err != nil {
			return err
		}
		if err := fn(&m); err != nil {
			return err
		}
		m.UpdatedAt = s.now()
		return putMove(ctx, tx, m)
	})
	return m, err
}

// CancelMove abandons a move the directory has not named the destination of.
func (s *Store) CancelMove(ctx context.Context, network int64) (Move, error) {
	return s.UpdateMove(ctx, network, func(m *Move) error {
		switch m.Phase {
		case MoveNamed, MoveDone:
			return ErrNamed
		case MoveCopying, MoveFrozen, MoveImported:
			m.Phase, m.NextAt, m.Error = MoveAbandoning, s.now(), ""
		}
		return nil
	})
}

// NameDestination names a moved network's destination in every entry of the network, logging
// each — edges and roles see it — and records the move named, in one transaction: once the
// destination holds it all (MoveImported).
func (s *Store) NameDestination(ctx context.Context, network int64) error {
	return s.txn(ctx, func(tx Ops) error {
		m, err := tx.LockMove(ctx, network)
		if err != nil {
			return err
		}
		if m.Phase != MoveImported {
			return invalid("the move of network %d is %s, not imported", network, m.Phase)
		}
		repos, err := tx.NetworkRepositories(ctx, network)
		if err != nil {
			return err
		}
		for _, n := range repos {
			r, err := tx.LockRepository(ctx, n.ID)
			if err != nil {
				return err
			}
			r.Cell = m.Destination
			if err := tx.UpdateRepository(ctx, r); err != nil {
				return err
			}
			if r.State != Creating {
				if err := s.logRepository(ctx, tx, r.ID, &r, logged{kind: identitypb.DirectoryEvent_KIND_MOVED}); err != nil {
					return err
				}
			}
		}
		now := s.now()
		m.Phase, m.UpdatedAt, m.NextAt, m.Error = MoveNamed, now, now, ""
		return putMove(ctx, tx, m)
	})
}
