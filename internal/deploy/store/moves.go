package store

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/move"
)

// Moving repositories between cells (docs/hosted.md, "Cells, as built"): the deployer's side of a
// move — its rows about the repositories. A repository moves while none of its deployments runs:
// a running one acts on this cell's cluster, and finishes here.

// Mover is the deployer's side of moves; its Purge deletes what moved away, a day after.
func (s *Store) Mover() *move.RepositoryRole {
	return &move.RepositoryRole{RoleName: cells.Deploy, Rows: s.b.Moves(), Work: s.Work(), Frozen: s.idle, Arrived: s.arrived, Thawed: s.thawed}
}

// idle refuses a freeze while a deployment of the repositories runs; once the gates closed, none
// starts.
func (s *Store) idle(ctx context.Context, repos []int64) error {
	running, err := s.b.RunningDeployments(ctx, repos)
	if err != nil {
		return err
	}
	if len(running) > 0 {
		return status.Errorf(codes.FailedPrecondition, "deployment %d is running: a repository moves once its deployments are over", running[0])
	}
	return nil
}

// thawed has the deployments a move held up start, its gates open again.
func (s *Store) thawed(ctx context.Context, _ []int64) error {
	return s.Work().Mark(ctx, TurnsWork, 0)
}

// arrived has the repositories planned, their services observed, and their waiting deployments
// started, here.
func (s *Store) arrived(ctx context.Context, repos []int64) error {
	if err := s.Work().Mark(ctx, TurnsWork, 0); err != nil {
		return err
	}
	for _, r := range repos {
		if err := s.Work().Mark(ctx, PlanWork, r); err != nil {
			return err
		}
		services, err := s.Services(ctx, r)
		if err != nil {
			return fmt.Errorf("repository %d's services: %w", r, err)
		}
		for _, sv := range services {
			if err := s.Work().Mark(ctx, ObserveWork, sv.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
