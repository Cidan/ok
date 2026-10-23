package service

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/id/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Moving fork networks between cells (docs/hosted.md, "Cells, as built"): what ok's administrators
// ask of the directory, which its runner (internal/id/mover) does.

// MoveNetwork moves a repository's fork network to another cell.
func (s *Server) MoveNetwork(ctx context.Context, req *identitypb.MoveNetworkRequest) (*identitypb.Move, error) {
	c, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.repositoryOf(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}
	m, err := s.cfg.Store.AskMove(ctx, r.ID, req.GetCell(), c.ID)
	if err != nil {
		return nil, moveError(err)
	}
	s.audit(ctx, c.Principal, "network.move", map[string]any{"network": m.Network, "from": m.Source, "to": m.Destination})
	s.moveAsked()
	return s.moveOut(ctx, m)
}

// ListMoves lists the moves, the latest of each network, newest first.
func (s *Server) ListMoves(ctx context.Context, _ *identitypb.ListMovesRequest) (*identitypb.ListMovesResponse, error) {
	if _, err := s.requireAdmin(ctx); err != nil {
		return nil, err
	}
	ms, err := s.cfg.Store.ListMoves(ctx, 200)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ListMovesResponse{}
	for _, m := range ms {
		pb, err := s.moveOut(ctx, m)
		if err != nil {
			return nil, err
		}
		out.Moves = append(out.Moves, pb)
	}
	return out, nil
}

// CancelMove abandons a move the directory has not named the destination of.
func (s *Server) CancelMove(ctx context.Context, req *identitypb.CancelMoveRequest) (*identitypb.Move, error) {
	c, err := s.requireAdmin(ctx)
	if err != nil {
		return nil, err
	}
	m, err := s.cfg.Store.CancelMove(ctx, req.GetNetworkId())
	if err != nil {
		return nil, moveError(err)
	}
	s.audit(ctx, c.Principal, "network.move.cancel", map[string]any{"network": m.Network})
	s.moveAsked()
	return s.moveOut(ctx, m)
}

func (s *Server) moveAsked() {
	if s.cfg.MoveAsked != nil {
		s.cfg.MoveAsked()
	}
}

// moveError maps a move's refusals to gRPC statuses.
func moveError(err error) error {
	switch {
	case errors.Is(err, store.ErrMoving):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrNamed):
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return tenancyError(err)
}

// moveOut is a move as Admin shows it: its repositories and administrator by name.
func (s *Server) moveOut(ctx context.Context, m store.Move) (*identitypb.Move, error) {
	out := &identitypb.Move{NetworkId: m.Network, Source: m.Source, Destination: m.Destination, Phase: string(m.Phase),
		AskedAt: timestamppb.New(m.AskedAt), UpdatedAt: timestamppb.New(m.UpdatedAt), Error: m.Error}
	if !m.FrozenAt.IsZero() {
		out.FrozenAt = timestamppb.New(m.FrozenAt)
	}
	if p, err := s.cfg.Store.PrincipalByID(ctx, m.AskedBy); err == nil {
		out.AskedBy = p.Name
	}
	repos, err := s.cfg.Store.NetworkRepositories(ctx, m.Network)
	if err != nil {
		return nil, grpcError(err)
	}
	for _, r := range repos {
		e, err := s.cfg.Store.Entry(ctx, r)
		if err != nil {
			return nil, grpcError(err)
		}
		out.Repositories = append(out.Repositories, e.GetFullName())
	}
	return out, nil
}
