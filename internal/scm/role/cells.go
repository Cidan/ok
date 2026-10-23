package role

import (
	"context"

	"github.com/Cidan/ok/internal/cells"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// peerSCMs reaches other cells' SCMs (service.PeerSCMs).
type peerSCMs struct{ *cells.Peers }

func (p peerSCMs) Moves(ctx context.Context, cell string) (okpb.MovesClient, error) {
	c, err := p.Conn(ctx, cell, cells.SCM)
	if err != nil {
		return nil, err
	}
	return okpb.NewMovesClient(c), nil
}

func (p peerSCMs) SCM(ctx context.Context, cell string) (scmpb.CellsClient, error) {
	c, err := p.Conn(ctx, cell, cells.SCM)
	if err != nil {
		return nil, err
	}
	return scmpb.NewCellsClient(c), nil
}
