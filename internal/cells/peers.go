package cells

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Roles a cell lists the addresses of.
const (
	SCM        = "scm"
	Control    = "control"
	Automation = "automation"
	Deploy     = "deploy"
	Secrets    = "secrets"
)

// Peers reaches other cells' roles where the directory says each is (identity's Cells), each
// address dialed once, as the caller's role.
type Peers struct {
	cells identitypb.CellsClient
	dial  func(addr string) (grpc.ClientConnInterface, func(), error)

	mu     sync.Mutex
	listed map[string]*identitypb.Cell // by name; listed again for a cell it lacks
	conns  map[string]grpc.ClientConnInterface
	closes []func()
}

// NewPeers reaches cells as the directory lists them, dialing each address with dial (which may
// return no closer).
func NewPeers(cells identitypb.CellsClient, dial func(addr string) (grpc.ClientConnInterface, func(), error)) *Peers {
	return &Peers{cells: cells, dial: dial, conns: map[string]grpc.ClientConnInterface{}}
}

// Conn is a connection to a cell's role.
func (p *Peers) Conn(ctx context.Context, cell, role string) (grpc.ClientConnInterface, error) {
	addr, err := p.addr(ctx, cell, role)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, closer, err := p.dial(addr)
	if err != nil {
		return nil, fmt.Errorf("dialing cell %s's %s at %s: %w", cell, role, addr, err)
	}
	p.conns[addr] = c
	if closer != nil {
		p.closes = append(p.closes, closer)
	}
	return c, nil
}

func (p *Peers) addr(ctx context.Context, cell, role string) (string, error) {
	p.mu.Lock()
	c, ok := p.listed[cell]
	p.mu.Unlock()
	if !ok {
		resp, err := p.cells.ListCells(ctx, &identitypb.ListCellsRequest{})
		if err != nil {
			return "", fmt.Errorf("listing the cells: %w", err)
		}
		listed := map[string]*identitypb.Cell{}
		for _, c := range resp.GetCells() {
			listed[c.GetName()] = c
		}
		p.mu.Lock()
		p.listed = listed
		p.mu.Unlock()
		if c, ok = listed[cell]; !ok {
			return "", fmt.Errorf("the directory knows no cell %s", cell)
		}
	}
	addr := map[string]string{SCM: c.GetScm(), Control: c.GetControl(), Automation: c.GetAutomation(), Deploy: c.GetDeploy(),
		Secrets: c.GetSecrets()}[role]
	if addr == "" {
		return "", fmt.Errorf("cell %s names no address for its %s", cell, role)
	}
	return addr, nil
}

// Close closes the connections it dialed.
func (p *Peers) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.closes {
		c()
	}
}
