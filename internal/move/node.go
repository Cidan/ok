package move

import (
	"context"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/serve/node"
)

// Node serves a role's moves (ok.v1.Moves), and runs Purge, when set, while the role runs: a
// node.Service and a node.Application.
type Node struct {
	name  string
	srv   *Server
	purge func(ctx context.Context) error
}

var (
	_ node.Service     = (*Node)(nil)
	_ node.Application = (*Node)(nil)
)

// NewNode serves srv, named name, running purge (nil: none).
func NewNode(name string, srv *Server, purge func(ctx context.Context) error) *Node {
	return &Node{name: name, srv: srv, purge: purge}
}

func (n *Node) Name() string               { return n.name }
func (*Node) Dependencies() []node.Node    { return nil }
func (*Node) Setup(context.Context) error  { return nil }
func (*Node) Healthy(context.Context) bool { return true }

func (n *Node) RegisterRPC(_ context.Context, reg grpc.ServiceRegistrar) error {
	n.srv.Register(reg)
	return nil
}

func (n *Node) Start(ctx context.Context) error {
	if n.purge == nil {
		<-ctx.Done()
		return nil
	}
	return n.purge(ctx)
}
