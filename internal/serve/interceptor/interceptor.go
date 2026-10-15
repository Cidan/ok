// Package interceptor holds the gRPC server interceptor chain the serving runtime installs on its
// shared server: authentication's interceptors first (serve.WithAuthn), so every handler runs
// behind them, then any the role adds.
package interceptor

import "google.golang.org/grpc"

// Chain accumulates unary and stream server interceptors in the order they should
// run (outermost first). The auth interceptors, once they exist, are added before
// any application interceptor; UseUnary/UseStream preserve insertion order, so the
// caller controls precedence.
type Chain struct {
	unary  []grpc.UnaryServerInterceptor
	stream []grpc.StreamServerInterceptor
}

// New returns an empty chain.
func New() *Chain { return &Chain{} }

// UseUnary appends unary interceptors, outermost first. Returns the chain for
// fluent use.
func (c *Chain) UseUnary(i ...grpc.UnaryServerInterceptor) *Chain {
	c.unary = append(c.unary, i...)
	return c
}

// UseStream appends stream interceptors, outermost first. Returns the chain for
// fluent use.
func (c *Chain) UseStream(i ...grpc.StreamServerInterceptor) *Chain {
	c.stream = append(c.stream, i...)
	return c
}

// ServerOptions returns the grpc.ServerOptions that install the chain. An empty
// chain yields no options (grpc.ChainUnaryInterceptor with no interceptors is a
// no-op, but returning nothing keeps the server options minimal).
func (c *Chain) ServerOptions() []grpc.ServerOption {
	var opts []grpc.ServerOption
	if len(c.unary) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(c.unary...))
	}
	if len(c.stream) > 0 {
		opts = append(opts, grpc.ChainStreamInterceptor(c.stream...))
	}
	return opts
}
