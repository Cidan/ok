package interceptor

import (
	"context"
	"testing"

	"google.golang.org/grpc"
)

func unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		return h(ctx, req)
	}
}

func stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		return h(srv, ss)
	}
}

func TestEmptyChainHasNoOptions(t *testing.T) {
	if opts := New().ServerOptions(); len(opts) != 0 {
		t.Fatalf("empty chain ServerOptions = %d, want 0", len(opts))
	}
}

func TestUnaryOnly(t *testing.T) {
	if opts := New().UseUnary(unary()).ServerOptions(); len(opts) != 1 {
		t.Fatalf("unary-only ServerOptions = %d, want 1", len(opts))
	}
}

func TestUnaryAndStream(t *testing.T) {
	c := New().UseUnary(unary(), unary()).UseStream(stream())
	if opts := c.ServerOptions(); len(opts) != 2 {
		t.Fatalf("ServerOptions = %d, want 2 (unary chain + stream chain)", len(opts))
	}
}
