// Package movetest serves roles' sides of moves for tests, and drives a move between two of them as
// the directory does (docs/hosted.md, "Cells, as built").
package movetest

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/serve/authn"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Serve serves a role's moves on loopback, every caller ok identity, reaching the same role of
// another cell with peer (nil for none): a client of it.
func Serve(t testing.TB, role move.Role, self okpb.Caller, peer okpb.MovesClient) okpb.MovesClient {
	t.Helper()
	srv := move.NewServer(role, self, func(context.Context, string) (okpb.MovesClient, error) { return peer, nil })
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	asIdentity := func(ctx context.Context) context.Context {
		return authn.NewContext(ctx, authn.Caller{Principal: "identity.ok.robot.example.com", Role: okpb.Caller_CALLER_IDENTITY})
	}
	g := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			return h(asIdentity(ctx), req)
		}),
		grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			return h(srv, &stream{ServerStream: ss, ctx: asIdentity(ss.Context())})
		}))
	srv.Register(g)
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return okpb.NewMovesClient(conn)
}

type stream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *stream) Context() context.Context { return s.ctx }

// Pair serves a role's side at a source and a destination, the destination copying from the
// source.
func Pair(t testing.TB, src, dst move.Role, self okpb.Caller) (srcMoves, dstMoves okpb.MovesClient) {
	t.Helper()
	srcMoves = Serve(t, src, self, nil)
	return srcMoves, Serve(t, dst, self, srcMoves)
}

// Steps are the hooks a test runs between a move's steps.
type Steps struct {
	// Frozen runs once the source froze the scope, before the rows are copied.
	Frozen func()
	// Imported runs once the destination holds it all, before it opens there.
	Imported func()
}

// Move moves a scope from src to dst as the directory does: objects, freeze, rows, open, finish.
func Move(t testing.TB, srcMoves, dstMoves okpb.MovesClient, scope *okpb.MoveScope, steps Steps) {
	t.Helper()
	ctx := context.Background()
	if _, err := dstMoves.Import(ctx, &okpb.ImportRequest{Scope: scope, Source: "a", Stage: okpb.ImportRequest_STAGE_OBJECTS}); err != nil {
		t.Fatalf("copying the objects: %v", err)
	}
	if _, err := srcMoves.Freeze(ctx, scope); err != nil {
		t.Fatalf("freezing: %v", err)
	}
	if steps.Frozen != nil {
		steps.Frozen()
	}
	if _, err := dstMoves.Import(ctx, &okpb.ImportRequest{Scope: scope, Source: "a", Stage: okpb.ImportRequest_STAGE_ALL}); err != nil {
		t.Fatalf("copying everything: %v", err)
	}
	if steps.Imported != nil {
		steps.Imported()
	}
	if _, err := dstMoves.Open(ctx, scope); err != nil {
		t.Fatalf("opening: %v", err)
	}
	if _, err := srcMoves.Finish(ctx, &okpb.FinishRequest{Scope: scope, Destination: "b"}); err != nil {
		t.Fatalf("finishing: %v", err)
	}
}
