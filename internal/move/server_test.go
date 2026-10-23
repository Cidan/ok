package move_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/serve/authn"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// fakeRole is a role's side of moves that records what it was asked.
type fakeRole struct {
	name   string
	frozen []int64
}

func (f *fakeRole) Name() string                                     { return f.name }
func (f *fakeRole) Tables(context.Context) ([]string, error)         { return nil, nil }
func (f *fakeRole) Imported(context.Context, move.Scope) error       { return nil }
func (f *fakeRole) Open(context.Context, move.Scope) error           { return nil }
func (f *fakeRole) Drop(context.Context, move.Scope) error           { return nil }
func (f *fakeRole) Thaw(context.Context, move.Scope) error           { return nil }
func (f *fakeRole) Finish(context.Context, move.Scope, string) error { return nil }
func (f *fakeRole) Objects() move.Objects                            { return nil }

func (f *fakeRole) Export(context.Context, move.Scope, string, *okpb.Row, int) ([]*okpb.Row, error) {
	return nil, nil
}

func (f *fakeRole) Import(context.Context, move.Scope, string, []*okpb.Row) error { return nil }

func (f *fakeRole) Freeze(_ context.Context, s move.Scope) error {
	f.frozen = append(f.frozen, s.Network)
	return nil
}

// One process serving several roles on one server registers Moves once, each request going to the
// side of the role its scope names; a role it serves none of is refused; and each side admits ok
// identity and its own role alone.
func TestMovesOfSeveralRoles(t *testing.T) {
	ctx := context.Background()
	scm, control := &fakeRole{name: "scm"}, &fakeRole{name: "control"}
	var caller okpb.Caller
	g := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		return h(authn.NewContext(ctx, authn.Caller{Principal: "a robot", Role: caller}), req)
	}))
	move.NewServer(scm, okpb.Caller_CALLER_SCM, nil).Register(g)
	move.NewServer(control, okpb.Caller_CALLER_CONTROL, nil).Register(g)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(lis)
	defer g.Stop()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := okpb.NewMovesClient(conn)

	caller = okpb.Caller_CALLER_IDENTITY
	if _, err := c.Freeze(ctx, &okpb.MoveScope{NetworkId: 1, Role: "scm"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Freeze(ctx, &okpb.MoveScope{NetworkId: 2, Role: "control"}); err != nil {
		t.Fatal(err)
	}
	if len(scm.frozen) != 1 || scm.frozen[0] != 1 || len(control.frozen) != 1 || control.frozen[0] != 2 {
		t.Errorf("frozen: the SCM %v, ok control %v", scm.frozen, control.frozen)
	}
	if _, err := c.Freeze(ctx, &okpb.MoveScope{NetworkId: 3, Role: "deploy"}); status.Code(err) != codes.NotFound {
		t.Errorf("a role not served here: %v", err)
	}
	caller = okpb.Caller_CALLER_CONTROL
	if _, err := c.Freeze(ctx, &okpb.MoveScope{NetworkId: 4, Role: "scm"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("ok control asking the SCM's side: %v", err)
	}
	if _, err := c.Freeze(ctx, &okpb.MoveScope{NetworkId: 5, Role: "control"}); err != nil {
		t.Errorf("ok control asking its own side: %v", err)
	}
}
