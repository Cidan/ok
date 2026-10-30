package wait

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/bus"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Server serves a service's Waits over its Store, to another role's live requests.
type Server struct {
	okpb.UnimplementedWaitsServer
	Store Store
}

// maxSubjects bounds the subjects one registration names.
const maxSubjects = 64

func (s *Server) Register(ctx context.Context, req *okpb.RegisterWaitRequest) (*okpb.RegisterWaitResponse, error) {
	if _, _, ok := bus.InboxQueue(req.GetInbox()); !ok {
		return nil, status.Error(codes.InvalidArgument, "the inbox is no inbox's address")
	}
	if n := len(req.GetSubjects()); n == 0 || n > maxSubjects {
		return nil, status.Errorf(codes.InvalidArgument, "a registration names 1 to %d subjects, not %d", maxSubjects, n)
	}
	lapses := req.GetLapses().AsTime()
	if !lapses.After(time.Now()) || lapses.After(time.Now().Add(2*Lease)) {
		return nil, status.Errorf(codes.InvalidArgument, "a registration lapses within %s", 2*Lease)
	}
	if err := s.Store.Register(ctx, req.GetInbox(), req.GetSubjects(), lapses); err != nil {
		return nil, status.Errorf(codes.Unavailable, "registering: %v", err)
	}
	return &okpb.RegisterWaitResponse{}, nil
}

func (s *Server) Unregister(ctx context.Context, req *okpb.UnregisterWaitRequest) (*okpb.UnregisterWaitResponse, error) {
	if err := s.Store.Unregister(ctx, req.GetInbox()); err != nil {
		return nil, status.Errorf(codes.Unavailable, "unregistering: %v", err)
	}
	return &okpb.UnregisterWaitResponse{}, nil
}

// Remote is another role's Waits, as a Registrar.
func Remote(conn grpc.ClientConnInterface) Registrar { return remote{okpb.NewWaitsClient(conn)} }

type remote struct{ c okpb.WaitsClient }

func (r remote) Register(ctx context.Context, inbox string, subjects []string, lapses time.Time) error {
	_, err := r.c.Register(ctx, &okpb.RegisterWaitRequest{Inbox: inbox, Subjects: subjects, Lapses: timestamppb.New(lapses)})
	return err
}

func (r remote) Unregister(ctx context.Context, inbox string) error {
	_, err := r.c.Unregister(ctx, &okpb.UnregisterWaitRequest{Inbox: inbox})
	return err
}
