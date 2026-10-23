package service

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/authz/authztest"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// matrixFacts answers as ok identity does for the permission matrix's callers, about its repository
// at one visibility.
type matrixFacts struct{ v tenancy.Visibility }

func (m matrixFacts) Facts(_ context.Context, in *identitypb.FactsRequest, _ ...grpc.CallOption) (*identitypb.FactsResponse, error) {
	if in.GetRepositoryId() != authztest.RepositoryID {
		return nil, status.Errorf(codes.NotFound, "no repository has the ID %d", in.GetRepositoryId())
	}
	r := authztest.Repository(m.v)
	out := &identitypb.FactsResponse{
		Repository: &identitypb.FactsResponse_Repository{Id: r.ID, Visibility: r.Visibility.String(), Propose: r.Propose.String(),
			OwnerId: r.Owner, OwnedByOrg: r.OwnedByOrg, BaseRole: r.Org.BaseRole.String(), PrivateForks: r.Org.PrivateForks},
		Caller: &identitypb.FactsResponse_Caller{Membership: tenancy.NotMember.String()},
	}
	for _, c := range authztest.Callers {
		if c.Caller.ID == "" || c.Caller.ID != in.GetCallerId() {
			continue
		}
		out.Caller = &identitypb.FactsResponse_Caller{Active: c.Caller.Active, Membership: c.Caller.Membership.String(), Internal: c.Caller.Internal}
		for _, g := range c.Caller.Granted {
			out.Caller.Granted = append(out.Caller.Granted, g.String())
		}
	}
	return out, nil
}

// requestFor is a request to a method naming the matrix's repository: at its top, in its lease, or
// in its beginning, wherever the method's request carries it.
func requestFor(t *testing.T, method string) proto.Message {
	t.Helper()
	name := protoreflect.FullName(strings.ReplaceAll(strings.TrimPrefix(method, "/"), "/", "."))
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	mt, err := protoregistry.GlobalTypes.FindMessageByName(d.(protoreflect.MethodDescriptor).Input().FullName())
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	m := mt.New()
	if g, ok := m.Interface().(*scmpb.GetRepositoryRequest); ok {
		g.Key = &scmpb.GetRepositoryRequest_Id{Id: authztest.RepositoryID}
		return g
	}
	if f := m.Descriptor().Fields().ByName("repository_id"); f != nil {
		m.Set(f, protoreflect.ValueOfInt64(authztest.RepositoryID))
		return m.Interface()
	}
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if f.Kind() != protoreflect.MessageKind || f.IsList() || f.IsMap() {
			continue
		}
		if id := f.Message().Fields().ByName("repository_id"); id != nil {
			inner := m.NewField(f).Message()
			inner.Set(id, protoreflect.ValueOfInt64(authztest.RepositoryID))
			m.Set(f, protoreflect.ValueOfMessage(inner))
			return m.Interface()
		}
	}
	t.Fatalf("%s's request names no repository, yet Authorize decides it", method)
	return nil
}

// The SCM's API holds to the permission matrix: for every method a person or robot calls, every
// kind of caller at every visibility is let through exactly when Authorize allows the method's
// action, refused as though there were nothing there when they cannot see the repository, and
// refused saying why when they see it but may not. ok's own roles, and a request no one is
// identified on (which the serving runtime refuses before it, TestCallers), are let through.
func TestAuthorizeMatrix(t *testing.T) {
	ctx := context.Background()
	for v := tenancy.Private; v <= tenancy.Public; v++ {
		a := &access{checker: authz.NewChecker(matrixFacts{v})}
		for method, action := range actions {
			if action == 0 {
				continue
			}
			req := requestFor(t, method)
			role := authn.NewContext(ctx, authn.Caller{Principal: "scm.ok.robot.example.com", ID: "prn_role", Role: okpb.Caller_CALLER_CONTROL})
			if err := a.check(role, method, req); err != nil {
				t.Errorf("%s, %s, ok's own role: %v", method, v, err)
			}
			if err := a.check(ctx, method, req); err != nil {
				t.Errorf("%s, %s, no one identified: %v", method, v, err)
			}
			for _, c := range authztest.Callers {
				if c.Caller.ID == "" {
					continue
				}
				as := authn.NewContext(ctx, authn.Caller{Principal: c.Name, ID: c.Caller.ID, Human: true, Token: c.Caller.Token})
				want := codes.OK
				switch {
				case !authztest.Visible(c, v):
					want = codes.NotFound
				case !authztest.Allowed(c, v, action):
					want = codes.PermissionDenied
				}
				if got := status.Code(a.check(as, method, req)); got != want {
					t.Errorf("%s, %s, %s (%s): %s, want %s", method, v, c.Name, action, got, want)
				}
			}
		}
	}
}

// Every method a person or robot may call is decided by Authorize — listed with its action, or as
// the directory's to decide — and every method listed is one there is.
func TestAuthorizeCoversEveryMethod(t *testing.T) {
	fd := scmpb.File_proto_ok_scm_v1_scm_proto
	for i := 0; i < fd.Services().Len(); i++ {
		svc := fd.Services().Get(i)
		callers := authn.Callers(string(svc.FullName()))
		if !slices.Contains(callers, okpb.Caller_CALLER_PEOPLE) && !slices.Contains(callers, okpb.Caller_CALLER_ROBOTS) {
			continue
		}
		for j := 0; j < svc.Methods().Len(); j++ {
			method := fmt.Sprintf("/%s/%s", svc.FullName(), svc.Methods().Get(j).Name())
			if _, ok := actions[method]; !ok {
				t.Errorf("%s: people and robots call it, and no action decides it", method)
			}
		}
	}
	for method := range actions {
		name := protoreflect.FullName(strings.ReplaceAll(strings.TrimPrefix(method, "/"), "/", "."))
		if _, err := protoregistry.GlobalFiles.FindDescriptorByName(name); err != nil {
			t.Errorf("%s is listed, and there is no such method", method)
		}
	}
}
