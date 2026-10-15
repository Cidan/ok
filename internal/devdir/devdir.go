// Package devdir is the directory of a development role run with no ok identity behind it: every
// well-formed name under the domain is an active principal — a person if it is
// <handle>.human.<domain> — and no group or ACL exists. A role uses it only when serving
// development principals on loopback (-dev), where whoever can connect is trusted already.
package devdir

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/serve/authn"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Directory is the development directory: an authn.Directory, and the part of identity's
// Directory service roles ask (GetPrincipal, GetSet).
type Directory struct{ Names identity.Names }

var _ authn.Directory = Directory{}

// kind is what a name is, if it is a principal's.
func (d Directory) kind(name string) (identity.MemberKind, bool) {
	k, ok := d.Names.KindOf(name)
	return k, ok && (k == identity.Human || k == identity.Robot)
}

func (d Directory) Principal(_ context.Context, name string) (authn.Principal, error) {
	k, ok := d.kind(name)
	if !ok {
		return authn.Principal{}, authn.ErrNotFound
	}
	return authn.Principal{Name: name, ID: "dev:" + name, Human: k == identity.Human, Active: true}, nil
}

// Credential knows no credential: a development role accepts development principals only.
func (Directory) Credential(context.Context, string) (authn.Principal, error) {
	return authn.Principal{}, authn.ErrNotFound
}

// Session knows no session.
func (Directory) Session(context.Context, string) (authn.Principal, error) {
	return authn.Principal{}, authn.ErrNotFound
}

func (d Directory) GetPrincipal(_ context.Context, in *identitypb.GetPrincipalRequest, _ ...grpc.CallOption) (*identitypb.GetPrincipalResponse, error) {
	name := in.GetName()
	k, ok := d.kind(name)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%s is no principal's name under %s", name, d.Names.Domain())
	}
	kind := identitypb.Kind_KIND_ROBOT
	if k == identity.Human {
		kind = identitypb.Kind_KIND_HUMAN
	}
	return &identitypb.GetPrincipalResponse{Principal: &identitypb.Principal{Id: "dev:" + name, Name: name, Kind: kind, Status: identitypb.Status_STATUS_ACTIVE}}, nil
}

func (Directory) GetSet(_ context.Context, in *identitypb.GetSetRequest, _ ...grpc.CallOption) (*identitypb.GetSetResponse, error) {
	return nil, status.Errorf(codes.NotFound, "a development directory holds no sets, and so no %s", in.GetName())
}
