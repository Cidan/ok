package worker

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/owners"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// identityRoster is ok identity's roster, for a repository that is not the tree's: its ACLs are the
// applied tree's, and everyone known is every active person in the directory.
type identityRoster struct {
	ctx   context.Context
	dir   identitypb.DirectoryClient
	names identity.Names
}

func (r identityRoster) ACL(acl string) ([]string, bool, error) {
	full := r.names.Of(identity.ACL, acl)
	if _, err := r.dir.GetSet(r.ctx, &identitypb.GetSetRequest{Name: full}); status.Code(err) == codes.NotFound {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	resp, err := r.dir.ExpandSet(r.ctx, &identitypb.ExpandSetRequest{Name: full})
	if err != nil {
		return nil, false, err
	}
	var handles []string
	for _, p := range resp.GetPrincipals() {
		if h, ok := owners.HumanHandle(r.names, p); ok {
			handles = append(handles, h)
		}
	}
	return handles, true, nil
}

func (r identityRoster) People() ([]string, error) {
	var out []string
	token := ""
	for {
		resp, err := r.dir.ListPrincipals(r.ctx, &identitypb.ListPrincipalsRequest{Kind: identitypb.Kind_KIND_HUMAN, PageSize: 1000, PageToken: token})
		if err != nil {
			return nil, err
		}
		for _, p := range resp.GetPrincipals() {
			if h, ok := owners.HumanHandle(r.names, p.GetName()); ok {
				out = append(out, h)
			}
		}
		if token = resp.GetNextPageToken(); token == "" {
			return out, nil
		}
	}
}
