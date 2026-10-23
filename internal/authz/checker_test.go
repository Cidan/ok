package authz

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// source answers facts about repository 7, owned by an organization granting bob write; it counts
// what it is asked.
type source struct{ asked int }

func (s *source) Facts(_ context.Context, in *identitypb.FactsRequest, _ ...grpc.CallOption) (*identitypb.FactsResponse, error) {
	s.asked++
	if in.GetRepositoryId() != 7 {
		return nil, status.Error(codes.NotFound, "no such repository")
	}
	out := &identitypb.FactsResponse{
		Repository: &identitypb.FactsResponse_Repository{Id: 7, Visibility: "private", Propose: "read", OwnerId: "prn_acme", OwnedByOrg: true, BaseRole: "none"},
		Caller:     &identitypb.FactsResponse_Caller{Membership: "none"},
	}
	if in.GetCallerId() == "prn_bob" {
		out.Caller = &identitypb.FactsResponse_Caller{Active: true, Membership: "member", Granted: []string{"write"}}
	}
	return out, nil
}

func TestChecker(t *testing.T) {
	ctx := context.Background()
	src := &source{}
	c := NewChecker(src)
	now := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return now }

	d, err := c.Decide(ctx, "prn_bob", nil, 7, Land)
	if err != nil || !d.Allowed || d.Role != tenancy.Write {
		t.Fatalf("bob landing: %+v, %v", d, err)
	}
	if d, _ = c.Decide(ctx, "prn_bob", nil, 7, Administer); d.Allowed {
		t.Errorf("bob administering: %+v", d)
	}
	if src.asked != 1 {
		t.Errorf("identity was asked %d times; the facts are kept", src.asked)
	}
	// A token narrows the caller, without asking again.
	if d, _ = c.Decide(ctx, "prn_bob", &Token{Role: tenancy.Read}, 7, Land); d.Allowed || d.Role != tenancy.Read || src.asked != 1 {
		t.Errorf("bob's read token landing: %+v, asked %d", d, src.asked)
	}
	// A stranger sees nothing of a private repository; neither does anyone of one there is none of.
	if d, _ = c.Decide(ctx, "prn_eve", nil, 7, ReadCode); d.Allowed || d.Visible {
		t.Errorf("a stranger reading: %+v", d)
	}
	if d, err = c.Decide(ctx, "prn_bob", nil, 8, ReadCode); err != nil || d.Allowed || d.Visible {
		t.Errorf("a repository there is none of: %+v, %v", d, err)
	}
	asked := src.asked
	// Past the TTL, or forgotten, the facts are read again.
	now = now.Add(TTL)
	c.Decide(ctx, "prn_bob", nil, 7, ReadCode)
	if src.asked != asked+1 {
		t.Errorf("past the TTL: asked %d times, want %d", src.asked, asked+1)
	}
	c.Forget(7)
	c.Decide(ctx, "prn_bob", nil, 7, ReadCode)
	c.ForgetCaller("prn_bob")
	c.Decide(ctx, "prn_bob", nil, 7, ReadCode)
	if src.asked != asked+3 {
		t.Errorf("forgotten: asked %d times, want %d", src.asked, asked+3)
	}
}

func TestFactsOf(t *testing.T) {
	f, err := FactsOf(&identitypb.FactsResponse{
		Repository: &identitypb.FactsResponse_Repository{Id: 3, Visibility: "internal", Propose: "write", Deleted: true, OwnerId: "prn_a",
			OwnedByOrg: true, BaseRole: "triage", PrivateForks: true},
		Caller: &identitypb.FactsResponse_Caller{Active: true, Membership: "owner", Granted: []string{"read", "maintain"}, Internal: true},
	}, "prn_c")
	want := Facts{
		Repository: Repository{ID: 3, Visibility: tenancy.Internal, Propose: tenancy.Write, State: Deleted, Owner: "prn_a", OwnedByOrg: true,
			Org: Org{BaseRole: tenancy.Triage, PrivateForks: true}},
		Caller: Caller{ID: "prn_c", Active: true, Membership: tenancy.Owner, Granted: []tenancy.Role{tenancy.Read, tenancy.Maintain}, Internal: true},
	}
	if err != nil || f.Repository != want.Repository || f.Caller.ID != want.Caller.ID || f.Caller.Membership != want.Caller.Membership ||
		len(f.Caller.Granted) != 2 || f.Caller.Granted[1] != tenancy.Maintain || !f.Caller.Internal || !f.Caller.Active {
		t.Errorf("FactsOf = %+v, %v; want %+v", f, err, want)
	}
	if _, err := FactsOf(&identitypb.FactsResponse{Repository: &identitypb.FactsResponse_Repository{Visibility: "secret"}}, ""); err == nil {
		t.Error("a visibility there is none of")
	}
}
