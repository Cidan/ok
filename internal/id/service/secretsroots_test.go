package service_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// An organization's owners name its own root for its repositories' secrets; its members read it;
// ok secrets reads a repository's — an organization's, or none for a person's — and no one else.
func TestSecretsRoots(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	if _, err := e.store.CreateRobot(ctx, secretsBot); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ada.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ada.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "acme", PrincipalId: e.beaID, Role: "member"}); err != nil {
		t.Fatal(err)
	}
	const root = "aws-kms:arn:aws:kms:us-east-1:111111111111:key/00000000-0000-0000-0000-000000000001"
	if _, err := e.bea.SetSecretsRoot(ctx, &identitypb.SetSecretsRootRequest{Handle: "acme", Root: root}); code(err) != codes.PermissionDenied {
		t.Errorf("a member naming the root: %v", err)
	}
	if _, err := e.ada.SetSecretsRoot(ctx, &identitypb.SetSecretsRootRequest{Handle: "acme", Root: "keyring:/etc/ok/keyring"}); code(err) != codes.InvalidArgument {
		t.Errorf("a keyring: %v", err)
	}
	if r, err := e.ada.SetSecretsRoot(ctx, &identitypb.SetSecretsRootRequest{Handle: "acme", Root: root}); err != nil || r.GetRoot() != root {
		t.Fatalf("an owner naming the root: %v, %v", r, err)
	}
	if r, err := e.bea.GetSecretsRoot(ctx, &identitypb.GetSecretsRootRequest{Handle: "acme"}); err != nil || r.GetRoot() != root {
		t.Errorf("a member reading it: %v, %v", r, err)
	}
	if _, err := identitypb.NewTenancyClient(e.as("cal.human.example.com")).GetSecretsRoot(ctx, &identitypb.GetSecretsRootRequest{Handle: "acme"}); code(err) != codes.PermissionDenied {
		t.Errorf("a stranger reading it: %v", err)
	}

	repoOf := func(owner string) int64 {
		t.Helper()
		r, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: owner, Name: "app", ForPersonId: e.adaID})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: r.Id}); err != nil {
			t.Fatal(err)
		}
		return r.Id
	}
	orgs, mine := repoOf("acme"), repoOf("ada")
	sec := identitypb.NewSecretsRootsClient(e.as(secretsBot))
	if r, err := sec.GetRepositorySecretsRoot(ctx, &identitypb.GetRepositorySecretsRootRequest{RepositoryId: orgs}); err != nil || r.GetRoot() != root {
		t.Errorf("the organization's repository's root: %v, %v", r, err)
	}
	if r, err := sec.GetRepositorySecretsRoot(ctx, &identitypb.GetRepositorySecretsRootRequest{RepositoryId: mine}); err != nil || r.GetRoot() != "" {
		t.Errorf("a person's repository's root: %v, %v", r, err)
	}
	if _, err := identitypb.NewSecretsRootsClient(e.as(ada)).GetRepositorySecretsRoot(ctx, &identitypb.GetRepositorySecretsRootRequest{RepositoryId: orgs}); code(err) != codes.PermissionDenied {
		t.Errorf("a person asking a repository's root: %v", err)
	}
	if _, err := e.ada.SetSecretsRoot(ctx, &identitypb.SetSecretsRootRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	if r, _ := sec.GetRepositorySecretsRoot(ctx, &identitypb.GetRepositorySecretsRootRequest{RepositoryId: orgs}); r.GetRoot() != "" {
		t.Errorf("forgotten: %v", r)
	}
}
