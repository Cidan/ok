package service_test

import (
	"context"
	"slices"
	"testing"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Search finds what its caller may see: a private repository its owner's, not a stranger's, ok
// control's for the person it names; and people and organizations by part of their handles.
func TestSearch(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	if _, err := e.ada.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct{ name, visibility string }{{"widgets", "private"}, {"widget-docs", "public"}} {
		entry, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "acme", Name: r.name, Visibility: r.visibility,
			ForPersonId: e.adaID})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: entry.Id}); err != nil {
			t.Fatal(err)
		}
	}
	names := func(resp *identitypb.SearchResponse, err error) []string {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range resp.GetRepositories() {
			out = append(out, r.GetOwner()+"/"+r.GetName())
		}
		return out
	}
	if got := names(e.ada.Search(ctx, &identitypb.SearchRequest{Query: "widget"})); !slices.Equal(got, []string{"acme/widgets", "acme/widget-docs"}) {
		t.Errorf("ada's search: %v", got)
	}
	if got := names(e.cal.Search(ctx, &identitypb.SearchRequest{Query: "widget"})); !slices.Equal(got, []string{"acme/widget-docs"}) {
		t.Errorf("a stranger's search: %v", got)
	}
	if got := names(e.control.Search(ctx, &identitypb.SearchRequest{Query: "acme/", ForPersonId: e.calID})); !slices.Equal(got, []string{"acme/widget-docs"}) {
		t.Errorf("ok control's search for cal: %v", got)
	}
	resp, err := e.cal.Search(ctx, &identitypb.SearchRequest{Query: "ac"})
	if err != nil || len(resp.GetOrganizations()) != 1 || resp.GetOrganizations()[0].GetHandle() != "acme" {
		t.Errorf("organizations for ac: %v, %v", resp, err)
	}
	resp, err = e.cal.Search(ctx, &identitypb.SearchRequest{Query: "be"})
	if err != nil || len(resp.GetPeople()) != 1 || resp.GetPeople()[0].GetHandle() != "bea" {
		t.Errorf("people for be: %v, %v", resp, err)
	}
	if resp, err := e.cal.Search(ctx, &identitypb.SearchRequest{Query: "e", Limit: 1}); err != nil || len(resp.GetPeople()) > 1 || len(resp.GetRepositories()) > 1 {
		t.Errorf("a search of limit 1: %v, %v", resp, err)
	}
}
