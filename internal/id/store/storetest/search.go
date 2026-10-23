package storetest

import (
	"context"
	"slices"
	"testing"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// Repositories, people, and organizations are found by part of their names, whatever the query's case,
// the closest first; owner/name narrows repositories to owners whose handles start with owner; and
// a suspended person, or a robot, is not found.
func testSearch(t *testing.T, s *store.Store) {
	ctx := context.Background()
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "bea", true), person("2", "sue", false)}}})
	ada, err := s.CreateAccount(ctx, "ada", "ada@example.com", "github", "gh-ada")
	if err != nil {
		t.Fatal(err)
	}
	acme, err := s.CreateOrganization(ctx, "acme", ada.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	globex, err := s.CreateOrganization(ctx, "globex", ada.ID, store.OrganizationSettings{BaseRole: tenancy.Read})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateRobot(ctx, "widget-bot.robot.example.com"); err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct {
		owner, name string
	}{{acme.ID, "widgets"}, {acme.ID, "widget"}, {globex.ID, "old-widgets-archive"}, {globex.ID, "gadgets"}, {ada.ID, "notes"}} {
		repo, err := s.ReserveRepository(ctx, store.NewRepository{Owner: r.owner, Name: r.name, Propose: tenancy.Read, CreatedBy: ada.ID})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ActivateRepository(ctx, repo.ID); err != nil {
			t.Fatal(err)
		}
	}
	names := func(rs []store.Repository) []string {
		var out []string
		for _, r := range rs {
			out = append(out, r.Name)
		}
		return out
	}
	for _, tc := range []struct {
		q    string
		want []string
	}{
		{"widget", []string{"widget", "widgets", "old-widgets-archive"}},
		{"WIDG", []string{"widget", "widgets", "old-widgets-archive"}},
		{"glob/widg", []string{"old-widgets-archive"}},
		{"acme/", []string{"widget", "widgets"}},
		{"dget", []string{"widget", "gadgets", "widgets", "old-widgets-archive"}},
		{"nothing-like-it", nil},
		{"100%", nil},
	} {
		rs, err := s.SearchRepositories(ctx, tc.q)
		if err != nil {
			t.Fatalf("%q: %v", tc.q, err)
		}
		if got := names(rs); !slices.Equal(got, tc.want) {
			t.Errorf("repositories for %q: %v, want %v", tc.q, got, tc.want)
		}
	}
	handles := func(q string) []string {
		t.Helper()
		ps, err := s.SearchPrincipals(ctx, q)
		if err != nil {
			t.Fatalf("%q: %v", q, err)
		}
		var out []string
		for _, p := range ps {
			out = append(out, p.Name)
		}
		return out
	}
	if got := handles("glo"); !slices.Equal(got, []string{"globex.org.example.com"}) {
		t.Errorf("organizations for glo: %v", got)
	}
	// Handles starting with it first, the shorter first.
	if got := handles("A"); len(got) < 2 || got[0] != "ada.human.example.com" || got[1] != "acme.org.example.com" {
		t.Errorf("principals for A: %v", got)
	}
	if got := handles("Bea"); !slices.Equal(got, []string{"bea.human.example.com"}) {
		t.Errorf("people for Bea: %v", got)
	}
	if got := handles("sue"); len(got) != 0 {
		t.Errorf("a suspended person was found: %v", got)
	}
	if got := handles("widget-bot"); len(got) != 0 {
		t.Errorf("a robot was found: %v", got)
	}
}
