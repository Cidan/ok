package storetest

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
)

// An organization designates one of its own active robots its deploy agent, or none; a robot is
// the agent of one organization at most, and a deleted robot no one's; and an owner's cells are
// those holding its made repositories.
func testDeployAgents(t *testing.T, s *store.Store) {
	ctx := context.Background()
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
	agent, err := s.CreateOrganizationRobot(ctx, acme.ID, "agent")
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CreateOrganizationRobot(ctx, globex.ID, "agent")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.DeployAgentOf(ctx, acme.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("an organization with no agent: %v", err)
	}
	if err := s.SetDeployAgent(ctx, acme.ID, other.ID, ada.ID); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("designating another organization's robot: %v", err)
	}
	if err := s.SetDeployAgent(ctx, acme.ID, agent.ID, ada.ID); err != nil {
		t.Fatal(err)
	}
	if a, err := s.DeployAgentOf(ctx, acme.ID); err != nil || a.Robot != agent.ID || a.SetBy != ada.ID {
		t.Errorf("acme's agent: %+v, %v", a, err)
	}
	if a, err := s.AgentAccount(ctx, agent.ID); err != nil || a.Organization != acme.ID {
		t.Errorf("the organization acme's robot deploys for: %+v, %v", a, err)
	}
	if _, err := s.AgentAccount(ctx, other.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a robot designated by no one: %v", err)
	}

	for _, r := range []struct{ name, cell string }{{"billing", "c1"}, {"storefront", "c2"}, {"db", "c1"}} {
		repo, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: r.name, Propose: tenancy.Read, Cell: r.cell, CreatedBy: ada.ID})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ActivateRepository(ctx, repo.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.ReserveRepository(ctx, store.NewRepository{Owner: acme.ID, Name: "half", Propose: tenancy.Read, Cell: "c3", CreatedBy: ada.ID}); err != nil {
		t.Fatal(err)
	}
	if cells, err := s.OwnerCells(ctx, acme.ID); err != nil || !slices.Equal(cells, []string{"c1", "c2"}) {
		t.Errorf("acme's cells: %v, %v; want those of made repositories", cells, err)
	}

	// Deleted, the robot deploys for no one.
	p, err := s.PrincipalByID(ctx, agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRobot(ctx, p.Name); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AgentAccount(ctx, agent.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a deleted robot's organization: %v", err)
	}
	if _, err := s.DeployAgentOf(ctx, acme.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("acme's agent, deleted: %v", err)
	}
	if err := s.SetDeployAgent(ctx, acme.ID, "", ada.ID); err != nil {
		t.Fatal(err)
	}
}
