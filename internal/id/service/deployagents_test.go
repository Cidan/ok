package service_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// An organization's owners designate its deploy agent; its members read it; the deployer asks which
// organization a connecting robot is the agent of; and the agent asks which cells it serves — no
// one else.
func TestDeployAgents(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	if _, err := e.ada.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ada.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "acme", PrincipalId: e.beaID, Role: "member"}); err != nil {
		t.Fatal(err)
	}
	robot, err := e.ada.CreateOrganizationRobot(ctx, &identitypb.CreateOrganizationRobotRequest{Handle: "acme", Name: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	robotID, robotName := robot.GetPrincipal().GetId(), robot.GetPrincipal().GetName()
	repo, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "acme", Name: "storefront", ForPersonId: e.adaID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: repo.Id}); err != nil {
		t.Fatal(err)
	}
	ada, bea := identitypb.NewDeployAgentsClient(e.as(ada)), identitypb.NewDeployAgentsClient(e.as(bea))
	deployer, agent := identitypb.NewDeployAgentsClient(e.as(deploy)), identitypb.NewDeployAgentsClient(e.as(robotName))
	if _, err := bea.SetDeployAgent(ctx, &identitypb.SetDeployAgentRequest{Handle: "acme", RobotId: robotID}); code(err) != codes.PermissionDenied {
		t.Errorf("a member designating the agent: %v", err)
	}
	if _, err := agent.DeployCells(ctx, &identitypb.DeployCellsRequest{}); code(err) != codes.PermissionDenied {
		t.Errorf("a robot designated by no one asking its cells: %v", err)
	}
	a, err := ada.SetDeployAgent(ctx, &identitypb.SetDeployAgentRequest{Handle: "acme", RobotId: robotID})
	if err != nil || a.GetRobot() != robotName || a.GetHandle() != "acme" {
		t.Fatalf("SetDeployAgent = %v, %v", a, err)
	}
	if a, err := bea.GetDeployAgent(ctx, &identitypb.GetDeployAgentRequest{Key: &identitypb.GetDeployAgentRequest_Handle{Handle: "acme"}}); err != nil || a.GetRobotId() != robotID {
		t.Errorf("a member reading the agent: %v, %v", a, err)
	}
	if _, err := identitypb.NewDeployAgentsClient(e.as("cal.human.example.com")).GetDeployAgent(ctx,
		&identitypb.GetDeployAgentRequest{Key: &identitypb.GetDeployAgentRequest_Handle{Handle: "acme"}}); code(err) != codes.PermissionDenied {
		t.Errorf("a stranger reading the agent: %v", err)
	}
	if _, err := ada.GetDeployAgent(ctx, &identitypb.GetDeployAgentRequest{Key: &identitypb.GetDeployAgentRequest_RobotId{RobotId: robotID}}); code(err) != codes.PermissionDenied {
		t.Errorf("a person asking whose agent a robot is: %v", err)
	}
	if a, err := deployer.GetDeployAgent(ctx, &identitypb.GetDeployAgentRequest{Key: &identitypb.GetDeployAgentRequest_RobotId{RobotId: robotID}}); err != nil || a.GetHandle() != "acme" {
		t.Errorf("the deployer asking whose agent the robot is: %v, %v", a, err)
	}
	if a, err := deployer.GetDeployAgent(ctx, &identitypb.GetDeployAgentRequest{Key: &identitypb.GetDeployAgentRequest_AccountId{AccountId: a.GetAccountId()}}); err != nil || a.GetRobotId() != robotID {
		t.Errorf("the deployer asking acme's agent: %v, %v", a, err)
	}
	if r, err := agent.AgentRepository(ctx, &identitypb.RepositoryRef{Id: repo.Id}); err != nil || !r.GetOwned() || r.GetFullName() != "acme/storefront" {
		t.Errorf("the agent asking of its organization's repository: %v, %v", r, err)
	}
	mine, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "bea", Name: "notes", Visibility: "public", ForPersonId: e.beaID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: mine.Id}); err != nil {
		t.Fatal(err)
	}
	if r, err := agent.AgentRepository(ctx, &identitypb.RepositoryRef{Id: mine.Id}); err != nil || r.GetOwned() {
		t.Errorf("the agent asking of a public repository of another's: %v, %v", r, err)
	}
	if _, err := ada.AgentRepository(ctx, &identitypb.RepositoryRef{Id: repo.Id}); code(err) != codes.PermissionDenied {
		t.Errorf("a person asking as an agent: %v", err)
	}
	cells, err := agent.DeployCells(ctx, &identitypb.DeployCellsRequest{})
	if err != nil || len(cells.GetCells()) != 1 || cells.GetCells()[0] != repo.GetCell() {
		t.Errorf("the agent's cells: %v, %v; want %s's", cells, err, repo.GetCell())
	}
	if _, err := ada.SetDeployAgent(ctx, &identitypb.SetDeployAgentRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := deployer.GetDeployAgent(ctx, &identitypb.GetDeployAgentRequest{Key: &identitypb.GetDeployAgentRequest_RobotId{RobotId: robotID}}); code(err) != codes.NotFound {
		t.Errorf("a robot no longer designated: %v", err)
	}
}
