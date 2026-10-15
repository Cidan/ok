package role

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// An installation that names no deployer deploys nothing: a landed CL's deployments are planned and
// none, no repository has a service, and what only a deployment could answer stays unavailable.
func TestNoDeployer(t *testing.T) {
	ctx := context.Background()
	d := noDeployer{deploypb.NewDeploymentsClient(unavailable{what: "ok's deployer"})}
	ds, err := d.ListDeployments(ctx, &deploypb.ListDeploymentsRequest{RepositoryId: 1})
	if err != nil || !ds.GetPlanned() || len(ds.GetDeployments()) != 0 {
		t.Errorf("ListDeployments = %v, %v", ds, err)
	}
	if sv, err := d.ListServices(ctx, &deploypb.ListServicesRequest{}); err != nil || len(sv.GetServices()) != 0 {
		t.Errorf("ListServices = %v, %v", sv, err)
	}
	if _, err := d.GetDeployment(ctx, &deploypb.GetDeploymentRequest{Id: 1}); status.Code(err) != codes.Unavailable {
		t.Errorf("GetDeployment: %v", err)
	}
}
