package service

import (
	"context"
	"errors"
	"net"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"github.com/Cidan/ok/internal/deploy/kubesim"
	"github.com/Cidan/ok/internal/deploy/observe"
	"github.com/Cidan/ok/internal/deploy/store"
	"github.com/Cidan/ok/internal/deploy/store/postgres"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/workflow"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

const (
	self    = "deploy.ok.robot.example.com"
	control = "control.ok.robot.example.com"
	admin   = "ada.human.example.com"
	person  = "alice.human.example.com"
)

// named attaches the principal a request's "principal" metadata names, as ok's runtime attaches the caller it resolved.
func named(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if p := md.Get("principal"); len(p) == 1 {
			// A principal's stable ID is its name here.
			return authn.NewContext(ctx, authn.Caller{Principal: p[0], ID: p[0]})
		}
	}
	return ctx
}

func as(p string) context.Context {
	if p == "" {
		return context.Background()
	}
	return metadata.AppendToOutgoingContext(context.Background(), "principal", p)
}

func newService(t *testing.T, objs ...runtime.Object) (deploypb.DeploymentsClient, *store.Store, *kubesim.Cluster) {
	t.Helper()
	pool := testdb.Pool(t)
	b, err := postgres.Open(context.Background(), pool)
	st := store.New(b)
	if err != nil {
		t.Fatal(err)
	}
	cluster := kubesim.New(t, objs...)
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		return h(named(ctx), req)
	}))
	deploypb.RegisterDeploymentsServer(srv, New(st))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return deploypb.NewDeploymentsClient(conn), st, cluster
}

func commit(b byte) []byte {
	c := make([]byte, 32)
	c[0] = b
	return c
}

// A deployment as ok control reads it, and what each caller may do with it.
func TestDeployments(t *testing.T) {
	ctx := context.Background()
	c, st, _ := newService(t)
	for i, ev := range []store.FeedEvent{
		{Seq: 1, Repository: 1, Created: true},
		{Seq: 2, Repository: 1, Head: commit(1), HeadSeq: 1},
		{Seq: 3, Repository: 1, Head: commit(2), HeadSeq: 2},
	} {
		if err := st.Apply(ctx, ev); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if err := st.ApplyDirectory(ctx, store.DirectoryEvent{Seq: 1, Repository: 1, FullName: "home"}); err != nil {
		t.Fatal(err)
	}
	web := workflow.Deploy{File: ".ok/workflow/d.yaml", Name: "web", Migrate: []workflow.Ref{{Target: "//web/schema:apply"}},
		Images:   []workflow.Image{{Container: "web", Target: "//web:push"}},
		Workload: &workflow.Workload{Namespace: "web", Kind: workflow.Deployment, Name: "web"}}
	if _, err := st.RecordPlan(ctx, store.PlanResult{Repository: 1, Head: commit(2), HeadSeq: 2, By: self,
		Deployments: []store.NewDeployment{{Deploy: web, Why: "first", Wave: 1}}}); err != nil {
		t.Fatal(err)
	}
	open, _ := st.Unfinished(ctx)
	id := open[0].ID
	if err := st.Start(ctx, id, ""); err != nil {
		t.Fatal(err)
	}
	report := `{"database":"web","server":"db:5432","steps":[{"description":"drop column public.t.old","sql":"ALTER TABLE t DROP COLUMN old",
		"hazards":[{"class":"DELETES_DATA","detail":"drops public.t.old","gated":true}]}],"refused":["DELETES_DATA"],"executed":false,"error":"refusing"}`
	if err := st.SetMigration(ctx, id, 0, store.MigrationWaiting, "refusing to apply", []byte(report), []string{"DELETES_DATA"}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetStage(ctx, id, store.Hazard); err != nil {
		t.Fatal(err)
	}
	if err := st.SetRoll(ctx, id, []byte(`{"stage":"canary","live":"blue","new":"green","replicas":3,"started":"2026-09-29T12:00:00Z"}`)); err != nil {
		t.Fatal(err)
	}

	// Only an identified caller reads: which one, ok's runtime decides (TestCallers).
	for p, want := range map[string]codes.Code{"": codes.Unauthenticated, control: codes.OK} {
		if _, err := c.ListDeployments(as(p), &deploypb.ListDeploymentsRequest{RepositoryId: 1}); status.Code(err) != want {
			t.Errorf("%q listing: %v, want %s", p, err, want)
		}
	}
	// A CL's landed commit, commit 1, is covered; a commit not on main is not.
	list, err := c.ListDeployments(as(control), &deploypb.ListDeploymentsRequest{RepositoryId: 1, CommitId: commit(1)})
	if err != nil || len(list.GetDeployments()) != 1 {
		t.Fatalf("covering commit 1: %v (%v)", list, err)
	}
	if !list.GetPlanned() {
		t.Errorf("a planned commit's deployments do not say it is planned")
	}
	if l, _ := c.ListDeployments(as(control), &deploypb.ListDeploymentsRequest{RepositoryId: 1, CommitId: commit(9)}); len(l.GetDeployments()) != 0 || l.GetPlanned() {
		t.Errorf("a commit not on main is covered")
	}
	d, err := c.GetDeployment(as(control), &deploypb.GetDeploymentRequest{Id: id})
	if err != nil {
		t.Fatal(err)
	}
	m := d.GetMigrations()[0]
	if d.GetStatus() != deploypb.Deployment_STATUS_RUNNING || d.GetStage() != deploypb.Deployment_STAGE_HAZARD || d.GetWorkload().GetName() != "web" ||
		m.GetStatus() != deploypb.Migration_STATUS_WAITING || len(m.GetPlan().GetSteps()) != 1 || !m.GetPlan().GetSteps()[0].GetHazards()[0].GetGated() ||
		d.GetRoll().GetStage() != "canary" || d.GetRoll().GetReplicas() != 3 || d.GetRoll().GetStageStarted() == nil ||
		len(d.GetEvents()) < 2 || !strings.HasPrefix(d.GetEvents()[0].GetMessage(), "planned at") {
		t.Errorf("the deployment %v", d)
	}

	// ok control acts for the person it names.
	if _, err := c.AllowHazards(as(control), &deploypb.AllowHazardsRequest{Id: id}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("control naming no one: %v", err)
	}
	d, err = c.AllowHazards(as(control), &deploypb.AllowHazardsRequest{Id: id, ForPersonId: admin})
	if err != nil || d.GetMigrations()[0].GetAllowedById() != admin || d.GetStage() != deploypb.Deployment_STAGE_MIGRATE {
		t.Fatalf("allowing: %v (%v)", d, err)
	}
	if _, err := c.AllowHazards(as(control), &deploypb.AllowHazardsRequest{Id: id, ForPersonId: admin}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("allowing twice: %v", err)
	}
	if _, err := c.RetryDeployment(as(control), &deploypb.RetryDeploymentRequest{Id: id, ForPersonId: person}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("retrying a running deployment: %v", err)
	}
	d, err = c.HaltDeployment(as(control), &deploypb.HaltDeploymentRequest{Id: id, ForPersonId: person})
	if err != nil || d.GetHaltById() != person {
		t.Fatalf("halting: %v (%v)", d, err)
	}
	if err := st.Finish(ctx, id, store.Halted, "halted"); err != nil {
		t.Fatal(err)
	}
	d, err = c.RetryDeployment(as(control), &deploypb.RetryDeploymentRequest{Id: id, ForPersonId: admin})
	if err != nil || d.GetRetryOf() != id || d.GetRequestedById() != admin || d.GetStatus() != deploypb.Deployment_STATUS_WAITING {
		t.Errorf("retrying: %v (%v)", d, err)
	}
	if _, err := c.GetDeployment(as(control), &deploypb.GetDeploymentRequest{Id: 999}); status.Code(err) != codes.NotFound {
		t.Errorf("a deployment that does not exist: %v", err)
	}
}

// Services are the deploys that roll a workload, as main last declared them, each with what ok
// deployed, its latest deployment, and what the cluster runs as the observer last read it: nothing
// before it has, and what it read — the workload, or why the cluster could not be read — after.
// The service never reads the cluster itself.
func TestServices(t *testing.T) {
	ctx := context.Background()
	const ref = "registry.example.com/smoke/serve@sha256:aa"
	c, st, cluster := newService(t, kubesim.Deployment("smoke", "serve", 2, ref))
	cluster.Kube.PrependReactor("get", "statefulsets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("the API server is down")
	})
	for i, ev := range []store.FeedEvent{
		{Seq: 1, Repository: 1, Created: true},
		{Seq: 2, Repository: 1, Head: commit(1), HeadSeq: 1},
		{Seq: 3, Repository: 1, Head: commit(2), HeadSeq: 2},
	} {
		if err := st.Apply(ctx, ev); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if err := st.ApplyDirectory(ctx, store.DirectoryEvent{Seq: 1, Repository: 1, FullName: "smoke"}); err != nil {
		t.Fatal(err)
	}
	serve := workflow.Deploy{File: ".ok/workflow/deploy.yaml", Name: "serve", Images: []workflow.Image{{Container: "serve", Target: "//serve:push"}},
		Workload: &workflow.Workload{Namespace: "smoke", Kind: workflow.Deployment, Name: "serve"}}
	gone := workflow.Deploy{File: ".ok/workflow/deploy.yaml", Name: "gone", Images: []workflow.Image{{Container: "gone", Target: "//gone:push"}},
		Workload: &workflow.Workload{Namespace: "smoke", Kind: workflow.StatefulSet, Name: "gone"}, Group: "old"}
	data := workflow.Deploy{File: ".ok/workflow/deploy.yaml", Name: "data", Migrate: []workflow.Ref{{Target: "//data:apply"}}}
	if _, err := st.RecordPlan(ctx, store.PlanResult{Repository: 1, Head: commit(1), HeadSeq: 1, By: self,
		Deployments: []store.NewDeployment{{Deploy: serve, Why: "first", Wave: 1}}, Services: []workflow.Deploy{serve, gone, data}}); err != nil {
		t.Fatal(err)
	}
	open, _ := st.Unfinished(ctx)
	first := open[0].ID
	if err := st.Start(ctx, first, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.SetPushed(ctx, first, "serve", ref); err != nil {
		t.Fatal(err)
	}
	if err := st.Finish(ctx, first, store.Succeeded, ""); err != nil {
		t.Fatal(err)
	}
	// A later plan no longer declares gone, which keeps the group it last had, and puts serve in a
	// group; its deployment of serve fails.
	grouped := serve
	grouped.Group = "edge"
	if _, err := st.RecordPlan(ctx, store.PlanResult{Repository: 1, Head: commit(2), HeadSeq: 2, BaseSeq: 1, By: self,
		Deployments: []store.NewDeployment{{Deploy: serve, Why: "again", Wave: 1}}, Services: []workflow.Deploy{grouped, data}}); err != nil {
		t.Fatal(err)
	}
	open, _ = st.Unfinished(ctx)
	second := open[0].ID
	if err := st.Start(ctx, second, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.Finish(ctx, second, store.Failed, "rolled back"); err != nil {
		t.Fatal(err)
	}

	unread, err := c.ListServices(as(control), &deploypb.ListServicesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, sv := range unread.GetServices() {
		if r := sv.GetRunning(); r.GetFound() || r.GetError() != "" || len(r.GetPods()) != 0 {
			t.Errorf("%s before the observer read it: %v", sv.GetName(), r)
		}
	}
	if err := (&observe.Observer{Store: st, Kube: cluster.Kube}).Round(ctx); err != nil {
		t.Fatal(err)
	}
	list, err := c.ListServices(as(control), &deploypb.ListServicesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetServices()) != 2 {
		t.Fatalf("services %v", list.GetServices())
	}
	g, s := list.GetServices()[0], list.GetServices()[1]
	if g.GetName() != "gone" || g.GetDeclared() || g.GetGroup() != "old" || g.GetDeployed() != nil || g.GetLatest() != nil || g.GetRunning().GetFound() || g.GetWorkload().GetKind() != "statefulset" ||
		g.GetRunning().GetError() != "the API server is down" {
		t.Errorf("gone %v", g)
	}
	r := s.GetRunning()
	if s.GetName() != "serve" || !s.GetDeclared() || s.GetGroup() != "edge" || s.GetRepository() != "smoke" || s.GetDeployed().GetId() != first ||
		s.GetDeployed().GetImages()[0].GetReference() != ref || s.GetLatest().GetId() != second ||
		s.GetLatest().GetStatus() != deploypb.Deployment_STATUS_FAILED {
		t.Errorf("serve %v", s)
	}
	if !r.GetFound() || r.GetLive() != "blue" || r.GetReplicas() != 2 || r.GetReady() != 2 || len(r.GetPods()) != 2 ||
		r.GetImages()[0].GetImage() != ref || r.GetPods()[0].GetImages()[0].GetImage() != ref || !r.GetPods()[0].GetReady() ||
		r.GetPods()[0].GetStarted() == nil || r.GetError() != "" {
		t.Errorf("serve running %v", r)
	}
	if l, _ := c.ListServices(as(control), &deploypb.ListServicesRequest{RepositoryId: 9}); len(l.GetServices()) != 0 {
		t.Errorf("another repository's services: %v", l.GetServices())
	}
	got, err := c.GetService(as(control), &deploypb.GetServiceRequest{Id: s.GetId()})
	if err != nil || len(got.GetRecent()) != 2 || got.GetRecent()[0].GetId() != second || got.GetRecent()[1].GetId() != first {
		t.Errorf("serve's recent deployments %v (%v)", got.GetRecent(), err)
	}
	if _, err := c.GetService(as(control), &deploypb.GetServiceRequest{Id: 999}); status.Code(err) != codes.NotFound {
		t.Errorf("a service that does not exist: %v", err)
	}
}

// TestCallers pins who may call the deployer: only ok control, which ok's runtime holds to the
// protocol's ok.v1.callers.
func TestCallers(t *testing.T) {
	if got := authn.Callers("ok.deploy.v1.Deployments"); !slices.Equal(got, []okpb.Caller{okpb.Caller_CALLER_CONTROL}) {
		t.Errorf("callers %v, want ok control", got)
	}
}
