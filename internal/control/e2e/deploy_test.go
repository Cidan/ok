package e2e

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/control/db"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// deployer is ok's deployer as ok control sees it: deployments a test sets, covering the
// commits it names, and every action ok control asks of it.
type deployer struct {
	deploypb.UnimplementedDeploymentsServer
	mu          sync.Mutex
	deployments map[int64]*deploypb.Deployment
	covers      map[int64][][]byte // deployment → the commits it covers
	asked       []string           // "retry 3 for alice.human.example.com"
	down        bool               // it cannot be reached
	unplanned   bool               // its plans have not reached the commits it is asked about
	next        int64
	services    []*deploypb.Service
}

func (d *deployer) ListServices(_ context.Context, req *deploypb.ListServicesRequest) (*deploypb.ListServicesResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.down {
		return nil, status.Error(codes.Unavailable, "the deployer is down")
	}
	out := &deploypb.ListServicesResponse{}
	for _, sv := range d.services {
		c := proto.Clone(sv).(*deploypb.Service)
		c.Recent = nil
		out.Services = append(out.Services, c)
	}
	return out, nil
}

func (d *deployer) GetService(_ context.Context, req *deploypb.GetServiceRequest) (*deploypb.Service, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, sv := range d.services {
		if sv.GetId() == req.GetId() {
			return proto.Clone(sv).(*deploypb.Service), nil
		}
	}
	return nil, status.Error(codes.NotFound, "no such service")
}

// startDeployer serves the deployer's stand-in on a loopback port.
func startDeployer(t *testing.T) (*deployer, string) {
	t.Helper()
	d := &deployer{deployments: map[int64]*deploypb.Deployment{}, covers: map[int64][][]byte{}, next: 100}
	srv := grpc.NewServer()
	deploypb.RegisterDeploymentsServer(srv, d)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return d, lis.Addr().String()
}

// add sets a deployment, covering commits.
func (d *deployer) add(dep *deploypb.Deployment, commits ...[]byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.deployments[dep.GetId()] = dep
	d.covers[dep.GetId()] = commits
}

func (d *deployer) actions() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.asked...)
}

func (d *deployer) ListDeployments(_ context.Context, req *deploypb.ListDeploymentsRequest) (*deploypb.ListDeploymentsResponse, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.down {
		return nil, status.Error(codes.Unavailable, "the deployer is down")
	}
	out := &deploypb.ListDeploymentsResponse{Planned: !d.unplanned}
	for id, dep := range d.deployments {
		if dep.GetRepositoryId() != req.GetRepositoryId() {
			continue
		}
		for _, c := range d.covers[id] {
			if bytes.Equal(c, req.GetCommitId()) {
				out.Deployments = append(out.Deployments, proto.Clone(dep).(*deploypb.Deployment))
			}
		}
	}
	return out, nil
}

func (d *deployer) GetDeployment(_ context.Context, req *deploypb.GetDeploymentRequest) (*deploypb.Deployment, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	dep, ok := d.deployments[req.GetId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such deployment")
	}
	return proto.Clone(dep).(*deploypb.Deployment), nil
}

func (d *deployer) RetryDeployment(_ context.Context, req *deploypb.RetryDeploymentRequest) (*deploypb.Deployment, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.asked = append(d.asked, "retry "+itoa(req.GetId())+" for "+req.GetForPerson())
	old := d.deployments[req.GetId()]
	d.next++
	dep := proto.Clone(old).(*deploypb.Deployment)
	dep.Id, dep.RetryOf, dep.Status, dep.Stage, dep.Reason = d.next, old.GetId(), deploypb.Deployment_STATUS_WAITING, deploypb.Deployment_STAGE_QUEUED, ""
	d.deployments[dep.Id] = dep
	d.covers[dep.Id] = d.covers[old.GetId()]
	return dep, nil
}

func (d *deployer) HaltDeployment(_ context.Context, req *deploypb.HaltDeploymentRequest) (*deploypb.Deployment, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.asked = append(d.asked, "halt "+itoa(req.GetId())+" for "+req.GetForPerson())
	dep := d.deployments[req.GetId()]
	if dep.GetHaltBy() != "" {
		return nil, status.Error(codes.FailedPrecondition, "deployment is halting already")
	}
	dep.HaltBy = req.GetForPerson()
	return proto.Clone(dep).(*deploypb.Deployment), nil
}

func (d *deployer) AllowHazards(_ context.Context, req *deploypb.AllowHazardsRequest) (*deploypb.Deployment, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.asked = append(d.asked, "allow "+itoa(req.GetId())+" for "+req.GetForPerson())
	dep := d.deployments[req.GetId()]
	dep.Stage = deploypb.Deployment_STAGE_MIGRATE
	dep.Migrations[0].Status, dep.Migrations[0].AllowedBy, dep.Migrations[0].Allowed = deploypb.Migration_STATUS_PENDING, req.GetForPerson(), dep.Migrations[0].GetRefused()
	return proto.Clone(dep).(*deploypb.Deployment), nil
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// dbChangeList reads a CL's projection in ok control's database.
func dbChangeList(e *env, repo string, n int64) (db.ChangeList, bool, error) {
	r, ok, err := e.db.RepositoryByName(context.Background(), repo)
	if err != nil || !ok {
		return db.ChangeList{}, false, err
	}
	return e.db.GetChangeList(context.Background(), r.ID, n)
}

// TestDeployments shows a merged CL's deployments, and lets the CL's author retry and halt one,
// and a repository admin allow a migration's hazards — no one else. dana, an operator, made the
// repository; bob wrote the CL; carol is neither.
func TestDeployments(t *testing.T) {
	e := newEnv(t)
	e.post(dana, "/repositories/new", url.Values{"name": {"svc"}})
	b := e.person(bob)
	b.jj("", "clone", "svc")
	b.write("svc/web/web.go", "package web\n")
	b.jj("svc", "describe", "-m", "add web")
	n := uploaded(b.jj("svc", "cl", "--ready"))[0]
	path := clPath("svc", n)
	e.post(alice, path+"/reply", grant("LGTM", "Approval"))
	e.prePassed("svc", n)
	e.submitted(bob, path, "submit")
	cl, _, err := dbChangeList(e, "svc", n)
	if err != nil {
		t.Fatal(err)
	}
	repo := cl.Repo

	// Its Deployments tab: before the deployer has planned it, it says so and refreshes itself;
	// planned with no deployment covering it, it is final.
	deploys := func(p page) (body string, live bool) {
		t.Helper()
		i := strings.Index(p.body, `id="cl-deploys"`)
		if i < 0 {
			t.Fatalf("no Deployments tab: %s", excerpt(p.body))
		}
		open := p.body[i : i+strings.IndexByte(p.body[i:], '>')]
		return p.body[i:], strings.Contains(open, `hx-get="`+path+`/deployments"`)
	}
	e.deployer.mu.Lock()
	e.deployer.unplanned = true
	e.deployer.mu.Unlock()
	if body, live := deploys(e.get(bob, path)); !strings.Contains(body, "The deployer has not planned this change yet.") || !live {
		t.Errorf("a merged CL the deployer has not planned: live %v, %s", live, excerpt(body))
	}
	e.deployer.mu.Lock()
	e.deployer.unplanned = false
	e.deployer.mu.Unlock()
	if body, live := deploys(e.get(bob, path)); !strings.Contains(body, "Nothing it changed is deployed.") || live {
		t.Fatalf("a merged CL with no deployments: live %v, %s", live, excerpt(body))
	}
	// The tab's own refresh renders the region alone.
	if p := e.do(bob, http.MethodGet, path+"/deployments", nil, map[string]string{"HX-Request": "true", "HX-Target": "div#cl-deploys"}); !strings.HasPrefix(p.body, `<div id="cl-deploys"`) {
		t.Errorf("the Deployments tab's refresh: %s", excerpt(p.body))
	}
	now := timestamppb.New(time.Now())
	e.deployer.add(&deploypb.Deployment{Id: 7, RepositoryId: repo, PlanId: 3, Deploy: "//.ok/workflow/d.yaml:deploy:web", Name: "web",
		CommitId: cl.LandedCommit, Why: "container web's image //web:push is affected", Wave: 1, RequestedBy: "deploy.ok.robot.example.com",
		Workload: &deploypb.Workload{Namespace: "web", Kind: "deployment", Name: "web"},
		Status:   deploypb.Deployment_STATUS_FAILED, Stage: deploypb.Deployment_STAGE_DONE, Reason: "rolled back: canary: pod web-green-3: container web: CrashLoopBackOff",
		CreatedAt: now, StartedAt: now, FinishedAt: now,
		Images: []*deploypb.Image{{Container: "web", Target: "//web:push", PushRun: 41, Reference: "registry.example.com/apps/web@sha256:ab"}},
		Roll:   &deploypb.Roll{Stage: "done", Outcome: "rolled back", Live: "blue", New: "green", Replicas: 3},
		Events: []*deploypb.Event{{At: now, Message: "planned at abc: container web's image //web:push is affected"}, {At: now, Message: "canary: one green pod beside 3 blue"}},
	}, cl.LandedCommit)
	e.deployer.add(&deploypb.Deployment{Id: 8, RepositoryId: repo, PlanId: 3, Deploy: "//.ok/workflow/d.yaml:deploy:data", Name: "data",
		CommitId: cl.LandedCommit, Why: "its migration is affected", Wave: 2, RequestedBy: "deploy.ok.robot.example.com",
		Status: deploypb.Deployment_STATUS_RUNNING, Stage: deploypb.Deployment_STAGE_HAZARD, CreatedAt: now, StartedAt: now,
		Migrations: []*deploypb.Migration{{Target: "//data/schema:apply", Status: deploypb.Migration_STATUS_WAITING, Refused: []string{"DELETES_DATA"},
			Plan: &deploypb.MigrationPlan{Database: "data", Steps: []*deploypb.MigrationPlan_Step{{Description: "drop column public.t.old",
				Sql: "ALTER TABLE public.t DROP COLUMN old", Hazards: []*deploypb.MigrationPlan_Hazard{{Class: "DELETES_DATA", Detail: "drops public.t.old", Gated: true}}}}}}},
	}, cl.LandedCommit)

	p := e.get(carol, path)
	for _, want := range []string{"web", "deployment web/web", "Failed", "rolled back: canary", "data", "Needs Allowance", "Waiting for an Allowance",
		`href="/svc/deploy/7?cl=` + itoa(n) + `"`} {
		if !p.has(want) {
			t.Errorf("the CL's deployments lack %q: %s", want, excerpt(p.body))
		}
	}

	// The failed deployment's page: its author may retry it; someone else may not.
	dpath := "/svc/deploy/7?cl=" + itoa(n)
	p = e.get(bob, dpath)
	for _, want := range []string{"Deployment 7", "CL #" + itoa(n), "registry.example.com/apps/web@sha256:ab", "Push Run 41", "Rolled back · green replacing blue · 3 replicas",
		"canary: one green pod beside 3 blue", "/svc/deploy/7/retry?cl="} {
		if !p.has(want) {
			t.Errorf("the deployment page lacks %q: %s", want, excerpt(p.body))
		}
	}
	if p := e.get(carol, dpath); p.has("/retry") || p.has("/halt") {
		t.Errorf("someone else is offered actions: %s", excerpt(p.body))
	}
	if got := notice(e.post(carol, "/svc/deploy/7/retry?cl="+itoa(n), nil)); got != "Only a repository admin or the author of a CL it deploys can retry this deployment" {
		t.Errorf("someone else retrying: %q", got)
	}
	// Without the CL it came from, the author is no one special.
	if p := e.get(bob, "/svc/deploy/7"); p.has("/retry") {
		t.Errorf("the author is offered Retry away from their CL")
	}
	p = e.do(bob, http.MethodPost, "/svc/deploy/7/retry?cl="+itoa(n), url.Values{}, map[string]string{"HX-Request": "true", "HX-Target": "div#repo-main"})
	if !strings.Contains(p.header.Get("HX-Location"), "/svc/deploy/101?cl="+itoa(n)) {
		t.Fatalf("retrying: %d %q", p.status, p.header.Get("HX-Location"))
	}
	if got := e.deployer.actions(); len(got) != 1 || got[0] != "retry 7 for "+bob {
		t.Errorf("the deployer was asked %v", got)
	}

	// The migration's hazards: shown, and allowed by a repository admin only.
	p = e.get(dana, "/svc/deploy/8")
	for _, want := range []string{"//data/schema:apply", "DELETES_DATA", "ALTER TABLE public.t DROP COLUMN old", "Its plan carries gated hazards", "/svc/deploy/8/allow", "/svc/deploy/8/halt"} {
		if !p.has(want) {
			t.Errorf("the waiting deployment lacks %q: %s", want, excerpt(p.body))
		}
	}
	if e.get(bob, "/svc/deploy/8?cl="+itoa(n)).has("/allow") {
		t.Errorf("the author is offered Allow")
	}
	if got := notice(e.post(bob, "/svc/deploy/8/allow?cl="+itoa(n), nil)); got != "Only a repository admin can allow this deployment" {
		t.Errorf("the author allowing: %q", got)
	}
	if p := e.post(dana, "/svc/deploy/8/allow", nil); !p.has("dana allowed DELETES_DATA") {
		t.Errorf("an admin allowing: %s", excerpt(p.body))
	}
	if p := e.post(bob, "/svc/deploy/8/halt?cl="+itoa(n), nil); !p.has("Halting") {
		t.Errorf("the author halting: %s", excerpt(p.body))
	}
	if got := notice(e.post(dana, "/svc/deploy/8/halt", nil)); got != "Deployment is halting already" {
		t.Errorf("halting twice: %q", got)
	}
	if got := e.deployer.actions(); strings.Join(got, "; ") != "retry 7 for "+bob+"; allow 8 for "+dana+"; halt 8 for "+bob+"; halt 8 for "+dana {
		t.Errorf("the deployer was asked %v", got)
	}

	// A deployment of another repository is not this one's; the deployer down still shows the CL.
	if p := e.do(bob, http.MethodGet, "/svc/deploy/999", nil, nil); p.status != http.StatusNotFound {
		t.Errorf("a deployment that does not exist: %d", p.status)
	}
	e.deployer.mu.Lock()
	e.deployer.down = true
	e.deployer.mu.Unlock()
	if p := e.get(bob, path); !p.has("The deployer could not be reached.") {
		t.Errorf("the deployer down: %s", excerpt(p.body))
	}
}
