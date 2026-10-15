package web

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/control/db"
	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/pages/servicegroup"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// services is the Services page: every group of services ok deploys, with its status
// (docs/deploy.md, "Services").
func (s *Server) services(w http.ResponseWriter, r *http.Request) {
	groups, err := s.serviceGroups(r.Context())
	if errors.Is(err, errDeployer) {
		slog.Warn("ok control: listing services", "err", err)
		pages.ServeServices(w, r, pages.ServicesView{Error: "The deployer could not be reached."})
		return
	}
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeServices(w, r, pages.ServicesView{Groups: groups})
}

// serviceGroup is a named group's page: its overview, or — a group of one — its service.
func (s *Server) serviceGroup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	groups, err := s.serviceGroups(ctx)
	if err != nil {
		s.groupFailed(w, r, err)
		return
	}
	g, ok := servicegroup.FindNamed(groups, chi.URLParam(r, "name"))
	if !ok {
		fail(w, r, review.ErrNotFound)
		return
	}
	if !g.Overview() {
		s.serveService(w, r, g, g.Members[0].ID)
		return
	}
	recent, err := s.groupRecent(ctx, g)
	if err != nil {
		s.groupFailed(w, r, err)
		return
	}
	pages.ServeServiceGroup(w, r, pages.ServiceGroupView{Group: g, Recent: recent})
}

// service is a service's page: its group's, opened on it.
func (s *Server) service(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id < 1 {
		fail(w, r, review.ErrNotFound)
		return
	}
	groups, err := s.serviceGroups(r.Context())
	if err != nil {
		s.groupFailed(w, r, err)
		return
	}
	g, ok := servicegroup.Find(groups, id)
	if !ok {
		fail(w, r, review.ErrNotFound)
		return
	}
	s.serveService(w, r, g, id)
}

// errDeployer wraps a failure to ask the deployer.
var errDeployer = errors.New("the deployer could not be reached")

// groupFailed answers a group's page that could not be read: a live refresh keeps its place and
// says why, and anything else is an error page.
func (s *Server) groupFailed(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, errDeployer) {
		slog.Warn("ok control: reading a service group", "err", err)
		pages.ServeServiceGroupUnavailable(w, r, "The deployer could not be reached.")
		return
	}
	fail(w, r, err)
}

// serviceGroups reads every service from the deployer, as the Services pages group them.
func (s *Server) serviceGroups(ctx context.Context) ([]servicegroup.Group, error) {
	resp, err := s.deploys.ListServices(ctx, &deploypb.ListServicesRequest{})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errDeployer, err)
	}
	cls, err := s.landedCLs(ctx, resp.GetServices())
	if err != nil {
		return nil, err
	}
	sums := make([]servicegroup.Service, 0, len(resp.GetServices()))
	for _, sv := range resp.GetServices() {
		sums = append(sums, serviceSummary(sv, cls))
	}
	return servicegroup.Of(sums), nil
}

// serviceSummary is a service as a group holds it: its status, what ok deployed, and how many
// of its pods are Ready.
func serviceSummary(sv *deploypb.Service, cls map[int64]map[string]int64) servicegroup.Service {
	st := serviceStatus(sv)
	run := sv.GetRunning()
	out := servicegroup.Service{
		ID: sv.GetId(), Name: sv.GetName(), Group: sv.GetGroup(), Repository: sv.GetRepository(), RepositoryHref: repoLink(sv.GetRepository()),
		Workload: workloadName(sv.GetWorkload()), Declared: sv.GetDeclared(), StatusLabel: st.label, StatusKind: st.kind, Reason: st.reason,
		Found: run.GetFound(), Ready: run.GetReady(), Replicas: run.GetReplicas(),
	}
	if d := sv.GetDeployed(); d != nil {
		var versions []string
		for _, im := range d.GetImages() {
			versions = append(versions, shortImage(im.GetReference()))
		}
		out.Version, out.DeployedID = strings.Join(versions, ", "), d.GetId()
		out.Commit, out.CommitHref = scm.Short(d.GetCommitId()), commitHref(sv.GetRepository(), d.GetCommitId())
		if n := cls[sv.GetRepositoryId()][string(d.GetCommitId())]; n != 0 {
			out.CL, out.CLHref = n, clHref(sv.GetRepository(), n)
		}
		out.Age, out.AgeTitle = age(deploymentTime(d))
	}
	return out
}

// groupRecentLimit is how many of a group's latest deployments its overview shows.
const groupRecentLimit = 10

// groupRecent is a group's latest deployments, across its services, newest first.
func (s *Server) groupRecent(ctx context.Context, g servicegroup.Group) ([]pages.ServiceDeployment, error) {
	svs := make([]*deploypb.Service, len(g.Members))
	errs := make([]error, len(g.Members))
	var wg sync.WaitGroup
	for i, m := range g.Members {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svs[i], errs[i] = s.deploys.GetService(ctx, &deploypb.GetServiceRequest{Id: m.ID})
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errDeployer, err)
		}
	}
	cls, err := s.landedCLs(ctx, svs)
	if err != nil {
		return nil, err
	}
	type dep struct {
		sv *deploypb.Service
		d  *deploypb.Deployment
	}
	var all []dep
	for _, sv := range svs {
		for _, d := range sv.GetRecent() {
			all = append(all, dep{sv, d})
		}
	}
	slices.SortFunc(all, func(a, b dep) int { return cmp.Compare(b.d.GetId(), a.d.GetId()) })
	out := make([]pages.ServiceDeployment, 0, min(len(all), groupRecentLimit))
	for _, x := range all[:min(len(all), groupRecentLimit)] {
		out = append(out, serviceDeployment(x.sv, x.d, cls))
	}
	return out, nil
}

// serveService serves a service's page in its group: what the deployer says of it, and the
// group's services beside it.
func (s *Server) serveService(w http.ResponseWriter, r *http.Request, g servicegroup.Group, id int64) {
	ctx := r.Context()
	sv, err := s.deploys.GetService(ctx, &deploypb.GetServiceRequest{Id: id})
	if status.Code(err) == codes.NotFound {
		fail(w, r, review.ErrNotFound)
		return
	}
	if err != nil {
		s.groupFailed(w, r, fmt.Errorf("%w: %w", errDeployer, err))
		return
	}
	cls, err := s.landedCLs(ctx, []*deploypb.Service{sv})
	if err != nil {
		fail(w, r, err)
		return
	}
	deployment := func(d *deploypb.Deployment) *pages.ServiceDeployment {
		if d == nil {
			return nil
		}
		out := serviceDeployment(sv, d, cls)
		return &out
	}
	st := serviceStatus(sv)
	run := sv.GetRunning()
	v := pages.ServiceView{
		ID: sv.GetId(), Name: sv.GetName(), Deploy: sv.GetDeploy(), Repository: sv.GetRepository(), RepositoryHref: repoLink(sv.GetRepository()),
		Workload: workloadName(sv.GetWorkload()), Declared: sv.GetDeclared(), StatusLabel: st.label, StatusKind: st.kind, Reason: st.reason,
		Found: run.GetFound(), Live: run.GetLive(), Ready: run.GetReady(), Replicas: run.GetReplicas(),
		Deployed: deployment(sv.GetDeployed()), Latest: deployment(sv.GetLatest()),
	}
	deployed := deployedImages(sv.GetDeployed())
	for _, im := range run.GetImages() {
		ref, ok := deployed[im.GetContainer()]
		v.Images = append(v.Images, pages.ServiceImage{Container: im.GetContainer(), Deployed: ref, Running: im.GetImage(),
			Matches: !ok || sameImage(ref, im.GetImage())})
	}
	for _, p := range run.GetPods() {
		pod := pages.ServicePod{Name: p.GetName(), Copy: p.GetCopy(), Ready: p.GetReady(), Restarts: p.GetRestarts(), Reason: p.GetReason(),
			Terminating: p.GetTerminating()}
		if p.GetStarted() != nil {
			pod.Age, pod.AgeTitle = age(p.GetStarted().AsTime())
		}
		for _, im := range p.GetImages() {
			ref, ok := deployed[im.GetContainer()]
			pod.Images = append(pod.Images, pages.ServicePodImage{Container: im.GetContainer(), Image: im.GetImage(),
				Matches: !ok || sameImage(ref, im.GetImage())})
		}
		v.Pods = append(v.Pods, pod)
	}
	for _, d := range sv.GetRecent() {
		v.Recent = append(v.Recent, *deployment(d))
	}
	pages.ServeServiceGroup(w, r, pages.ServiceGroupView{Group: g, Selected: id, Service: &v})
}

// serviceDeployment is a deployment of a service: its status, stage, and why; the commit it
// deployed and the CL that landed it.
func serviceDeployment(sv *deploypb.Service, d *deploypb.Deployment, cls map[int64]map[string]int64) pages.ServiceDeployment {
	repo := sv.GetRepository()
	label, kind := deploymentStatus(d)
	out := pages.ServiceDeployment{ID: d.GetId(), Service: sv.GetName(), Href: deploymentHref(repo, d.GetId(), 0), StatusLabel: label, StatusKind: kind,
		Stage: stageLabel(d), Reason: d.GetReason(), Commit: scm.Short(d.GetCommitId()), CommitHref: commitHref(repo, d.GetCommitId())}
	if n := cls[sv.GetRepositoryId()][string(d.GetCommitId())]; n != 0 {
		out.CL, out.CLHref = n, clHref(repo, n)
	}
	out.Age, out.AgeTitle = age(deploymentTime(d))
	return out
}

// landedCLs maps the commits services' deployments deployed to the CLs that landed them, by
// repository.
func (s *Server) landedCLs(ctx context.Context, svs []*deploypb.Service) (map[int64]map[string]int64, error) {
	commits := map[int64][][]byte{}
	for _, sv := range svs {
		for _, d := range append([]*deploypb.Deployment{sv.GetDeployed(), sv.GetLatest()}, sv.GetRecent()...) {
			if d != nil {
				commits[sv.GetRepositoryId()] = append(commits[sv.GetRepositoryId()], d.GetCommitId())
			}
		}
	}
	out := map[int64]map[string]int64{}
	for repo, cs := range commits {
		m, err := db.LandedBy(ctx, s.db.Q(), repo, cs)
		if err != nil {
			return nil, err
		}
		out[repo] = m
	}
	return out, nil
}

// serviceState is a service's status: its label, its tone's key, and why.
type serviceState struct{ label, kind, reason string }

// serviceStatus derives a service's status, the first that holds: a deployment under way, the
// last deployment failed, never deployed, the cluster unreadable, too few Ready pods (or no
// workload), images other than those ok deployed, or healthy.
func serviceStatus(sv *deploypb.Service) serviceState {
	run, latest, deployed := sv.GetRunning(), sv.GetLatest(), sv.GetDeployed()
	switch {
	case latest != nil && (latest.GetStatus() == deploypb.Deployment_STATUS_WAITING || latest.GetStatus() == deploypb.Deployment_STATUS_RUNNING):
		why := "Deployment " + strconv.FormatInt(latest.GetId(), 10)
		if st := stageLabel(latest); st != "" {
			why += ": " + st
		}
		return serviceState{"Deploying", "deploying", why}
	case latest != nil && latest.GetId() != deployed.GetId() &&
		(latest.GetStatus() == deploypb.Deployment_STATUS_FAILED || latest.GetStatus() == deploypb.Deployment_STATUS_HALTED):
		return serviceState{"Last Deploy Failed", "failed", "Deployment " + strconv.FormatInt(latest.GetId(), 10) + ": " + latest.GetReason()}
	case deployed == nil:
		return serviceState{"Not Deployed Yet", "undeployed", ""}
	case run.GetError() != "":
		return serviceState{"Unknown", "unknown", "The cluster could not be read: " + run.GetError()}
	case !run.GetFound():
		return serviceState{"Degraded", "degraded", "Its workload does not exist"}
	case run.GetReady() < run.GetReplicas():
		return serviceState{"Degraded", "degraded", fmt.Sprintf("%d of %d pods Ready", run.GetReady(), run.GetReplicas())}
	}
	want := deployedImages(deployed)
	for _, im := range run.GetImages() {
		if ref, ok := want[im.GetContainer()]; ok && !sameImage(ref, im.GetImage()) {
			return serviceState{"Drifted", "drifted", fmt.Sprintf("Container %s runs %s; ok deployed %s", im.GetContainer(), shortImage(im.GetImage()), shortImage(ref))}
		}
	}
	return serviceState{"Healthy", "healthy", fmt.Sprintf("%d of %d pods Ready", run.GetReady(), run.GetReplicas())}
}

// deployedImages are a deployment's images, by container.
func deployedImages(d *deploypb.Deployment) map[string]string {
	out := map[string]string{}
	for _, im := range d.GetImages() {
		if im.GetReference() != "" {
			out[im.GetContainer()] = im.GetReference()
		}
	}
	return out
}

// sameImage reports whether two image references name the same image: the same digest, or the
// same reference.
func sameImage(a, b string) bool {
	_, da, okA := strings.Cut(a, "@")
	_, db, okB := strings.Cut(b, "@")
	if okA && okB {
		return da == db
	}
	return a == b
}

// shortImage is an image reference without its registry, its digest cut to 12 hex digits:
// "smoke/serve@674607bcb740".
func shortImage(ref string) string {
	name, digest, ok := strings.Cut(ref, "@")
	if i := strings.IndexByte(name, '/'); i >= 0 && strings.ContainsAny(name[:i], ".:") {
		name = name[i+1:]
	}
	if !ok {
		return name
	}
	_, hexPart, _ := strings.Cut(digest, ":")
	return name + "@" + hexPart[:min(12, len(hexPart))]
}
