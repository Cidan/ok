package web

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/control/db"
	pages "github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// clDeploys is a CL's Deployments tab: the deployments that cover its commit, once it has landed
// (docs/deploy.md, "On the CL"), newest first, and whether the deployer has planned it yet.
// When the deployer cannot be asked, the tab says so and the page still renders.
func (s *Server) clDeploys(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList) pages.CLDeploys {
	d := pages.CLDeploys{Href: clHref(repo.GetName(), cl.Number) + "/deployments", ChangeEvents: clHref(repo.GetName(), cl.Number) + "/events",
		Landed: cl.Status == db.StatusSubmitted && len(cl.LandedCommit) > 0}
	if !d.Landed {
		return d
	}
	resp, err := s.deploys.ListDeployments(ctx, &deploypb.ListDeploymentsRequest{RepositoryId: repo.GetId(), CommitId: cl.LandedCommit})
	if err != nil {
		slog.Warn("ok control: listing deployments", "repository", repo.GetName(), "cl", cl.Number, "err", err)
		d.Error = "The deployer could not be reached."
		return d
	}
	d.Planned = resp.GetPlanned()
	for _, dep := range resp.GetDeployments() {
		label, kind := deploymentStatus(dep)
		item := pages.CLDeployment{ID: dep.GetId(), Name: dep.GetName(), Workload: workloadName(dep.GetWorkload()), StatusLabel: label,
			StatusKind: kind, Stage: stageLabel(dep), Reason: people.HaltReason(ctx, dep.GetReason(), dep.GetHaltById()), Href: deploymentHref(repo.GetName(), dep.GetId(), cl.Number),
			Unfinished: dep.GetStatus() == deploypb.Deployment_STATUS_WAITING || dep.GetStatus() == deploypb.Deployment_STATUS_RUNNING}
		item.Age, item.AgeTitle = age(deploymentTime(dep))
		d.Items = append(d.Items, item)
	}
	return d
}

// clDeploysTab is the Deployments tab's live region, for its refreshes.
func (s *Server) clDeploysTab(w http.ResponseWriter, r *http.Request) {
	t, err := s.loadCL(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	pages.ServeCLDeploys(w, r, s.clDeploys(r.Context(), t.repo, t.cl))
}

// deploymentHref is a deployment's page, from a CL's (0: none).
func deploymentHref(repo string, id, cl int64) string {
	h := repoLink(repo) + "/deploy/" + strconv.FormatInt(id, 10)
	if cl > 0 {
		h += "?" + url.Values{"cl": {strconv.FormatInt(cl, 10)}}.Encode()
	}
	return h
}

func workloadName(w *deploypb.Workload) string {
	if w == nil {
		return ""
	}
	return w.GetKind() + " " + w.GetNamespace() + "/" + w.GetName()
}

// deploymentStatus is a deployment's status chip: a running one waiting for an allowance is
// flagged.
func deploymentStatus(d *deploypb.Deployment) (label, kind string) {
	switch d.GetStatus() {
	case deploypb.Deployment_STATUS_WAITING:
		return "Waiting", "waiting"
	case deploypb.Deployment_STATUS_RUNNING:
		if d.GetStage() == deploypb.Deployment_STAGE_HAZARD {
			return "Needs Allowance", "hazard"
		}
		if d.GetHaltById() != "" || d.GetHaltReason() != "" {
			return "Halting", "halted"
		}
		return "Running", "running"
	case deploypb.Deployment_STATUS_SUCCEEDED:
		return "Succeeded", "succeeded"
	case deploypb.Deployment_STATUS_FAILED:
		return "Failed", "failed"
	case deploypb.Deployment_STATUS_HALTED:
		return "Halted", "halted"
	}
	return "Canceled", "canceled"
}

// stageLabel says where a deployment not yet over is.
func stageLabel(d *deploypb.Deployment) string {
	switch d.GetStatus() {
	case deploypb.Deployment_STATUS_WAITING:
		return "for its turn"
	case deploypb.Deployment_STATUS_RUNNING:
	default:
		return ""
	}
	switch d.GetStage() {
	case deploypb.Deployment_STAGE_PUSH:
		return "Pushing"
	case deploypb.Deployment_STAGE_MIGRATE:
		return "Migrating"
	case deploypb.Deployment_STAGE_HAZARD:
		return "Waiting for an Allowance"
	case deploypb.Deployment_STAGE_ROLL:
		switch d.GetRoll().GetStage() {
		case "canary":
			return "Canary"
		case "full":
			return "Rolling Out"
		case "rollback":
			return "Rolling Back"
		}
		return "Rolling"
	}
	return ""
}

// rollLabel describes a deployment's roll: its stage and its copies.
func rollLabel(d *deploypb.Deployment) string {
	r := d.GetRoll()
	if r == nil {
		return ""
	}
	var parts []string
	switch r.GetStage() {
	case "canary":
		parts = append(parts, "Canary")
	case "full":
		parts = append(parts, "Full")
	case "rollback":
		parts = append(parts, "Rolling Back")
	case "done":
		parts = append(parts, upperFirst(r.GetOutcome()))
	}
	if r.GetNew() != "" {
		parts = append(parts, r.GetNew()+" replacing "+r.GetLive())
	}
	if r.GetReplicas() > 0 {
		parts = append(parts, pluralN(int(r.GetReplicas()), "replica", "replicas"))
	}
	return strings.Join(parts, " · ")
}

func pluralN(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + many
}

// deploymentTime is when a deployment last changed state: it finished, started, or was made.
func deploymentTime(d *deploypb.Deployment) time.Time {
	switch {
	case d.GetFinishedAt() != nil:
		return d.GetFinishedAt().AsTime()
	case d.GetStartedAt() != nil:
		return d.GetStartedAt().AsTime()
	}
	return d.GetCreatedAt().AsTime()
}

// deployTarget is a deployment a request names, in the repository its path names, and the CL
// the viewer came from, if any.
type deployTarget struct {
	repo *scmpb.Repository
	d    *deploypb.Deployment
	cl   *db.ChangeList
}

func (s *Server) loadDeployment(r *http.Request) (*deployTarget, error) {
	repo, err := s.repository(r)
	if err != nil {
		return nil, err
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id < 1 {
		return nil, review.ErrNotFound
	}
	d, err := s.deploys.GetDeployment(r.Context(), &deploypb.GetDeploymentRequest{Id: id})
	if err != nil {
		return nil, err
	}
	if d.GetRepositoryId() != repo.GetId() {
		return nil, review.ErrNotFound
	}
	t := &deployTarget{repo: repo, d: d}
	if n, err := strconv.ParseInt(r.URL.Query().Get("cl"), 10, 64); err == nil && n > 0 {
		cl, ok, err := s.db.GetChangeList(r.Context(), repo.GetId(), n)
		if err != nil {
			return nil, err
		}
		if ok && cl.Status == db.StatusSubmitted {
			t.cl = &cl
		}
	}
	return t, nil
}

// authorOfDeployed reports whether me wrote the CL the viewer came from and the deployment
// covers its landed commit — which, with being a repository admin, is who may retry or halt it
// (review.MayActOnDeployment).
func (s *Server) authorOfDeployed(ctx context.Context, t *deployTarget, me string) (bool, error) {
	if t.cl == nil || t.cl.Author != me {
		return false, nil
	}
	resp, err := s.deploys.ListDeployments(ctx, &deploypb.ListDeploymentsRequest{RepositoryId: t.repo.GetId(), CommitId: t.cl.LandedCommit, Limit: 500})
	if err != nil {
		return false, err
	}
	for _, d := range resp.GetDeployments() {
		if d.GetId() == t.d.GetId() {
			return true, nil
		}
	}
	return false, nil
}

// deploymentView builds a deployment's page for me.
func (s *Server) deploymentView(ctx context.Context, t *deployTarget, me, notice string) (pages.DeploymentView, error) {
	d := t.d
	repoName := t.repo.GetName()
	var clNumber int64
	if t.cl != nil {
		clNumber = t.cl.Number
	}
	base := deploymentHref(repoName, d.GetId(), 0)
	query := ""
	if clNumber > 0 {
		query = "?" + url.Values{"cl": {strconv.FormatInt(clNumber, 10)}}.Encode()
	}
	label, kind := deploymentStatus(d)
	v := pages.DeploymentView{
		Chrome: chrome(t.repo, "main", t.repo.GetHead(), ""),
		ID:     d.GetId(), Name: d.GetName(), Deploy: d.GetDeploy(),
		Commit: scm.Short(d.GetCommitId()), CommitHref: commitHref(repoName, d.GetCommitId()),
		Why: d.GetWhy(), Workload: workloadName(d.GetWorkload()), RequestedBy: people.Handle(ctx, d.GetRequestedById()),
		StatusLabel: label, StatusKind: kind, Stage: stageLabel(d), Reason: people.HaltReason(ctx, d.GetReason(), d.GetHaltById()), Roll: rollLabel(d),
		Live:         d.GetStatus() == deploypb.Deployment_STATUS_WAITING || d.GetStatus() == deploypb.Deployment_STATUS_RUNNING,
		StatusHref:   base + "/status" + query,
		ChangeEvents: base + "/events",
		Notice:       notice,
	}
	if clNumber > 0 {
		v.CLNumber, v.CLHref = clNumber, clHref(repoName, clNumber)
	}
	switch {
	case d.GetRetryOf() != 0:
		v.Wave = "a retry of deployment " + strconv.FormatInt(d.GetRetryOf(), 10)
	case d.GetPlanId() != 0:
		v.Wave = strconv.Itoa(int(d.GetWave())) + " of plan " + strconv.FormatInt(d.GetPlanId(), 10)
	}
	if d.GetStartedAt() != nil {
		v.Started = d.GetStartedAt().AsTime().UTC().Format("2006-01-02 15:04:05 UTC")
	}
	if d.GetFinishedAt() != nil {
		v.Finished = d.GetFinishedAt().AsTime().UTC().Format("2006-01-02 15:04:05 UTC")
	}
	for _, im := range d.GetImages() {
		v.Images = append(v.Images, pages.DeploymentImage{Container: im.GetContainer(), Target: im.GetTarget(), Reference: im.GetReference(), Run: im.GetPushRun()})
	}
	for _, m := range d.GetMigrations() {
		mv := pages.DeploymentMigration{Target: m.GetTarget(), Refused: m.GetRefused(), Allowed: m.GetAllowed(), Output: m.GetOutput()}
		if m.GetAllowedById() != "" {
			mv.AllowedBy = people.Handle(ctx, m.GetAllowedById())
		}
		switch m.GetStatus() {
		case deploypb.Migration_STATUS_APPLIED:
			mv.StatusLabel, mv.StatusKind = "Applied", "applied"
		case deploypb.Migration_STATUS_WAITING:
			mv.StatusLabel, mv.StatusKind = "Needs Allowance", "hazard"
		case deploypb.Migration_STATUS_FAILED:
			mv.StatusLabel, mv.StatusKind = "Failed", "failed"
		default:
			mv.StatusLabel, mv.StatusKind = "Pending", "pending"
		}
		for _, st := range m.GetPlan().GetSteps() {
			step := pages.MigrationStep{Description: st.GetDescription(), SQL: st.GetSql()}
			for _, h := range st.GetHazards() {
				step.Hazards = append(step.Hazards, pages.MigrationHazard{Class: h.GetClass(), Detail: h.GetDetail(), Gated: h.GetGated()})
			}
			mv.Steps = append(mv.Steps, step)
		}
		v.Migrations = append(v.Migrations, mv)
	}
	for _, e := range d.GetEvents() {
		at, title := age(e.GetAt().AsTime())
		v.Events = append(v.Events, pages.DeploymentEvent{At: at, AtTitle: title, Message: e.GetMessage()})
	}

	// Actions: Retry and Halt for a repository admin and the author of the CL the viewer came
	// from; Allow for a repository admin.
	author, err := s.authorOfDeployed(ctx, t, me)
	if err != nil {
		return v, err
	}
	may := s.review.MayActOnDeployment(ctx, t.repo, me, review.DeploymentRetry, author) == nil
	switch d.GetStatus() {
	case deploypb.Deployment_STATUS_FAILED, deploypb.Deployment_STATUS_HALTED, deploypb.Deployment_STATUS_CANCELED:
		if may {
			v.RetryHref = base + "/retry" + query
		}
	case deploypb.Deployment_STATUS_WAITING, deploypb.Deployment_STATUS_RUNNING:
		if may && d.GetHaltById() == "" && d.GetHaltReason() == "" {
			v.HaltHref = base + "/halt" + query
		}
		if d.GetStage() == deploypb.Deployment_STAGE_HAZARD && s.review.MayActOnDeployment(ctx, t.repo, me, review.DeploymentAllow, author) == nil {
			v.AllowHref = base + "/allow" + query
		}
	}
	return v, nil
}

// deploymentPage is a deployment's page.
func (s *Server) deploymentPage(w http.ResponseWriter, r *http.Request) {
	s.serveDeployment(w, r, "", pages.ServeDeploymentView)
}

// deploymentStatusFragment is a deployment page's status panel, which refreshes itself.
func (s *Server) deploymentStatusFragment(w http.ResponseWriter, r *http.Request) {
	s.serveDeployment(w, r, "", pages.ServeDeploymentStatus)
}

func (s *Server) serveDeployment(w http.ResponseWriter, r *http.Request, notice string, serve func(http.ResponseWriter, *http.Request, pages.DeploymentView)) {
	t, err := s.loadDeployment(r)
	if err != nil {
		fail(w, r, err)
		return
	}
	v, err := s.deploymentView(r.Context(), t, principal(r), notice)
	if err != nil {
		fail(w, r, err)
		return
	}
	serve(w, r, v)
}

// deploymentAction takes an action on a deployment for the viewer, who must be allowed it; the
// deployer's refusal is the page's notice.
func (s *Server) deploymentAction(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		t, err := s.loadDeployment(r)
		if err != nil {
			fail(w, r, err)
			return
		}
		me := principal(r)
		author, err := s.authorOfDeployed(ctx, t, me)
		if err != nil {
			fail(w, r, err)
			return
		}
		if err := s.review.MayActOnDeployment(ctx, t.repo, me, action, author); err != nil {
			s.serveDeployment(w, r, err.Error(), pages.ServeDeploymentView)
			return
		}
		var d *deploypb.Deployment
		switch action {
		case "retry":
			d, err = s.deploys.RetryDeployment(ctx, &deploypb.RetryDeploymentRequest{Id: t.d.GetId(), ForPersonId: me})
		case "halt":
			d, err = s.deploys.HaltDeployment(ctx, &deploypb.HaltDeploymentRequest{Id: t.d.GetId(), ForPersonId: me})
		case "allow":
			d, err = s.deploys.AllowHazards(ctx, &deploypb.AllowHazardsRequest{Id: t.d.GetId(), ForPersonId: me})
		}
		if status.Code(err) == codes.FailedPrecondition {
			s.serveDeployment(w, r, upperFirst(status.Convert(err).Message()), pages.ServeDeploymentView)
			return
		}
		if err != nil {
			fail(w, r, err)
			return
		}
		if action == "retry" {
			var cl int64
			if t.cl != nil {
				cl = t.cl.Number
			}
			pages.Navigate(w, r, deploymentHref(t.repo.GetName(), d.GetId(), cl))
			return
		}
		t.d = d
		v, err := s.deploymentView(ctx, t, me, "")
		if err != nil {
			fail(w, r, err)
			return
		}
		pages.ServeDeploymentView(w, r, v)
	}
}
