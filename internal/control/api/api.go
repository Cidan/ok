// Package api is ok control's ChangeLists service (ok control/proto): a CL's whole
// lifecycle for a person — where it stands, followed as it changes, and every action its page
// offers — for ok's jj and, through the REST surface, for anything that speaks JSON over HTTPS.
//
// It is the CL page's twin: every action goes through the same review service, under the same
// rules of who may do what, and its links lead to the same pages.
package api

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/serve/authn"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// How a watch follows a CL: it reads it every Poll (the CL page's live regions' cadence), and
// ends after MaxWatch, for the client to watch again.
const (
	Poll     = 2 * time.Second
	MaxWatch = time.Hour
)

// API serves the ChangeLists service.
type API struct {
	controlpb.UnimplementedChangeListsServer

	db      db.Store
	scm     *scm.Client
	runs    automationpb.RunsClient
	deploys deploypb.DeploymentsClient
	review  *review.Service
	base    string // ok's public URL, for links: https://ok.example.com
	poll    time.Duration
}

// New returns the service over ok control's clients; base is its public address.
func New(d db.Store, s *scm.Client, runs automationpb.RunsClient, deploys deploypb.DeploymentsClient, rv *review.Service, base string) *API {
	return &API{db: d, scm: s, runs: runs, deploys: deploys, review: rv, base: strings.TrimRight(base, "/"), poll: Poll}
}

// SetPoll sets how often a watch reads its CL (tests).
func (a *API) SetPoll(d time.Duration) { a.poll = d }

// caller is the person a request is for. Only a person acts here, and never from another site:
// a browser sends a cross-site request with the person's session, so one carrying a foreign
// Origin, or Sec-Fetch-Site other than same-origin or none, is refused — as the CL page's forms
// refuse it.
func (a *API) caller(ctx context.Context) (string, error) {
	c, ok := authn.FromContext(ctx)
	if !ok || !c.Human || !people.IsHuman(c.Principal) {
		return "", status.Error(codes.PermissionDenied, "ok's ChangeLists serve people only")
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, site := range md.Get("sec-fetch-site") {
			if site != "same-origin" && site != "none" {
				return "", status.Error(codes.PermissionDenied, "a request from another site is refused")
			}
		}
		for _, origin := range md.Get("origin") {
			if !a.sameOrigin(origin) {
				return "", status.Error(codes.PermissionDenied, "a request from another site is refused")
			}
		}
	}
	return c.Principal, nil
}

func (a *API) sameOrigin(origin string) bool {
	o, err := url.Parse(origin)
	b, err2 := url.Parse(a.base)
	return err == nil && err2 == nil && o.Scheme == b.Scheme && o.Host == b.Host
}

// toStatus maps what the review service and ok's other services answer onto a gRPC status: a
// refusal meant for the person is FailedPrecondition, with its words.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, review.ErrNotFound):
		return status.Error(codes.NotFound, "no such changelist")
	case review.IsUserError(err):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

func invalid(msg string) error { return status.Error(codes.InvalidArgument, msg) }

// begin is how every method starts: the caller, and the CL the request names.
func (a *API) begin(ctx context.Context, ref *controlpb.CLRef) (string, *target, error) {
	me, err := a.caller(ctx)
	if err != nil {
		return "", nil, err
	}
	t, err := a.load(ctx, ref)
	if err != nil {
		return "", nil, toStatus(err)
	}
	return me, t, nil
}

// after answers an action with where its CL stands now, re-read.
func (a *API) after(ctx context.Context, ref *controlpb.CLRef) (*controlpb.CLStatus, error) {
	st, err := a.statusOf(ctx, ref, nil)
	return st, toStatus(err)
}

// ─── Status ───

func (a *API) Status(ctx context.Context, req *controlpb.CLRef) (*controlpb.CLStatus, error) {
	if _, err := a.caller(ctx); err != nil {
		return nil, err
	}
	st, err := a.statusOf(ctx, req, nil)
	return st, toStatus(err)
}

func (a *API) Watch(req *controlpb.CLRef, stream grpc.ServerStreamingServer[controlpb.CLStatus]) error {
	ctx := stream.Context()
	if _, err := a.caller(ctx); err != nil {
		return err
	}
	deadline := time.Now().Add(MaxWatch)
	var last *controlpb.CLStatus
	var bc blockedCache
	for {
		st, err := a.statusOf(ctx, req, &bc)
		if err != nil {
			return toStatus(err)
		}
		if last == nil || !proto.Equal(st, last) {
			if err := stream.Send(st); err != nil {
				return err
			}
			last = st
		}
		if Finished(st) || time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return toStatus(ctx.Err())
		case <-time.After(a.poll):
		}
	}
}

// ─── Review ───

// principals are the people a request names, as principals.
func principals(names []string) []string {
	var out []string
	for _, n := range names {
		if strings.TrimSpace(n) != "" {
			out = append(out, people.Principal(n))
		}
	}
	return out
}

func (a *API) RequestReview(ctx context.Context, req *controlpb.ReviewersRequest) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	if err := a.review.RequestReview(ctx, t.repo, t.cl, me, principals(req.GetReviewers()), req.GetMessage()); err != nil {
		return nil, toStatus(err)
	}
	return a.after(ctx, req.GetCl())
}

func (a *API) ReturnToDraft(ctx context.Context, req *controlpb.CLRef) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := a.review.ReturnToDraft(ctx, t.repo, t.cl, me); err != nil {
		return nil, toStatus(err)
	}
	return a.after(ctx, req)
}

func (a *API) AddReviewers(ctx context.Context, req *controlpb.ReviewersRequest) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	if len(req.GetReviewers()) == 0 {
		return nil, invalid("name the reviewers to add")
	}
	if err := a.review.AddReviewers(ctx, t.repo, t.cl, me, principals(req.GetReviewers()), req.GetMessage()); err != nil {
		return nil, toStatus(err)
	}
	return a.after(ctx, req.GetCl())
}

func (a *API) RemoveReviewers(ctx context.Context, req *controlpb.ReviewersRequest) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	if len(req.GetReviewers()) == 0 {
		return nil, invalid("name the reviewers to remove")
	}
	for _, r := range principals(req.GetReviewers()) {
		if err := a.review.RemoveReviewer(ctx, t.repo, t.cl, me, r); err != nil {
			return nil, toStatus(err)
		}
	}
	return a.after(ctx, req.GetCl())
}

func (a *API) Reply(ctx context.Context, req *controlpb.ReplyRequest) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	grants := map[db.Label]bool{}
	for _, g := range req.GetGrants() {
		switch g.GetLabel() {
		case controlpb.Label_LABEL_LGTM:
			grants[db.LabelLGTM] = g.GetGranted()
		case controlpb.Label_LABEL_APPROVAL:
			grants[db.LabelApproval] = g.GetGranted()
		default:
			return nil, invalid("there is no review label " + g.GetLabel().String())
		}
	}
	if len(grants) == 0 && strings.TrimSpace(req.GetMessage()) == "" {
		return nil, invalid("a reply grants or revokes a label, or says something")
	}
	if err := a.review.Reply(ctx, t.cl, me, grants, req.GetMessage()); err != nil {
		return nil, toStatus(err)
	}
	return a.after(ctx, req.GetCl())
}

func (a *API) Comment(ctx context.Context, req *controlpb.CommentRequest) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	if req.GetReplyTo() == 0 && req.GetFile() == "" {
		// A comment on the CL itself is a reply that grants nothing.
		err = a.review.Reply(ctx, t.cl, me, map[db.Label]bool{}, req.GetBody())
	} else {
		patchset := req.GetPatchset()
		if patchset == 0 {
			patchset = t.cl.CurrentPatchset
		}
		side := db.SideNone
		if req.GetLine() > 0 {
			side = db.SidePatchset
			if req.GetSide() == controlpb.CommentRequest_SIDE_BASE {
				side = db.SideBase
			}
		}
		err = a.review.Comment(ctx, t.cl, me, patchset, req.GetFile(), req.GetLine(), side, req.GetReplyTo(), req.GetBody())
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return a.after(ctx, req.GetCl())
}

func (a *API) Resolve(ctx context.Context, req *controlpb.ResolveRequest) (*controlpb.CLStatus, error) {
	_, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	if err := a.review.Resolve(ctx, t.cl, req.GetThread(), req.GetResolved()); err != nil {
		return nil, toStatus(err)
	}
	return a.after(ctx, req.GetCl())
}

// ─── Lifecycle ───

func (a *API) Submit(ctx context.Context, req *controlpb.SubmitRequest) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	if err := a.review.Submit(ctx, t.repo, t.cl, t.scm, me, req.GetStack()); err != nil {
		return nil, toStatus(err)
	}
	return a.after(ctx, req.GetCl())
}

func (a *API) CancelSubmit(ctx context.Context, req *controlpb.CLRef) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := a.review.CancelSubmit(ctx, t.repo, t.cl, me); err != nil {
		return nil, toStatus(err)
	}
	return a.after(ctx, req)
}

func (a *API) Abandon(ctx context.Context, req *controlpb.AbandonRequest) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	if err := a.review.Abandon(ctx, t.repo, t.cl, me, req.GetReason()); err != nil {
		return nil, toStatus(err)
	}
	return a.after(ctx, req.GetCl())
}

func (a *API) Restore(ctx context.Context, req *controlpb.CLRef) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := a.review.Restore(ctx, t.repo, t.cl, me); err != nil {
		return nil, toStatus(err)
	}
	return a.after(ctx, req)
}

func (a *API) Rollback(ctx context.Context, req *controlpb.RollbackRequest) (*controlpb.RollbackResponse, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	n, err := a.review.Rollback(ctx, t.cl, me, req.GetReason())
	if err != nil {
		return nil, toStatus(err)
	}
	st, err := a.after(ctx, &controlpb.CLRef{Repository: t.repo.GetName(), Number: n})
	if err != nil {
		return nil, err
	}
	return &controlpb.RollbackResponse{Rollback: st}, nil
}

// ─── Checks ───

// run reads a run of t's checks: one of another CL's, or none of a CL's at all, is not found
// here.
func (a *API) run(ctx context.Context, t *target, id int64) (*automationpb.Run, error) {
	r, err := a.runs.GetRun(ctx, &automationpb.GetRunRequest{Id: id})
	if status.Code(err) == codes.NotFound || (err == nil && (r.GetRepositoryId() != t.repo.GetId() || r.GetOrigin().GetClNumber() != t.cl.Number)) {
		return nil, status.Errorf(codes.NotFound, "CL %d has no run %d", t.cl.Number, id)
	}
	return r, err
}

func (a *API) Rerun(ctx context.Context, req *controlpb.RunsRequest) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	ids := append([]int64(nil), req.GetRuns()...)
	if req.GetFailed() {
		pre, err := a.runs.GetPre(ctx, &automationpb.GetPreRequest{RepositoryId: t.repo.GetId(), ClNumber: t.cl.Number, Patchset: t.cl.CurrentPatchset})
		if err != nil {
			return nil, toStatus(err)
		}
		for _, r := range append([]*automationpb.Run{pre.GetPlan()}, pre.GetJobs()...) {
			switch r.GetStatus() {
			case automationpb.Run_STATUS_FAILED, automationpb.Run_STATUS_ERROR, automationpb.Run_STATUS_CANCELED:
				ids = append(ids, r.GetId())
			}
		}
	}
	if len(ids) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "nothing to rerun: name runs, or ask for the failed ones of a pre that has some")
	}
	for _, id := range ids {
		r, err := a.run(ctx, t, id)
		if err != nil {
			return nil, err
		}
		if err := a.review.MayRerun(ctx, t.repo, t.cl, me, r.GetOrigin().GetPatchset()); err != nil {
			return nil, toStatus(err)
		}
		if _, err := a.runs.RerunRun(ctx, &automationpb.RerunRunRequest{Id: id, ForPerson: me}); err != nil && status.Code(err) != codes.FailedPrecondition {
			return nil, err
		}
	}
	return a.after(ctx, req.GetCl())
}

func (a *API) CancelRuns(ctx context.Context, req *controlpb.RunsRequest) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	if len(req.GetRuns()) == 0 {
		return nil, invalid("name the runs to cancel")
	}
	if err := a.review.MayCancelRuns(ctx, t.repo, t.cl, me); err != nil {
		return nil, toStatus(err)
	}
	for _, id := range req.GetRuns() {
		if _, err := a.run(ctx, t, id); err != nil {
			return nil, err
		}
		if _, err := a.runs.CancelRun(ctx, &automationpb.CancelRunRequest{Id: id, ForPerson: me,
			Reason: "canceled by " + people.Handle(me)}); err != nil && status.Code(err) != codes.FailedPrecondition {
			return nil, err
		}
	}
	return a.after(ctx, req.GetCl())
}

func (a *API) ReadLog(req *controlpb.ReadLogRequest, stream grpc.ServerStreamingServer[controlpb.LogChunk]) error {
	ctx := stream.Context()
	_, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return err
	}
	r, err := a.run(ctx, t, req.GetRun())
	if err != nil {
		return err
	}
	offset := req.GetOffset()
	if offset < 0 {
		offset = max(r.GetLogSize()+offset, 0)
	}
	in, err := a.runs.ReadLog(ctx, &automationpb.ReadLogRequest{Id: r.GetId(), Offset: offset, Follow: req.GetFollow()})
	if err != nil {
		return err
	}
	for {
		chunk, err := in.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&controlpb.LogChunk{Offset: chunk.GetOffset(), Data: strings.ToValidUTF8(string(chunk.GetData()), "�"),
			End: chunk.GetEnd()}); err != nil {
			return err
		}
		if chunk.GetEnd() {
			return nil
		}
	}
}

// ─── Deployments ───

func (a *API) RetryDeployment(ctx context.Context, req *controlpb.DeploymentRequest) (*controlpb.CLStatus, error) {
	return a.deploymentAction(ctx, req, review.DeploymentRetry)
}

func (a *API) HaltDeployment(ctx context.Context, req *controlpb.DeploymentRequest) (*controlpb.CLStatus, error) {
	return a.deploymentAction(ctx, req, review.DeploymentHalt)
}

func (a *API) AllowHazards(ctx context.Context, req *controlpb.DeploymentRequest) (*controlpb.CLStatus, error) {
	return a.deploymentAction(ctx, req, review.DeploymentAllow)
}

// deploymentAction takes action on a deployment of the CL's landed commit, for a person allowed
// it (review.MayActOnDeployment).
func (a *API) deploymentAction(ctx context.Context, req *controlpb.DeploymentRequest, action string) (*controlpb.CLStatus, error) {
	me, t, err := a.begin(ctx, req.GetCl())
	if err != nil {
		return nil, err
	}
	if t.cl.Status != db.StatusSubmitted || len(t.cl.LandedCommit) == 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "CL %d has not landed: it has no deployments", t.cl.Number)
	}
	resp, err := a.deploys.ListDeployments(ctx, &deploypb.ListDeploymentsRequest{RepositoryId: t.repo.GetId(), CommitId: t.cl.LandedCommit, Limit: 500})
	if err != nil {
		return nil, err
	}
	covers := false
	for _, d := range resp.GetDeployments() {
		covers = covers || d.GetId() == req.GetDeployment()
	}
	if !covers {
		return nil, status.Errorf(codes.NotFound, "deployment %d does not deploy CL %d", req.GetDeployment(), t.cl.Number)
	}
	if err := a.review.MayActOnDeployment(ctx, t.repo, me, action, t.cl.Author == me); err != nil {
		return nil, toStatus(err)
	}
	switch action {
	case review.DeploymentRetry:
		_, err = a.deploys.RetryDeployment(ctx, &deploypb.RetryDeploymentRequest{Id: req.GetDeployment(), ForPerson: me})
	case review.DeploymentHalt:
		_, err = a.deploys.HaltDeployment(ctx, &deploypb.HaltDeploymentRequest{Id: req.GetDeployment(), ForPerson: me})
	case review.DeploymentAllow:
		_, err = a.deploys.AllowHazards(ctx, &deploypb.AllowHazardsRequest{Id: req.GetDeployment(), ForPerson: me})
	}
	if err != nil {
		return nil, err
	}
	return a.after(ctx, req.GetCl())
}
