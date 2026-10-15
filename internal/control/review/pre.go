package review

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/control/db"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// Automation is what review uses of ok automation (docs/automation.md): each patchset's
// pre, which gates submit, and the landing queue, which a submit hands its CLs to.
type Automation interface {
	GetPre(ctx context.Context, in *automationpb.GetPreRequest, opts ...grpc.CallOption) (*automationpb.Pre, error)
	Submit(ctx context.Context, in *automationpb.SubmitRequest, opts ...grpc.CallOption) (*automationpb.Submission, error)
	CancelSubmission(ctx context.Context, in *automationpb.CancelSubmissionRequest, opts ...grpc.CallOption) (*automationpb.Submission, error)
	ListSubmissions(ctx context.Context, in *automationpb.ListSubmissionsRequest, opts ...grpc.CallOption) (*automationpb.ListSubmissionsResponse, error)
}

// unavailable is what a gate says when ok automation cannot be read: it blocks submit, as a
// reason, never as a failure.
const unavailable = "ok can't read this change's checks right now: try again in a moment"

// Submitting returns a CL's active submission, or nil when it has none.
func (s *Service) Submitting(ctx context.Context, repo, number int64) (*automationpb.Submission, error) {
	resp, err := s.auto.ListSubmissions(ctx, &automationpb.ListSubmissionsRequest{RepositoryId: repo, ClNumber: number, Limit: 1})
	if err != nil {
		return nil, err
	}
	for _, sub := range resp.GetSubmissions() {
		if sub.GetStatus() == automationpb.Submission_STATUS_ACTIVE {
			return sub, nil
		}
	}
	return nil, nil
}

// SubmittingBlocked says why a CL can't be submitted again — it is submitting — or "" when it
// is not; ok automation being unreachable blocks it too.
func (s *Service) SubmittingBlocked(ctx context.Context, cl db.ChangeList) string {
	sub, err := s.Submitting(ctx, cl.Repo, cl.Number)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("ok control: reading submissions", "repository", cl.Repo, "cl", cl.Number, "err", err)
		}
		return unavailable
	}
	if sub == nil {
		return ""
	}
	if sub.GetNote() != "" {
		return "Submitting: " + sub.GetNote()
	}
	return "Submitting: post is running"
}

// PreBlocked says why a CL's current patchset's pre does not let it land — it has not run, is
// still running, or did not pass — or "" when it passed. pre always blocks submit: ok
// automation being unreachable blocks it too, as a reason, never as a failure.
func (s *Service) PreBlocked(ctx context.Context, cl db.ChangeList) string {
	pre, err := s.auto.GetPre(ctx, &automationpb.GetPreRequest{RepositoryId: cl.Repo, ClNumber: cl.Number, Patchset: cl.CurrentPatchset})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("ok control: reading pre for submit", "repository", cl.Repo, "cl", cl.Number, "err", err)
		}
		return unavailable
	}
	return PreReason(pre, cl.CurrentPatchset)
}

// PreReason words a patchset's pre for the Submit button: "" when it passed, else what stands in
// the way — the first run that did not pass.
func PreReason(pre *automationpb.Pre, patchset int32) string {
	switch pre.GetVerdict() {
	case automationpb.Pre_VERDICT_PASSED:
		return ""
	case automationpb.Pre_VERDICT_PENDING:
		return fmt.Sprintf("Checks are still running on patchset %d", patchset)
	case automationpb.Pre_VERDICT_FAILED:
		for _, r := range append([]*automationpb.Run{pre.GetPlan()}, pre.GetJobs()...) {
			if r.GetStatus() < automationpb.Run_STATUS_FAILED {
				continue
			}
			return fmt.Sprintf("Checks failed on patchset %d: %s %s", patchset, jobName(r.GetOrigin().GetJob()), outcomeWord(r.GetStatus()))
		}
		return fmt.Sprintf("Checks failed on patchset %d", patchset)
	}
	return fmt.Sprintf("Checks haven't run on patchset %d yet", patchset)
}

// jobName is a job's name: a workflow job's //<file>:<phase>:<name> is its last part.
func jobName(id string) string {
	if strings.HasPrefix(id, "//") {
		if i := strings.LastIndexByte(id, ':'); i >= 0 {
			return id[i+1:]
		}
	}
	return id
}

func outcomeWord(st automationpb.Run_Status) string {
	switch st {
	case automationpb.Run_STATUS_FAILED:
		return "failed"
	case automationpb.Run_STATUS_CANCELED:
		return "was canceled"
	}
	return "could not run"
}
