package review

import (
	"context"
	"log/slog"

	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/control/db"
)

// MayRerun refuses a person who may not rerun a check of patchset of cl: whoever may land on the
// repository (write) reruns its checks, and only the current patchset's of a CL under review — a
// post run is its submission's, rerun by submitting again.
func (s *Service) MayRerun(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string, patchset int32) error {
	if err := s.requireAction(ctx, repo, principal, authz.Land); err != nil {
		return err
	}
	if !cl.Status.Active() {
		return refuse("CL %d is %s: only an active changelist's checks are rerun", cl.Number, statusWord(cl.Status))
	}
	if patchset != cl.CurrentPatchset {
		return refuse("That run is of patchset %d; only the current patchset's (%d) checks are rerun", patchset, cl.CurrentPatchset)
	}
	return nil
}

// MayRunPre refuses a person who may not run cl's pre by hand: whoever may land on the repository
// (write), on a draft or a CL in review.
func (s *Service) MayRunPre(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string) error {
	if err := s.requireAction(ctx, repo, principal, authz.Land); err != nil {
		return err
	}
	if !cl.Status.Active() {
		return refuse("CL %d is %s: pre runs on a draft or a changelist in review", cl.Number, statusWord(cl.Status))
	}
	return nil
}

// MayCancelRuns refuses a person who may not cancel cl's checks: whoever may land on the
// repository (write).
func (s *Service) MayCancelRuns(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string) error {
	return s.requireAction(ctx, repo, principal, authz.Land)
}

// Deployment actions.
const (
	DeploymentRetry = "retry"
	DeploymentHalt  = "halt"
	DeploymentAllow = "allow"
)

// MayActOnDeployment refuses a person who may not take action on a deployment: a repository's
// admins and maintainers retry and halt any, and the author of a CL it deploys (author, covered
// by it) who may land on the repository (write) those; allowing a migration's hazards is an
// admin's alone.
func (s *Service) MayActOnDeployment(ctx context.Context, repo *scmpb.Repository, principal, action string, author bool) error {
	if s.IsAdmin(ctx, repo, principal) {
		return nil
	}
	if action == DeploymentAllow {
		return refuse("Only a repository admin can allow this deployment")
	}
	if s.May(ctx, repo, principal, authz.Maintain) || author && s.May(ctx, repo, principal, authz.Land) {
		return nil
	}
	return refuse("Only a repository admin or maintainer, or the author of a CL it deploys, can %s this deployment", action)
}

// Admins are the repository's admins, by stable ID, sorted by name; none when ok identity cannot
// say.
func (s *Service) Admins(ctx context.Context, repo *scmpb.Repository) []string {
	admins, err := s.access.Admins(ctx, repo.GetId())
	if err != nil {
		slog.Warn("review: asking ok identity who administers a repository", "repository", repo.GetId(), "err", err)
		return nil
	}
	return admins
}
