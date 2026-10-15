package review

import (
	"context"
	"log/slog"
	"slices"

	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"

	"github.com/Cidan/ok/internal/control/db"
)

// MayRerun refuses a person who may not rerun a check of patchset of cl: its author and the
// repository's admins rerun its checks, and only the current patchset's of a CL under review —
// a post run is its submission's, rerun by submitting again.
func (s *Service) MayRerun(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string, patchset int32) error {
	if err := s.requireManage(ctx, repo, cl, principal, "rerun the checks of"); err != nil {
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

// MayCancelRuns refuses a person who may not cancel cl's checks: its author and the repository's
// admins.
func (s *Service) MayCancelRuns(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string) error {
	return s.requireManage(ctx, repo, cl, principal, "cancel the checks of")
}

// Deployment actions.
const (
	DeploymentRetry = "retry"
	DeploymentHalt  = "halt"
	DeploymentAllow = "allow"
)

// MayActOnDeployment refuses a person who may not take action on a deployment: a repository
// admin retries and halts any, and the author of a CL it deploys (author, covered by it) those;
// allowing a migration's hazards is an admin's alone.
func (s *Service) MayActOnDeployment(ctx context.Context, repo *scmpb.Repository, principal, action string, author bool) error {
	if s.IsAdmin(ctx, repo, principal) {
		return nil
	}
	if action == DeploymentAllow {
		return refuse("Only a repository admin can allow this deployment")
	}
	if !author {
		return refuse("Only a repository admin or the author of a CL it deploys can %s this deployment", action)
	}
	return nil
}

// Admins are the repository's admins: its creator, and ok's administrators, sorted. When ok
// identity cannot say who administers ok, the creator alone.
func (s *Service) Admins(ctx context.Context, repo *scmpb.Repository) []string {
	out := []string{}
	if c := repo.GetCreatedBy(); c != "" {
		out = append(out, c)
	}
	if s.people != nil {
		admins, err := s.people.Admins(ctx)
		if err != nil {
			slog.Warn("review: listing ok's administrators", "err", err)
		}
		for _, p := range admins {
			if !slices.Contains(out, p) {
				out = append(out, p)
			}
		}
	}
	slices.Sort(out)
	return out
}
