package review

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// submitAttempts bounds how often Submit re-validates against a main that moved under it.
const submitAttempts = 5

// landRejection is the SCM's refusal of a landing, when it is one.
func landRejection(err error) *scmpb.LandRejection {
	if status.Code(err) != codes.FailedPrecondition {
		return nil
	}
	for _, d := range status.Convert(err).Details() {
		if r, ok := d.(*scmpb.LandRejection); ok {
			return r
		}
	}
	return nil
}

// rejectionMessage words a landing refusal for the person submitting.
func rejectionMessage(r *scmpb.LandRejection) string {
	switch r.GetReason() {
	case scmpb.LandRejection_REASON_PATH_CONFLICT:
		return fmt.Sprintf("CL %d conflicts with main in %s: run jj sync, resolve the conflicts, and upload a new patchset with jj cl",
			r.GetNumber(), strings.Join(r.GetPaths(), ", "))
	case scmpb.LandRejection_REASON_ALREADY_ON_MAIN:
		return fmt.Sprintf("CL %d changes nothing main does not already have", r.GetNumber())
	case scmpb.LandRejection_REASON_STALE_PATCHSET:
		return fmt.Sprintf("CL %d has a newer patchset: review it before submitting", r.GetNumber())
	case scmpb.LandRejection_REASON_CONFLICTED:
		return fmt.Sprintf("CL %d has unresolved conflicts: resolve them and upload a new patchset with jj cl", r.GetNumber())
	}
	return upperFirst(r.GetMessage())
}

// landing are the CLs a submit lands, bottom first, with their evaluations.
type landing struct {
	items []db.ChangeList
	scms  []*scmpb.ChangeList
}

// plan collects what submitting cl lands: cl alone, or with stack its unsubmitted
// ancestors too, bottom first. A parent that must land first but isn't included, or one
// that was abandoned, refuses.
func (s *Service) plan(ctx context.Context, cl db.ChangeList, c *scmpb.ChangeList, stack bool) (*landing, error) {
	l := &landing{items: []db.ChangeList{cl}, scms: []*scmpb.ChangeList{c}}
	cur := cl
	for cur.ParentNumber != 0 {
		parent, pc, err := s.ChangeList(ctx, cur.Repo, cur.ParentNumber)
		if err != nil {
			return nil, err
		}
		switch parent.Status {
		case db.StatusSubmitted:
			return l, nil
		case db.StatusAbandoned:
			return nil, refuse("Parent CL %d is abandoned: drop it locally (jj abandon) and upload this change again", parent.Number)
		}
		if !stack {
			sub, err := s.Submitting(ctx, parent.Repo, parent.Number)
			if err == nil && sub != nil {
				return nil, refuse("CL %d, which this is stacked on, is submitting: submit this once it lands, or cancel it and submit the stack", parent.Number)
			}
			return nil, refuse("Submit the parent changelist (CL %d) first, or submit the stack", parent.Number)
		}
		l.items = append([]db.ChangeList{parent}, l.items...)
		l.scms = append([]*scmpb.ChangeList{pc}, l.scms...)
		cur = parent
	}
	return l, nil
}

// check runs every gate but the landing itself against main's head: each CL open and not
// already submitting, the principal allowed to submit it, its review requirements met, and its
// current patchset's pre passed.
func (s *Service) check(ctx context.Context, repo *scmpb.Repository, l *landing, principal string, head []byte) error {
	for i, cl := range l.items {
		if cl.Status == db.StatusDraft {
			if len(l.items) == 1 {
				return refuse("Request review first: a draft can't be submitted")
			}
			return refuse("CL %d is a draft: request review on it first", cl.Number)
		}
		if cl.Status != db.StatusOpen {
			return refuse("CL %d is %s", cl.Number, statusWord(cl.Status))
		}
		if reason := s.SubmittingBlocked(ctx, cl); reason != "" {
			if len(l.items) == 1 || reason == unavailable {
				return refuse("%s", reason)
			}
			return refuse("CL %d is already submitting", cl.Number)
		}
		if !s.CanManage(ctx, repo, cl, principal) {
			if len(l.items) == 1 {
				return refuse("Only the author or a repository admin can submit this changelist")
			}
			return refuse("Only CL %d's author or a repository admin can submit it", cl.Number)
		}
		ev, err := s.Evaluate(ctx, cl, l.scms[i], head)
		if err != nil {
			return err
		}
		reason := ev.Status.BlockedReason()
		if reason == "" {
			reason = s.PreBlocked(ctx, cl)
		}
		if reason != "" {
			if len(l.items) == 1 {
				return refuse("%s", reason)
			}
			return refuse("CL %d: %s", cl.Number, strings.ToLower(reason[:1])+reason[1:])
		}
	}
	return nil
}

func (l *landing) request(repo int64, principal string, head []byte, dryRun bool) *scmpb.LandRequest {
	req := &scmpb.LandRequest{RepositoryId: repo, Actor: principal, ExpectedHead: head, DryRun: dryRun}
	for _, cl := range l.items {
		req.Items = append(req.Items, &scmpb.LandRequest_Item{Number: cl.Number, Patchset: cl.CurrentPatchset})
	}
	return req
}

// Blocked reports why principal can't submit a CL (or, with stack, its stack) right now,
// or "" when they can: the same gates Submit runs, with a dry run for the landing, so
// the Submit button's tooltip never disagrees with Submit.
func (s *Service) Blocked(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, c *scmpb.ChangeList, principal string, stack bool) (string, error) {
	blocked := func(err error) (string, error) {
		if IsUserError(err) {
			return err.Error(), nil
		}
		return "", err
	}
	l, err := s.plan(ctx, cl, c, stack)
	if err != nil {
		return blocked(err)
	}
	head := repo.GetHead()
	if err := s.check(ctx, repo, l, principal, head); err != nil {
		return blocked(err)
	}
	_, err = s.scm.LandingChecks.DryRunLand(ctx, l.request(repo.GetId(), principal, head, true))
	if r := landRejection(err); r != nil {
		return rejectionMessage(r), nil
	}
	return "", err
}

// StackSize is how many CLs submitting cl's stack would land: cl and its unsubmitted
// ancestors (1 when it stacks on nothing unsubmitted).
func (s *Service) StackSize(ctx context.Context, cl db.ChangeList) (int, error) {
	n := 1
	cur := cl
	for cur.ParentNumber != 0 {
		parent, ok, err := s.db.GetChangeList(ctx, cur.Repo, cur.ParentNumber)
		if err != nil || !ok || parent.Status == db.StatusSubmitted {
			return n, err
		}
		n++
		cur = parent
	}
	return n, nil
}

// Submit hands a CL — with stack, together with its unsubmitted ancestors, bottom first — to
// ok automation's landing queue on principal's behalf, once every gate passes against main's
// head and a dry run of the landing is clean. The queue runs post on the CLs' candidates and
// lands them once it passes (docs/automation.md, "The landing queue"). When main moves
// between the gates and the dry run, it validates against the new head.
func (s *Service) Submit(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, c *scmpb.ChangeList, principal string, stack bool) error {
	for attempt := 0; attempt < submitAttempts; attempt++ {
		r, err := s.scm.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo.GetId()}})
		if err != nil {
			return err
		}
		l, err := s.plan(ctx, cl, c, stack)
		if err != nil {
			return err
		}
		if err := s.check(ctx, r, l, principal, r.GetHead()); err != nil {
			return err
		}
		_, err = s.scm.LandingChecks.DryRunLand(ctx, l.request(repo.GetId(), principal, r.GetHead(), true))
		if rej := landRejection(err); rej != nil {
			if rej.GetReason() == scmpb.LandRejection_REASON_HEAD_MOVED {
				continue
			}
			return refuse("%s", rejectionMessage(rej))
		}
		if err != nil {
			return err
		}
		req := &automationpb.SubmitRequest{RepositoryId: repo.GetId(), ForPerson: principal}
		for _, item := range l.items {
			req.Cls = append(req.Cls, &automationpb.SubmitRequest_CL{Number: item.Number, Patchset: item.CurrentPatchset})
		}
		_, err = s.auto.Submit(ctx, req)
		if status.Code(err) == codes.FailedPrecondition {
			return refuse("%s", upperFirst(status.Convert(err).Message()))
		}
		return err
	}
	return refuse("main kept moving while this changelist was being submitted: try again")
}

// CancelSubmit stops a CL's submission on principal's behalf: nothing of it lands. Its author or
// a repository admin may.
func (s *Service) CancelSubmit(ctx context.Context, repo *scmpb.Repository, cl db.ChangeList, principal string) error {
	if err := s.requireManage(ctx, repo, cl, principal, "cancel the submission of"); err != nil {
		return err
	}
	sub, err := s.Submitting(ctx, cl.Repo, cl.Number)
	if err != nil {
		return err
	}
	if sub == nil {
		return refuse("CL %d is not submitting", cl.Number)
	}
	_, err = s.auto.CancelSubmission(ctx, &automationpb.CancelSubmissionRequest{Id: sub.GetId(), ForPerson: principal,
		Reason: "canceled by " + people.Handle(principal)})
	return err
}
