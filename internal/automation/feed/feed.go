// Package feed follows the SCM's change feed (docs/scm.md, "Events") into ok automation: pre is
// planned at the top of each stack in review — a CL in review that no CL in review is stacked on
// — over the whole stack, and a worker takes its planning run (docs/automation.md, "pre",
// "Execution"). A draft runs none until it is sent for review, or by hand (StartPre). As the top
// moves — a CL entering review stacked on it, or leaving review from on top of it — the old top's
// unfinished pre is canceled and the new top's planned, or resumed where a stop canceled it. A CL
// that lands or is abandoned has its unfinished pre runs canceled. For the landing queue it
// records main's history —
// each commit main moves to, and the CL it is — and cancels a submission whose CL gets a new
// patchset, is abandoned, or returns to draft.
//
// Every replica follows the SCM's events, as the bus's stream brings them and each repository's
// log fills in, sharing the repositories (the stream's group, automation; internal/bus/follow).
// A replica applies a repository's event only holding the repository's claim (feedClaim), so its
// events are applied one at a time, in order; an event's plan is recorded once however often it is
// seen, and each repository's cursor never moves back.
package feed

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/work"
	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// SCM is what the follower reads from the SCM. StartPre needs ChangeLists and Repositories alone.
type SCM struct {
	Events       scmpb.EventsClient
	ChangeLists  scmpb.ChangeListsClient
	Repositories scmpb.RepositoriesClient
	Browse       scmpb.BrowseClient
}

// Follower plans the patchsets the SCM's change feed announces.
type Follower struct {
	store  *store.Store
	stream bus.Stream
	scm    SCM
	log    *slog.Logger
	// holder names this replica's claims on repositories (feedClaim).
	holder string
}

// New builds a follower of the SCM's events on stream.
func New(s *store.Store, stream bus.Stream, scm SCM, log *slog.Logger) *Follower {
	return &Follower{store: s, stream: stream, scm: scm, log: log, holder: work.NewHolder()}
}

// feedClaim is a repository's events being applied, as keyed work (internal/work): applying one is
// several transactions and calls to the SCM, which no row lock can span, so the replica applying a
// repository's event claims the repository first, and the others leave it until it lets go. Its
// events are applied once each, in order, however many replicas see them.
const feedClaim = "feed"

// feedLease is how long a claim lasts unrenewed: a replica that dies applying an event holds its
// repository this long.
const feedLease = 30 * time.Second

// Run follows the SCM's events until ctx ends, starting again after a failure.
func (f *Follower) Run(ctx context.Context) error {
	fl := &follow.Follower{Stream: f.stream, Topic: scmfeed.Topic, Group: "automation", Log: scmfeed.Log{Events: f.scm.Events}, Apply: applier{f}}
	for {
		if err := fl.Run(ctx); ctx.Err() == nil {
			f.log.Warn("feed: stopped following", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

// applier is the follower as a follow.Applier: the repository claimed, an event applied, then its
// cursor moved past it.
type applier struct{ *Follower }

func (a applier) Cursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return a.store.Cursors(ctx, repos)
}

func (a applier) Apply(ctx context.Context, e bus.Event) error {
	held, release, ok, err := work.Claim(ctx, a.store.Work(), feedClaim, e.Key, a.holder, feedLease)
	if err != nil {
		return err
	}
	if !ok {
		return follow.ErrBusy
	}
	defer release()
	cursor, err := a.store.Cursor(held, e.Key)
	if err != nil || e.Seq <= cursor {
		return err
	}
	ev, err := scmfeed.Decode(e)
	if err != nil {
		return err
	}
	if err := a.Follower.Apply(held, ev); err != nil {
		return fmt.Errorf("event %d of repository %d: %w", e.Seq, e.Key, err)
	}
	if err := a.store.SetCursor(held, e.Key, e.Seq); err != nil {
		return err
	}
	feedLag.With(scmfeed.Kind(ev)).Observe(time.Since(ev.GetAt().AsTime()).Seconds())
	return nil
}

var feedLag = metrics.NewHistogram("ok_automation_feed_lag_seconds",
	"How long after the SCM wrote an event ok automation applied it, by the event's kind: an upload's pre is planned once its event is.",
	metrics.Seconds, "kind")

// Apply acts on one event. An error is one worth retrying: the event is applied again later.
func (f *Follower) Apply(ctx context.Context, ev *scmpb.Event) error {
	repo := ev.GetRepositoryId()
	var err error
	switch k := ev.GetKind().(type) {
	case *scmpb.Event_Uploaded:
		u := k.Uploaded
		if !u.GetCreated() {
			if err := f.cancelSubmissions(ctx, repo, u.GetNumber(), u.GetPatchset(),
				fmt.Sprintf("patchset %d of CL %d was uploaded while it was submitting", u.GetPatchset(), u.GetNumber())); err != nil {
				return err
			}
		}
		err = f.settle(ctx, repo, u.GetNumber(), u.GetUploaderId())
	case *scmpb.Event_Transitioned:
		t := k.Transitioned
		switch t.GetTo() {
		case scmpb.ChangeListStatus_CHANGE_LIST_STATUS_ABANDONED:
			if err := f.cancelSubmissions(ctx, repo, t.GetNumber(), 0, fmt.Sprintf("CL %d was abandoned", t.GetNumber())); err != nil {
				return err
			}
			if err = f.store.EndPre(ctx, repo, t.GetNumber(), fmt.Sprintf("CL %d was abandoned", t.GetNumber())); err == nil {
				err = f.settle(ctx, repo, t.GetNumber(), t.GetActorId())
			}
		case scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN, scmpb.ChangeListStatus_CHANGE_LIST_STATUS_DRAFT:
			if t.GetTo() == scmpb.ChangeListStatus_CHANGE_LIST_STATUS_DRAFT {
				if err := f.cancelSubmissions(ctx, repo, t.GetNumber(), 0, fmt.Sprintf("CL %d returned to draft", t.GetNumber())); err != nil {
					return err
				}
			}
			err = f.settle(ctx, repo, t.GetNumber(), t.GetActorId())
		}
	case *scmpb.Event_Landed:
		l := k.Landed
		if err := f.store.RecordLandedChange(ctx, repo, l.GetCommitId(), l.GetNumber()); err != nil {
			return err
		}
		err = f.store.EndPre(ctx, repo, l.GetNumber(), fmt.Sprintf("CL %d landed", l.GetNumber()))
	case *scmpb.Event_HeadMoved:
		m := k.HeadMoved
		err = f.store.RecordMainCommit(ctx, repo, m.GetHeadSeq(), m.GetNewHead())
	}
	if st, ok := status.FromError(err); ok && err != nil {
		switch st.Code() {
		case codes.NotFound, codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied:
			// The SCM refuses it for what it is, and always will: plan nothing, and move on.
			f.log.Warn("feed: not planned", "repository", repo, "seq", ev.GetRepositorySeq(), "err", err)
			return nil
		}
	}
	return err
}

// cancelSubmissions cancels a CL's active submissions — those of patchsets below patchset, or
// every one when patchset is 0 — saying why.
func (f *Follower) cancelSubmissions(ctx context.Context, repo, number int64, patchset int32, reason string) error {
	n, err := f.store.CancelSubmissionsOf(ctx, repo, number, patchset, reason)
	if err == nil && n > 0 {
		f.log.Info("feed: canceled submissions", "repository", repo, "cl", number, "submissions", n, "reason", reason)
	}
	return err
}

// settle decides pre for a CL whose state changed, and for the CL it is stacked on: pre runs at
// the top of a stack — a CL in review that no CL in review is stacked on — over the whole stack.
// The CL, when it is the top, is planned, or its pre resumed where a stop canceled it. The CL
// under it, when in review, stops being the top as this one is in review — its unfinished pre is
// canceled — and is the top again, planned or resumed, as this one is not.
func (f *Follower) settle(ctx context.Context, repo, number int64, actor string) error {
	cl, err := f.scm.ChangeLists.GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: number})
	if err != nil {
		return err
	}
	open := cl.GetStatus() == scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN
	if open {
		top, err := f.isTop(ctx, repo, cl)
		if err != nil {
			return err
		}
		if top {
			if err := f.planTop(ctx, repo, cl, actor, ""); err != nil {
				return err
			}
		}
	}
	parent := cl.GetParentNumber()
	if parent == 0 {
		return nil
	}
	p, err := f.scm.ChangeLists.GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: parent})
	if err != nil || p.GetStatus() != scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN {
		return err
	}
	if open {
		return f.store.EndPre(ctx, repo, parent, fmt.Sprintf("CL %d, in review, is stacked on it: pre runs at the stack's top", number))
	}
	top, err := f.isTop(ctx, repo, p)
	if err != nil || !top {
		return err
	}
	return f.planTop(ctx, repo, p, actor, fmt.Sprintf("CL %d, stacked on it, is not in review: CL %d is the top of its stack", number, parent))
}

// isTop reports whether a CL in review is the top of its stack: no CL in review is stacked on it.
// An SCM predating ListChangeLists' parent_number lists every CL in review, which the parent check
// sorts.
func (f *Follower) isTop(ctx context.Context, repo int64, cl *scmpb.ChangeList) (bool, error) {
	if cl.GetStatus() != scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN {
		return false, nil
	}
	req := &scmpb.ListChangeListsRequest{RepositoryId: repo, ParentNumber: cl.GetNumber(),
		Statuses: []scmpb.ChangeListStatus{scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN}}
	for {
		resp, err := f.scm.ChangeLists.ListChangeLists(ctx, req)
		if err != nil {
			return false, err
		}
		for _, c := range resp.GetChangeLists() {
			if c.GetParentNumber() == cl.GetNumber() {
				return false, nil
			}
		}
		if resp.GetNextPageToken() == "" {
			return true, nil
		}
		req.PageToken = resp.GetNextPageToken()
	}
}

// maxStack bounds how far stackBase walks down a stack: the SCM's bound on one landing.
const maxStack = 1000

// stackBase is the commit a CL's stack leaves main from — the base of the current patchset of
// its lowest CL not landed — and how many CLs are under it: pre at the top diffs against it, so
// every path any CL of the stack changes is planned.
func (f *Follower) stackBase(ctx context.Context, repo int64, cl *scmpb.ChangeList, ps *scmpb.Patchset) ([]byte, int, error) {
	base, below := ps.GetBaseCommitId(), 0
	for cur := cl; cur.GetParentNumber() != 0 && below < maxStack; {
		p, err := f.scm.ChangeLists.GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: cur.GetParentNumber()})
		if err != nil {
			return nil, 0, err
		}
		if st := p.GetStatus(); st != scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN && st != scmpb.ChangeListStatus_CHANGE_LIST_STATUS_DRAFT {
			break
		}
		pps := patchsetOf(p, p.GetCurrentPatchset())
		if pps == nil {
			break
		}
		base, cur = pps.GetBaseCommitId(), p
		below++
	}
	return base, below, nil
}

// patchsetOf is a CL's patchset, or nil.
func patchsetOf(cl *scmpb.ChangeList, number int32) *scmpb.Patchset {
	for _, p := range cl.GetPatchsets() {
		if p.GetNumber() == number {
			return p
		}
	}
	return nil
}

// planTop plans pre on a CL's current patchset, over its whole stack, saying why (unset: its
// upload) — or, when it is planned already, resumes the jobs a stop canceled.
func (f *Follower) planTop(ctx context.Context, repo int64, cl *scmpb.ChangeList, requestedBy, why string) error {
	number, patchset := cl.GetNumber(), cl.GetCurrentPatchset()
	ps := patchsetOf(cl, patchset)
	if ps == nil {
		return status.Errorf(codes.NotFound, "CL %d has no patchset %d", number, patchset)
	}
	base, below, err := f.stackBase(ctx, repo, cl, ps)
	if err != nil {
		return err
	}
	r, err := f.scm.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
	if err != nil {
		return err
	}
	data, err := json.Marshal(workflow.Change{
		Phase:      workflow.Pre,
		Repository: workflow.Repository{ID: repo, Name: r.GetName()},
		CL:         number,
		Patchset:   patchset,
		Author:     cl.GetAuthor(),
		Commit:     hex.EncodeToString(ps.GetCommitId()),
		Base:       hex.EncodeToString(base),
	})
	if err != nil {
		return err
	}
	if requestedBy == "" {
		requestedBy = ps.GetUploadedById()
	}
	if why == "" {
		why = fmt.Sprintf("patchset %d was uploaded", patchset)
	}
	if below > 0 {
		why += fmt.Sprintf(": it is the top of a stack of %d, and pre runs over all of it", below+1)
	}
	plan, created, err := f.store.CreatePlan(ctx, store.PlanRequest{
		RepositoryID:    repo,
		CLNumber:        number,
		Patchset:        patchset,
		CommitID:        ps.GetCommitId(),
		WorkflowsCommit: r.GetHead(),
		Context:         data,
		RequestedBy:     requestedBy,
		Why:             why,
	})
	if err != nil {
		return err
	}
	if created {
		f.log.Info("feed: planning pre", "repository", r.GetName(), "cl", number, "patchset", patchset, "plan", plan.ID, "stack", below+1)
		return nil
	}
	n, err := f.store.ResumePre(ctx, repo, number, patchset, requestedBy)
	if err == nil && n > 0 {
		f.log.Info("feed: resumed pre", "repository", r.GetName(), "cl", number, "patchset", patchset, "runs", n)
	}
	return err
}

// StartPre runs pre by hand on a CL's current patchset — a draft, or a CL under its stack's top —
// over everything under it, as at a stack's top; or, when it is planned already, resumes the jobs
// a stop canceled. It returns the patchset.
func (f *Follower) StartPre(ctx context.Context, repo, number int64, requestedBy string) (int32, error) {
	cl, err := f.scm.ChangeLists.GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: number})
	if err != nil {
		return 0, err
	}
	switch cl.GetStatus() {
	case scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN, scmpb.ChangeListStatus_CHANGE_LIST_STATUS_DRAFT:
	default:
		return 0, status.Errorf(codes.FailedPrecondition, "CL %d is neither a draft nor in review: no pre runs on it", number)
	}
	return cl.GetCurrentPatchset(), f.planTop(ctx, repo, cl, requestedBy, "run by hand")
}
