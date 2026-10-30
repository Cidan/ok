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
// A replica applies a repository's events only holding the repository's claim (feedClaim), a page
// of its log at a time: each CL's events in order, the page's CLs beside each other. An event's
// plan is recorded once however often it is seen, and each repository's cursor never moves back.
package feed

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
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
	store *store.Store
	scm   SCM
	log   *slog.Logger
	// holder names this replica's claims on repositories (feedClaim).
	holder string
	// applying is called as each event of a page begins and ends being applied (tests).
	applying func(ev *scmpb.Event, enter bool)
}

// New builds a follower of the SCM's events.
func New(s *store.Store, scm SCM, log *slog.Logger) *Follower {
	return &Follower{store: s, scm: scm, log: log, holder: work.NewHolder()}
}

// feedClaim is a repository's events being applied, as keyed work (internal/work): applying one is
// several transactions and calls to the SCM, which no row lock can span, so the replica applying a
// repository's event claims the repository first, and the others leave it until it lets go. Its
// events are applied once each, in order, however many replicas see them.
const feedClaim = "feed"

// feedLease is how long a claim lasts unrenewed: a replica that dies applying an event holds its
// repository this long.
const feedLease = 30 * time.Second

// Run follows the SCM's events until ctx ends.
func (f *Follower) Run(ctx context.Context) error {
	return (&follow.Follower{Topic: scmfeed.Topic, Log: scmfeed.Log{Events: f.scm.Events}, Apply: applier{f},
		Work: f.store.Work(), Register: scmfeed.Register(f.scm.Events), Logger: f.log}).Run(ctx)
}

// applier is the follower as a follow.BatchApplier: the repository claimed, a page of its events
// applied, then its cursor moved past them.
type applier struct{ *Follower }

func (a applier) Cursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return a.store.Cursors(ctx, repos)
}

func (a applier) Apply(ctx context.Context, e bus.Event) error {
	return a.ApplyBatch(ctx, []bus.Event{e})
}

// clsAtOnce bounds how many of a page's CLs a replica applies events of at once.
const clsAtOnce = 32

// ApplyBatch applies a page of a repository's events: each CL's in order, and the page's CLs beside
// each other, between the events that name no CL, which are applied in their place. One at a time, a repository's events were applied at about five a second — a claim,
// the SCM asked about the CL and its stack, a plan, a cursor — and a herd of 3,000 CLs waited
// eleven minutes for pre. An event's effects are its CL's and its stack's, each a transaction
// under the CL's lock (store.LockCL) acting on what the SCM says is the case now, not on what was
// as the event was written (settle): so CLs' events commute, and a page applied again — as part
// of one is after a failure — applies each CL's events in order again, to where they were. The
// cursor moves past the events applied, to the first that failed.
func (a applier) ApplyBatch(ctx context.Context, evs []bus.Event) error {
	repo := evs[0].Key
	// Applied already — by another replica: nothing to claim.
	cursors, err := a.store.Cursors(ctx, []int64{repo})
	if err != nil || cursors[repo] >= evs[len(evs)-1].Seq {
		return err
	}
	held, release, ok, err := work.Claim(ctx, a.store.Work(), feedClaim, repo, a.holder, feedLease)
	if err != nil {
		return err
	}
	if !ok {
		return follow.ErrBusy
	}
	defer release()
	cursor, err := a.store.Cursor(held, repo)
	if err != nil {
		return err
	}
	var page []*scmpb.Event
	var seqs []int64
	for _, e := range evs {
		if e.Seq <= cursor {
			continue
		}
		ev, err := scmfeed.Decode(e)
		if err != nil {
			return err
		}
		page, seqs = append(page, ev), append(seqs, e.Seq)
	}
	if len(page) == 0 {
		return nil
	}
	// Runs of the page between the events that name no CL — main moving, whose recording reads what
	// the landings before it recorded — each applied whole, its CLs' events beside each other, before
	// the event after it, alone, and the run after that.
	applied := make([]bool, len(page))
	for start := 0; start < len(page); {
		end := start
		for end < len(page) && clOf(page[end]) != 0 {
			end++
		}
		if end == start {
			end++ // an event of no CL, alone
		}
		if err := a.applyRun(held, repo, page, seqs, start, end, applied); err != nil {
			return errors.Join(err, a.moveCursor(held, repo, seqs, applied))
		}
		start = end
	}
	return a.moveCursor(held, repo, seqs, applied)
}

// applyRun applies page[start:end], each CL's events in order and the CLs beside each other,
// marking each event applied.
func (a applier) applyRun(ctx context.Context, repo int64, page []*scmpb.Event, seqs []int64, start, end int, applied []bool) error {
	var order [][]int
	byCL := map[int64]int{}
	for i := start; i < end; i++ {
		cl := clOf(page[i])
		g, ok := byCL[cl]
		if !ok || cl == 0 {
			g = len(order)
			byCL[cl] = g
			order = append(order, nil)
		}
		order[g] = append(order[g], i)
	}
	errs := make([]error, len(order))
	sem := make(chan struct{}, clsAtOnce)
	var wg sync.WaitGroup
	for g, idx := range order {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			for _, i := range idx {
				if a.applying != nil {
					a.applying(page[i], true)
				}
				err := a.Follower.Apply(ctx, page[i])
				if a.applying != nil {
					a.applying(page[i], false)
				}
				if err != nil {
					errs[g] = fmt.Errorf("event %d of repository %d: %w", seqs[i], repo, err)
					return
				}
				applied[i] = true
				feedLag.With(scmfeed.Kind(page[i])).Observe(time.Since(page[i].GetAt().AsTime()).Seconds())
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// moveCursor moves the repository's cursor past the page's events applied, to the first that was
// not.
func (a applier) moveCursor(ctx context.Context, repo int64, seqs []int64, applied []bool) error {
	done := 0
	for done < len(applied) && applied[done] {
		done++
	}
	if done == 0 {
		return nil
	}
	return a.store.SetCursor(ctx, repo, seqs[done-1])
}

// clOf is the CL an event is of: 0 for none.
func clOf(ev *scmpb.Event) int64 {
	switch k := ev.GetKind().(type) {
	case *scmpb.Event_Uploaded:
		return k.Uploaded.GetNumber()
	case *scmpb.Event_Transitioned:
		return k.Transitioned.GetNumber()
	case *scmpb.Event_Landed:
		return k.Landed.GetNumber()
	}
	return 0
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
