// Package feed follows the SCM's change feed (docs/scm.md, "Events") into ok
// automation: each patchset uploaded to an open or draft CL gets a plan of its pre, whose
// planning run a worker takes (docs/automation.md, "Execution"). A CL that lands or is
// abandoned has its unfinished pre runs canceled; one restored resumes its current patchset's
// pre, or has it planned if it never was. For the landing queue it records main's history —
// each commit main moves to, and the CL it is — and cancels a submission whose CL gets a new
// patchset, is abandoned, or returns to draft.
//
// Every replica follows the SCM's events, as the bus's stream brings them and each repository's
// log fills in, sharing the repositories (the stream's group, automation; internal/bus/follow).
// An event's plan is recorded once however often it is seen, and each repository's cursor never
// moves back, so applying an event again plans nothing twice.
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
	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// SCM is what the follower reads from the SCM.
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
}

// New builds a follower of the SCM's events on stream.
func New(s *store.Store, stream bus.Stream, scm SCM, log *slog.Logger) *Follower {
	return &Follower{store: s, stream: stream, scm: scm, log: log}
}

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

// applier is the follower as a follow.Applier and follow.Legacy: an event applied, then its
// repository's cursor moved past it.
type applier struct{ *Follower }

func (a applier) Cursor(ctx context.Context, repo int64) (int64, error) {
	return a.store.Cursor(ctx, repo)
}

func (a applier) Apply(ctx context.Context, e bus.Event) error {
	cursor, err := a.store.Cursor(ctx, e.Key)
	if err != nil || e.Seq <= cursor {
		return err
	}
	ev, err := scmfeed.Decode(e)
	if err != nil {
		return err
	}
	if err := a.Follower.Apply(ctx, ev); err != nil {
		return fmt.Errorf("event %d of repository %d: %w", e.Seq, e.Key, err)
	}
	return a.store.SetCursor(ctx, e.Key, e.Seq)
}

func (a applier) LegacyCursor(ctx context.Context) (int64, error) { return a.store.LegacyCursor(ctx) }
func (a applier) Seed(ctx context.Context, repo, seq int64) error {
	return a.store.Seed(ctx, repo, seq)
}
func (a applier) LeaveLegacy(ctx context.Context) error { return a.store.LeaveLegacy(ctx) }

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
		_, err = f.plan(ctx, repo, u.GetNumber(), u.GetPatchset(), u.GetUploader())
	case *scmpb.Event_Transitioned:
		t := k.Transitioned
		switch t.GetTo() {
		case scmpb.ChangeListStatus_CHANGE_LIST_STATUS_ABANDONED:
			if err := f.cancelSubmissions(ctx, repo, t.GetNumber(), 0, fmt.Sprintf("CL %d was abandoned", t.GetNumber())); err != nil {
				return err
			}
			err = f.store.EndPre(ctx, repo, t.GetNumber(), fmt.Sprintf("CL %d was abandoned", t.GetNumber()))
		case scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN, scmpb.ChangeListStatus_CHANGE_LIST_STATUS_DRAFT:
			if t.GetTo() == scmpb.ChangeListStatus_CHANGE_LIST_STATUS_DRAFT {
				if err := f.cancelSubmissions(ctx, repo, t.GetNumber(), 0, fmt.Sprintf("CL %d returned to draft", t.GetNumber())); err != nil {
					return err
				}
			}
			var patchset int32
			patchset, err = f.plan(ctx, repo, t.GetNumber(), 0, t.GetActor())
			if err == nil && patchset > 0 && t.GetFrom() == scmpb.ChangeListStatus_CHANGE_LIST_STATUS_ABANDONED {
				var n int
				if n, err = f.store.ResumePre(ctx, repo, t.GetNumber(), patchset, t.GetActor()); err == nil && n > 0 {
					f.log.Info("feed: resumed pre", "repository", repo, "cl", t.GetNumber(), "patchset", patchset, "runs", n)
				}
			}
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

// plan records the pre plan of a CL's current patchset, if it is patchset (0: whichever is
// current) and the CL is open or a draft: a later patchset's upload plans that one instead, and
// a closed CL runs no pre. It returns the patchset it planned, or found planned (0: none).
func (f *Follower) plan(ctx context.Context, repo, number int64, patchset int32, requestedBy string) (int32, error) {
	cl, err := f.scm.ChangeLists.GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: number})
	if err != nil {
		return 0, err
	}
	switch cl.GetStatus() {
	case scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN, scmpb.ChangeListStatus_CHANGE_LIST_STATUS_DRAFT:
	default:
		return 0, nil
	}
	if patchset == 0 {
		patchset = cl.GetCurrentPatchset()
	}
	if cl.GetCurrentPatchset() != patchset {
		return 0, nil
	}
	var ps *scmpb.Patchset
	for _, p := range cl.GetPatchsets() {
		if p.GetNumber() == patchset {
			ps = p
		}
	}
	if ps == nil {
		return 0, status.Errorf(codes.NotFound, "CL %d has no patchset %d", number, patchset)
	}
	r, err := f.scm.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
	if err != nil {
		return 0, err
	}
	data, err := json.Marshal(workflow.Change{
		Phase:      workflow.Pre,
		Repository: workflow.Repository{ID: repo, Name: r.GetName()},
		CL:         number,
		Patchset:   patchset,
		Author:     cl.GetAuthor(),
		Commit:     hex.EncodeToString(ps.GetCommitId()),
		Base:       hex.EncodeToString(ps.GetBaseCommitId()),
	})
	if err != nil {
		return 0, err
	}
	if requestedBy == "" {
		requestedBy = ps.GetUploadedBy()
	}
	plan, created, err := f.store.CreatePlan(ctx, store.PlanRequest{
		RepositoryID:    repo,
		CLNumber:        number,
		Patchset:        patchset,
		CommitID:        ps.GetCommitId(),
		WorkflowsCommit: r.GetHead(),
		Context:         data,
		RequestedBy:     requestedBy,
	})
	if err != nil {
		return 0, err
	}
	if created {
		f.log.Info("feed: planning pre", "repository", r.GetName(), "cl", number, "patchset", patchset, "plan", plan.ID)
	}
	return patchset, nil
}
