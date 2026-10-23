// Package feed follows the SCM's change feed (docs/scm.md, "Events") into ok
// control's database: the repository and CL projections, the change log's uploads,
// transitions, and landings, the reviewers an upload names, and the approved projection, which a new
// patchset, a restore, or a landing that edits OWNERS or the groups-and-ACLs tree can change.
//
// Every replica follows the SCM's events, as the bus's stream brings them and each repository's
// log fills in, sharing the repositories (the stream's group, control; internal/bus/follow); each
// event is applied in one transaction with its repository's cursor, advanced only past an event
// not yet applied: even two followers could never apply an event twice.
package feed

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/owners"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Follower applies the SCM's change feed.
type Follower struct {
	db     db.Store
	stream bus.Stream
	scm    *scm.Client
	review *review.Service

	// retry holds CLs whose approval could not be re-derived, for the next pass.
	mu    sync.Mutex
	retry map[[2]int64]bool
}

// New builds a follower of the SCM's events on stream.
func New(d db.Store, stream bus.Stream, c *scm.Client, r *review.Service) *Follower {
	return &Follower{db: d, stream: stream, scm: c, review: r, retry: map[[2]int64]bool{}}
}

// Run follows the SCM's events until ctx ends, starting again after a failure, and retries the
// approvals it could not re-derive every 30 seconds.
func (f *Follower) Run(ctx context.Context) error {
	go func() {
		retry := time.NewTicker(30 * time.Second)
		defer retry.Stop()
		for {
			select {
			case <-retry.C:
				f.refreshPending(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
	fl := &follow.Follower{Stream: f.stream, Topic: scmfeed.Topic, Group: "control", Log: scmfeed.Log{Events: f.scm.Events}, Apply: applier{f}}
	for {
		if err := fl.Run(ctx); ctx.Err() == nil {
			slog.Warn("feed: stopped following", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

// applier is the follower as a follow.Applier.
type applier struct{ *Follower }

func (a applier) Cursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return a.db.FollowCursors(ctx, scmfeed.Topic, repos)
}

func (a applier) Apply(ctx context.Context, e bus.Event) error {
	ev, err := scmfeed.Decode(e)
	if err != nil {
		return err
	}
	return a.Follower.Apply(ctx, ev)
}

// transitionEvent maps an SCM transition onto the change log.
func transitionEvent(t *scmpb.ChangeListTransitioned) db.Event {
	e := db.Event{Actor: t.GetActorId(), From: db.Status(t.GetFrom()), To: db.Status(t.GetTo()), Message: t.GetReason()}
	switch {
	case e.To == db.StatusAbandoned:
		e.Kind = db.EventAbandoned
	case e.From == db.StatusAbandoned:
		e.Kind = db.EventRestored
	case e.To == db.StatusOpen:
		e.Kind = db.EventReviewRequested
	default:
		e.Kind = db.EventReturnedToDraft
	}
	return e
}

// Apply applies one event, in one transaction with its repository's cursor, then re-derives
// the approval of the CLs it can change.
func (f *Follower) Apply(ctx context.Context, ev *scmpb.Event) error {
	repo := ev.GetRepositoryId()
	ctx = move.WithRepository(ctx, repo)
	var number int64
	var cl *scmpb.ChangeList
	switch k := ev.GetKind().(type) {
	case *scmpb.Event_Uploaded:
		number = k.Uploaded.GetNumber()
	case *scmpb.Event_Transitioned:
		number = k.Transitioned.GetNumber()
	case *scmpb.Event_Landed:
		number = k.Landed.GetNumber()
	}
	if number != 0 {
		c, err := f.scm.ChangeLists.GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: number})
		if err != nil {
			return err
		}
		cl = c
	}
	// The people an upload assigns, decided before the transaction: asking ok identity holds no
	// transaction open.
	assignable := map[string]bool{}
	if u := ev.GetUploaded(); u != nil {
		for _, p := range u.GetReviewerIds() {
			assignable[p] = p != cl.GetAuthorId() && people.IsHumanID(ctx, p)
		}
	}
	var refresh [][2]int64
	if moved := ev.GetHeadMoved(); moved != nil {
		edits, err := f.editsOwners(ctx, repo, moved)
		if err != nil {
			return err
		}
		if edits {
			active, err := f.db.ActiveChangeLists(ctx, repo)
			if err != nil {
				return err
			}
			for _, a := range active {
				refresh = append(refresh, [2]int64{repo, a.Number})
			}
		}
	}
	applied := false
	err := f.db.Txn(ctx, func(tx db.Ops) error {
		applied = false
		cursor, err := tx.LockFollowCursor(ctx, scmfeed.Topic, repo)
		if err != nil {
			return err
		}
		if cursor >= ev.GetRepositorySeq() {
			return nil
		}
		if cl != nil {
			if _, err := tx.UpsertChangeList(ctx, scm.ChangeListOut(cl)); err != nil {
				return err
			}
		}
		at := ev.GetAt().AsTime()
		switch k := ev.GetKind().(type) {
		case *scmpb.Event_Uploaded:
			u := k.Uploaded
			if err := tx.RecordEvent(ctx, repo, number, db.Event{At: at, Actor: u.GetUploaderId(), Kind: db.EventUploaded, Patchset: u.GetPatchset()}); err != nil {
				return err
			}
			// A rollback CL's creation shows on the CLs it rolls back.
			if u.GetCreated() {
				for _, n := range cl.GetRollback().GetChangeLists() {
					if err := tx.RecordEvent(ctx, repo, n, db.Event{At: at, Actor: u.GetUploaderId(), Kind: db.EventRolledBack, Related: number}); err != nil {
						return err
					}
				}
			}
			for _, p := range u.GetReviewerIds() {
				if !assignable[p] {
					continue
				}
				added, err := tx.AddReviewer(ctx, repo, number, p, u.GetUploaderId())
				if err != nil {
					return err
				}
				if added {
					if err := tx.RecordEvent(ctx, repo, number, db.Event{At: at, Actor: u.GetUploaderId(), Kind: db.EventReviewerAdded, Subject: p, Patchset: u.GetPatchset()}); err != nil {
						return err
					}
				}
			}
			refresh = append(refresh, [2]int64{repo, number})
		case *scmpb.Event_Transitioned:
			e := transitionEvent(k.Transitioned)
			e.At, e.Patchset = at, cl.GetCurrentPatchset()
			if err := tx.RecordEvent(ctx, repo, number, e); err != nil {
				return err
			}
			if e.To.Active() {
				refresh = append(refresh, [2]int64{repo, number})
			}
		case *scmpb.Event_Landed:
			l := k.Landed
			if err := tx.RecordEvent(ctx, repo, number, db.Event{At: at, Actor: l.GetSubmitterId(), Kind: db.EventSubmitted, Patchset: l.GetPatchset(), Commit: l.GetCommitId()}); err != nil {
				return err
			}
		case *scmpb.Event_RepositoryCreated:
			// Its name is the directory's, whose events this follower's twin applies (Directory).
			if err := tx.CreateRepository(ctx, db.Repository{ID: repo, CreatedBy: k.RepositoryCreated.GetCreatedById(), CreatedAt: at}); err != nil {
				return err
			}
		case *scmpb.Event_HeadMoved:
			if err := tx.MoveHead(ctx, repo, k.HeadMoved.GetHeadSeq(), at); err != nil {
				return err
			}
			// For push webhooks (docs/hosted.md, "ok control at scale, as built").
			if err := tx.QueueHeadMoved(ctx, repo, k.HeadMoved.GetHeadSeq(), k.HeadMoved.GetNewHead()); err != nil {
				return err
			}
		}
		applied = true
		return tx.SetFollowCursor(ctx, scmfeed.Topic, repo, ev.GetRepositorySeq())
	})
	if err != nil || !applied {
		return err
	}
	for _, r := range refresh {
		f.refresh(ctx, r)
	}
	return nil
}

// editsOwners reports whether a move of main changed what approval is decided by: an
// OWNERS-config file, the groups-and-ACLs tree, whose ACLs approve changes to it, or the
// repository's settings, which say where that tree is.
func (f *Follower) editsOwners(ctx context.Context, repo int64, moved *scmpb.HeadMoved) (bool, error) {
	dir, err := f.review.TreeDir(ctx, repo, moved.GetNewHead())
	if err != nil {
		return false, err
	}
	from, err := f.scm.Tree(ctx, repo, moved.GetOldHead())
	if err != nil {
		return false, err
	}
	to, err := f.scm.Tree(ctx, repo, moved.GetNewHead())
	if err != nil {
		return false, err
	}
	resp, err := f.scm.Browse.DiffTrees(ctx, &scmpb.DiffTreesRequest{RepositoryId: repo, FromTree: from, ToTree: to})
	if err != nil {
		return false, err
	}
	for _, c := range resp.GetChanges() {
		if owners.IsOwnersFile(c.GetPath()) || c.GetPath() == workflow.SettingsFile || c.GetOldPath() == workflow.SettingsFile ||
			(dir != "" && (strings.HasPrefix(c.GetPath(), dir+"/") || strings.HasPrefix(c.GetOldPath(), dir+"/"))) {
			return true, nil
		}
	}
	return false, nil
}

// refresh re-derives a CL's approval, keeping it for a retry when that fails.
func (f *Follower) refresh(ctx context.Context, cl [2]int64) {
	if err := f.review.Refresh(ctx, cl[0], cl[1]); err != nil {
		slog.Warn("feed: re-deriving approval", "repository", cl[0], "cl", cl[1], "err", err)
		f.mu.Lock()
		f.retry[cl] = true
		f.mu.Unlock()
	}
}

// refreshPending retries the CLs whose approval could not be re-derived.
func (f *Follower) refreshPending(ctx context.Context) {
	f.mu.Lock()
	pending := f.retry
	f.retry = map[[2]int64]bool{}
	f.mu.Unlock()
	for cl := range pending {
		f.refresh(ctx, cl)
	}
}
