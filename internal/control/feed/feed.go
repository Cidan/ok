// Package feed follows the SCM's change feed (docs/scm.md, "Events") into ok
// control's database: the repository and CL projections, the change log's uploads,
// transitions, and landings, the reviewers an upload names, and the approved projection, which a new
// patchset, a restore, or a landing that edits OWNERS or the groups-and-ACLs tree can change.
//
// Every replica follows the SCM's events, as the bus's stream brings them and each repository's
// log fills in, sharing the repositories (the stream's group, control; internal/bus/follow); the
// replica that claims a repository (feedClaim) applies a page of its log at a time, each page in one
// transaction with its cursor, advanced only past events not yet applied: even two followers could
// never apply an event twice.
package feed

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/owners"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/work"
	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Follower applies the SCM's change feed.
type Follower struct {
	db     db.Store
	stream bus.Stream
	scm    *scm.Client
	review *review.Service
	// holder names this replica's claims on repositories (feedClaim).
	holder string

	// retry holds CLs whose approval could not be re-derived, for the next pass.
	mu    sync.Mutex
	retry map[[2]int64]bool
}

// New builds a follower of the SCM's events on stream.
func New(d db.Store, stream bus.Stream, c *scm.Client, r *review.Service) *Follower {
	return &Follower{db: d, stream: stream, scm: c, review: r, holder: work.NewHolder(), retry: map[[2]int64]bool{}}
}

// feedClaim is a repository's events being applied, as keyed work (internal/work): every replica
// follows the stream, and the one that claims a repository applies a page of its log at a time,
// each page in one transaction; the others leave it until it lets go. Every replica applying every
// event of a busy monorepo, each a transaction racing the others for its cursor's row, ran minutes
// behind it.
const feedClaim = "control-feed"

// feedLease is how long a claim lasts unrenewed: a replica that dies applying a page holds its
// repository this long.
const feedLease = 30 * time.Second

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

// applier is the follower as a follow.BatchApplier: the repository claimed, a page of its events
// applied, its cursor moved past them.
type applier struct{ *Follower }

func (a applier) Cursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return a.db.FollowCursors(ctx, scmfeed.Topic, repos)
}

func (a applier) Apply(ctx context.Context, e bus.Event) error {
	return a.ApplyBatch(ctx, []bus.Event{e})
}

func (a applier) ApplyBatch(ctx context.Context, evs []bus.Event) error {
	repo := evs[0].Key
	// Applied already — by another replica, as most a busy repository's are: nothing to claim.
	cursors, err := a.db.FollowCursors(ctx, scmfeed.Topic, []int64{repo})
	if err != nil || cursors[repo] >= evs[len(evs)-1].Seq {
		return err
	}
	held, release, ok, err := work.Claim(ctx, a.db.Work(), feedClaim, repo, a.holder, feedLease)
	if err != nil {
		return err
	}
	if !ok {
		return follow.ErrBusy
	}
	defer release()
	out := make([]*scmpb.Event, 0, len(evs))
	for _, e := range evs {
		if e.Seq <= cursors[repo] {
			continue
		}
		ev, err := scmfeed.Decode(e)
		if err != nil {
			return err
		}
		out = append(out, ev)
	}
	return a.Follower.applyAll(held, repo, out)
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

// numberOf is the CL an event is about, 0 for none.
func numberOf(ev *scmpb.Event) int64 {
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

// prefetchAtOnce bounds the CLs a page's application reads from the SCM at once, and the approvals
// it re-derives at once after.
const prefetchAtOnce = 16

// Apply applies one event (applyAll).
func (f *Follower) Apply(ctx context.Context, ev *scmpb.Event) error {
	return f.applyAll(ctx, ev.GetRepositoryId(), []*scmpb.Event{ev})
}

// applyAll applies a run of a repository's events, in order, in one transaction with its cursor —
// those after it — then re-derives the approval of the CLs they can change. What they need of the
// other services is read first, outside the transaction: each CL they name once, as the SCM has it
// now (as applying each when it came read it), the people its uploads assign, and whether the
// moves of main among them, taken together, changed what approval is decided by.
func (f *Follower) applyAll(ctx context.Context, repo int64, evs []*scmpb.Event) error {
	if len(evs) == 0 {
		return nil
	}
	ctx = move.WithRepository(ctx, repo)
	var numbers []int64
	cls := map[int64]*scmpb.ChangeList{}
	for _, ev := range evs {
		if n := numberOf(ev); n != 0 {
			if _, ok := cls[n]; !ok {
				cls[n] = nil
				numbers = append(numbers, n)
			}
		}
	}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(prefetchAtOnce)
	for _, n := range numbers {
		g.Go(func() error {
			c, err := f.scm.ChangeLists.GetChangeList(gctx, &scmpb.GetChangeListRequest{RepositoryId: repo, Number: n})
			if err != nil {
				return err
			}
			mu.Lock()
			cls[n] = c
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	// The people an upload assigns, decided before the transaction: asking ok identity holds no
	// transaction open.
	type assignment struct {
		number int64
		person string
	}
	assignable := map[assignment]bool{}
	var first, last *scmpb.HeadMoved
	for _, ev := range evs {
		if u := ev.GetUploaded(); u != nil {
			for _, p := range u.GetReviewerIds() {
				a := assignment{u.GetNumber(), p}
				if _, ok := assignable[a]; !ok {
					assignable[a] = p != cls[u.GetNumber()].GetAuthorId() && people.IsHumanID(ctx, p)
				}
			}
		}
		if m := ev.GetHeadMoved(); m != nil {
			if first == nil {
				first = m
			}
			last = m
		}
	}
	var refreshAll []int64
	if first != nil {
		edits, err := f.editsOwners(ctx, repo, &scmpb.HeadMoved{OldHead: first.GetOldHead(), NewHead: last.GetNewHead(), HeadSeq: last.GetHeadSeq()})
		if err != nil {
			return err
		}
		if edits {
			active, err := f.db.ActiveChangeLists(ctx, repo)
			if err != nil {
				return err
			}
			for _, a := range active {
				refreshAll = append(refreshAll, a.Number)
			}
		}
	}
	var applied []*scmpb.Event
	var refresh []int64
	err := f.db.Txn(ctx, func(tx db.Ops) error {
		applied, refresh = nil, nil
		cursor, err := tx.LockFollowCursor(ctx, scmfeed.Topic, repo)
		if err != nil {
			return err
		}
		upserted := map[int64]bool{}
		for _, ev := range evs {
			if ev.GetRepositorySeq() <= cursor {
				continue
			}
			number := numberOf(ev)
			cl := cls[number]
			if cl != nil && !upserted[number] {
				if _, err := tx.UpsertChangeList(ctx, scm.ChangeListOut(cl)); err != nil {
					return err
				}
				upserted[number] = true
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
					if !assignable[assignment{number, p}] {
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
				refresh = append(refresh, number)
			case *scmpb.Event_Transitioned:
				e := transitionEvent(k.Transitioned)
				e.At, e.Patchset = at, cl.GetCurrentPatchset()
				if err := tx.RecordEvent(ctx, repo, number, e); err != nil {
					return err
				}
				if e.To.Active() {
					refresh = append(refresh, number)
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
			applied = append(applied, ev)
		}
		if len(applied) == 0 {
			return nil
		}
		return tx.SetFollowCursor(ctx, scmfeed.Topic, repo, applied[len(applied)-1].GetRepositorySeq())
	})
	if err != nil || len(applied) == 0 {
		return err
	}
	for _, ev := range applied {
		feedLag.With(scmfeed.Kind(ev)).Observe(time.Since(ev.GetAt().AsTime()).Seconds())
	}
	f.refreshAll(ctx, repo, append(refreshAll, refresh...))
	return nil
}

// refreshAll re-derives the approval of a repository's CLs, each once, a few at once.
func (f *Follower) refreshAll(ctx context.Context, repo int64, numbers []int64) {
	seen := map[int64]bool{}
	var wg sync.WaitGroup
	sem := make(chan struct{}, prefetchAtOnce)
	for _, n := range numbers {
		if seen[n] {
			continue
		}
		seen[n] = true
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			f.refresh(ctx, [2]int64{repo, n})
		})
	}
	wg.Wait()
}

var feedLag = metrics.NewHistogram("ok_control_feed_lag_seconds",
	"How long after the SCM wrote an event ok control applied it, by the event's kind: how far behind its change logs, lists of CLs, reviewers, approvals, and webhooks run.",
	metrics.Seconds, "kind")

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
