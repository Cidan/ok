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
	"encoding/binary"
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
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Follower applies the SCM's change feed.
type Follower struct {
	db     db.Store
	scm    *scm.Client
	review *review.Service
	// holder names this replica's claims on repositories (feedClaim).
	holder string
}

// New builds a follower of the SCM's events.
func New(d db.Store, c *scm.Client, r *review.Service) *Follower {
	return &Follower{db: d, scm: c, review: r, holder: work.NewHolder()}
}

// approvalKind is the step that re-derives a CL's approval its events changed: a job the
// transaction that applies them enqueues as it is about to commit, naming the repository, the CL,
// and the place the transaction moves the repository's cursor to, which the queue retries until
// the approval is derived. A job that beats its transaction's commit waits for the cursor.
const approvalKind = "approval"

// approvalRetry is how long after a failed re-derivation it is tried again, and commitWithin how
// long a job waits for its transaction's cursor before it takes the transaction as one that never
// committed — another's applies its events, with jobs of its own.
const (
	approvalRetry = 30 * time.Second
	commitWithin  = time.Minute
)

// feedClaim is a repository's events being applied, as keyed work (internal/work): every replica
// follows the stream, and the one that claims a repository applies a page of its log at a time,
// each page in one transaction; the others leave it until it lets go. Every replica applying every
// event of a busy monorepo, each a transaction racing the others for its cursor's row, ran minutes
// behind it.
const feedClaim = "control-feed"

// feedLease is how long a claim lasts unrenewed: a replica that dies applying a page holds its
// repository this long.
const feedLease = 30 * time.Second

// Run follows the SCM's events until ctx ends, and re-derives the approvals a first try could not
// as their jobs come (approvalKind).
func (f *Follower) Run(ctx context.Context) error {
	go work.Consume(ctx, f.db.Work(), approvalKind, 4, approvalRetry, f.approvalStep, nil)
	go work.Consume(ctx, f.db.Work(), headKind, 4, time.Second, f.headStep, nil)
	return (&follow.Follower{Topic: scmfeed.Topic, Log: scmfeed.Log{Events: f.scm.Events}, Apply: applier{f},
		Work: f.db.Work(), Register: scmfeed.Register(f.scm.Events)}).Run(ctx)
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
				// For the watchers of CLs whose landing main's head decides (api.WatchStatus): those
				// the move can change hear of it once it commits (headStep).
				if err := tx.PendStep(ctx, headKind, repo, time.Time{}, headBody(k.HeadMoved.GetOldHead(), k.HeadMoved.GetNewHead())); err != nil {
					return err
				}
			}
			applied = append(applied, ev)
		}
		if len(applied) == 0 {
			return nil
		}
		seq := applied[len(applied)-1].GetRepositorySeq()
		if err := pendApprovals(ctx, tx, repo, seq, append(refreshAll, refresh...)); err != nil {
			return err
		}
		// The actions waiting for the feed to apply what they did read its cursor again.
		kind, key, body := wait.Changed(review.FeedSubject(repo))
		if err := tx.PendStep(ctx, kind, key, time.Time{}, body); err != nil {
			return err
		}
		return tx.SetFollowCursor(ctx, scmfeed.Topic, repo, seq)
	})
	if err != nil || len(applied) == 0 {
		return err
	}
	for _, ev := range applied {
		feedLag.With(scmfeed.Kind(ev)).Observe(time.Since(ev.GetAt().AsTime()).Seconds())
	}
	return nil
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

// approvalStep re-derives a CL's approval once the transaction that enqueued its job committed: the
// repository's cursor at or past the job's place.
func (f *Follower) approvalStep(ctx context.Context, repo int64, body []byte, age time.Duration) error {
	if len(body) != 16 {
		return nil
	}
	number, seq := int64(binary.BigEndian.Uint64(body)), int64(binary.BigEndian.Uint64(body[8:]))
	cursor, err := f.db.FollowCursor(ctx, scmfeed.Topic, repo)
	switch {
	case err != nil:
		return err
	case cursor < seq && age < commitWithin:
		return work.Later{After: min(max(age, 20*time.Millisecond), 2*time.Second)}
	case cursor < seq:
		return nil
	}
	if err := f.review.Refresh(ctx, repo, number); err != nil {
		slog.Warn("feed: re-deriving approval", "repository", repo, "cl", number, "err", err)
		return err
	}
	return nil
}

// pendApprovals pends, in tx, the re-derivation of each CL's approval among numbers, once each,
// once tx — which moves repo's cursor to seq — commits.
func pendApprovals(ctx context.Context, tx db.Ops, repo, seq int64, numbers []int64) error {
	seen := map[int64]bool{}
	for _, n := range numbers {
		if seen[n] {
			continue
		}
		seen[n] = true
		body := binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, uint64(n)), uint64(seq))
		if err := tx.PendStep(ctx, approvalKind, repo, time.Time{}, body); err != nil {
			return err
		}
	}
	return nil
}

// headKind is the step of a move of main's head: the watchers of the CLs it can change hear of it
// (headStep), once the transaction that applies it commits.
const headKind = "head"

// maxMovedPaths bounds the paths of a move of main whose watchers are told by path: past it, every
// watcher of the repository's CLs is.
const maxMovedPaths = 4096

// headBody is a head step's body: the head main moved from, its length first, and the one it moved to.
func headBody(old, new []byte) []byte {
	return append(append([]byte{byte(len(old))}, old...), new...)
}

func parseHead(b []byte) (old, new []byte, ok bool) {
	if len(b) == 0 || len(b) < 1+int(b[0]) {
		return nil, nil, false
	}
	return b[1 : 1+b[0]], b[1+b[0]:], true
}

// headStep tells the watchers of a repository's CLs that a move of main's head can change: those
// registered on a path it changed, or on a directory a file it changed is directly in (a CL that
// adds a file there); every one, when it changed what governs them all (review.Governs) or more
// paths than maxMovedPaths; and those of CLs too large to watch by path, at every move.
func (f *Follower) headStep(ctx context.Context, repo int64, body []byte, _ time.Duration) error {
	old, new, ok := parseHead(body)
	if !ok {
		return nil
	}
	subjects, err := f.movedSubjects(ctx, repo, old, new)
	if err != nil {
		return err
	}
	waits := f.db.Waits()
	inboxes, err := waits.Waiting(ctx, subjects, time.Now())
	if err != nil {
		return err
	}
	return bus.Reply(ctx, waits.Jobs().Queue, inboxes, "", []byte(wait.HeadSubject(repo)))
}

// movedSubjects are the subjects a move of main from old to new changes.
func (f *Follower) movedSubjects(ctx context.Context, repo int64, old, new []byte) ([]string, error) {
	subjects := []string{wait.HeadSubject(repo)}
	if len(old) == 0 {
		return append(subjects, wait.GoverningSubject(repo)), nil
	}
	from, err := f.scm.Tree(ctx, repo, old)
	if err != nil {
		return nil, err
	}
	to, err := f.scm.Tree(ctx, repo, new)
	if err != nil {
		return nil, err
	}
	resp, err := f.scm.Browse.DiffTrees(ctx, &scmpb.DiffTreesRequest{RepositoryId: repo, FromTree: from, ToTree: to})
	if err != nil {
		return nil, err
	}
	treeDir, err := f.review.TreeDir(ctx, repo, new)
	if err != nil {
		return nil, err
	}
	return append(subjects, changedSubjects(repo, resp.GetChanges(), treeDir)...), nil
}

// changedSubjects are the subjects of a move of main that made changes: each path it changed and
// the directory each is directly in; or what governs every CL, when it changed any of that —
// treeDir is the groups-and-ACLs tree's ("" for none) — or more than maxMovedPaths.
func changedSubjects(repo int64, changes []*scmpb.PathChange, treeDir string) []string {
	paths := map[string]bool{}
	for _, c := range changes {
		for _, p := range []string{c.GetPath(), c.GetOldPath()} {
			if p == "" {
				continue
			}
			if review.Governs(p, treeDir) {
				return []string{wait.GoverningSubject(repo)}
			}
			paths[p], paths[review.DirOf(p)] = true, true
		}
	}
	if len(paths) > maxMovedPaths {
		return []string{wait.GoverningSubject(repo)}
	}
	out := make([]string, 0, len(paths))
	for p := range paths {
		out = append(out, wait.PathSubject(repo, p))
	}
	return out
}
