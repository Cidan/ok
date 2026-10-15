// Package feed follows the SCM's change feed (docs/scm.md, "Events") into ok
// control's database: the repository and CL projections, the change log's uploads,
// transitions, and landings, the reviewers an upload names, and the approved projection, which a new
// patchset, a restore, or a landing that edits OWNERS or the groups-and-ACLs tree can change.
//
// One replica follows the feed at a time (its lock), and each event is applied in
// one transaction with the cursor, advanced only past an event not yet applied: even two
// followers could never apply an event twice.
package feed

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/people"
	"github.com/Cidan/ok/internal/control/review"
	"github.com/Cidan/ok/internal/control/scm"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/owners"
	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Follower applies the SCM's change feed.
type Follower struct {
	db     *db.DB
	scm    *scm.Client
	review *review.Service

	// retry holds CLs whose approval could not be re-derived, for the next pass.
	mu    sync.Mutex
	retry map[[2]int64]bool
}

// New builds a follower.
func New(d *db.DB, c *scm.Client, r *review.Service) *Follower {
	return &Follower{db: d, scm: c, review: r, retry: map[[2]int64]bool{}}
}

// Run follows the feed until ctx ends: it waits to be the leader, then applies events as
// they come — until the stream breaks or the leader's lock is lost — reconnecting after a
// failure.
func (f *Follower) Run(ctx context.Context) error {
	for {
		err := f.db.LeadFeed(ctx, f.follow)
		if !errors.Is(err, lock.ErrNotAcquired) && ctx.Err() == nil {
			slog.Warn("feed: stopped following", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

// follow applies the feed from the cursor until the stream breaks or ctx ends.
func (f *Follower) follow(ctx context.Context) error {
	cursor, err := db.Cursor(ctx, f.db.Q())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := f.scm.Events.Watch(ctx, &scmpb.WatchRequest{AfterSeq: cursor})
	if err != nil {
		return err
	}
	retry := time.NewTicker(30 * time.Second)
	defer retry.Stop()
	events := make(chan *scmpb.Event)
	errs := make(chan error, 1)
	go func() {
		for {
			ev, err := stream.Recv()
			if err != nil {
				errs <- err
				return
			}
			select {
			case events <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		select {
		case ev := <-events:
			if err := f.Apply(ctx, ev); err != nil {
				return err
			}
		case err := <-errs:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-retry.C:
			f.refreshPending(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

// transitionEvent maps an SCM transition onto the change log.
func transitionEvent(t *scmpb.ChangeListTransitioned) db.Event {
	e := db.Event{Actor: t.GetActor(), From: db.Status(t.GetFrom()), To: db.Status(t.GetTo()), Message: t.GetReason()}
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

// Apply applies one event, in one transaction with the cursor, then re-derives the
// approval of the CLs it can change.
func (f *Follower) Apply(ctx context.Context, ev *scmpb.Event) error {
	repo := ev.GetRepositoryId()
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
	var refresh [][2]int64
	if moved := ev.GetHeadMoved(); moved != nil {
		edits, err := f.editsOwners(ctx, repo, moved)
		if err != nil {
			return err
		}
		if edits {
			active, err := db.ActiveChangeLists(ctx, f.db.Q(), repo)
			if err != nil {
				return err
			}
			for _, a := range active {
				refresh = append(refresh, [2]int64{repo, a.Number})
			}
		}
	}
	applied := false
	err := f.db.InTx(ctx, func(tx pgx.Tx) error {
		applied = false
		if _, err := tx.Exec(ctx, `INSERT INTO feed_cursor (seq) VALUES (0) ON CONFLICT DO NOTHING`); err != nil {
			return err
		}
		var cursor int64
		if err := tx.QueryRow(ctx, `SELECT seq FROM feed_cursor FOR UPDATE`).Scan(&cursor); err != nil {
			return err
		}
		if cursor >= ev.GetSeq() {
			return nil
		}
		if cl != nil {
			if _, err := db.UpsertChangeList(ctx, tx, scm.ChangeListOut(cl)); err != nil {
				return err
			}
		}
		at := ev.GetAt().AsTime()
		switch k := ev.GetKind().(type) {
		case *scmpb.Event_Uploaded:
			u := k.Uploaded
			if err := db.RecordEvent(ctx, tx, repo, number, db.Event{At: at, Actor: u.GetUploader(), Kind: db.EventUploaded, Patchset: u.GetPatchset()}); err != nil {
				return err
			}
			// A rollback CL's creation shows on the CLs it rolls back.
			if u.GetCreated() {
				for _, n := range cl.GetRollback().GetChangeLists() {
					if err := db.RecordEvent(ctx, tx, repo, n, db.Event{At: at, Actor: u.GetUploader(), Kind: db.EventRolledBack, Related: number}); err != nil {
						return err
					}
				}
			}
			for _, p := range u.GetReviewers() {
				if p == cl.GetAuthor() || !people.IsHuman(p) {
					continue
				}
				added, err := db.AddReviewer(ctx, tx, repo, number, p, u.GetUploader())
				if err != nil {
					return err
				}
				if added {
					if err := db.RecordEvent(ctx, tx, repo, number, db.Event{At: at, Actor: u.GetUploader(), Kind: db.EventReviewerAdded, Subject: p, Patchset: u.GetPatchset()}); err != nil {
						return err
					}
				}
			}
			refresh = append(refresh, [2]int64{repo, number})
		case *scmpb.Event_Transitioned:
			e := transitionEvent(k.Transitioned)
			e.At, e.Patchset = at, cl.GetCurrentPatchset()
			if err := db.RecordEvent(ctx, tx, repo, number, e); err != nil {
				return err
			}
			if e.To.Active() {
				refresh = append(refresh, [2]int64{repo, number})
			}
		case *scmpb.Event_Landed:
			l := k.Landed
			if err := db.RecordEvent(ctx, tx, repo, number, db.Event{At: at, Actor: l.GetSubmitter(), Kind: db.EventSubmitted, Patchset: l.GetPatchset(), Commit: l.GetCommitId()}); err != nil {
				return err
			}
		case *scmpb.Event_RepositoryCreated:
			if err := db.CreateRepository(ctx, tx, db.Repository{ID: repo, Name: k.RepositoryCreated.GetName(), CreatedBy: k.RepositoryCreated.GetCreatedBy(), CreatedAt: at}); err != nil {
				return err
			}
		case *scmpb.Event_RepositoryRenamed:
			if err := db.RenameRepository(ctx, tx, repo, k.RepositoryRenamed.GetName()); err != nil {
				return err
			}
		case *scmpb.Event_HeadMoved:
			if err := db.MoveHead(ctx, tx, repo, k.HeadMoved.GetHeadSeq(), at); err != nil {
				return err
			}
		}
		applied = true
		return db.SetCursor(ctx, tx, ev.GetSeq())
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
