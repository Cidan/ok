package feed

import (
	"context"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/move"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Directory follows the directory's events (dirfeed; docs/hosted.md, "Tenancy, as built") into
// the repository projection: each repository's full name, and whether it is deleted, as it is made,
// renamed, transferred, deleted, restored, or purged. Every replica follows, sharing the
// repositories (the stream's group, control), each event applied once with its repository's
// cursor, as the SCM's are.
type Directory struct {
	db     db.Store
	events dirfeed.Events
}

// NewDirectory builds a follower of the directory's events.
func NewDirectory(d db.Store, events dirfeed.Events) *Directory {
	return &Directory{db: d, events: events}
}

// Run follows the directory's events until ctx ends.
func (f *Directory) Run(ctx context.Context) error {
	return (&follow.Follower{Topic: dirfeed.Topic, Log: dirfeed.Log{Events: f.events}, Apply: directoryApplier{f},
		Work: f.db.Work(), Register: dirfeed.Register(f.events)}).Run(ctx)
}

// directoryApplier is the follower as a follow.Applier.
type directoryApplier struct{ *Directory }

func (a directoryApplier) Cursors(ctx context.Context, repos []int64) (map[int64]int64, error) {
	return a.db.FollowCursors(ctx, dirfeed.Topic, repos)
}

func (a directoryApplier) Apply(ctx context.Context, e bus.Event) error {
	ev, err := dirfeed.Decode(e)
	if err != nil {
		return err
	}
	if err := a.ApplyDirectory(ctx, ev); err != nil {
		return err
	}
	return nil
}

// ApplyDirectory applies one of the directory's events, once: one at or before its repository's
// cursor is skipped.
func (f *Directory) ApplyDirectory(ctx context.Context, ev *identitypb.DirectoryEvent) error {
	repo := ev.GetRepositoryId()
	// Every cell's ok control names every repository, wherever it is or moves: no move gates it.
	ctx = move.Untagged(ctx)
	return f.db.Txn(ctx, func(tx db.Ops) error {
		cursor, err := tx.LockFollowCursor(ctx, dirfeed.Topic, repo)
		if err != nil {
			return err
		}
		if cursor >= ev.GetSeq() {
			return nil
		}
		if ev.GetKind() == identitypb.DirectoryEvent_KIND_PURGED {
			if err := tx.DropRepository(ctx, repo); err != nil {
				return err
			}
		} else if r := ev.GetRepository(); r != nil {
			if err := tx.NameRepository(ctx, db.Repository{ID: repo, Name: r.GetFullName(), Deleted: r.GetState() == "deleted",
				CreatedBy: r.GetCreatedById(), CreatedAt: r.GetCreatedAt().AsTime()}); err != nil {
				return err
			}
		}
		return tx.SetFollowCursor(ctx, dirfeed.Topic, repo, ev.GetSeq())
	})
}
