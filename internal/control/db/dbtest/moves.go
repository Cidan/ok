package dbtest

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/move/movetest"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// A network's repositories move between two cells' ok control: their CLs, reviewers, grants,
// comments, history, and the SCM's feed's cursors go — not their entries, which every cell names as
// the directory does, nor anyone's favorites, nor the directory's cursors. Frozen, the source
// refuses writes about them; the destination refuses them until it opens; then the source refuses
// them, naming the cell, and drops them.
func testMoves(t *testing.T, src, dst db.Store) {
	ctx := move.Untagged(context.Background())
	const repo, fork, other = 7, 8, 9
	for _, s := range []db.Store{src, dst} {
		for _, r := range []int64{repo, fork, other} {
			if err := s.CreateRepository(ctx, db.Repository{ID: r, Name: fmt.Sprint("acme/r", r), CreatedBy: alice, CreatedAt: epoch}); err != nil {
				t.Fatal(err)
			}
		}
	}
	// The destination's directory follower named it since.
	if err := dst.NameRepository(ctx, db.Repository{ID: repo, Name: "acme/renamed", CreatedBy: alice, CreatedAt: epoch}); err != nil {
		t.Fatal(err)
	}
	for _, r := range []int64{repo, fork, other} {
		c := cl(1, alice, db.StatusOpen, 0, 1)
		c.Repo = r
		if _, err := src.UpsertChangeList(ctx, c); err != nil {
			t.Fatal(err)
		}
		if _, err := src.AddReviewer(ctx, r, 1, bob, alice); err != nil {
			t.Fatal(err)
		}
		if _, err := src.SetGrant(ctx, r, 1, bob, db.LabelLGTM, true, 1); err != nil {
			t.Fatal(err)
		}
		root, err := src.CreateComment(ctx, r, db.Comment{Number: 1, Patchset: 1, Author: bob, FilePath: "f.go", Line: 3, Side: 1, Body: "why?"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := src.CreateComment(ctx, r, db.Comment{Number: 1, Patchset: 1, Author: alice, ParentID: root, Body: "because"}); err != nil {
			t.Fatal(err)
		}
		if err := src.RecordEvent(ctx, r, 1, db.Event{Actor: alice, Kind: db.EventReviewRequested, From: db.StatusDraft, To: db.StatusOpen}); err != nil {
			t.Fatal(err)
		}
		if err := src.SetFollowCursor(ctx, scmfeed.Topic, r, 40+r); err != nil {
			t.Fatal(err)
		}
		if err := src.SetFollowCursor(ctx, dirfeed.Topic, r, 10+r); err != nil {
			t.Fatal(err)
		}
	}
	if err := src.SetFavorite(ctx, alice, repo, true); err != nil {
		t.Fatal(err)
	}
	// What a CL reads as, whole.
	read := func(s db.Store, r int64) string {
		t.Helper()
		c, ok, err := s.GetChangeList(ctx, r, 1)
		if err != nil || !ok {
			return fmt.Sprintf("no CL: %v", err)
		}
		reviewers, err1 := s.Reviewers(ctx, r, 1)
		grants, err2 := s.Grants(ctx, r, 1)
		comments, err3 := s.Comments(ctx, r, 1)
		events, err4 := s.Events(ctx, r, 1)
		cursor, err5 := s.FollowCursor(ctx, scmfeed.Topic, r)
		for _, err := range []error{err1, err2, err3, err4, err5} {
			if err != nil {
				t.Fatal(err)
			}
		}
		return fmt.Sprintf("%+v\n%+v\n%+v\n%+v\n%+v\ncursor %d", c, reviewers, grants, comments, events, cursor)
	}
	want := map[int64]string{repo: read(src, repo), fork: read(src, fork)}

	scope := &okpb.MoveScope{NetworkId: repo, Repositories: []int64{repo, fork}, Role: cells.Control}
	srcMover := &move.RepositoryRole{RoleName: cells.Control, Rows: src.Moves(), Work: src.Work()}
	dstMover := &move.RepositoryRole{RoleName: cells.Control, Rows: dst.Moves(), Work: dst.Work()}
	srcMoves, dstMoves := movetest.Pair(t, srcMover, dstMover, okpb.Caller_CALLER_CONTROL)
	write := func(s db.Store, r int64) error {
		_, err := s.AddReviewer(move.WithRepository(context.Background(), r), r, 1, "carol.human.example.com", alice)
		return err
	}
	movetest.Move(t, srcMoves, dstMoves, scope, movetest.Steps{
		Frozen: func() {
			for _, r := range []int64{repo, fork} {
				if err := write(src, r); !move.IsFrozen(err) {
					t.Errorf("a write about repository %d while frozen: %v", r, err)
				}
				if err := src.Txn(move.WithRepository(context.Background(), r), func(db.Ops) error { return nil }); !move.IsFrozen(err) {
					t.Errorf("a transaction about repository %d while frozen: %v", r, err)
				}
			}
			if err := write(src, other); err != nil {
				t.Errorf("a write about a repository not moving: %v", err)
			}
		},
		Imported: func() {
			if err := write(dst, repo); !move.IsFrozen(err) {
				t.Errorf("a write at the destination before it opened: %v", err)
			}
		},
	})
	for r, w := range want {
		if got := read(dst, r); got != w {
			t.Errorf("repository %d at the destination:\n%s\nwant\n%s", r, got, w)
		}
	}
	if err := write(dst, repo); err != nil {
		t.Errorf("a write at the destination once open: %v", err)
	}
	if got, ok, err := dst.RepositoryByName(ctx, "acme/renamed"); err != nil || !ok || got.ID != repo {
		t.Errorf("the destination's name of the repository: %+v, %v, %v", got, ok, err)
	}
	if cursor, err := dst.FollowCursor(ctx, dirfeed.Topic, repo); err != nil || cursor != 0 {
		t.Errorf("the destination's directory cursor: %d, %v; want its own", cursor, err)
	}
	if favs, err := dst.Favorites(ctx, alice); err != nil || favs[repo] {
		t.Errorf("the destination's favorites: %v, %v; want none moved", favs, err)
	}
	if _, moved := cells.MovedOf(write(src, repo)); !moved {
		t.Error("a write at the source after the move is not refused as moved")
	}

	// A day later, the source drops it, its entry and favorites staying.
	if due, err := src.Moves().MovedBefore(ctx, time.Now().Add(move.Kept+time.Minute), math.MinInt64, 10); err != nil || len(due) != 2 {
		t.Fatalf("moved away: %v, %v", due, err)
	}
	if booked, err := src.Work().Marked(ctx, move.MovedWork, math.MinInt64, 10); err != nil || len(booked) != 2 ||
		time.Until(booked[0].At) < move.Kept-time.Hour {
		t.Fatalf("the purges booked as the move finished: %+v, %v; want each a day on", booked, err)
	}
	if _, err := srcMoves.Drop(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := src.GetChangeList(ctx, repo, 1); err != nil || ok {
		t.Errorf("the source's CL after the drop: %v, %v", ok, err)
	}
	if got, ok, err := src.RepositoryByName(ctx, "acme/r7"); err != nil || !ok || got.ID != repo {
		t.Errorf("the source's entry after the drop: %+v, %v, %v", got, ok, err)
	}
	if got := read(src, other); got == "" {
		t.Error("the repository not moving is gone")
	}
}
