package dbtest

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/move"
)

// Each event recorded and each comment made waits in its repository's outbox, oldest first, its
// outbox's work marked — inside a transaction or out — until it is done; GetEvent reads one back.
func testOutbox(t *testing.T, s db.Store) {
	ctx := move.WithRepository(context.Background(), 1)
	if _, err := s.UpsertChangeList(ctx, cl(1, alice, db.StatusOpen, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordEvent(ctx, 1, 1, db.Event{Actor: alice, Kind: db.EventUploaded, Patchset: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	var commentID int64
	if err := s.Txn(ctx, func(tx db.Ops) error {
		var err error
		commentID, err = tx.CreateComment(ctx, 1, db.Comment{Number: 1, Patchset: 1, Author: bob, Body: "looks good"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := s.Txn(ctx, func(tx db.Ops) error {
		return tx.RecordEvent(ctx, 1, 1, db.Event{Actor: bob, Kind: db.EventLabelGranted, Label: db.LabelLGTM, Patchset: 1})
	}); err != nil {
		t.Fatal(err)
	}
	es, err := s.Outbox(ctx, 1, 10)
	if err != nil || len(es) != 3 {
		t.Fatalf("the outbox: %+v, %v", es, err)
	}
	if es[0].EventID == 0 || es[1].CommentID != commentID || es[2].EventID == 0 || es[0].Number != 1 {
		t.Errorf("the outbox's order: %+v", es)
	}
	ev, ok, err := s.GetEvent(ctx, 1, 1, es[2].EventID)
	if err != nil || !ok || ev.Kind != db.EventLabelGranted || ev.Actor != bob {
		t.Errorf("GetEvent = %+v, %v, %v", ev, ok, err)
	}
	if _, ok, err := s.GetEvent(ctx, 1, 2, es[2].EventID); err != nil || ok {
		t.Errorf("an event of another CL: %v, %v", ok, err)
	}
	if repos, err := s.OutboxRepositories(ctx); err != nil || !slices.Equal(repos, []int64{1}) {
		t.Errorf("repositories with an outbox: %v, %v", repos, err)
	}
	due, err := s.Work().Due(ctx, db.OutboxWork, 10)
	if err != nil || !slices.Equal(due, []int64{1}) {
		t.Errorf("the outbox's work: %v, %v", due, err)
	}
	if err := s.DoneOutbox(ctx, 1, []int64{es[0].ID, es[1].ID}); err != nil {
		t.Fatal(err)
	}
	if left, err := s.Outbox(ctx, 1, 10); err != nil || len(left) != 1 || left[0].ID != es[2].ID {
		t.Errorf("after two are done: %+v, %v", left, err)
	}
}
