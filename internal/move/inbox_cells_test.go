package move_test

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/id/idtest"
	"github.com/Cidan/ok/internal/move"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// A person's inbox spans cells (docs/hosted.md, "ok control at scale, as built"): a CL of a
// repository in each of two cells, each recorded by its own cell's ok control, lists in the
// directory as each cell's outbox drains — a review waiting on ada in one, her own CL in the other.
func TestInboxAcrossCells(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scmLis := map[string]net.Listener{"c1": listen(t), "c2": listen(t)}
	ctlLis := map[string]net.Listener{"c1": listen(t), "c2": listen(t)}
	var list []config.IdentityCell
	for _, name := range []string{"c1", "c2"} {
		list = append(list, config.IdentityCell{Name: name, SCM: scmLis[name].Addr().String(), Control: ctlLis[name].Addr().String()})
	}
	id := idtest.StartWithCells(t, list, "ada", "bea")
	roles := map[string]cellRoles{}
	for _, name := range []string{"c1", "c2"} {
		roles[name] = startCell(t, ctx, id, name, scmLis[name], ctlLis[name])
	}
	ada, bea := id.ID(ada), id.ID("bea.human.example.com")
	repos := map[string]int64{}
	for _, name := range []string{"one", "two"} {
		r, err := scmpb.NewRepositoriesClient(dialAs(t, scmLis["c1"].Addr().String(), "ada.human.example.com")).CreateRepository(ctx,
			&scmpb.CreateRepositoryRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		e, err := id.Store.Repository(ctx, r.GetId())
		if err != nil {
			t.Fatal(err)
		}
		repos[e.Cell] = r.GetId()
	}
	if len(repos) != 2 {
		t.Fatalf("both repositories in one cell: %v", repos)
	}
	// A CL in each cell's ok control: bea's in c1, reviewed by ada; ada's in c2 — written once the
	// cell's ok control holds its repository, as a CL's own events reach it only after its
	// repository's creation: its outbox takes a repository it does not hold for one purged.
	record := func(cell, author string, reviewer string) {
		t.Helper()
		repo := repos[cell]
		d := roles[cell].control.DB
		for held := time.Now().Add(30 * time.Second); ; {
			rs, err := d.RepositoriesByID(ctx, "", []int64{repo})
			if err != nil {
				t.Fatal(err)
			}
			if len(rs) == 1 {
				break
			}
			if time.Now().After(held) {
				t.Fatalf("cell %s's ok control never held repository %d", cell, repo)
			}
			time.Sleep(50 * time.Millisecond)
		}
		about := move.WithRepository(ctx, repo)
		now := time.Now()
		if err := d.Txn(about, func(tx db.Ops) error {
			if _, err := tx.UpsertChangeList(about, db.ChangeList{Repo: repo, Number: 1, ChangeID: []byte{1}, Author: author, Status: db.StatusOpen,
				CurrentPatchset: 1, Description: "a change in " + cell, CreatedAt: now, UpdatedAt: now}); err != nil {
				return err
			}
			if reviewer != "" {
				if _, err := tx.AddReviewer(about, repo, 1, reviewer, author); err != nil {
					return err
				}
			}
			return tx.RecordEvent(about, repo, 1, db.Event{Actor: author, Kind: db.EventReviewRequested, Patchset: 1})
		}); err != nil {
			t.Fatal(err)
		}
	}
	record("c1", bea, ada)
	record("c2", ada, "")

	inbox := identitypb.NewInboxClient(id.As("control.ok.robot.example.com"))
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := inbox.ListInbox(ctx, &identitypb.ListInboxRequest{ForPersonId: ada})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, e := range resp.GetEntries() {
			kind := "authored"
			if e.GetRelation() == identitypb.InboxEntry_RELATION_REVIEWING {
				kind = "reviewing"
			}
			if e.GetAttention() {
				kind += "!"
			}
			got = append(got, e.GetTitle()+": "+kind)
		}
		if slices.Equal(got, []string{"a change in c1: reviewing!", "a change in c2: authored"}) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ada's inbox across cells: %v", got)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
