package outbox

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/db/postgres"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/testdb"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// A CL's inbox entries: its author's, waiting on them while it is open with unresolved threads;
// each reviewer's but its author's, waiting on them while it is open and they have neither granted
// a label on nor commented on its current patchset — what they did on an earlier one does not count.
func TestEntries(t *testing.T) {
	ctx := move.WithRepository(context.Background(), 1)
	s, err := postgres.Open(context.Background(), testdb.Pool(t))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	cl := db.ChangeList{Repo: 1, Number: 1, ChangeID: []byte{1}, Author: "ada", Status: db.StatusOpen, CurrentPatchset: 2,
		Description: "a change\n\nits body", CreatedAt: now, UpdatedAt: now}
	if _, err := s.UpsertChangeList(ctx, cl); err != nil {
		t.Fatal(err)
	}
	for _, r := range []string{"ada", "bea", "cal", "dee"} {
		if _, err := s.AddReviewer(ctx, 1, 1, r, "ada"); err != nil {
			t.Fatal(err)
		}
	}
	// bea granted on the current patchset; cal commented on it; dee only on the first.
	if _, err := s.SetGrant(ctx, 1, 1, "bea", db.LabelLGTM, true, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateComment(ctx, 1, db.Comment{Number: 1, Patchset: 2, Author: "cal", Body: "a question", FilePath: "a.go", Line: 1,
		Side: db.SidePatchset}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetGrant(ctx, 1, 1, "dee", db.LabelLGTM, true, 1); err != nil {
		t.Fatal(err)
	}
	got := func(c db.ChangeList) map[string]bool {
		t.Helper()
		es, err := Entries(ctx, s, Repository{ID: 1, Name: "acme/app"}, c)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, e := range es {
			key := e.GetPrincipalId()
			if e.GetRelation() == identitypb.InboxEntry_RELATION_AUTHORED {
				key += " (author)"
			}
			if e.GetTitle() != "a change" || e.GetRepository() != "acme/app" || e.GetStatus() != StatusName(c.Status) {
				t.Errorf("an entry: %v", e)
			}
			out[key] = e.GetAttention()
		}
		return out
	}
	// cal's comment is an unresolved thread: it waits on ada.
	want := map[string]bool{"ada (author)": true, "bea": false, "cal": false, "dee": true}
	if g := got(cl); !maps.Equal(g, want) {
		t.Errorf("an open CL's entries: %v, want %v", g, want)
	}
	// A draft waits on no one.
	draft := cl
	draft.Status = db.StatusDraft
	if g := got(draft); !maps.Equal(g, map[string]bool{"ada (author)": false, "bea": false, "cal": false, "dee": false}) {
		t.Errorf("a draft's entries: %v", g)
	}
}
