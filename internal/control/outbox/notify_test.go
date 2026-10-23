package outbox

import (
	"context"
	"slices"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/db/postgres"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/testdb"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// delivery records what it is asked to notify.
type delivery struct {
	identitypb.DeliveryClient
	got []*identitypb.Notification
}

func (d *delivery) Notify(_ context.Context, req *identitypb.NotifyRequest, _ ...grpc.CallOption) (*identitypb.NotifyResponse, error) {
	d.got = append(d.got, req.GetNotifications()...)
	return &identitypb.NotifyResponse{}, nil
}

// Who each kind of event and comment notifies, never the one who did it: a reviewer added to an open
// CL, but not to a draft until review is asked; the author of a grant, a landing, and a rollback;
// the reviewers of an abandon; the author and the thread of a comment. A review asked again on the
// same patchset is the same notification.
func TestNotifier(t *testing.T) {
	ctx := move.WithRepository(context.Background(), 1)
	s, err := postgres.Open(context.Background(), testdb.Pool(t))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_800_000_000, 0)
	cl := db.ChangeList{Repo: 1, Number: 1, ChangeID: []byte{1}, Author: "ada", Status: db.StatusDraft, CurrentPatchset: 1,
		Description: "a change", CreatedAt: now, UpdatedAt: now}
	if _, err := s.UpsertChangeList(ctx, cl); err != nil {
		t.Fatal(err)
	}
	d := &delivery{}
	n := &Notifier{DB: s, Delivery: d, Installation: "acme"}
	r := Repository{ID: 1, Name: "app"}
	// drain hands the notifier what is in the outbox, as the drainer does, and empties it.
	drain := func() []string {
		t.Helper()
		d.got = nil
		es, err := s.Outbox(ctx, 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		if err := n.Consume(ctx, r, es); err != nil {
			t.Fatal(err)
		}
		if err := s.DoneOutbox(ctx, 1, ids(es)); err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, g := range d.got {
			if g.GetLink() != "/acme/app/cl/1" || g.GetTitle() != "a change" {
				t.Errorf("a notification's link and title: %v", g)
			}
			out = append(out, g.GetPrincipalId()+" "+g.GetKind().String()+" "+g.GetText())
		}
		sort.Strings(out)
		return out
	}
	record := func(e db.Event) {
		t.Helper()
		if err := s.RecordEvent(ctx, 1, 1, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, rv := range []string{"bea", "cal"} {
		if _, err := s.AddReviewer(ctx, 1, 1, rv, "ada"); err != nil {
			t.Fatal(err)
		}
	}
	record(db.Event{Actor: "ada", Kind: db.EventReviewerAdded, Subject: "bea", Patchset: 1})
	if got := drain(); len(got) != 0 {
		t.Errorf("a reviewer added to a draft: %v", got)
	}
	cl.Status = db.StatusOpen
	if _, err := s.UpsertChangeList(ctx, cl); err != nil {
		t.Fatal(err)
	}
	record(db.Event{Actor: "ada", Kind: db.EventReviewRequested, Patchset: 1})
	record(db.Event{Actor: "ada", Kind: db.EventReviewerAdded, Subject: "bea", Patchset: 1})
	got := drain()
	want := []string{
		"bea KIND_REVIEW_REQUESTED asked you to review it", "bea KIND_REVIEW_REQUESTED asked you to review it",
		"cal KIND_REVIEW_REQUESTED asked you to review it",
	}
	if !slices.Equal(got, want) {
		t.Errorf("review asked: %v, want %v", got, want)
	}
	// bea's, asked twice on patchset 1, are one notification: the directory keeps one per source.
	var beas []string
	for _, g := range d.got {
		if g.GetPrincipalId() == "bea" {
			beas = append(beas, g.GetSource())
		}
	}
	if len(beas) != 2 || beas[0] != beas[1] {
		t.Errorf("bea's review notifications' sources: %v, want one", beas)
	}

	record(db.Event{Actor: "bea", Kind: db.EventLabelGranted, Label: db.LabelLGTM, Patchset: 1})
	record(db.Event{Actor: "ada", Kind: db.EventLabelGranted, Label: db.LabelApproval, Patchset: 1})
	record(db.Event{Actor: "dee", Kind: db.EventSubmitted, Patchset: 1})
	record(db.Event{Actor: "bea", Kind: db.EventRolledBack, Related: 2})
	record(db.Event{Actor: "cal", Kind: db.EventAbandoned, Message: "not   needed"})
	want = []string{
		"ada KIND_REPLIED granted LGTM on patchset 1",
		"ada KIND_ROLLED_BACK is rolling it back in #2",
		"ada KIND_SUBMITTED landed it on main",
		"bea KIND_ABANDONED abandoned it: not needed",
	}
	if got := drain(); !slices.Equal(got, want) {
		t.Errorf("events: %v, want %v", got, want)
	}

	// bea starts a thread; cal answers in it; ada answers: each notifies the others.
	root, err := s.CreateComment(ctx, 1, db.Comment{Number: 1, Patchset: 1, Author: "bea", Body: "why?", FilePath: "a.go", Line: 3, Side: db.SidePatchset})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateComment(ctx, 1, db.Comment{Number: 1, Patchset: 1, Author: "cal", Body: "agreed", ParentID: root, FilePath: "a.go", Line: 3,
		Side: db.SidePatchset}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateComment(ctx, 1, db.Comment{Number: 1, Patchset: 1, Author: "ada", Body: "because", ParentID: root, FilePath: "a.go", Line: 3,
		Side: db.SidePatchset}); err != nil {
		t.Fatal(err)
	}
	want = []string{
		"ada KIND_COMMENTED commented on a.go: agreed",
		"ada KIND_COMMENTED commented on a.go: why?",
		"bea KIND_COMMENTED commented on a.go: agreed",
		"bea KIND_COMMENTED commented on a.go: because",
		"cal KIND_COMMENTED commented on a.go: because",
	}
	if got := drain(); !slices.Equal(got, want) {
		t.Errorf("comments: %v, want %v", got, want)
	}
}
