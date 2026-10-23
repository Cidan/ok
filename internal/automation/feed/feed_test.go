package feed

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/automation/store/postgres"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/workflow"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	b, err := postgres.Open(context.Background(), testdb.Pool(t))
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store.New(b, blobs, store.DefaultConfig)
}

// moved is event seq of repository 5: main moved to a commit of its own, at seq.
func moved(t *testing.T, seq int64) bus.Event {
	t.Helper()
	e, err := scmfeed.Encode(&scmpb.Event{RepositoryId: 5, RepositorySeq: seq,
		Kind: &scmpb.Event_HeadMoved{HeadMoved: &scmpb.HeadMoved{NewHead: bytes.Repeat([]byte{byte(seq)}, 32), HeadSeq: seq}}})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// A replica applies a repository's event only holding the repository's claim: while another holds
// it, the event is refused as busy and nothing moves; let go, it applies.
func TestApplyClaims(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	a := applier{New(s, nil, SCM{}, slog.New(slog.DiscardHandler))}
	if err := a.Apply(ctx, moved(t, 1)); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Work().Take(ctx, feedClaim, 5, "another replica", time.Minute); err != nil || !ok {
		t.Fatalf("another replica claiming the repository: %v, %v", ok, err)
	}
	if err := a.Apply(ctx, moved(t, 2)); !errors.Is(err, follow.ErrBusy) {
		t.Fatalf("applying while another replica holds the repository: %v, want follow.ErrBusy", err)
	}
	if c, _ := s.Cursor(ctx, 5); c != 1 {
		t.Fatalf("the cursor moved to %d under another's claim", c)
	}
	if _, err := s.Work().Release(ctx, feedClaim, 5, "another replica"); err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(ctx, moved(t, 2)); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.Cursor(ctx, 5); c != 2 {
		t.Errorf("the cursor at %d once the claim was let go, want 2", c)
	}
}

// Two replicas applying one repository's events at once, each in order from its own reading of the
// cursor, apply each once: the cursor ends at the last, and every one is in main's history.
func TestApplyTogether(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	const n = 40
	var wg sync.WaitGroup
	for range 2 {
		a := applier{New(s, nil, SCM{}, slog.New(slog.DiscardHandler))}
		wg.Go(func() {
			for seq := int64(1); seq <= n; {
				err := a.Apply(ctx, moved(t, seq))
				if errors.Is(err, follow.ErrBusy) {
					continue // the other replica applies it: look again
				}
				if err != nil {
					t.Errorf("applying %d: %v", seq, err)
					return
				}
				c, err := s.Cursor(ctx, 5)
				if err != nil {
					t.Error(err)
					return
				}
				seq = c + 1
			}
		})
	}
	wg.Wait()
	if c, _ := s.Cursor(ctx, 5); c != n {
		t.Fatalf("the cursor at %d, want %d", c, n)
	}
	st, err := s.QueueState(ctx, 5, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Main) != n {
		t.Fatalf("main's history holds %d commits, want %d", len(st.Main), n)
	}
	for i, m := range st.Main {
		if m.Seq != int64(i+1) || m.Commit[0] != byte(i+1) {
			t.Fatalf("main's history at %d: %+v", i+1, m)
		}
	}
}

const stackRepo = 7

// fakeSCM is the CLs of one repository, as the SCM serves them to the follower.
type fakeSCM struct {
	scmpb.ChangeListsClient
	scmpb.RepositoriesClient
	cls map[int64]*scmpb.ChangeList
}

func (f *fakeSCM) GetChangeList(_ context.Context, req *scmpb.GetChangeListRequest, _ ...grpc.CallOption) (*scmpb.ChangeList, error) {
	if c, ok := f.cls[req.GetNumber()]; ok {
		return c, nil
	}
	return nil, status.Errorf(codes.NotFound, "no CL %d", req.GetNumber())
}

// ListChangeLists ignores parent_number, as an SCM predating it does: the follower sorts.
func (f *fakeSCM) ListChangeLists(_ context.Context, req *scmpb.ListChangeListsRequest, _ ...grpc.CallOption) (*scmpb.ListChangeListsResponse, error) {
	out := &scmpb.ListChangeListsResponse{}
	for _, c := range f.cls {
		if len(req.GetStatuses()) == 0 || slices.Contains(req.GetStatuses(), c.GetStatus()) {
			out.ChangeLists = append(out.ChangeLists, c)
		}
	}
	return out, nil
}

func (f *fakeSCM) GetRepository(context.Context, *scmpb.GetRepositoryRequest, ...grpc.CallOption) (*scmpb.Repository, error) {
	return &scmpb.Repository{Id: stackRepo, Name: "svc", Head: commitID("main")}, nil
}

// commitID is a 32-byte commit ID named by s.
func commitID(s string) []byte {
	b := make([]byte, 32)
	copy(b, s)
	return b
}

const (
	draft = scmpb.ChangeListStatus_CHANGE_LIST_STATUS_DRAFT
	open  = scmpb.ChangeListStatus_CHANGE_LIST_STATUS_OPEN
)

// put records CL n, at patchset ps, stacked on parent (0: on main), in status st.
func (f *fakeSCM) put(n, parent int64, ps int32, st scmpb.ChangeListStatus) {
	base := commitID("main")
	if parent != 0 {
		p := f.cls[parent]
		base = p.GetPatchsets()[len(p.GetPatchsets())-1].GetCommitId()
	}
	c := f.cls[n]
	if c == nil {
		c = &scmpb.ChangeList{RepositoryId: stackRepo, Number: n, Author: "alice"}
		f.cls[n] = c
	}
	c.Status, c.ParentNumber, c.CurrentPatchset = st, parent, ps
	c.Patchsets = append(c.Patchsets, &scmpb.Patchset{Number: ps, CommitId: commitID(strings.Repeat(string(rune('a'+n)), int(ps))),
		BaseCommitId: base, UploadedById: "alice"})
}

func uploaded(n int64, ps int32) *scmpb.Event {
	return &scmpb.Event{RepositoryId: stackRepo, Kind: &scmpb.Event_Uploaded{Uploaded: &scmpb.ChangeListUploaded{Number: n, Patchset: ps,
		Created: ps == 1, UploaderId: "alice"}}}
}

func transitioned(n int64, from, to scmpb.ChangeListStatus) *scmpb.Event {
	return &scmpb.Event{RepositoryId: stackRepo, Kind: &scmpb.Event_Transitioned{Transitioned: &scmpb.ChangeListTransitioned{Number: n,
		From: from, To: to, ActorId: "alice"}}}
}

// Pre runs at the top of a stack in review, over the whole stack; nowhere in draft but by hand; and
// moves with the top (docs/automation.md, "pre").
func TestPreAtTheTop(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	scm := &fakeSCM{cls: map[int64]*scmpb.ChangeList{}}
	f := New(st, nil, SCM{ChangeLists: scm, Repositories: scm}, slog.New(slog.DiscardHandler))
	apply := func(evs ...*scmpb.Event) {
		t.Helper()
		for _, ev := range evs {
			if err := f.Apply(ctx, ev); err != nil {
				t.Fatal(err)
			}
		}
	}
	// pre is a CL's current patchset's pre: its plan's base, its planning run's status and why
	// (nil: none).
	type preOf struct {
		base, why string
		run       store.Status
	}
	pre := func(n int64) *preOf {
		t.Helper()
		p, err := st.GetPre(ctx, stackRepo, n, scm.cls[n].GetCurrentPatchset())
		if err != nil {
			t.Fatal(err)
		}
		if p.Plan == nil {
			return nil
		}
		var c workflow.Change
		if err := json.Unmarshal(p.Plan.Context, &c); err != nil {
			t.Fatal(err)
		}
		return &preOf{base: c.Base, why: p.PlanRun.Why, run: p.PlanRun.Status}
	}
	main := hex.EncodeToString(commitID("main"))

	// A stack of three, uploaded as drafts: no pre.
	scm.put(1, 0, 1, draft)
	scm.put(2, 1, 1, draft)
	scm.put(3, 2, 1, draft)
	apply(uploaded(1, 1), uploaded(2, 1), uploaded(3, 1))
	for n := int64(1); n <= 3; n++ {
		if p := pre(n); p != nil {
			t.Fatalf("draft CL %d has pre: %+v", n, p)
		}
	}
	// Sent for review top first: the top is planned, over all three — and only the top.
	for _, n := range []int64{3, 2, 1} {
		scm.cls[n].Status = open
		apply(transitioned(n, draft, open))
	}
	if p := pre(3); p == nil || p.base != main || p.run != store.StatusQueued || !strings.Contains(p.why, "the top of a stack of 3") {
		t.Fatalf("the top's pre: %+v", p)
	}
	for _, n := range []int64{1, 2} {
		if p := pre(n); p != nil {
			t.Errorf("CL %d, under the top, has pre: %+v", n, p)
		}
	}

	// A fourth CL, in review on the top, is the top: the old top's pre stops.
	scm.put(4, 3, 1, open)
	apply(uploaded(4, 1))
	if p := pre(4); p == nil || p.base != main || !strings.Contains(p.why, "the top of a stack of 4") {
		t.Fatalf("the new top's pre: %+v", p)
	}
	if p := pre(3); p == nil || p.run != store.StatusCanceled {
		t.Fatalf("the old top's pre: %+v", p)
	}
	// It returns to draft: CL 3 is the top again, its pre resumed.
	scm.cls[4].Status = draft
	apply(transitioned(4, open, draft))
	if p := pre(3); p == nil || p.run != store.StatusQueued {
		t.Fatalf("the top again: %+v", p)
	}

	// A new patchset of the whole stack, in review: the top alone is planned, on its new patchset.
	scm.put(1, 0, 2, open)
	scm.put(2, 1, 2, open)
	scm.put(3, 2, 2, open)
	apply(uploaded(1, 2), uploaded(2, 2), uploaded(3, 2))
	if p := pre(3); p == nil || p.base != main || p.run != store.StatusQueued {
		t.Fatalf("the top's new pre: %+v", p)
	}
	for _, n := range []int64{1, 2} {
		if p := pre(n); p != nil {
			t.Errorf("CL %d's new patchset, under the top, has pre: %+v", n, p)
		}
	}

	// By hand, on the draft at the top of it all: over everything under it.
	if ps, err := f.StartPre(ctx, stackRepo, 4, "bob"); err != nil || ps != 1 {
		t.Fatalf("StartPre: %d, %v", ps, err)
	}
	if p := pre(4); p == nil || p.base != main || p.run != store.StatusQueued {
		t.Fatalf("the draft's pre by hand: %+v", p)
	}

	// A CL stacked on nothing is its own stack: planned on its own base.
	scm.put(9, 0, 1, open)
	apply(uploaded(9, 1))
	if p := pre(9); p == nil || p.base != main || strings.Contains(p.why, "stack") {
		t.Fatalf("a lone CL's pre: %+v", p)
	}
}
