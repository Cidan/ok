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
	a := applier{New(s, SCM{}, slog.New(slog.DiscardHandler))}
	if err := a.Apply(ctx, moved(t, 1)); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Work().Take(ctx, feedClaim, 5, "another replica", time.Minute); err != nil || !got.Took {
		t.Fatalf("another replica claiming the repository: %v, %v", got, err)
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
		a := applier{New(s, SCM{}, slog.New(slog.DiscardHandler))}
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
	f := New(st, SCM{ChangeLists: scm, Repositories: scm}, slog.New(slog.DiscardHandler))
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

// slowSCM is a fakeSCM whose GetChangeList takes a while, counting the calls under way: in all, and
// for each CL. It refuses the CLs of fail, as an SCM that is down does.
type slowSCM struct {
	*fakeSCM
	mu        sync.Mutex
	inFlight  map[int64]int
	now, most int
	beside    bool // a CL was asked about twice at once
	fail      map[int64]bool
}

func (s *slowSCM) GetChangeList(ctx context.Context, req *scmpb.GetChangeListRequest, opts ...grpc.CallOption) (*scmpb.ChangeList, error) {
	n := req.GetNumber()
	s.mu.Lock()
	s.inFlight[n]++
	s.beside = s.beside || s.inFlight[n] > 1
	s.now++
	s.most = max(s.most, s.now)
	failing := s.fail[n]
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inFlight[n]--
		s.now--
		s.mu.Unlock()
	}()
	time.Sleep(10 * time.Millisecond)
	if failing {
		return nil, status.Error(codes.Unavailable, "the SCM is down")
	}
	return s.fakeSCM.GetChangeList(ctx, req, opts...)
}

// A page of a repository's events is applied each CL's in order and the page's CLs beside each
// other — a herd's uploads planned together, not one after another; a CL's events never beside each
// other — and the cursor moves past the events applied, to the first that failed. The page applied
// again plans what failed, and each CL once.
func TestApplyPage(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	scm := &slowSCM{fakeSCM: &fakeSCM{cls: map[int64]*scmpb.ChangeList{}}, inFlight: map[int64]int{}, fail: map[int64]bool{}}
	a := applier{New(st, SCM{ChangeLists: scm, Repositories: scm}, slog.New(slog.DiscardHandler))}
	const n = 64
	var page []bus.Event
	add := func(ev *scmpb.Event) {
		ev.RepositorySeq = int64(len(page) + 1)
		e, err := scmfeed.Encode(ev)
		if err != nil {
			t.Fatal(err)
		}
		page = append(page, e)
	}
	// Each CL's two events side by side in the page.
	for cl := int64(1); cl <= n; cl++ {
		scm.put(cl, 0, 1, open)
		scm.put(cl, 0, 2, open)
		add(uploaded(cl, 1))
		add(uploaded(cl, 2))
	}
	const failing = 40
	scm.fail[failing] = true
	if err := a.ApplyBatch(ctx, page); err == nil || !strings.Contains(err.Error(), "event 79 of repository 7") {
		t.Fatalf("applying a page with CL %d's events failing: %v", failing, err)
	}
	if c, _ := st.Cursor(ctx, stackRepo); c != 2*failing-2 {
		t.Fatalf("the cursor at %d, want %d: before the first event that failed", c, 2*failing-2)
	}
	planned := func(cl int64) bool {
		t.Helper()
		p, err := st.GetPre(ctx, stackRepo, cl, 2)
		if err != nil {
			t.Fatal(err)
		}
		return p.Plan != nil
	}
	for cl := int64(1); cl <= n; cl++ {
		if planned(cl) != (cl != failing) {
			t.Errorf("CL %d planned %v", cl, planned(cl))
		}
	}
	if scm.beside || scm.most < 8 {
		t.Errorf("a CL's events applied beside each other %v; at most %d CLs at once", scm.beside, scm.most)
	}
	scm.fail[failing] = false
	if err := a.ApplyBatch(ctx, page); err != nil {
		t.Fatal(err)
	}
	if c, _ := st.Cursor(ctx, stackRepo); c != 2*n {
		t.Fatalf("the cursor at %d once the page applied, want %d", c, 2*n)
	}
	if !planned(failing) {
		t.Errorf("CL %d not planned once its events applied", failing)
	}
	for cl := int64(1); cl <= n; cl++ {
		runs, err := st.List(ctx, store.ListQuery{RepositoryID: stackRepo, CLNumber: cl, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		plans := 0
		for _, r := range runs {
			if r.Job.Kind == store.KindPlan {
				plans++
			}
		}
		if plans != 1 {
			t.Errorf("CL %d planned %d times", cl, plans)
		}
	}
}

// An event that names no CL — main moving, whose recording reads the landings recorded before it — is
// applied after every event before it in the page, and before every event after it: the CLs' events
// beside each other only between such events.
func TestApplyPageInOrderAroundMain(t *testing.T) {
	ctx := context.Background()
	st := testStore(t)
	scm := &slowSCM{fakeSCM: &fakeSCM{cls: map[int64]*scmpb.ChangeList{}}, inFlight: map[int64]int{}, fail: map[int64]bool{}}
	f := New(st, SCM{ChangeLists: scm, Repositories: scm}, slog.New(slog.DiscardHandler))
	var mu sync.Mutex
	type mark struct {
		moved bool
		enter bool
		seq   int64
	}
	var timeline []mark
	f.applying = func(ev *scmpb.Event, enter bool) {
		mu.Lock()
		defer mu.Unlock()
		timeline = append(timeline, mark{moved: ev.GetHeadMoved() != nil, enter: enter, seq: ev.GetRepositorySeq()})
	}
	a := applier{f}
	var page []bus.Event
	add := func(ev *scmpb.Event) {
		ev.RepositorySeq = int64(len(page) + 1)
		e, err := scmfeed.Encode(ev)
		if err != nil {
			t.Fatal(err)
		}
		page = append(page, e)
	}
	for cl := int64(1); cl <= 32; cl++ {
		scm.put(cl, 0, 1, open)
		if cl == 17 {
			add(&scmpb.Event{RepositoryId: stackRepo, Kind: &scmpb.Event_HeadMoved{HeadMoved: &scmpb.HeadMoved{
				NewHead: commitID("moved"), HeadSeq: 1}}})
		}
		add(uploaded(cl, 1))
	}
	if err := a.ApplyBatch(ctx, page); err != nil {
		t.Fatal(err)
	}
	const movedAt = 17
	var movedIn, movedOut = -1, -1
	for i, m := range timeline {
		if m.moved && m.enter {
			movedIn = i
		}
		if m.moved && !m.enter {
			movedOut = i
		}
	}
	if movedIn < 0 || movedOut != movedIn+1 {
		t.Fatalf("main's move applied beside other events: entered at %d, left at %d", movedIn, movedOut)
	}
	for i, m := range timeline {
		if m.moved {
			continue
		}
		if m.seq < movedAt && i > movedIn || m.seq > movedAt && i < movedOut {
			t.Fatalf("event %d (enter %v) at %d of the timeline, around main's move at %d–%d", m.seq, m.enter, i, movedIn, movedOut)
		}
	}
	if c, _ := st.Cursor(ctx, stackRepo); c != int64(len(page)) {
		t.Errorf("the cursor at %d, want %d", c, len(page))
	}
	if scm.most < 8 {
		t.Errorf("at most %d CLs applied at once", scm.most)
	}
}
