package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// logOf is a repository's whole log, in order.
func (e *clEnv) logOf() []*scmpb.Event {
	e.t.Helper()
	var out []*scmpb.Event
	for after := int64(0); ; {
		page, err := e.s.ReadEvents(context.Background(), e.repo, after, 0)
		if err != nil {
			e.t.Fatal(err)
		}
		if len(page) == 0 {
			return out
		}
		out = append(out, page...)
		after = page[len(page)-1].GetRepositorySeq()
	}
}

// checkLog fails the test unless the repository's log runs 1, 2, 3, … with no gap and names only
// numbered CLs, and the CLs its uploads made are numbered in the order they were placed.
func (e *clEnv) checkLog() []*scmpb.Event {
	e.t.Helper()
	log := e.logOf()
	var made []int64
	for i, ev := range log {
		if ev.GetRepositorySeq() != int64(i+1) {
			e.t.Fatalf("event %d of the log is at %d", i+1, ev.GetRepositorySeq())
		}
		if n := changeListRef(ev); n != nil && *n <= 0 {
			e.t.Fatalf("event %d names CL %d", i+1, *n)
		}
		if u := ev.GetUploaded(); u.GetCreated() {
			made = append(made, u.GetNumber())
		}
	}
	if !slices.IsSorted(made) {
		e.t.Fatalf("the log places the CLs it makes out of their numbers' order: %v", made)
	}
	return log
}

// A CL uploaded after another's upload answered is numbered after it, and its events placed after
// its, however many upload at once beside each — numbers and places each given once, with no gap:
// on PostgreSQL from the counters in each upload's transaction, on Spanner by the passes after
// them.
func TestSequencedInOrder(t *testing.T) {
	e := newCLEnv(t)
	const waves, width = 4, 8
	var all []int64
	before := int64(0)
	for w := range waves {
		commits := make([]encoding.ID, width)
		for i := range commits {
			commits[i] = e.commit(alice, newChange(), encoding.RootCommitID, fmt.Sprintf("wave %d, %d", w, i),
				map[string]string{fmt.Sprintf("w%d/f%d", w, i): "x"})
		}
		got := make([]int64, width)
		errs := make([]error, width)
		var wg sync.WaitGroup
		for i, c := range commits {
			wg.Go(func() {
				res, err := e.upload(alice, Ready, nil, c)
				if err == nil {
					got[i] = res[0].Number
				}
				errs[i] = err
			})
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("wave %d, upload %d: %v", w, i, err)
			}
			if got[i] <= before {
				t.Fatalf("wave %d made CL %d, not after the waves before it (to %d)", w, got[i], before)
			}
		}
		before = slices.Max(got)
		all = append(all, got...)
	}
	slices.Sort(all)
	for i, n := range all {
		if n != int64(i+1) {
			t.Fatalf("the CLs made are numbered %v, not 1 to %d", all, len(all))
		}
	}
	// The repository's creation, then the uploads'.
	if log := e.checkLog(); len(log) != 1+waves*width {
		t.Fatalf("the log holds %d events, for the repository and %d uploads", len(log), waves*width)
	}
}

// pendingRows are how many rows a repository's writers left pending.
func (e *clEnv) pendingRows() int {
	return dbOf(e.s).count(e.t, "sequence_pending", "repository_id = @repo", map[string]any{"repo": e.repo})
}

// What a writer leaves pending when it stops between its commit and its pass — its answer lost —
// is sequenced by the next read that meets it, or by any replica's sweep; nothing reads a
// provisional number meanwhile.
func TestSequencedAfterAWriterStops(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	if !e.s.b.SequencesAfterCommit() {
		t.Skip("a PostgreSQL transaction sequences itself")
	}
	e.s.skipSequencing = true
	first := newChange()
	if _, err := e.upload(alice, Ready, nil, e.commit(alice, first, encoding.RootCommitID, "one", map[string]string{"a": "1"})); err == nil {
		t.Fatal("an upload left unsequenced answered with its CL's number")
	}
	if n := e.pendingRows(); n != 2 {
		t.Fatalf("the upload left %d rows pending, want its CL's and its event's", n)
	}
	// A read that meets the provisional CL runs the pass first.
	cls, err := e.s.LookupChanges(ctx, e.repo, []encoding.ChangeID{first})
	if err != nil || len(cls) != 1 || cls[0].Number != 1 {
		t.Fatalf("the CL read back: %+v, %v", cls, err)
	}
	if n := e.pendingRows(); n != 0 {
		t.Fatalf("%d rows pending after the read's pass", n)
	}
	// Another upload and a transition left pending; another replica's sweep sequences them.
	if _, err := e.upload(alice, Ready, nil, e.commit(alice, newChange(), encoding.RootCommitID, "two", map[string]string{"b": "2"})); err == nil {
		t.Fatal("an upload left unsequenced answered with its CL's number")
	}
	if _, ev, err := e.s.Transition(ctx, e.repo, 1, Abandon, alice, "gone"); err != nil || ev.GetRepositorySeq() != 0 {
		t.Fatalf("a transition left unsequenced: %v at %d, %v", ev, ev.GetRepositorySeq(), err)
	}
	r := reopen(t, e.s)
	if err := r.sweepOnce(ctx, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if n := e.pendingRows(); n != 0 {
		t.Fatalf("%d rows pending after the sweep", n)
	}
	e.s.skipSequencing = false
	log := e.checkLog()
	if len(log) != 4 || log[1].GetUploaded().GetNumber() != 1 || log[2].GetUploaded().GetNumber() != 2 ||
		log[3].GetTransitioned().GetNumber() != 1 {
		t.Fatalf("the log: %v", log)
	}
	if c := e.cl(2); c.Number != 2 {
		t.Fatalf("CL 2: %+v", c)
	}
}

// A replica's pass sequences another replica's writes beside its own, and each writer learns its
// places and numbers, whichever pass gave them.
func TestSequencedAcrossReplicas(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	if !e.s.b.SequencesAfterCommit() {
		t.Skip("a PostgreSQL transaction sequences itself")
	}
	r := reopen(t, e.s)
	ran := 0
	r.beforeSequenced = func() {
		ran++
		if err := e.s.sweepOnce(ctx, time.Now().Add(time.Second)); err != nil {
			t.Error(err)
		}
	}
	other := &clEnv{t: t, s: r, repo: e.repo}
	res, err := other.upload(alice, Ready, nil, other.commit(alice, newChange(), encoding.RootCommitID, "one", map[string]string{"a": "1"}))
	if err != nil || len(res) != 1 || res[0].Number != 1 {
		t.Fatalf("the upload: %+v, %v", res, err)
	}
	_, ev, err := r.Transition(ctx, e.repo, 1, Abandon, alice, "gone")
	if err != nil || ev.GetRepositorySeq() != 3 || ev.GetTransitioned().GetNumber() != 1 {
		t.Fatalf("the transition's event: %v, %v", ev, err)
	}
	if ran != 2 {
		t.Fatalf("the other replica sequenced %d of the 2 writes first", ran)
	}
	if log := e.checkLog(); len(log) != 3 {
		t.Fatalf("the log: %v", log)
	}
}

// Uploads at once on three replicas, every replica's sweep running beside them so passes meet and
// abort each other, each learn the places and numbers their passes gave — whichever replica's — and
// the log runs without a gap.
func TestSequencedUnderContention(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := newCLEnv(t)
	if !e.s.b.SequencesAfterCommit() {
		t.Skip("a PostgreSQL transaction sequences itself")
	}
	replicas := []*Store{e.s, reopen(t, e.s), reopen(t, e.s)}
	var sweeps sync.WaitGroup
	defer sweeps.Wait()
	defer cancel()
	for _, r := range replicas {
		sweeps.Go(func() {
			for ctx.Err() == nil {
				if err := r.sweepOnce(ctx, time.Now()); err != nil && ctx.Err() == nil {
					t.Error(err)
				}
			}
		})
	}
	const waves, width = 3, 24
	made := 0
	for w := range waves {
		commits := make([]encoding.ID, width)
		for i := range commits {
			commits[i] = e.commit(alice, newChange(), encoding.RootCommitID, fmt.Sprintf("wave %d, %d", w, i),
				map[string]string{fmt.Sprintf("w%d/f%d", w, i): "x"})
		}
		errs := make([]error, width)
		var wg sync.WaitGroup
		for i, c := range commits {
			wg.Go(func() {
				on := &clEnv{t: t, s: replicas[i%len(replicas)], repo: e.repo}
				res, err := on.upload(alice, Ready, nil, c)
				if err == nil && (len(res) != 1 || res[0].Number <= 0) {
					err = fmt.Errorf("the upload's CLs: %+v", res)
				}
				errs[i] = err
			})
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("wave %d, upload %d: %v", w, i, err)
			}
		}
		made += width
	}
	cancel()
	sweeps.Wait()
	if log := e.checkLog(); len(log) != 1+made {
		t.Fatalf("the log holds %d events, for the repository and %d uploads", len(log), made)
	}
}

// A writer reads its events' places back once its pass has run, though another replica's pass
// placed them, the relay published them, and compaction ran meanwhile: an event stays in the tail
// a while after it is written (eventsSettle), and compaction moves only those past it.
func TestWriterReadsItsEventsPastCompaction(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	if !e.s.b.SequencesAfterCommit() {
		t.Skip("a PostgreSQL transaction places its own events")
	}
	r := reopen(t, e.s)
	st := &stream{}
	r.beforeSequenced = func() {
		if err := e.s.sweepOnce(ctx, time.Now().Add(time.Second)); err != nil {
			t.Error(err)
		}
		drain(t, e.s, st)
		if _, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil {
			t.Error(err)
		}
	}
	other := &clEnv{t: t, s: r, repo: e.repo}
	res, err := other.upload(alice, Ready, nil, other.commit(alice, newChange(), encoding.RootCommitID, "one", map[string]string{"a": "1"}))
	if err != nil || len(res) != 1 || res[0].Number != 1 {
		t.Fatalf("the upload: %+v, %v", res, err)
	}
	if seqs, _ := st.published(t, e.repo); !seqs[2] {
		t.Fatalf("the relay published %v before the writer read its events, want its event, 2, among them", seqs)
	}
	e.checkLog()
}

// A stack of CLs made by one upload is numbered bottom first, and each names the one under it by
// its number — stacked on a CL the same transaction made, its parent was provisional too.
func TestSequencedStack(t *testing.T) {
	e := newCLEnv(t)
	c1 := e.commit(alice, newChange(), encoding.RootCommitID, "one", map[string]string{"a": "1"})
	c2 := e.commit(alice, newChange(), c1, "two", map[string]string{"b": "2"})
	c3 := e.commit(alice, newChange(), c2, "three", map[string]string{"c": "3"})
	res := e.mustUpload(alice, Ready, c1, c2, c3)
	for i, r := range res {
		if r.Number != int64(i+1) {
			t.Fatalf("the stack is numbered %+v", res)
		}
	}
	for n, parent := range map[int64]int64{1: 0, 2: 1, 3: 2} {
		c := e.cl(n)
		if c.ParentNumber != parent {
			t.Errorf("CL %d is stacked on %d, want %d", n, c.ParentNumber, parent)
		}
	}
	children, _, err := e.s.ListChangeLists(context.Background(), e.repo, nil, "", 2, 0, 0)
	if err != nil || len(children) != 1 || children[0].Number != 3 {
		t.Fatalf("the CLs stacked on CL 2: %+v, %v", children, err)
	}
	e.checkLog()
}

// unpinnedTrees are how many of a repository's trees are unpinned: rows, and the trees publishes
// stored, in the content index.
func (e *clEnv) unpinnedTrees() int {
	n := dbOf(e.s).count(e.t, "object", "repository_id = @repo AND kind = @kind AND NOT pinned",
		map[string]any{"repo": e.repo, "kind": int64(encoding.KindTree)})
	for _, k := range indexOf(e.t, e.s, e.repo).Sorted() {
		if k.Entry.Kind == encoding.KindTree && !k.Entry.Pinned {
			n++
		}
	}
	return n
}

// An upload pins all its commits newly reach, found before its transaction (planPin) and pinned in
// it a kind at a time; a tree the plan found that a sweep took before the transaction fails it
// (errMissing), rather than leave a pinned commit over a tree that is gone.
func TestUploadPinsWhatItPlanned(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	deep := map[string]string{}
	for i := range 6 {
		deep[fmt.Sprintf("a/b/c/d/e/f%d/g", i)] = fmt.Sprint(i)
	}
	c1 := e.commit(alice, newChange(), encoding.RootCommitID, "deep", deep)
	if n := e.unpinnedTrees(); n == 0 {
		t.Fatal("the published trees are pinned before any upload")
	}
	e.mustUpload(alice, Ready, c1)
	if n := e.unpinnedTrees(); n != 0 {
		t.Fatalf("%d of the upload's trees are unpinned", n)
	}

	c2 := e.commit(alice, newChange(), c1, "deeper", map[string]string{"a/b/c/d/e/f9/g": "9"})
	plan, err := e.s.planPin(ctx, e.repo, []ref{{table: tCommit, id: c2}})
	if err != nil {
		t.Fatal(err)
	}
	// The trees are the content index's, a publish having stored them: the plan pins them there.
	var trees []encoding.ID
	for _, r := range plan.content {
		if r.table == tObject && r.kind == encoding.KindTree {
			trees = append(trees, r.id)
		}
	}
	if len(trees) != 7 { // the root, a, b, c, d, e, and f9
		t.Fatalf("the plan pins %d trees, want the 7 the change rewrote", len(trees))
	}
	dropFromIndex(t, e.s, e.repo, encoding.KindTree, trees[0])
	err = e.s.inTx(ctx, func(ctx context.Context, tx *txn) error { return e.s.pinPlanned(ctx, tx, e.repo, plan) })
	if !errors.Is(err, errMissing) {
		t.Fatalf("pinning a plan whose tree was swept: %v, want errMissing", err)
	}
	if n := e.unpinnedTrees(); n != len(trees)-1 {
		t.Fatalf("%d trees unpinned after the refused pin, want %d", n, len(trees)-1)
	}
}

// A writer that stops between its commit and its pass has what it left pending sequenced by its
// sequence step's job, SequenceAfter on, and its events published after: no replica looks for what
// is pending.
func TestSequencedByItsJob(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	e := newCLEnv(t)
	if !e.s.b.SequencesAfterCommit() {
		t.Skip("a PostgreSQL transaction sequences itself")
	}
	st := &stream{}
	r := reopen(t, e.s)
	relays, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan struct{})
	go func() { defer close(done); r.Relay(relays, st) }()
	e.s.skipSequencing, e.s.sequenceAfter = true, 200*time.Millisecond
	if _, err := e.upload(alice, Ready, nil, e.commit(alice, newChange(), encoding.RootCommitID, "one", map[string]string{"a": "1"})); err == nil {
		t.Fatal("an upload left unsequenced answered with its CL's number")
	}
	if n := e.pendingRows(); n != 2 {
		t.Fatalf("the upload left %d rows pending, want its CL's and its event's", n)
	}
	for e.pendingRows() != 0 {
		select {
		case <-ctx.Done():
			t.Fatal("the stopped writer's rows were never sequenced")
		case <-time.After(20 * time.Millisecond):
		}
	}
	e.s.skipSequencing = false
	for {
		if seqs, _ := st.published(t, e.repo); seqs[2] {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("the stopped writer's event was never published")
		case <-time.After(20 * time.Millisecond):
		}
	}
	if c := e.cl(1); c.Number != 1 {
		t.Errorf("the CL: %+v", c)
	}
	stop()
	<-done
}

// A repository's passes run one at a time across replicas: uploads at once on three, each waiting
// for a pass, never have two passes of the repository under way, and every one learns its number
// and places — the pass serving it this replica's or another's, heard of — the log without a gap,
// and fewer passes run than uploads.
func TestOnePassAtATime(t *testing.T) {
	e := newCLEnv(t)
	if !e.s.b.SequencesAfterCommit() {
		t.Skip("a PostgreSQL transaction sequences itself")
	}
	replicas := []*Store{e.s, reopen(t, e.s), reopen(t, e.s)}
	var mu sync.Mutex
	now, most, passes := 0, 0, 0
	for _, r := range replicas {
		r.passing = func(repo int64, enter bool) {
			mu.Lock()
			defer mu.Unlock()
			if enter {
				now++
				passes++
				most = max(most, now)
			} else {
				now--
			}
		}
	}
	const waves, width = 3, 24
	for w := range waves {
		commits := make([]encoding.ID, width)
		for i := range commits {
			commits[i] = e.commit(alice, newChange(), encoding.RootCommitID, fmt.Sprintf("wave %d, %d", w, i),
				map[string]string{fmt.Sprintf("w%d/f%d", w, i): "x"})
		}
		errs := make([]error, width)
		var wg sync.WaitGroup
		for i, c := range commits {
			wg.Go(func() {
				on := &clEnv{t: t, s: replicas[i%len(replicas)], repo: e.repo}
				res, err := on.upload(alice, Ready, nil, c)
				if err == nil && (len(res) != 1 || res[0].Number <= 0) {
					err = fmt.Errorf("the upload's CLs: %+v", res)
				}
				errs[i] = err
			})
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("wave %d, upload %d: %v", w, i, err)
			}
		}
	}
	if log := e.checkLog(); len(log) != 1+waves*width {
		t.Fatalf("the log holds %d events, for the repository and %d uploads", len(log), waves*width)
	}
	t.Logf("%d uploads, %d passes", waves*width, passes)
	if most != 1 || passes >= waves*width {
		t.Errorf("at most %d passes at once, %d passes for %d uploads", most, passes, waves*width)
	}
}
