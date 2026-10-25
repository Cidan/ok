package spannerdata_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/lock/spannerlock"
	"github.com/Cidan/ok/internal/testdb"
)

func client(t *testing.T) *spanner.Client {
	t.Helper()
	return testdb.SpannerClient(t, testdb.Spanner(t, spannerdata.WorkDDL, spannerdata.CursorsDDL, spannerlock.DDL))
}

// rw runs fn in a read-write transaction of its own.
func rw(t *testing.T, c *spanner.Client, fn func(context.Context, *spanner.ReadWriteTransaction) error) {
	t.Helper()
	if _, err := c.ReadWriteTransaction(context.Background(), fn); err != nil {
		t.Fatal(err)
	}
}

// Work is marked, taken by one holder at a time with its mark cleared, released unless marked
// again meanwhile — when its holder keeps it to do it again — and due when marked and unheld, or
// when its holder's lease lapsed; a mark for later is due then, or sooner if marked sooner.
func TestWork(t *testing.T) {
	ctx := context.Background()
	c := client(t)
	w := spannerdata.Work(c)
	due := func() []int64 {
		t.Helper()
		keys, err := w.Due(ctx, "q", 10)
		if err != nil {
			t.Fatal(err)
		}
		return keys
	}
	take := func(key int64, holder string, ttl time.Duration) bool {
		t.Helper()
		ok, err := w.Take(ctx, "q", key, holder, ttl)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if got := due(); len(got) != 0 {
		t.Fatalf("due before any mark: %v", got)
	}
	for _, k := range []int64{2, 1, 2} {
		if err := w.Mark(ctx, "q", k); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Mark(ctx, "other", 3); err != nil {
		t.Fatal(err)
	}
	if got := due(); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("due: %v, want 1 and 2", got)
	}
	if !take(1, "a", time.Minute) || take(1, "b", time.Minute) {
		t.Fatal("work taken by two holders, or by none")
	}
	if got := due(); !slices.Equal(got, []int64{2}) {
		t.Fatalf("due with 1 held: %v", got)
	}
	if err := w.Mark(ctx, "q", 1); err != nil {
		t.Fatal(err)
	}
	if got := due(); !slices.Equal(got, []int64{2}) {
		t.Fatalf("due with 1 held and marked: %v", got)
	}
	if again, err := w.Release(ctx, "q", 1, "a"); err != nil || !again {
		t.Fatalf("releasing work marked meanwhile: %v, %v", again, err)
	}
	if again, err := w.Release(ctx, "q", 1, "b"); err != nil || again {
		t.Fatalf("another releasing it: %v, %v", again, err)
	}
	if ok, err := w.Renew(ctx, "q", 1, "b", time.Minute); err != nil || ok {
		t.Fatalf("another renewing it: %v, %v", ok, err)
	}
	if ok, err := w.Renew(ctx, "q", 1, "a", time.Minute); err != nil || !ok {
		t.Fatalf("its holder renewing it: %v, %v", ok, err)
	}
	if again, err := w.Release(ctx, "q", 1, "a"); err != nil || again {
		t.Fatalf("releasing clean work: %v, %v", again, err)
	}
	if !take(2, "a", time.Millisecond) {
		t.Fatal("taking 2")
	}
	time.Sleep(20 * time.Millisecond)
	if got := due(); !slices.Equal(got, []int64{2}) {
		t.Fatalf("due with 2's lease lapsed: %v", got)
	}
	if !take(2, "b", time.Minute) {
		t.Fatal("taking work whose holder's lease lapsed")
	}
	if ok, _ := w.Renew(ctx, "q", 2, "a", time.Minute); ok {
		t.Error("the lapsed holder renewed work another took")
	}
	if got := due(); len(got) != 0 {
		t.Errorf("due at the end: %v", got)
	}
	later := time.Now().Add(time.Hour)
	if err := w.MarkAt(ctx, "q", 3, later); err != nil {
		t.Fatal(err)
	}
	if got := due(); len(got) != 0 {
		t.Errorf("due with 3 marked for an hour on: %v", got)
	}
	if err := w.MarkAt(ctx, "q", 3, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := due(); !slices.Equal(got, []int64{3}) {
		t.Errorf("due once 3 was marked for a second ago: %v", got)
	}
	if err := w.MarkAt(ctx, "q", 3, later); err != nil {
		t.Fatal(err)
	}
	if got := due(); !slices.Equal(got, []int64{3}) {
		t.Errorf("a later mark delayed one due sooner: %v", got)
	}
	// Taken and released with a mark due later, it is due then, not now.
	if !take(3, "a", time.Minute) {
		t.Fatal("taking 3")
	}
	if err := w.MarkAt(ctx, "q", 3, later); err != nil {
		t.Fatal(err)
	}
	if again, err := w.Release(ctx, "q", 3, "a"); err != nil || again {
		t.Fatalf("releasing work marked for later: %v, %v", again, err)
	}
	if got := due(); len(got) != 0 {
		t.Errorf("due once 3 was released, marked for later: %v", got)
	}
}

// A mark of work marked already writes nothing, so transactions marking the same work — a
// repository's every upload marks its compaction — do not wait on one another: one commits while
// another that marked it first is still open. The open one's read still holds the mark: taking the
// work, which clears it, waits until that one commits.
func TestMarksDoNotWaitOnEachOther(t *testing.T) {
	ctx := context.Background()
	c := client(t)
	w := spannerdata.Work(c)
	if err := w.Mark(ctx, "m", 1); err != nil {
		t.Fatal(err)
	}
	marked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	done := func() { once.Do(func() { close(release) }) }
	defer done()
	first := make(chan error, 1)
	go func() {
		var signal sync.Once
		_, err := c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			if err := spannerdata.MarkWork(ctx, tx, "m", 1); err != nil {
				return err
			}
			signal.Do(func() { close(marked) })
			<-release
			return nil
		})
		first <- err
	}()
	<-marked
	second := make(chan error, 1)
	go func() {
		_, err := c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			return spannerdata.MarkWork(ctx, tx, "m", 1)
		})
		second <- err
	}()
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a mark waited on another open transaction's mark of the same work")
	}
	taken := make(chan error, 1)
	go func() {
		_, err := w.Take(ctx, "m", 1, "a", time.Minute)
		taken <- err
	}()
	select {
	case err := <-taken:
		t.Fatalf("the work was taken while a transaction that marked it was open (%v)", err)
	case <-time.After(2 * time.Second):
	}
	done()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-taken; err != nil {
		t.Fatal(err)
	}
}

// An idle mark marks work that is new, or whose holder's lease lapsed unmarked, due now; work held,
// or marked to be done later, it leaves as it is.
func TestMarkWorkIdle(t *testing.T) {
	ctx := context.Background()
	c := client(t)
	w := spannerdata.Work(c)
	if ok, err := w.Take(ctx, "k", 2, "a", time.Minute); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := w.MarkAt(ctx, "k", 3, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if ok, err := w.Take(ctx, "k", 4, "a", time.Millisecond); err != nil || !ok {
		t.Fatal(ok, err)
	}
	time.Sleep(10 * time.Millisecond)
	for key := int64(1); key <= 4; key++ {
		rw(t, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			return spannerdata.MarkWorkIdle(ctx, tx, "k", key)
		})
	}
	if got, err := w.Due(ctx, "k", 10); err != nil || !slices.Equal(got, []int64{1, 4}) {
		t.Errorf("due = %v, %v; want 1 and 4", got, err)
	}
	if again, err := w.Release(ctx, "k", 2, "a"); err != nil || again {
		t.Errorf("releasing the held work: again %v, %v; an idle mark marked it", again, err)
	}
}

// A transaction under keyed work's fence commits while its holder holds the work — past its lease
// too, until another takes it, as no one else writes — and is refused (data.ErrFenced) once another
// holds it; a context with no fence checks nothing.
func TestCheckFence(t *testing.T) {
	ctx := context.Background()
	c := client(t)
	w := spannerdata.Work(c)
	in := func(ctx context.Context) error {
		return spannerdata.Txn(ctx, c, func(context.Context, *spanner.ReadWriteTransaction) error { return nil })
	}
	if err := in(ctx); err != nil {
		t.Errorf("no fence: %v", err)
	}
	if ok, err := w.Take(ctx, "k", 1, "a", time.Second); err != nil || !ok {
		t.Fatalf("taking the work: %v %v", ok, err)
	}
	fa := data.WithFence(ctx, data.Fence{Kind: "k", Key: 1, Holder: "a"})
	if err := in(fa); err != nil {
		t.Errorf("its holder, within its lease: %v", err)
	}
	if err := in(data.WithFence(ctx, data.Fence{Kind: "k", Key: 2, Holder: "a"})); !errors.Is(err, data.ErrFenced) {
		t.Errorf("work there is none of: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := in(fa); err != nil {
		t.Errorf("its holder, past its lease, no one else holding it: %v", err)
	}
	if ok, err := w.Take(ctx, "k", 1, "b", time.Minute); err != nil || !ok {
		t.Fatalf("another taking it: %v %v", ok, err)
	}
	if err := in(fa); !errors.Is(err, data.ErrFenced) {
		t.Errorf("its first holder, once another holds it: %v", err)
	}
	if err := in(data.Unfenced(fa)); err != nil {
		t.Errorf("unfenced: %v", err)
	}
	if err := in(data.WithFence(ctx, data.Fence{Kind: "k", Key: 1, Holder: "b"})); err != nil {
		t.Errorf("the new holder: %v", err)
	}
}

// Neither a fenced transaction nor a mark holds off the work's holder's renewals: the fence reads the
// holder's cell and a mark the mark's, which a renewal does not write.
func TestRenewalBesideAFence(t *testing.T) {
	ctx := context.Background()
	c := client(t)
	w := spannerdata.Work(c)
	if ok, err := w.Take(ctx, "k", 1, "a", time.Minute); err != nil || !ok {
		t.Fatalf("taking the work: %v %v", ok, err)
	}
	for _, tc := range []struct {
		name string
		ctx  context.Context
		in   func(ctx context.Context, tx *spanner.ReadWriteTransaction) error
	}{
		{"a fenced transaction", data.WithFence(ctx, data.Fence{Kind: "k", Key: 1, Holder: "a"}),
			func(context.Context, *spanner.ReadWriteTransaction) error { return nil }},
		{"a mark", ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			return spannerdata.MarkWork(ctx, tx, "k", 1)
		}},
	} {
		begun, release := make(chan struct{}), make(chan struct{})
		done := make(chan error, 1)
		go func() {
			var once sync.Once
			done <- spannerdata.Txn(tc.ctx, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
				if err := tc.in(ctx, tx); err != nil {
					return err
				}
				once.Do(func() { close(begun) })
				<-release
				return nil
			})
		}()
		<-begun
		renewed := make(chan error, 1)
		go func() {
			ok, err := w.Renew(ctx, "k", 1, "a", time.Minute)
			if err == nil && !ok {
				err = errors.New("the lease was not its holder's")
			}
			renewed <- err
		}()
		select {
		case err := <-renewed:
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
		case <-time.After(10 * time.Second):
			close(release)
			t.Fatalf("a renewal waited on %s", tc.name)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
	}
}

// A key's cursor starts at 0; setting and advancing (never back) move it; two transactions at once,
// each locking a key's cursor and moving it on by one, move it by two.
func TestCursors(t *testing.T) {
	ctx := context.Background()
	c := client(t)
	at := func(key int64) int64 {
		t.Helper()
		seq, err := spannerdata.Cursor(ctx, c.Single(), "t", key)
		if err != nil {
			t.Fatal(err)
		}
		return seq
	}
	if at(1) != 0 {
		t.Fatal("a key with no cursor is not at 0")
	}
	set := func(key, seq int64) {
		rw(t, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			return spannerdata.SetCursor(ctx, tx, "t", key, seq)
		})
	}
	advance := func(key, seq int64) {
		rw(t, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			return spannerdata.AdvanceCursor(ctx, tx, "t", key, seq)
		})
	}
	if set(1, 5); at(1) != 5 {
		t.Fatalf("setting: %d", at(1))
	}
	if advance(1, 3); at(1) != 5 {
		t.Fatalf("advancing back: %d", at(1))
	}
	if advance(1, 7); at(1) != 7 {
		t.Fatalf("advancing: %d", at(1))
	}
	if set(1, 2); at(1) != 2 {
		t.Fatalf("setting back: %d", at(1))
	}
	if at(2) != 0 {
		t.Fatal("keys share a cursor")
	}
	if got, err := spannerdata.Cursors(ctx, c.Single(), "t", []int64{1, 2}); err != nil || len(got) != 1 || got[1] != 2 {
		t.Errorf("Cursors = %v, %v", got, err)
	}
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
				seq, err := spannerdata.LockCursor(ctx, tx, "t", 3)
				if err != nil {
					return err
				}
				return spannerdata.SetCursor(ctx, tx, "t", 3, seq+1)
			})
			errs <- err
		}()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if at(3) != 2 {
		t.Errorf("two transactions moving key 3's cursor on by one moved it to %d", at(3))
	}
}
