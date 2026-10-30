package spannerdata_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/lock/spannerlock"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/work/worktest"
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

// The work table on Spanner passes the keyed work stores' suite.
func TestWork(t *testing.T) {
	worktest.Run(t, func(t *testing.T) worktest.Subject {
		c := client(t)
		return worktest.Subject{Store: spannerdata.Work(c),
			MarkOpen: func(ctx context.Context, kind string, key int64, commit <-chan struct{}) (<-chan struct{}, <-chan error) {
				written, done := make(chan struct{}), make(chan error, 1)
				go func() {
					var once sync.Once
					done <- spannerdata.Txn(ctx, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
						if err := spannerdata.MarkWork(ctx, tx, kind, key); err != nil {
							return err
						}
						once.Do(func() { close(written) })
						<-commit
						return nil
					})
				}()
				return written, done
			}}
	})
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
		first <- spannerdata.Txn(ctx, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			if err := spannerdata.MarkWork(ctx, tx, "m", 1); err != nil {
				return err
			}
			signal.Do(func() { close(marked) })
			<-release
			return nil
		})
	}()
	<-marked
	second := make(chan error, 1)
	go func() {
		second <- spannerdata.Txn(ctx, c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			return spannerdata.MarkWork(ctx, tx, "m", 1)
		})
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
	if got, err := w.Take(ctx, "k", 1, "a", time.Second); err != nil || !got.Took {
		t.Fatalf("taking the work: %v %v", got, err)
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
	if got, err := w.Take(ctx, "k", 1, "b", time.Minute); err != nil || !got.Took {
		t.Fatalf("another taking it: %v %v", got, err)
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
	if got, err := w.Take(ctx, "k", 1, "a", time.Minute); err != nil || !got.Took {
		t.Fatalf("taking the work: %v %v", got, err)
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
