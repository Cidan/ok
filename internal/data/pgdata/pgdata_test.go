package pgdata_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/testdb"
)

func pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	p := testdb.Pool(t)
	if _, err := p.Exec(context.Background(), `CREATE TABLE t (id int PRIMARY KEY, n int NOT NULL);
		INSERT INTO t VALUES (1, 0), (2, 0)`); err != nil {
		t.Fatal(err)
	}
	return p
}

// Two transactions that lock rows in opposite orders deadlock; PostgreSQL aborts one, and Txn
// runs it again, so both commit.
func TestTxnRetriesADeadlock(t *testing.T) {
	ctx := context.Background()
	p := pool(t)
	var attempts atomic.Int32
	var both sync.WaitGroup
	both.Add(2)
	var once [2]sync.Once
	run := func(side int, first, second int) error {
		return pgdata.Txn(ctx, p, func(tx pgx.Tx) error {
			attempts.Add(1)
			if _, err := tx.Exec(ctx, `UPDATE t SET n = n + 1 WHERE id = $1`, first); err != nil {
				return err
			}
			once[side].Do(func() {
				both.Done()
				both.Wait() // each holds its first row before either asks for its second
			})
			_, err := tx.Exec(ctx, `UPDATE t SET n = n + 1 WHERE id = $1`, second)
			return err
		})
	}
	var wg sync.WaitGroup
	var errA, errB error
	wg.Go(func() { errA = run(0, 1, 2) })
	wg.Go(func() { errB = run(1, 2, 1) })
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("the transactions: %v, %v", errA, errB)
	}
	if n := attempts.Load(); n < 3 {
		t.Errorf("%d attempts: the deadlock's victim was not run again", n)
	}
	var sum int
	if err := p.QueryRow(ctx, `SELECT sum(n) FROM t`).Scan(&sum); err != nil || sum != 4 {
		t.Errorf("the rows' sum: %d, %v; want 4, each transaction's two updates once", sum, err)
	}
}

// An error Txn does not retry ends it at once.
func TestTxnStopsOnOtherErrors(t *testing.T) {
	p := pool(t)
	n := 0
	err := pgdata.Txn(context.Background(), p, func(tx pgx.Tx) error {
		n++
		_, err := tx.Exec(context.Background(), `INSERT INTO t VALUES (1, 0)`)
		return err
	})
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "23505" || n != 1 {
		t.Errorf("a unique violation: %v after %d attempts", err, n)
	}
}

// A snapshot reads and refuses writes.
func TestSnapshot(t *testing.T) {
	ctx := context.Background()
	p := pool(t)
	err := pgdata.Snapshot(ctx, p, func(tx pgx.Tx) error {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM t`).Scan(&n); err != nil || n != 2 {
			t.Errorf("the snapshot's read: %d, %v", n, err)
		}
		_, err := tx.Exec(ctx, `UPDATE t SET n = 1`)
		return err
	})
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "25006" {
		t.Errorf("a write in a snapshot: %v, want read_only_sql_transaction", err)
	}
}

// Listen hands over what is notified on its channels after it is ready, and returns when its
// context ends.
func TestListen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := pool(t)
	ready := make(chan struct{})
	got := make(chan [2]string, 4)
	done := make(chan error, 1)
	go func() {
		done <- pgdata.Listen(ctx, p, []string{"ok_a", "ok-b"}, func() { close(ready) },
			func(channel, payload string) { got <- [2]string{channel, payload} })
	}()
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("Listen was never ready")
	}
	for _, n := range [][2]string{{"ok_a", "1"}, {"ok-b", "2"}, {"ok_c", "3"}, {"ok_a", "4"}} {
		if _, err := p.Exec(ctx, `SELECT pg_notify($1, $2)`, n[0], n[1]); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range [][2]string{{"ok_a", "1"}, {"ok-b", "2"}, {"ok_a", "4"}} {
		select {
		case n := <-got:
			if n != want {
				t.Errorf("notification %v, want %v", n, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("no notification; want %v", want)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Listen ended with %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Listen outlived its context")
	}
}

// A key's cursor starts at 0; locking makes its row and holds it; setting and advancing (never
// back) move it.
func TestCursors(t *testing.T) {
	ctx := context.Background()
	p := pool(t)
	if _, err := p.Exec(ctx, pgdata.CursorsSchema); err != nil {
		t.Fatal(err)
	}
	at := func(key int64) int64 {
		t.Helper()
		seq, err := pgdata.Cursor(ctx, p, "t", key)
		if err != nil {
			t.Fatal(err)
		}
		return seq
	}
	if at(1) != 0 {
		t.Fatal("a key with no cursor is not at 0")
	}
	if err := pgdata.SetCursor(ctx, p, "t", 1, 5); err != nil || at(1) != 5 {
		t.Fatalf("setting: %d, %v", at(1), err)
	}
	if err := pgdata.AdvanceCursor(ctx, p, "t", 1, 3); err != nil || at(1) != 5 {
		t.Fatalf("advancing back: %d, %v", at(1), err)
	}
	if err := pgdata.AdvanceCursor(ctx, p, "t", 1, 7); err != nil || at(1) != 7 {
		t.Fatalf("advancing: %d, %v", at(1), err)
	}
	if err := pgdata.SetCursor(ctx, p, "t", 1, 2); err != nil || at(1) != 2 {
		t.Fatalf("setting: %d, %v", at(1), err)
	}
	if at(2) != 0 {
		t.Fatal("keys share a cursor")
	}
	// Two transactions locking key 3's cursor, which has no row, wait on one another.
	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- pgx.BeginFunc(ctx, p, func(tx pgx.Tx) error {
			if _, err := pgdata.LockCursor(ctx, tx, "t", 3); err != nil {
				return err
			}
			close(locked)
			<-release
			return pgdata.SetCursor(ctx, tx, "t", 3, 1)
		})
	}()
	<-locked
	second := make(chan int64, 1)
	go func() {
		pgx.BeginFunc(ctx, p, func(tx pgx.Tx) error {
			seq, err := pgdata.LockCursor(ctx, tx, "t", 3)
			second <- seq
			return err
		})
	}()
	select {
	case <-second:
		t.Fatal("the second lock did not wait for the first")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if seq := <-second; seq != 1 {
		t.Errorf("the second lock read %d, want the first's 1", seq)
	}
}

// Work is marked, taken by one holder at a time with its mark cleared, released unless marked
// again meanwhile — when its holder keeps it to do it again — and due when marked and unheld, or
// when its holder's lease lapsed.
func TestWork(t *testing.T) {
	ctx := context.Background()
	p := pool(t)
	if _, err := p.Exec(ctx, pgdata.WorkSchema); err != nil {
		t.Fatal(err)
	}
	due := func() []int64 {
		t.Helper()
		keys, err := pgdata.DueWork(ctx, p, "q", 10)
		if err != nil {
			t.Fatal(err)
		}
		return keys
	}
	take := func(key int64, holder string, ttl time.Duration) bool {
		t.Helper()
		ok, err := pgdata.TakeWork(ctx, p, "q", key, holder, ttl)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if got := due(); len(got) != 0 {
		t.Fatalf("due before any mark: %v", got)
	}
	for _, k := range []int64{2, 1, 2} {
		if err := pgdata.MarkWork(ctx, p, "q", k); err != nil {
			t.Fatal(err)
		}
	}
	if err := pgdata.MarkWork(ctx, p, "other", 3); err != nil {
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
	// Marked while held: the holder keeps it, to do it again, and then lets it go.
	if err := pgdata.MarkWork(ctx, p, "q", 1); err != nil {
		t.Fatal(err)
	}
	if got := due(); !slices.Equal(got, []int64{2}) {
		t.Fatalf("due with 1 held and marked: %v", got)
	}
	if again, err := pgdata.ReleaseWork(ctx, p, "q", 1, "a"); err != nil || !again {
		t.Fatalf("releasing work marked meanwhile: %v, %v", again, err)
	}
	if again, err := pgdata.ReleaseWork(ctx, p, "q", 1, "b"); err != nil || again {
		t.Fatalf("another releasing it: %v, %v", again, err)
	}
	if ok, err := pgdata.RenewWork(ctx, p, "q", 1, "b", time.Minute); err != nil || ok {
		t.Fatalf("another renewing it: %v, %v", ok, err)
	}
	if ok, err := pgdata.RenewWork(ctx, p, "q", 1, "a", time.Minute); err != nil || !ok {
		t.Fatalf("its holder renewing it: %v, %v", ok, err)
	}
	if again, err := pgdata.ReleaseWork(ctx, p, "q", 1, "a"); err != nil || again {
		t.Fatalf("releasing clean work: %v, %v", again, err)
	}
	if !take(2, "a", time.Millisecond) {
		t.Fatal("taking 2")
	}
	time.Sleep(20 * time.Millisecond)
	// Its holder's lease lapsed: it is due, and another takes it.
	if got := due(); !slices.Equal(got, []int64{2}) {
		t.Fatalf("due with 2's lease lapsed: %v", got)
	}
	if !take(2, "b", time.Minute) {
		t.Fatal("taking work whose holder's lease lapsed")
	}
	if ok, _ := pgdata.RenewWork(ctx, p, "q", 2, "a", time.Minute); ok {
		t.Error("the lapsed holder renewed work another took")
	}
	if got := due(); len(got) != 0 {
		t.Errorf("due at the end: %v", got)
	}
	// Marked for later: not due until then; marked again sooner, due sooner; a plain mark is due now.
	later := time.Now().Add(time.Hour)
	if err := pgdata.MarkWorkAt(ctx, p, "q", 3, later); err != nil {
		t.Fatal(err)
	}
	if got := due(); len(got) != 0 {
		t.Errorf("due with 3 marked for an hour on: %v", got)
	}
	if err := pgdata.MarkWorkAt(ctx, p, "q", 3, time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := due(); !slices.Equal(got, []int64{3}) {
		t.Errorf("due once 3 was marked for a second ago: %v", got)
	}
	if err := pgdata.MarkWorkAt(ctx, p, "q", 3, later); err != nil {
		t.Fatal(err)
	}
	if got := due(); !slices.Equal(got, []int64{3}) {
		t.Errorf("a later mark put off what was due: %v", got)
	}
	// Held, marked for later: let go, still marked; due when its time comes.
	if !take(3, "a", time.Minute) {
		t.Fatal("taking 3")
	}
	if err := pgdata.MarkWorkAt(ctx, p, "q", 3, time.Now().Add(50*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if again, err := pgdata.ReleaseWork(ctx, p, "q", 3, "a"); err != nil || again {
		t.Fatalf("releasing work marked for later: %v, %v", again, err)
	}
	if got := due(); len(got) != 0 {
		t.Errorf("due before the later mark's time: %v", got)
	}
	time.Sleep(100 * time.Millisecond)
	if got := due(); !slices.Equal(got, []int64{3}) {
		t.Errorf("due once the later mark's time came: %v", got)
	}
	if !take(3, "b", time.Minute) {
		t.Fatal("taking 3 when due")
	}
	if again, err := pgdata.ReleaseWork(ctx, p, "q", 3, "b"); err != nil || again {
		t.Fatalf("releasing it: %v, %v", again, err)
	}

	// A key with no row: taking it makes its row, held.
	if !take(9, "a", time.Minute) || take(9, "b", time.Minute) {
		t.Error("taking a key with no row: not taken, or taken twice")
	}
	if again, err := pgdata.ReleaseWork(ctx, p, "q", 9, "a"); err != nil || again || !take(9, "b", time.Minute) {
		t.Errorf("releasing it for another: %v, %v", again, err)
	}
}

// A transaction under keyed work's fence commits while its holder holds the work, and is refused
// (data.ErrFenced) once the lease lapsed and another took it; a context with no fence checks
// nothing.
func TestCheckFence(t *testing.T) {
	ctx := context.Background()
	p := testdb.Pool(t)
	if _, err := p.Exec(ctx, pgdata.WorkSchema+lock.Schema); err != nil {
		t.Fatal(err)
	}
	in := func(ctx context.Context) error {
		return pgx.BeginFunc(ctx, p, func(tx pgx.Tx) error { return pgdata.CheckFence(ctx, tx) })
	}
	if err := in(ctx); err != nil {
		t.Errorf("no fence: %v", err)
	}
	if ok, err := pgdata.TakeWork(ctx, p, "k", 1, "a", time.Second); err != nil || !ok {
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
	if err := in(fa); !errors.Is(err, data.ErrFenced) {
		t.Errorf("its holder, past its lease: %v", err)
	}
	if ok, err := pgdata.TakeWork(ctx, p, "k", 1, "b", time.Minute); err != nil || !ok {
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

// An idle mark marks work that is new, or whose holder's lease lapsed unmarked, due now; work held,
// or marked to be done later, it leaves as it is.
func TestMarkWorkIdle(t *testing.T) {
	ctx := context.Background()
	p := testdb.Pool(t)
	if _, err := p.Exec(ctx, pgdata.WorkSchema); err != nil {
		t.Fatal(err)
	}
	due := func(want ...int64) {
		t.Helper()
		got, err := pgdata.DueWork(ctx, p, "k", 10)
		slices.Sort(got)
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("due = %v, %v; want %v", got, err, want)
		}
	}
	// 1 is new; 2 is held; 3 is marked for later; 4's holder's lease lapsed, unmarked.
	if ok, err := pgdata.TakeWork(ctx, p, "k", 2, "a", time.Minute); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := pgdata.MarkWorkAt(ctx, p, "k", 3, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if ok, err := pgdata.TakeWork(ctx, p, "k", 4, "a", time.Millisecond); err != nil || !ok {
		t.Fatal(ok, err)
	}
	time.Sleep(10 * time.Millisecond)
	for key := int64(1); key <= 4; key++ {
		if err := pgdata.MarkWorkIdle(ctx, p, "k", key); err != nil {
			t.Fatal(err)
		}
	}
	due(1, 4)
	// 2's holder lets go with nothing marked meanwhile: the idle mark left it alone.
	if again, err := pgdata.ReleaseWork(ctx, p, "k", 2, "a"); err != nil || again {
		t.Errorf("releasing the held work: again %v, %v; an idle mark marked it", again, err)
	}
	due(1, 4)
}
