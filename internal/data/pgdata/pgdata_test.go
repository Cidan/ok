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

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/testdb"
)

func pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.New(context.Background(), testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
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

// A key's cursor starts at 0; locking makes its row and holds it; setting, advancing (never back),
// and seeding (only when none) move it.
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
	if err := pgdata.SeedCursor(ctx, p, "t", 1, 5); err != nil || at(1) != 5 {
		t.Fatalf("seeding: %d, %v", at(1), err)
	}
	if err := pgdata.SeedCursor(ctx, p, "t", 1, 9); err != nil || at(1) != 5 {
		t.Fatalf("seeding a key with a cursor: %d, %v", at(1), err)
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
}
