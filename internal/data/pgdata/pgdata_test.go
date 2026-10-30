package pgdata_test

import (
	"context"
	"errors"
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
	"github.com/Cidan/ok/internal/work/worktest"
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

// The work table on PostgreSQL passes the keyed work stores' suite.
func TestWork(t *testing.T) {
	worktest.Run(t, func(t *testing.T) worktest.Subject {
		p := testdb.Pool(t)
		if _, err := p.Exec(context.Background(), pgdata.WorkSchema); err != nil {
			t.Fatal(err)
		}
		return worktest.Subject{Store: pgdata.Work(p),
			MarkOpen: func(ctx context.Context, kind string, key int64, commit <-chan struct{}) (<-chan struct{}, <-chan error) {
				written, done := make(chan struct{}), make(chan error, 1)
				go func() {
					var once sync.Once
					done <- pgdata.Txn(ctx, p, func(tx pgx.Tx) error {
						if err := pgdata.MarkWork(ctx, tx, kind, key); err != nil {
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
	if got, err := pgdata.TakeWork(ctx, p, "k", 1, "a", time.Second); err != nil || !got.Took {
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
	if err := in(fa); !errors.Is(err, data.ErrFenced) {
		t.Errorf("its holder, past its lease: %v", err)
	}
	if got, err := pgdata.TakeWork(ctx, p, "k", 1, "b", time.Minute); err != nil || !got.Took {
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
