package move_test

import (
	"context"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/testdb"
)

// A move's finish books each repository's purge a day on; its purge, when it comes, deletes what
// the role holds of a repository that moved away a day ago or more — one that moved less long ago
// is booked for its day, and one that moved back meanwhile keeps what it brought.
func TestPurgeMovedAway(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t)
	if _, err := pool.Exec(ctx, move.GateDDL+`;`+pgdata.WorkSchema+`;
		CREATE TABLE thing (repository_id bigint NOT NULL, v text NOT NULL, PRIMARY KEY (repository_id, v))`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO thing VALUES (1, 'a'), (2, 'b'), (3, 'c')`); err != nil {
		t.Fatal(err)
	}
	ws := pgdata.Work(pool)
	r := &move.RepositoryRole{RoleName: "test", Work: ws,
		Rows: &move.PostgresRows{Pool: pool, Tables: []string{"thing"}, Where: func(string) string { return "repository_id = ANY(@repos)" }}}
	if err := r.Finish(ctx, move.Scope{Repositories: []int64{1, 2, 3}}, "elsewhere"); err != nil {
		t.Fatal(err)
	}
	booked, err := ws.Marked(ctx, move.MovedWork, 0, 10)
	if err != nil || len(booked) != 3 || time.Until(booked[0].At) < move.Kept-time.Minute {
		t.Fatalf("the purges booked: %+v, %v; want three, a day on", booked, err)
	}
	// 1 moved away two days ago; 2 an hour ago; 3 came back.
	if _, err := pool.Exec(ctx, `UPDATE move_gate SET moved_at = now() - interval '2 days' WHERE repository_id = 1;
		UPDATE move_gate SET moved_at = now() - interval '1 hour' WHERE repository_id = 2`); err != nil {
		t.Fatal(err)
	}
	if err := move.SetGates(ctx, pool, []int64{3}, "", ""); err != nil {
		t.Fatal(err)
	}
	// Their purges come now.
	for _, repo := range []int64{1, 2, 3} {
		if err := ws.Mark(ctx, move.MovedWork, repo); err != nil {
			t.Fatal(err)
		}
	}
	pctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); r.Purge(pctx) }()
	left := func() map[int64]bool {
		rows, err := pool.Query(ctx, `SELECT repository_id FROM thing`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[int64]bool{}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out[id] = true
		}
		return out
	}
	for deadline := time.Now().Add(10 * time.Second); left()[1]; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("what moved away two days ago was never deleted")
		}
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		marks, err := ws.Marked(ctx, move.MovedWork, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(marks) == 1 && marks[0].Key == 2 && time.Until(marks[0].At) > move.Kept-2*time.Hour {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("repository 2's purge, an hour after it moved, never booked for its day: %+v", marks)
		}
	}
	stop()
	<-done
	if got := left(); !got[2] || !got[3] {
		t.Errorf("rows left %v; want 2's, moved an hour ago, and 3's, moved back", got)
	}
}
