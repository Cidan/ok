package postgres_test

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/migrate"
	"github.com/Cidan/ok/internal/scm/store/postgres"
	"github.com/Cidan/ok/internal/testdb"
)

// The events written before repositories had logs of their own are numbered in their repositories'
// logs, in the feed's order, and each repository's counter starts after its last; the feed's own
// counter and numbers go, and an event is keyed by its repository's log.
func TestRepositoryEventsMigration(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t)
	if _, err := migrate.Apply(ctx, pool, "scm", postgres.Migrations[:2]); err != nil {
		t.Fatal(err)
	}
	var repos [3]int64
	for i := range repos {
		if err := pool.QueryRow(ctx, `INSERT INTO repository (name, head, created_by) VALUES ($1, $2, 'ada') RETURNING id`,
			[]string{"a", "b", "c"}[i], make([]byte, 32)).Scan(&repos[i]); err != nil {
			t.Fatal(err)
		}
	}
	// The feed: a, b, a, a, b; c has no events.
	feed := []int64{repos[0], repos[1], repos[0], repos[0], repos[1]}
	for i, r := range feed {
		if _, err := pool.Exec(ctx, `INSERT INTO event (seq, repository_id, body) VALUES ($1, $2, '')`, i+1, r); err != nil {
			t.Fatal(err)
		}
	}
	// Through 0006, which keeps the feed's numbers to read the order by.
	if _, err := migrate.Apply(ctx, pool, "scm", postgres.Migrations[:6]); err != nil {
		t.Fatal(err)
	}
	rows, err := pool.Query(ctx, `SELECT repository_id, repository_seq FROM event ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	var got [][2]int64
	for rows.Next() {
		var r, s int64
		if err := rows.Scan(&r, &s); err != nil {
			t.Fatal(err)
		}
		got = append(got, [2]int64{r, s})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := [][2]int64{{repos[0], 1}, {repos[1], 1}, {repos[0], 2}, {repos[0], 3}, {repos[1], 2}}
	if len(got) != len(want) {
		t.Fatalf("events %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events %v, want %v", got, want)
		}
	}
	counters := map[int64]int64{}
	crows, err := pool.Query(ctx, `SELECT repository_id, last FROM repository_event_counter`)
	if err != nil {
		t.Fatal(err)
	}
	for crows.Next() {
		var r, l int64
		if err := crows.Scan(&r, &l); err != nil {
			t.Fatal(err)
		}
		counters[r] = l
	}
	if len(counters) != 2 || counters[repos[0]] != 3 || counters[repos[1]] != 2 {
		t.Errorf("counters %v, want a's at 3 and b's at 2, c with none", counters)
	}
	var pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM event_pending`).Scan(&pending); err != nil || pending != 0 {
		t.Errorf("%d events pending (%v): the old ones are read from the log, not relayed", pending, err)
	}
	if _, err := migrate.Apply(ctx, pool, "scm", postgres.Migrations); err != nil {
		t.Fatal(err)
	}
	var numbered bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'event' AND column_name = 'seq')`).Scan(&numbered); err != nil || numbered {
		t.Errorf("events still carry the feed's numbers: %v, %v", numbered, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO event (repository_id, repository_seq, body) VALUES ($1, 1, '')`, repos[2]); err != nil {
		t.Errorf("an event: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO event (repository_id, repository_seq, body) VALUES ($1, 1, '')`, repos[2]); err == nil {
		t.Error("two events at one place in a repository's log")
	}
	var counter bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('event_counter') IS NOT NULL`).Scan(&counter); err != nil || counter {
		t.Errorf("the old feed's counter is still there: %v, %v", counter, err)
	}
	// The repositories keep the IDs their sequence gave them; a new one names its own.
	var kept int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM repository WHERE id = ANY($1)`, repos[:]).Scan(&kept); err != nil || kept != 3 {
		t.Errorf("%d of the 3 repositories keep their IDs (%v)", kept, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO repository (name, head, created_by) VALUES ('d', $1, 'ada')`, make([]byte, 32)); err == nil {
		t.Error("a repository made with no ID: its ID is the SCM's to choose")
	}
}
