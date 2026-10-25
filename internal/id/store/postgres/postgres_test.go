package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/id/store/postgres"
	"github.com/Cidan/ok/internal/id/store/storetest"
	"github.com/Cidan/ok/internal/testdb"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Backend {
		ctx := context.Background()
		pool := testdb.Pool(t)
		b, err := postgres.Open(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := postgres.Open(ctx, pool); err != nil {
			t.Fatalf("opening again: %v", err)
		}
		return b
	})
}

// Search's trigram indexes are made where pg_trgm can be, as on the test server.
func TestSearchIndexes(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t)
	if _, err := postgres.Open(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes
		WHERE indexname IN ('repositories_name_trgm', 'principals_handle_trgm', 'principals_display_trgm')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("%d of search's 3 trigram indexes", n)
	}
}

// SetsHolding reads the sets holding what it is asked through the members' GIN indexes: each
// branch's predicate is one an index serves — planned with sequential scans off, as a table larger
// than these is planned anyway, it scans no table of sets — and it finds what holds.
func TestSetsHoldingUsesTheIndexes(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t)
	b, err := postgres.Open(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO principals (id, name, kind, status, email, display_name) VALUES ('prn_org', 'acme.org.example.com', 'org', 'active', '', '')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO teams (name, organization_id, members)
			SELECT 't' || i || '.acme.team.example.com', 'prn_org', ARRAY['prn_' || i, 'prn_' || (i + 1)] FROM generate_series(1, 3000) AS i;
		INSERT INTO synced_groups (name, external_id, members)
			SELECT 'g' || i || '.group.example.com', 'g' || i, ARRAY['p' || i || '.human.example.com'] FROM generate_series(1, 3000) AS i;
		UPDATE teams SET members = array_append(members, 'prn_ada') WHERE name IN ('t7.acme.team.example.com', 't700.acme.team.example.com');
		UPDATE synced_groups SET members = array_append(members, 'ada.human.example.com') WHERE name = 'g9.group.example.com';
		ANALYZE teams; ANALYZE synced_groups`); err != nil {
		t.Fatal(err)
	}
	plan := func() string {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		if _, err := conn.Exec(ctx, `SET enable_seqscan = off`); err != nil {
			t.Fatal(err)
		}
		defer conn.Exec(ctx, `RESET enable_seqscan`)
		rows, err := conn.Query(ctx, `EXPLAIN `+postgres.SetsHoldingSQL, []string{"ada.human.example.com"}, []string{"prn_ada"}, []string{})
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var lines []string
		for rows.Next() {
			var l string
			if err := rows.Scan(&l); err != nil {
				t.Fatal(err)
			}
			lines = append(lines, l)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return strings.Join(lines, "\n")
	}()
	for _, index := range []string{"tree_sets_members", "synced_groups_members", "teams_members"} {
		if !strings.Contains(plan, "Bitmap Index Scan on "+index) {
			t.Errorf("SetsHolding reads no index %s:\n%s", index, plan)
		}
	}
	for _, table := range []string{"tree_sets", "synced_groups", "teams"} {
		if strings.Contains(plan, "Seq Scan on "+table+" ") {
			t.Errorf("SetsHolding scans %s:\n%s", table, plan)
		}
	}
	hs, err := b.SetsHolding(ctx, []string{"ada.human.example.com"}, []string{"prn_ada"}, nil)
	if err != nil || len(hs) != 3 {
		t.Errorf("SetsHolding found %+v holding ada, %v; want t7, t700, and g9", hs, err)
	}
}
