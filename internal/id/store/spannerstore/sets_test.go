package spannerstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata/spannertest"
	"github.com/Cidan/ok/internal/id/store/spannerstore"
)

// SetsHolding reads the sets holding what it is asked through the members' search indexes, and
// SetRecords the sets named by their keys: among thousands of teams, synced groups, and tree sets,
// a person in a few, or a few named, scan rows in proportion to those few, not to the directory.
func TestSetsHoldingUsesTheIndexes(t *testing.T) {
	ctx := context.Background()
	admin, client := spannertest.Database(t, spannerstore.Migrations)
	if _, err := spannerstore.Open(ctx, admin, client); err != nil {
		t.Fatal(err)
	}
	const many = 3000
	var ms []*spanner.Mutation
	ms = append(ms, spanner.Insert("principals", []string{"id", "name", "kind", "status", "email", "display_name", "created_at", "updated_at"},
		[]any{"prn_org", "acme.org.example.com", "org", "active", "", "", time.Now(), time.Now()}))
	flush := func() {
		if _, err := client.Apply(ctx, ms); err != nil {
			t.Fatal(err)
		}
		ms = nil
	}
	for i := range many {
		holds := []string{fmt.Sprintf("prn_%d", i), fmt.Sprintf("prn_%d", i+1)}
		names := []string{fmt.Sprintf("p%d.human.example.com", i)}
		if i%1000 == 0 {
			holds, names = append(holds, "prn_ada"), append(names, "ada.human.example.com")
		}
		ms = append(ms,
			spanner.Insert("teams", []string{"name", "organization_id", "members"}, []any{fmt.Sprintf("t%d.acme.team.example.com", i), "prn_org", holds}),
			spanner.Insert("synced_groups", []string{"external_id", "name", "members", "deleted", "updated_at"},
				[]any{fmt.Sprintf("g%d", i), fmt.Sprintf("g%d.group.example.com", i), names, false, time.Now()}))
		if i%200 == 0 {
			ms = append(ms, spanner.Insert("tree_sets", []string{"name", "kind", "path", "owner", "members"},
				[]any{fmt.Sprintf("f%d.group.example.com", i), "group", fmt.Sprintf("f%d", i), "admins.acl.example.com", names}))
		}
		if len(ms) >= 600 {
			flush()
		}
	}
	flush()
	stats := func(what, sql string, params map[string]any, want int) {
		t.Helper()
		it := client.Single().QueryWithStats(ctx, spanner.Statement{SQL: sql, Params: params})
		rows := 0
		if err := it.Do(func(*spanner.Row) error { rows++; return nil }); err != nil {
			t.Fatal(err)
		}
		var scanned int64
		if _, err := fmt.Sscan(fmt.Sprint(it.QueryStats["rows_scanned"]), &scanned); err != nil {
			t.Fatalf("the statement's stats: %v", it.QueryStats)
		}
		if rows != want {
			t.Errorf("%s: %d sets, want %d", what, rows, want)
		}
		if scanned > 100 {
			t.Errorf("%s scanned %d rows for %d sets among %d: its indexes are unused", what, scanned, rows, 2*many+many/200)
		}
		t.Logf("%s: %d sets, %d rows scanned, %v", what, rows, scanned, it.QueryStats["cpu_time"])
	}
	// ada is in three teams, three synced groups, and three tree sets: 9 of 6,015 sets.
	stats("SetsHolding", spannerstore.SetsHoldingSQL,
		map[string]any{"names": []string{"ada.human.example.com"}, "ids": []string{"prn_ada"}, "teams": []string{}}, 9)
	stats("SetRecords", spannerstore.SetRecordsSQL,
		map[string]any{"names": []string{"t7.acme.team.example.com", "g7.group.example.com", "f200.group.example.com", "none.group.example.com"}}, 3)
}
