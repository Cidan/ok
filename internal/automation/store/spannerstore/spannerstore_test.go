package spannerstore_test

import (
	"context"
	"slices"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/automation/store/spannerstore"
	"github.com/Cidan/ok/internal/automation/store/storetest"
	"github.com/Cidan/ok/internal/data/spannerdata/spannertest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Backend {
		admin, client := spannertest.Database(t, spannerstore.Migrations)
		b, err := spannerstore.Open(context.Background(), admin, client)
		if err != nil {
			t.Fatal(err)
		}
		return b
	})
}

// The statements over a CL's pre runs read only the CL's runs, through run_cl: a read-write
// transaction locks what it reads, and a read of every queued or running run (through run_leased,
// by status, as Spanner reads them unhinted) would conflict with every other repository's plans.
func TestCLRunsReadByTheCL(t *testing.T) {
	ctx := context.Background()
	_, c := spannertest.Database(t, spannerstore.Migrations)
	params := map[string]any{"repo": int64(1), "cl": int64(1), "pre": int64(1), "below": int64(2), "patchset": int64(1), "queued": int64(1),
		"running": int64(2), "canceled": int64(5), "reason": "r"}
	for name, sql := range spannerstore.CLRunStatements {
		var scans []string
		if _, err := c.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			scans = nil
			mode := spannerpb.ExecuteSqlRequest_PLAN
			it := tx.QueryWithOptions(ctx, spanner.Statement{SQL: sql, Params: params}, spanner.QueryOptions{Mode: &mode})
			defer it.Stop()
			if err := it.Do(func(*spanner.Row) error { return nil }); err != nil {
				return err
			}
			for _, n := range it.QueryPlan.GetPlanNodes() {
				if n.GetDisplayName() == "Scan" {
					scans = append(scans, n.GetMetadata().GetFields()["scan_target"].GetStringValue())
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Contains(scans, "run_cl") || slices.Contains(scans, "run_leased") || slices.Contains(scans, "run_queued") {
			t.Errorf("%s reads %v, not the CL's runs through run_cl", name, scans)
		}
	}
}
