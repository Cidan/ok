package service_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Administrators set an account's quota of CI, and reset it; ok automation alone reads quotas, and
// reports its cell's use, which the others' quotas carry as elsewhere.
func TestQuotas(t *testing.T) {
	ctx := context.Background()
	e := newEnvWith(t, true)
	admin := identitypb.NewAdminClient(e.as(ada))
	if _, err := identitypb.NewAdminClient(e.as(bea)).SetQuota(ctx, &identitypb.SetQuotaRequest{Account: "bea", Concurrent: 100}); code(err) != codes.PermissionDenied {
		t.Errorf("a person setting their own quota: %v", err)
	}
	q, err := admin.SetQuota(ctx, &identitypb.SetQuotaRequest{Account: "bea", Concurrent: 2, Minutes: 60})
	if err != nil || q.GetConcurrent() != 2 || q.GetMinutes() != 60 || q.GetDefault() {
		t.Fatalf("SetQuota = %v, %v", q, err)
	}
	if q, err := admin.GetQuota(ctx, &identitypb.GetQuotaRequest{Account: "nobody"}); code(err) != codes.NotFound {
		t.Errorf("the quota of no account: %v, %v", q, err)
	}

	if _, err := identitypb.NewQuotasClient(e.as(control)).GetQuotas(ctx, &identitypb.GetQuotasRequest{}); code(err) != codes.PermissionDenied {
		t.Errorf("ok control reading quotas: %v", err)
	}
	cells := identitypb.NewQuotasClient(e.as(automation))
	if _, err := cells.ReportUsage(ctx, &identitypb.ReportUsageRequest{Cell: "c2", Month: "2027-03",
		Usage: []*identitypb.AccountUsage{{AccountId: q.GetAccountId(), Seconds: 90, Running: 1}}}); err != nil {
		t.Fatal(err)
	}
	for cell, want := range map[string]int64{"c1": 90, "c2": 0} {
		got, err := cells.GetQuotas(ctx, &identitypb.GetQuotasRequest{AccountIds: []string{q.GetAccountId()}, Cell: cell, Month: "2027-03"})
		if err != nil || len(got.GetQuotas()) != 1 || got.GetQuotas()[0].GetSecondsElsewhere() != want || got.GetQuotas()[0].GetConcurrent() != 2 {
			t.Errorf("bea's quota as %s asks: %v, %v; want %d seconds elsewhere", cell, got, err, want)
		}
	}
	if q, err := admin.SetQuota(ctx, &identitypb.SetQuotaRequest{Account: "bea", Reset_: true}); err != nil || !q.GetDefault() {
		t.Errorf("resetting bea's quota: %v, %v", q, err)
	}
}
