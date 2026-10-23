package quota

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/automation/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// directory is a fake of the directory's quotas: what each account's is, and what was asked.
type directory struct {
	quotas map[string]*identitypb.Quota
	asked  []*identitypb.GetQuotasRequest
}

func (d *directory) GetQuotas(_ context.Context, req *identitypb.GetQuotasRequest, _ ...grpc.CallOption) (*identitypb.GetQuotasResponse, error) {
	d.asked = append(d.asked, req)
	out := &identitypb.GetQuotasResponse{}
	for _, a := range req.GetAccountIds() {
		out.Quotas = append(out.Quotas, d.quotas[a])
	}
	return out, nil
}

func (d *directory) ReportUsage(context.Context, *identitypb.ReportUsageRequest, ...grpc.CallOption) (*identitypb.ReportUsageResponse, error) {
	return &identitypb.ReportUsageResponse{}, nil
}

// A quota is read from the directory for this cell and month, kept Kept, and read again after: its
// minutes as seconds, with the other cells' use.
func TestCache(t *testing.T) {
	d := &directory{quotas: map[string]*identitypb.Quota{
		"acme": {Concurrent: 4, Minutes: 10, Weight: 2, RunningElsewhere: 1, SecondsElsewhere: 30},
		"bea":  {Weight: 1},
	}}
	c := NewCache(d, "us-1")
	now := time.Date(2027, 3, 31, 23, 59, 45, 0, time.UTC)
	c.now = func() time.Time { return now }
	ctx := context.Background()
	got, err := c.Get(ctx, []string{"acme", "bea"})
	if err != nil {
		t.Fatal(err)
	}
	if want := (store.Quota{Concurrent: 4, Seconds: 600, Weight: 2, RunningElsewhere: 1, SecondsElsewhere: 30}); got["acme"] != want {
		t.Errorf("acme's quota: %+v, want %+v", got["acme"], want)
	}
	if len(d.asked) != 1 || d.asked[0].GetCell() != "us-1" || d.asked[0].GetMonth() != "2027-03" || len(d.asked[0].GetAccountIds()) != 2 {
		t.Fatalf("asked: %v", d.asked)
	}
	now = now.Add(Kept - time.Second)
	if _, err := c.Get(ctx, []string{"acme"}); err != nil || len(d.asked) != 1 {
		t.Errorf("a kept quota read again: %v, %d asks", err, len(d.asked))
	}
	d.quotas["acme"].Concurrent = 8
	now = now.Add(2 * time.Second)
	got, err = c.Get(ctx, []string{"acme", "bea"})
	if err != nil || got["acme"].Concurrent != 8 || len(d.asked) != 2 || d.asked[1].GetMonth() != "2027-04" {
		t.Errorf("after Kept: %+v, %v, asked %v", got, err, d.asked)
	}
}
