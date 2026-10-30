package quota

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/automation/store/postgres"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/testdb"
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

// A quota is read from the directory for this cell and month each time it is asked: its minutes
// as seconds, with the other cells' use.
func TestReader(t *testing.T) {
	d := &directory{quotas: map[string]*identitypb.Quota{
		"acme": {Concurrent: 4, Minutes: 10, RunningElsewhere: 1, SecondsElsewhere: 30},
		"bea":  {},
	}}
	now := time.Date(2027, 3, 31, 23, 59, 45, 0, time.UTC)
	r := Reader{Quotas: d, Cell: "us-1", now: func() time.Time { return now }}
	ctx := context.Background()
	got, err := r.Get(ctx, []string{"acme", "bea"})
	if err != nil {
		t.Fatal(err)
	}
	if want := (store.Quota{Concurrent: 4, Seconds: 600, RunningElsewhere: 1, SecondsElsewhere: 30}); got["acme"] != want {
		t.Errorf("acme's quota: %+v, want %+v", got["acme"], want)
	}
	if len(d.asked) != 1 || d.asked[0].GetCell() != "us-1" || d.asked[0].GetMonth() != "2027-03" || len(d.asked[0].GetAccountIds()) != 2 {
		t.Fatalf("asked: %v", d.asked)
	}
	d.quotas["acme"].Concurrent = 8
	now = now.Add(time.Minute)
	got, err = r.Get(ctx, []string{"acme"})
	if err != nil || got["acme"].Concurrent != 8 || len(d.asked) != 2 || d.asked[1].GetMonth() != "2027-04" {
		t.Errorf("a quota changed, a month on: %+v, %v, asked %v", got, err, d.asked)
	}
}

// The cell's report is keyed work: done as a reporter starts — on one replica of those that start
// together — and booked Every on by each report, which is all that brings the next.
func TestReporterBooksTheNext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, err := postgres.Open(ctx, testdb.Pool(t))
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st := store.New(b, blobs, store.DefaultConfig)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	for range 2 {
		wg.Go(func() { (&Reporter{Store: st, Quotas: &directory{}, Cell: "c1", Log: slog.Default()}).Run(ctx) })
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		marks, err := st.Work().Marked(ctx, Work, -1, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(marks) == 1 && marks[0].Key == 0 && time.Until(marks[0].At) > Every-2*time.Second {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the next report never booked Every on: %+v", marks)
		}
	}
}
