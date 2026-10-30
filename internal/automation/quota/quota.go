// Package quota is ok automation's view of accounts' quotas of CI (docs/hosted.md, "Hosted
// automation, as built"): the directory's quotas, kept a while, with the other cells' use, which
// each cell reports.
package quota

import (
	"context"
	"log/slog"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/work"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Reader reads accounts' quotas from the directory each time it is asked: a store.Quotas. No
// replica keeps them, so a quota changed is admitted by at once (docs/state.md).
type Reader struct {
	Quotas identitypb.QuotasClient
	// Cell is this cell's: its own use is not elsewhere.
	Cell string
	now  func() time.Time // the clock the month is read by (tests); nil is time.Now
}

// Get is accounts' quotas, as the directory says them now.
func (r Reader) Get(ctx context.Context, accounts []string) (map[string]store.Quota, error) {
	out := make(map[string]store.Quota, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	resp, err := r.Quotas.GetQuotas(ctx, &identitypb.GetQuotasRequest{AccountIds: accounts, Cell: r.Cell, Month: store.MonthOf(now())})
	if err != nil {
		return nil, err
	}
	for i, q := range resp.GetQuotas() {
		if i >= len(accounts) {
			break
		}
		out[accounts[i]] = store.Quota{Concurrent: int64(q.GetConcurrent()), Seconds: q.GetMinutes() * 60,
			RunningElsewhere: q.GetRunningElsewhere(), SecondsElsewhere: q.GetSecondsElsewhere()}
	}
	return out, nil
}

// Every is how often a cell reports its use.
const Every = 15 * time.Second

// Reporter reports a cell's use this month to the directory, every Every: a node.Application's
// Start. Its report is keyed work (Work), done by one replica at a time, which books the next; the
// totals are read from the cell's database, so each report stands for the cell whichever replica
// made it.
type Reporter struct {
	Store  *store.Store
	Quotas identitypb.QuotasClient
	Cell   string
	Log    *slog.Logger
}

// Work is the cell's report of its use, as keyed work.
const Work = "usage"

// Run reports until ctx ends: once as it starts, and every Every after.
func (r *Reporter) Run(ctx context.Context) error {
	ws := r.Store.Work()
	if err := ws.MarkAt(ctx, Work, 0, time.Now()); err != nil && ctx.Err() == nil {
		r.Log.Warn("quota: booking the cell's report", "err", err)
	}
	return (&work.Runner{
		Kind:  Work,
		Store: ws,
		Do: func(ctx context.Context, _ int64) error {
			err := r.Report(ctx)
			if merr := ws.MarkAt(ctx, Work, 0, time.Now().Add(Every)); merr != nil && err == nil {
				err = merr
			}
			return err
		},
		Log: r.Log,
	}).Run(ctx)
}

// Report reports the cell's use this month once.
func (r *Reporter) Report(ctx context.Context) error {
	month, usage, err := r.Store.MonthUsage(ctx)
	if err != nil || len(usage) == 0 {
		return err
	}
	req := &identitypb.ReportUsageRequest{Cell: r.Cell, Month: month}
	for _, u := range usage {
		req.Usage = append(req.Usage, &identitypb.AccountUsage{AccountId: u.Account, Seconds: u.Seconds, Running: u.Running})
	}
	_, err = r.Quotas.ReportUsage(ctx, req)
	return err
}
