// Package quota is ok automation's view of accounts' quotas of CI (docs/hosted.md, "Hosted
// automation, as built"): the directory's quotas, kept a while, with the other cells' use, which
// each cell reports.
package quota

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Kept is how long a quota read from the directory is used before it is read again.
const Kept = 30 * time.Second

// Cache keeps accounts' quotas, read from the directory, for Kept: a store.Quotas.
type Cache struct {
	Quotas identitypb.QuotasClient
	// Cell is this cell's: its own use is not elsewhere.
	Cell string
	now  func() time.Time

	mu   sync.Mutex
	kept map[string]entry
}

type entry struct {
	q  store.Quota
	at time.Time
}

// NewCache reads quotas through client, for a cell.
func NewCache(client identitypb.QuotasClient, cell string) *Cache {
	return &Cache{Quotas: client, Cell: cell, now: time.Now, kept: map[string]entry{}}
}

// Get is accounts' quotas: those kept, and those not read from the directory now.
func (c *Cache) Get(ctx context.Context, accounts []string) (map[string]store.Quota, error) {
	now := c.now()
	out := make(map[string]store.Quota, len(accounts))
	var missing []string
	c.mu.Lock()
	for _, a := range accounts {
		if e, ok := c.kept[a]; ok && now.Sub(e.at) < Kept {
			out[a] = e.q
		} else {
			missing = append(missing, a)
		}
	}
	c.mu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}
	resp, err := c.Quotas.GetQuotas(ctx, &identitypb.GetQuotasRequest{AccountIds: missing, Cell: c.Cell, Month: store.MonthOf(now)})
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, q := range resp.GetQuotas() {
		if i >= len(missing) {
			break
		}
		sq := store.Quota{Concurrent: int64(q.GetConcurrent()), Seconds: q.GetMinutes() * 60, Weight: int64(q.GetWeight()),
			RunningElsewhere: q.GetRunningElsewhere(), SecondsElsewhere: q.GetSecondsElsewhere()}
		out[missing[i]] = sq
		c.kept[missing[i]] = entry{q: sq, at: now}
	}
	return out, nil
}

// Every is how often a cell reports its use.
const Every = 15 * time.Second

// Reporter reports a cell's use this month to the directory, every Every: a node.Application's
// Start. Every replica reports the same totals, read from the cell's database, so each report
// stands for the cell whichever replica made it.
type Reporter struct {
	Store  *store.Store
	Quotas identitypb.QuotasClient
	Cell   string
	Log    *slog.Logger
}

// Run reports until ctx ends.
func (r *Reporter) Run(ctx context.Context) error {
	for {
		if err := r.Report(ctx); err != nil && ctx.Err() == nil {
			r.Log.Warn("quota: reporting the cell's use", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(Every):
		}
	}
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
