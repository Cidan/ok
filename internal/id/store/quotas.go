package store

import (
	"context"
	"time"
)

// Quotas of CI (docs/hosted.md, "Hosted automation, as built"): an account's — a person's or an
// organization's — concurrent runs, run minutes in a calendar month (zero for unlimited), and
// weight in fair share.

// Quota is an account's quota of CI.
type Quota struct {
	Concurrent int
	Minutes    int64
	Weight     int
	// Default: the account has none of its own, and this is the directory's default.
	Default bool
}

// SetDefaultQuota sets the quota of an account an administrator set none. Call it before the
// store serves.
func (s *Store) SetDefaultQuota(q Quota) {
	q.Default = true
	if q.Weight <= 0 {
		q.Weight = 1
	}
	s.defaultQuota = q
}

// QuotasOf are accounts' quotas, by account ID, each its own or the default.
func (s *Store) QuotasOf(ctx context.Context, accounts []string) (map[string]Quota, error) {
	set, err := s.b.AccountQuotas(ctx, accounts)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Quota, len(accounts))
	for _, a := range accounts {
		q, ok := set[a]
		if !ok {
			q = s.defaultQuota
			if q.Weight == 0 {
				q.Weight = 1
			}
			q.Default = true
		}
		out[a] = q
	}
	return out, nil
}

// Usage is an account's use of CI in a month: run seconds, and running runs.
type Usage struct {
	Account string
	Seconds int64
	Running int64
}

// runningFresh is how recent a cell's report of an account's running runs is to count: a cell that
// stopped reporting runs nothing that holds an account back.
const runningFresh = 2 * time.Minute

// ReportUsage records a cell's use this month by accounts.
func (s *Store) ReportUsage(ctx context.Context, cell, month string, usage []Usage) error {
	if cell == "" || !validMonth(month) {
		return invalid("a report names its cell and its month, YYYY-MM")
	}
	return s.b.ReportUsage(ctx, cell, month, usage, s.now())
}

// UsageElsewhere sums accounts' use in a month in the cells but one.
func (s *Store) UsageElsewhere(ctx context.Context, accounts []string, cell, month string) (map[string]Usage, error) {
	if !validMonth(month) {
		return map[string]Usage{}, nil
	}
	return s.b.UsageElsewhere(ctx, accounts, cell, month, s.now().Add(-runningFresh))
}

func validMonth(m string) bool {
	_, err := time.Parse("2006-01", m)
	return err == nil
}

// SetQuota sets an account's quota; ResetQuota gives it the default again.
func (s *Store) SetQuota(ctx context.Context, account string, q Quota) error {
	if q.Concurrent < 0 || q.Minutes < 0 || q.Weight < 0 {
		return invalid("a quota's concurrent runs, minutes, and weight are not negative")
	}
	if q.Weight == 0 {
		q.Weight = 1
	}
	return s.b.SetAccountQuota(ctx, account, q)
}

func (s *Store) ResetQuota(ctx context.Context, account string) error {
	return s.b.ClearAccountQuota(ctx, account)
}
