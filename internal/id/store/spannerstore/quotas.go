package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) AccountQuotas(ctx context.Context, accounts []string) (map[string]store.Quota, error) {
	if accounts == nil {
		accounts = []string{}
	}
	out := map[string]store.Quota{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT principal_id, concurrent, minutes FROM account_quotas WHERE principal_id IN UNNEST(@ids)`,
			map[string]any{"ids": accounts}, func(row *spanner.Row) error {
				var id string
				var c, m int64
				if err := row.Columns(&id, &c, &m); err != nil {
					return err
				}
				out[id] = store.Quota{Concurrent: int(c), Minutes: m}
				return nil
			})
	})
	return out, err
}

func (x queries) SetAccountQuota(ctx context.Context, account string, q store.Quota) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdate("account_quotas", []string{"principal_id", "concurrent", "minutes"},
			[]any{account, int64(q.Concurrent), q.Minutes})})
	})
}

func (x queries) ClearAccountQuota(ctx context.Context, account string) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.Delete("account_quotas", spanner.Key{account})})
	})
}

func (x queries) ReportUsage(ctx context.Context, cell, month string, usage []store.Usage, at time.Time) error {
	if len(usage) == 0 {
		return nil
	}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		ms := make([]*spanner.Mutation, 0, len(usage))
		for _, u := range usage {
			ms = append(ms, spanner.InsertOrUpdate("account_usage", []string{"account_id", "month", "cell", "seconds", "running", "reported_at"},
				[]any{u.Account, month, cell, u.Seconds, u.Running, at}))
		}
		return tx.BufferWrite(ms)
	})
}

func (x queries) UsageElsewhere(ctx context.Context, accounts []string, cell, month string, since time.Time) (map[string]store.Usage, error) {
	if accounts == nil {
		accounts = []string{}
	}
	out := map[string]store.Usage{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT account_id, SUM(seconds), SUM(IF(reported_at >= @since, running, 0))
			FROM account_usage WHERE account_id IN UNNEST(@accounts) AND month = @month AND cell != @cell GROUP BY account_id`,
			map[string]any{"accounts": accounts, "month": month, "cell": cell, "since": since}, func(row *spanner.Row) error {
				var u store.Usage
				if err := row.Columns(&u.Account, &u.Seconds, &u.Running); err != nil {
					return err
				}
				out[u.Account] = u
				return nil
			})
	})
	return out, err
}
