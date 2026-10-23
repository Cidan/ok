package postgres

import (
	"context"
	"time"

	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) AccountQuotas(ctx context.Context, accounts []string) (map[string]store.Quota, error) {
	rows, err := x.q.Query(ctx, `SELECT principal_id, concurrent, minutes, weight FROM account_quotas WHERE principal_id = ANY($1)`, accounts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]store.Quota{}
	for rows.Next() {
		var id string
		var q store.Quota
		if err := rows.Scan(&id, &q.Concurrent, &q.Minutes, &q.Weight); err != nil {
			return nil, err
		}
		out[id] = q
	}
	return out, rows.Err()
}

func (x queries) SetAccountQuota(ctx context.Context, account string, q store.Quota) error {
	_, err := x.q.Exec(ctx, `INSERT INTO account_quotas (principal_id, concurrent, minutes, weight) VALUES ($1, $2, $3, $4)
		ON CONFLICT (principal_id) DO UPDATE SET concurrent = $2, minutes = $3, weight = $4`, account, q.Concurrent, q.Minutes, q.Weight)
	return err
}

func (x queries) ClearAccountQuota(ctx context.Context, account string) error {
	_, err := x.q.Exec(ctx, `DELETE FROM account_quotas WHERE principal_id = $1`, account)
	return err
}

func (x queries) ReportUsage(ctx context.Context, cell, month string, usage []store.Usage, at time.Time) error {
	if len(usage) == 0 {
		return nil
	}
	accounts, seconds, running := make([]string, len(usage)), make([]int64, len(usage)), make([]int64, len(usage))
	for i, u := range usage {
		accounts[i], seconds[i], running[i] = u.Account, u.Seconds, u.Running
	}
	_, err := x.q.Exec(ctx, `INSERT INTO account_usage (account_id, month, cell, seconds, running, reported_at)
		SELECT a, $2, $1, s, r, $6 FROM unnest($3::text[], $4::bigint[], $5::bigint[]) AS u(a, s, r)
		ON CONFLICT (account_id, month, cell) DO UPDATE SET seconds = EXCLUDED.seconds, running = EXCLUDED.running,
			reported_at = EXCLUDED.reported_at`, cell, month, accounts, seconds, running, at)
	return err
}

func (x queries) UsageElsewhere(ctx context.Context, accounts []string, cell, month string, since time.Time) (map[string]store.Usage, error) {
	rows, err := x.q.Query(ctx, `SELECT account_id, sum(seconds), sum(CASE WHEN reported_at >= $4 THEN running ELSE 0 END)
		FROM account_usage WHERE account_id = ANY($1) AND month = $3 AND cell <> $2 GROUP BY account_id`, accounts, cell, month, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]store.Usage{}
	for rows.Next() {
		var u store.Usage
		if err := rows.Scan(&u.Account, &u.Seconds, &u.Running); err != nil {
			return nil, err
		}
		out[u.Account] = u
	}
	return out, rows.Err()
}
