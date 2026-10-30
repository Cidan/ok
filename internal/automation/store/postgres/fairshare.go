package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/automation/store"
)

// Dispatch and admission (docs/hosted.md, "Hosted automation, as built"): a run's account is its
// repository's owner (repository_owner), "" for one the directory has not named.

// claimable are the queued runs of a pool ($1) of repositories no move holds, each with its account:
// the pool's demand.
const claimable = `FROM run r LEFT JOIN repository_owner o ON o.repository_id = r.repository_id
	WHERE r.status = 1 AND r.pool = $1
		AND NOT EXISTS (SELECT 1 FROM move_gate g WHERE g.repository_id = r.repository_id AND g.move_state <> '')`

func (x queries) RunningByAccount(ctx context.Context, pool string) (map[string]store.Running, error) {
	rows, err := x.q.Query(ctx, `SELECT coalesce(o.account_id, ''), count(*) FILTER (WHERE r.pool = $1), count(*)
		FROM run r LEFT JOIN repository_owner o ON o.repository_id = r.repository_id WHERE r.status = 2 GROUP BY 1`, pool)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]store.Running{}
	for rows.Next() {
		var a string
		var r store.Running
		if err := rows.Scan(&a, &r.InPool, &r.Total); err != nil {
			return nil, err
		}
		out[a] = r
	}
	return out, rows.Err()
}

func (x queries) MonthSeconds(ctx context.Context, month string, accounts []string) (map[string]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT account_id, seconds FROM account_usage WHERE month = $1 AND account_id = ANY($2)`, month, accounts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var a string
		var s int64
		if err := rows.Scan(&a, &s); err != nil {
			return nil, err
		}
		out[a] = s
	}
	return out, rows.Err()
}

func (x queries) ClaimRun(ctx context.Context, id int64, job, pool, worker string, ttl time.Duration, account string, slot int64, keyHash, handle []byte) (*store.Run, error) {
	var slotAccount *string
	var slotN *int64
	if slot >= 0 {
		slotAccount, slotN = &account, &slot
	}
	r, err := scanRun(x.q.QueryRow(ctx, `UPDATE run
		SET status = 2, worker = $3, lease_expires = now() + make_interval(secs => $4), started_at = now(), slot_account = $5, slot = $6,
			lease_key_hash = $7, lease_handle = $8
		WHERE id = $1 AND status = 1 AND pool = $2 AND dispatch_job = $9
		RETURNING `+runColumns, id, pool, worker, ttl.Seconds(), slotAccount, slotN, keyHash, handle, job))
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	return r, err
}

func (x queries) LeaseKeyHolds(ctx context.Context, id int64, keyHash []byte) (bool, error) {
	var held bool
	err := x.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM run WHERE id = $1 AND status = 2 AND lease_key_hash = $2)`, id, keyHash).Scan(&held)
	return held, err
}

func (x queries) Slots(ctx context.Context, account string) ([]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT slot FROM account_slot WHERE account_id = $1`, account)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// TakeSlot inserts the slot's row: a slot another run holds — committed, or once its transaction
// commits — is the insert's conflict.
func (x queries) TakeSlot(ctx context.Context, account string, slot, run int64) error {
	tag, err := x.q.Exec(ctx, `INSERT INTO account_slot (account_id, slot, run_id, repository_id)
		SELECT $1, $2, id, repository_id FROM run WHERE id = $3 ON CONFLICT (account_id, slot) DO NOTHING`, account, slot, run)
	if err == nil && tag.RowsAffected() == 0 {
		return store.ErrSlotTaken
	}
	return err
}

func (x queries) SetOwner(ctx context.Context, repo int64, account string) error {
	_, err := x.q.Exec(ctx, `INSERT INTO repository_owner (repository_id, account_id) VALUES ($1, $2)
		ON CONFLICT (repository_id) DO UPDATE SET account_id = $2`, repo, account)
	return err
}

func (x queries) DropOwner(ctx context.Context, repo int64) error {
	_, err := x.q.Exec(ctx, `DELETE FROM repository_owner WHERE repository_id = $1`, repo)
	return err
}

func (x queries) MonthUsage(ctx context.Context, month string) ([]store.Usage, error) {
	rows, err := x.q.Query(ctx, `WITH running AS (
			SELECT coalesce(o.account_id, '') AS account_id, count(*) AS n
			FROM run r LEFT JOIN repository_owner o ON o.repository_id = r.repository_id WHERE r.status = 2 GROUP BY 1
		)
		SELECT coalesce(u.account_id, running.account_id), coalesce(u.seconds, 0), coalesce(running.n, 0)
		FROM (SELECT account_id, seconds FROM account_usage WHERE month = $1) u FULL JOIN running ON running.account_id = u.account_id
		ORDER BY 1`, month)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Usage, error) {
		var u store.Usage
		err := row.Scan(&u.Account, &u.Seconds, &u.Running)
		return u, err
	})
}

func (x queries) OwnerOf(ctx context.Context, repo int64) (string, error) {
	var a string
	err := x.q.QueryRow(ctx, `SELECT account_id FROM repository_owner WHERE repository_id = $1`, repo).Scan(&a)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return a, err
}

func (x queries) QueuedByAccount(ctx context.Context, pool string) (map[string]int64, time.Time, error) {
	rows, err := x.q.Query(ctx, `SELECT coalesce(o.account_id, ''), count(*), min(r.queued_at) `+claimable+` GROUP BY 1`, pool)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer rows.Close()
	out := map[string]int64{}
	var oldest time.Time
	for rows.Next() {
		var a string
		var n int64
		var at time.Time
		if err := rows.Scan(&a, &n, &at); err != nil {
			return nil, time.Time{}, err
		}
		out[a] = n
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}
	return out, oldest, rows.Err()
}

func (x queries) RunningOnWorkers(ctx context.Context, pool string) (map[string]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT coalesce(worker, ''), count(*) FROM run WHERE status = 2 AND pool = $1 GROUP BY 1`, pool)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var w string
		var n int64
		if err := rows.Scan(&w, &n); err != nil {
			return nil, err
		}
		out[w] = n
	}
	return out, rows.Err()
}

func (x queries) SetDispatchJobs(ctx context.Context, jobs map[int64]string) error {
	if len(jobs) == 0 {
		return nil
	}
	ids, names := make([]int64, 0, len(jobs)), make([]string, 0, len(jobs))
	for id, job := range jobs {
		ids, names = append(ids, id), append(names, job)
	}
	_, err := x.q.Exec(ctx, `UPDATE run SET dispatch_job = j.job FROM unnest($1::bigint[], $2::text[]) AS j(id, job)
		WHERE run.id = j.id AND run.status = 1`, ids, names)
	return err
}

func (x queries) QueuedRuns(ctx context.Context, repos []int64) ([]store.Queued, error) {
	if len(repos) == 0 {
		return nil, nil
	}
	rows, err := x.q.Query(ctx, `SELECT id, repository_id, pool FROM run WHERE repository_id = ANY($1) AND status = 1`, repos)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[store.Queued])
}

func (x queries) UndispatchedRuns(ctx context.Context) ([]store.Queued, error) {
	rows, err := x.q.Query(ctx, `SELECT id, repository_id, pool FROM run WHERE status = 1 AND dispatch_job IS NULL`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[store.Queued])
}
