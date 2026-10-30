package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/workflow"
)

// Dispatch and admission (docs/hosted.md, "Hosted automation, as built"): a run's account is its
// repository's owner (repository_owner), "" for one the directory has not named.

// poolOf is a pool's name as a run records it.
func poolOf(p string) string {
	if p == "" {
		return workflow.DefaultPool
	}
	return p
}

// claimable are the queued runs of a pool (@pool) of repositories no move holds, each with its
// account: the pool's demand.
const claimable = `FROM run r LEFT JOIN repository_owner o ON o.repository_id = r.repository_id
	WHERE r.status = 1 AND r.pool = @pool
		AND NOT EXISTS (SELECT 1 FROM move_gate g WHERE g.repository_id = r.repository_id AND g.move_state != '')`

func (x queries) RunningByAccount(ctx context.Context, pool string) (map[string]store.Running, error) {
	out := map[string]store.Running{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT IFNULL(o.account_id, ''), COUNTIF(r.pool = @pool), COUNT(*)
			FROM run r LEFT JOIN repository_owner o ON o.repository_id = r.repository_id WHERE r.status = 2
			GROUP BY IFNULL(o.account_id, '')`, map[string]any{"pool": pool}, func(row *spanner.Row) error {
			var a string
			var n store.Running
			if err := row.Columns(&a, &n.InPool, &n.Total); err != nil {
				return err
			}
			out[a] = n
			return nil
		})
	})
	return out, err
}

func (x queries) MonthSeconds(ctx context.Context, month string, accounts []string) (map[string]int64, error) {
	if accounts == nil {
		accounts = []string{}
	}
	out := map[string]int64{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT account_id, SUM(seconds) FROM account_seconds WHERE month = @month AND account_id IN UNNEST(@accounts)
			GROUP BY account_id`, map[string]any{"month": month, "accounts": accounts}, func(row *spanner.Row) error {
			var a string
			var s int64
			if err := row.Columns(&a, &s); err != nil {
				return err
			}
			out[a] = s
			return nil
		})
	})
	return out, err
}

func (x queries) ClaimRun(ctx context.Context, id int64, job, pool, worker string, ttl time.Duration, account string, slot int64, keyHash, handle []byte) (*store.Run, error) {
	var out *store.Run
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		out = nil
		slotAccount, slotN := spanner.NullString{}, spanner.NullInt64{}
		if slot >= 0 {
			slotAccount, slotN = spanner.NullString{StringVal: account, Valid: true}, spanner.NullInt64{Int64: slot, Valid: true}
		}
		n, err := spannerdata.Update(ctx, tx, `UPDATE run SET status = 2, worker = @worker,
				lease_expires = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl MICROSECOND), started_at = CURRENT_TIMESTAMP(),
				slot_account = @slot_account, slot = @slot, lease_key_hash = @key_hash, lease_handle = @handle
			WHERE run_id = @id AND status = 1 AND pool = @pool AND dispatch_job = @job`,
			map[string]any{"id": id, "job": job, "pool": pool, "worker": worker, "ttl": micros(ttl), "slot_account": slotAccount, "slot": slotN,
				"key_hash": keyHash, "handle": handle})
		if err != nil || n == 0 {
			return err
		}
		out, err = oneRun(runs(ctx, tx, `SELECT `+runColumns+` FROM run WHERE run_id = @id`, map[string]any{"id": id}))
		return err
	})
	return out, err
}

func (x queries) LeaseKeyHolds(ctx context.Context, id int64, keyHash []byte) (bool, error) {
	var held bool
	err := x.Read(func(r spannerdata.Reader) error {
		var n int64
		ok, err := spannerdata.First(ctx, r, `SELECT 1 FROM run WHERE run_id = @id AND status = 2 AND lease_key_hash = @key_hash`,
			map[string]any{"id": id, "key_hash": keyHash}, &n)
		held = ok
		return err
	})
	return held, err
}

func (x queries) Slots(ctx context.Context, account string) ([]int64, error) {
	var out []int64
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT slot FROM account_slot WHERE account_id = @account`, map[string]any{"account": account},
			func(row *spanner.Row) error {
				var n int64
				if err := row.Columns(&n); err != nil {
					return err
				}
				out = append(out, n)
				return nil
			})
	})
	return out, err
}

// TakeSlot inserts the slot's row: a slot another run holds, committed or not, is the insert's
// conflict — refused at once, or once the other commits, when Spanner runs this one again.
func (x queries) TakeSlot(ctx context.Context, account string, slot, run int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		_, err := spannerdata.Update(ctx, tx, `INSERT INTO account_slot (account_id, slot, run_id, repository_id)
			SELECT @account, @slot, run_id, repository_id FROM run WHERE run_id = @run`,
			map[string]any{"account": account, "slot": slot, "run": run})
		if spanner.ErrCode(err) == codes.AlreadyExists {
			return store.ErrSlotTaken
		}
		return err
	})
}

// usageShards are how many rows an account's month's seconds are kept in.
const usageShards = 16

// stopRunning charges a run that was running its seconds since it started, by Spanner's clock, to
// its account's month — the row of the account's its ID picks — and lets its slot go, in tx
// before the run changes.
func stopRunning(ctx context.Context, tx *spanner.ReadWriteTransaction, id int64) error {
	var account, month string
	var seconds int64
	var slotAccount spanner.NullString
	var slot spanner.NullInt64
	ok, err := spannerdata.First(ctx, tx, `SELECT IFNULL(o.account_id, ''), FORMAT_TIMESTAMP('%Y-%m', CURRENT_TIMESTAMP(), 'UTC'),
			GREATEST(0, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.started_at, SECOND)), r.slot_account, r.slot
		FROM run r LEFT JOIN repository_owner o ON o.repository_id = r.repository_id
		WHERE r.run_id = @id AND r.status = 2 AND r.started_at IS NOT NULL`, map[string]any{"id": id}, &account, &month, &seconds, &slotAccount, &slot)
	if err != nil || !ok {
		return err
	}
	var muts []*spanner.Mutation
	if slotAccount.Valid && slot.Valid {
		muts = append(muts, spanner.Delete("account_slot", spanner.Key{slotAccount.StringVal, slot.Int64}))
	}
	shard := ((id % usageShards) + usageShards) % usageShards
	var had int64
	row, err := tx.ReadRow(ctx, "account_seconds", spanner.Key{account, month, shard}, []string{"seconds"})
	switch {
	case spanner.ErrCode(err) == codes.NotFound:
	case err != nil:
		return err
	default:
		if err := row.Columns(&had); err != nil {
			return err
		}
	}
	// The read holds the row's key, present or not: a run ending beside this one in its shard
	// waits, and adds to what this one wrote.
	muts = append(muts, spanner.InsertOrUpdate("account_seconds", []string{"account_id", "month", "shard", "seconds"},
		[]any{account, month, shard, had + seconds}))
	return tx.BufferWrite(muts)
}

func (x queries) SetOwner(ctx context.Context, repo int64, account string) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdate("repository_owner", []string{"repository_id", "account_id"},
			[]any{repo, account})})
	})
}

func (x queries) DropOwner(ctx context.Context, repo int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.Delete("repository_owner", spanner.Key{repo})})
	})
}

func (x queries) MonthUsage(ctx context.Context, month string) ([]store.Usage, error) {
	var out []store.Usage
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `WITH running AS (
				SELECT IFNULL(o.account_id, '') AS account_id, COUNT(*) AS n
				FROM run r LEFT JOIN repository_owner o ON o.repository_id = r.repository_id WHERE r.status = 2
				GROUP BY IFNULL(o.account_id, '')
			), used AS (SELECT account_id, SUM(seconds) AS seconds FROM account_seconds WHERE month = @month GROUP BY account_id)
			SELECT IFNULL(used.account_id, running.account_id), IFNULL(used.seconds, 0), IFNULL(running.n, 0)
			FROM used FULL JOIN running ON running.account_id = used.account_id
			ORDER BY 1`, map[string]any{"month": month}, func(row *spanner.Row) error {
			var u store.Usage
			if err := row.Columns(&u.Account, &u.Seconds, &u.Running); err != nil {
				return err
			}
			out = append(out, u)
			return nil
		})
	})
	return out, err
}

func (x queries) OwnerOf(ctx context.Context, repo int64) (string, error) {
	var a string
	err := x.Read(func(r spannerdata.Reader) error {
		a = ""
		_, err := spannerdata.First(ctx, r, `SELECT account_id FROM repository_owner WHERE repository_id = @repo`, map[string]any{"repo": repo}, &a)
		return err
	})
	return a, err
}

func (x queries) QueuedByAccount(ctx context.Context, pool string) (map[string]int64, time.Time, error) {
	out := map[string]int64{}
	var oldest time.Time
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		oldest = time.Time{}
		return spannerdata.Each(ctx, r, `SELECT IFNULL(o.account_id, ''), COUNT(*), MIN(r.queued_at) `+claimable+`
			GROUP BY IFNULL(o.account_id, '')`, map[string]any{"pool": pool}, func(row *spanner.Row) error {
			var a string
			var n int64
			var at time.Time
			if err := row.Columns(&a, &n, &at); err != nil {
				return err
			}
			out[a] = n
			if oldest.IsZero() || at.Before(oldest) {
				oldest = at
			}
			return nil
		})
	})
	return out, oldest, err
}

func (x queries) RunningOnWorkers(ctx context.Context, pool string) (map[string]int64, error) {
	out := map[string]int64{}
	err := x.Read(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT IFNULL(worker, ''), COUNT(*) FROM run WHERE status = 2 AND pool = @pool GROUP BY IFNULL(worker, '')`,
			map[string]any{"pool": pool}, func(row *spanner.Row) error {
				var w string
				var n int64
				if err := row.Columns(&w, &n); err != nil {
					return err
				}
				out[w] = n
				return nil
			})
	})
	return out, err
}

func (x queries) SetDispatchJobs(ctx context.Context, jobs map[int64]string) error {
	if len(jobs) == 0 {
		return nil
	}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for id, job := range jobs {
			if _, err := spannerdata.Update(ctx, tx, `UPDATE run SET dispatch_job = @job WHERE run_id = @id AND status = @queued`,
				map[string]any{"id": id, "job": job, "queued": int64(store.StatusQueued)}); err != nil {
				return err
			}
		}
		return nil
	})
}

func (x queries) QueuedRuns(ctx context.Context, repos []int64) ([]store.Queued, error) {
	if len(repos) == 0 {
		return nil, nil
	}
	var out []store.Queued
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT run_id, repository_id, pool FROM run WHERE repository_id IN UNNEST(@repos) AND status = @queued`,
			map[string]any{"repos": repos, "queued": int64(store.StatusQueued)}, func(row *spanner.Row) error {
				var q store.Queued
				if err := row.Columns(&q.ID, &q.RepositoryID, &q.Pool); err != nil {
					return err
				}
				out = append(out, q)
				return nil
			})
	})
	return out, err
}

func (x queries) UndispatchedRuns(ctx context.Context) ([]store.Queued, error) {
	var out []store.Queued
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT run_id, repository_id, pool FROM run WHERE status = @queued AND dispatch_job IS NULL`,
			map[string]any{"queued": int64(store.StatusQueued)}, func(row *spanner.Row) error {
				var q store.Queued
				if err := row.Columns(&q.ID, &q.RepositoryID, &q.Pool); err != nil {
					return err
				}
				out = append(out, q)
				return nil
			})
	})
	return out, err
}
