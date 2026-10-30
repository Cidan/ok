package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Sequencing ───

// SequencesAfterCommit: a repository's CL numbers and places in its log are given by its
// sequencing pass, after the transaction that makes them commits (docs/scm.md, "Sequencing").
func (b *Backend) SequencesAfterCommit() bool { return true }

// MutationBudget is Spanner's 80,000 mutations a commit, less a quarter for what a writer's count
// of its own rows misses.
func (b *Backend) MutationBudget() int { return 60_000 }

func (x queries) SequencedAt(ctx context.Context, repo int64) (time.Time, error) {
	var at time.Time
	_, err := x.one(ctx, `SELECT began_at FROM sequence_pass WHERE repository_id = @repo`, map[string]any{"repo": repo}, &at)
	return at, err
}

func (x queries) CompactionCursor(ctx context.Context, repo int64) ([]byte, error) {
	var c []byte
	_, err := x.one(ctx, `SELECT cursor FROM compaction_cursor WHERE repository_id = @repo`, map[string]any{"repo": repo}, &c)
	return c, err
}

func (x queries) SetCompactionCursor(ctx context.Context, repo int64, cursor []byte) error {
	_, err := x.exec(ctx, `INSERT OR UPDATE INTO compaction_cursor (repository_id, cursor) VALUES (@repo, @cursor)`,
		map[string]any{"repo": repo, "cursor": cursor})
	return err
}

func (x queries) SetSequencedAt(ctx context.Context, repo int64, began time.Time) error {
	_, err := x.exec(ctx, `INSERT OR UPDATE INTO sequence_pass (repository_id, began_at)
		SELECT @repo, GREATEST(@began, IFNULL((SELECT began_at FROM sequence_pass WHERE repository_id = @repo), @began))`,
		map[string]any{"repo": repo, "began": began})
	return err
}

// pendingKey is a pending row's key past its repository, as a STRUCT parameter.
type pendingKey struct {
	C time.Time `spanner:"c"`
	T int64     `spanner:"t"`
	P int64     `spanner:"p"`
}

// keyTime is a pending row's commit timestamp as its key holds it: in UTC, with no monotonic
// reading, so the same row read twice gives equal keys.
func keyTime(t time.Time) time.Time { return t.UTC().Round(0) }

func pendingKeys(keys []backend.PendingKey) []pendingKey {
	out := make([]pendingKey, len(keys))
	for i, k := range keys {
		out[i] = pendingKey{k.CommittedAt, k.Txn, k.Place}
	}
	return out
}

// Pend writes the rows as mutations, keyed by the commit timestamp Spanner gives them as the
// transaction commits.
func (x queries) Pend(ctx context.Context, rows []backend.Pending) error {
	if len(rows) == 0 {
		return nil
	}
	muts := make([]*spanner.Mutation, len(rows))
	for i, r := range rows {
		cols := map[string]any{"repository_id": r.Repository, "committed_at": spanner.CommitTimestamp, "txn": r.Key.Txn,
			"place": r.Key.Place}
		if r.ChangeList != 0 {
			cols["changelist_id"] = r.ChangeList
		} else {
			cols["event_at"], cols["body"] = micros(r.At), r.Body
		}
		muts[i] = spanner.InsertMap("sequence_pending", cols)
	}
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error { return tx.BufferWrite(muts) })
}

// PendingRows reads unlocked, as of now: a strong read sees every row committed before it, and a
// row committed after carries a later timestamp than any it returns.
func (x queries) PendingRows(ctx context.Context, repo int64, limit int) ([]backend.Pending, error) {
	var out []backend.Pending
	err := x.unlocked(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT committed_at, txn, place, changelist_id, event_at, body FROM sequence_pending
			WHERE repository_id = @repo ORDER BY committed_at, txn, place LIMIT @limit`,
			map[string]any{"repo": repo, "limit": int64(limit)}, func(row *spanner.Row) error {
				p := backend.Pending{Repository: repo}
				var cl spanner.NullInt64
				var at spanner.NullTime
				if err := row.Columns(&p.Key.CommittedAt, &p.Key.Txn, &p.Key.Place, &cl, &at, &p.Body); err != nil {
					return err
				}
				p.Key.CommittedAt, p.ChangeList, p.At = keyTime(p.Key.CommittedAt), cl.Int64, at.Time
				out = append(out, p)
				return nil
			})
	})
	return out, err
}

func (x queries) PendingHeld(ctx context.Context, repo int64, keys []backend.PendingKey) (map[backend.PendingKey]bool, error) {
	out := map[backend.PendingKey]bool{}
	if len(keys) == 0 {
		return out, nil
	}
	err := x.rows(ctx, `SELECT committed_at, txn, place FROM sequence_pending WHERE repository_id = @repo
		AND STRUCT<c TIMESTAMP, t INT64, p INT64>(committed_at, txn, place) IN UNNEST(@keys)`,
		map[string]any{"repo": repo, "keys": pendingKeys(keys)}, func() { clear(out) }, func(row *spanner.Row) error {
			var k backend.PendingKey
			if err := row.Columns(&k.CommittedAt, &k.Txn, &k.Place); err != nil {
				return err
			}
			k.CommittedAt = keyTime(k.CommittedAt)
			out[k] = true
			return nil
		}, "keys")
	return out, err
}

func (x queries) DeletePending(ctx context.Context, repo int64, keys []backend.PendingKey) error {
	if len(keys) == 0 {
		return nil
	}
	ks := make([]spanner.KeySet, len(keys))
	for i, k := range keys {
		ks[i] = spanner.Key{repo, k.CommittedAt, k.Txn, k.Place}
	}
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.Delete("sequence_pending", spanner.KeySets(ks...))})
	})
}

// NumberChangeLists numbers each CL from its provisional number, and moves what names it as a
// parent by that number — CLs and patchsets, found through their parent indexes, read in the pass's
// transaction and so held — to its number: every write by key, as mutations, so it scans and locks
// no other row.
func (x queries) NumberChangeLists(ctx context.Context, repo int64, ids, numbers []int64) error {
	given := make(map[int64]int64, len(ids))
	provisional := make([]int64, len(ids))
	for i, id := range ids {
		given[-id], provisional[i] = numbers[i], -id
	}
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var muts []*spanner.Mutation
		for i, id := range ids {
			muts = append(muts, spanner.Update("changelist", []string{"repository_id", "id", "number"}, []any{repo, id, numbers[i]}))
		}
		p := map[string]any{"repo": repo, "provisional": provisional}
		if err := spannerdata.Each(ctx, tx, `SELECT id, parent_number FROM changelist@{FORCE_INDEX=changelist_parent}
			WHERE repository_id = @repo AND parent_number IS NOT NULL AND parent_number IN UNNEST(@provisional)`, p, func(row *spanner.Row) error {
			var id, parent int64
			if err := row.Columns(&id, &parent); err != nil {
				return err
			}
			muts = append(muts, spanner.Update("changelist", []string{"repository_id", "id", "parent_number"}, []any{repo, id, given[parent]}))
			return nil
		}); err != nil {
			return err
		}
		if err := spannerdata.Each(ctx, tx, `SELECT cl_id, number, parent_number FROM patchset@{FORCE_INDEX=patchset_parent}
			WHERE repository_id = @repo AND parent_number IS NOT NULL AND parent_number IN UNNEST(@provisional)`, p, func(row *spanner.Row) error {
			var cl, number, parent int64
			if err := row.Columns(&cl, &number, &parent); err != nil {
				return err
			}
			muts = append(muts, spanner.Update("patchset", []string{"repository_id", "cl_id", "number", "parent_number"},
				[]any{repo, cl, number, given[parent]}))
			return nil
		}); err != nil {
			return err
		}
		return tx.BufferWrite(muts)
	})
}

func (x queries) ChangeListNumbers(ctx context.Context, repo int64, ids []int64) (map[int64]int64, error) {
	out := map[int64]int64{}
	if len(ids) == 0 {
		return out, nil
	}
	err := x.rows(ctx, `SELECT id, number FROM changelist WHERE repository_id = @repo AND id IN UNNEST(@ids)`,
		map[string]any{"repo": repo, "ids": ids}, func() { clear(out) }, func(row *spanner.Row) error {
			var id, n int64
			if err := row.Columns(&id, &n); err != nil {
				return err
			}
			out[id] = n
			return nil
		}, "ids")
	return out, err
}

func (x queries) TransactionEvents(ctx context.Context, repo, txn int64) ([]int64, error) {
	return x.int64s(ctx, `SELECT repository_seq FROM event@{FORCE_INDEX=event_txn} WHERE repository_id = @repo AND txn = @txn
		ORDER BY repository_seq`, map[string]any{"repo": repo, "txn": txn})
}

func (x queries) TxnPending(ctx context.Context, repo, txn int64) (bool, error) {
	var pending bool
	err := x.unlocked(func(r spannerdata.Reader) error {
		pending = false
		return spannerdata.Each(ctx, r, `SELECT 1 FROM sequence_pending WHERE repository_id = @repo AND txn = @txn LIMIT 1`,
			map[string]any{"repo": repo, "txn": txn}, func(*spanner.Row) error { pending = true; return nil })
	})
	return pending, err
}

func (x queries) PendingRepositories(ctx context.Context, before time.Time, limit int) ([]int64, error) {
	var out []int64
	err := x.unlocked(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT DISTINCT repository_id FROM sequence_pending WHERE committed_at < @before
			ORDER BY repository_id LIMIT @limit`, map[string]any{"before": before, "limit": int64(limit)}, func(row *spanner.Row) error {
			var id int64
			if err := row.Columns(&id); err != nil {
				return err
			}
			out = append(out, id)
			return nil
		})
	})
	return out, err
}
