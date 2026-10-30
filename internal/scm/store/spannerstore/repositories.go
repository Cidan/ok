package spannerstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/data/spannertxn"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Repositories ───

const repositoryColumns = `repository_id, network_id, IFNULL(parent_id, 0), IFNULL(parent_seq, 0), head, head_seq, created_at, created_by`

func scanRepository(row *spanner.Row) (backend.Repository, error) {
	var r backend.Repository
	var net int64
	var head []byte
	if err := row.Columns(&r.ID, &net, &r.Parent, &r.ParentSeq, &head, &r.HeadSeq, &r.CreatedAt, &r.CreatedBy); err != nil {
		return r, err
	}
	r.Network = backend.Network(net)
	var err error
	r.Head, err = toID(head)
	return r, err
}

// repository reads a repository in r; ok is false when there is none.
func repository(ctx context.Context, r spannerdata.Reader, id int64, lock string) (backend.Repository, bool, error) {
	var out backend.Repository
	found := false
	err := spannerdata.Each(ctx, r, `SELECT `+repositoryColumns+` FROM repository WHERE repository_id = @id`+lock, map[string]any{"id": id},
		func(row *spanner.Row) error {
			var err error
			out, err = scanRepository(row)
			found = true
			return err
		})
	return out, found, err
}

func (x queries) InsertRepository(ctx context.Context, id int64, net backend.Network, head encoding.ID, createdBy string) (backend.Repository, error) {
	var r backend.Repository
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"id": id, "net": int64(net), "head": head[:], "by": createdBy}
		if _, err := spannerdata.Update(ctx, tx, `INSERT OR IGNORE INTO network (network_id, created_at, trainer_id)
			VALUES (@net, `+now+`, @id)`, p); err != nil {
			return err
		}
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO repository (repository_id, network_id, head, head_seq, created_at, created_by,
				op_content_pinned)
			VALUES (@id, @net, @head, 0, `+now+`, @by, TRUE)`, p); err != nil {
			return err
		}
		var err error
		r, _, err = repository(ctx, tx, id, "")
		return err
	})
	if unique(err) {
		return r, errors.Join(backend.ErrDuplicate, err)
	}
	return r, err
}

func (x queries) InsertFork(ctx context.Context, id int64, parent backend.Repository, createdBy string) (backend.Repository, error) {
	var r backend.Repository
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO repository (repository_id, network_id, parent_id, parent_seq, head, head_seq,
				created_at, created_by, op_content_pinned)
			VALUES (@id, @net, @parent, @seq, @head, @seq, `+now+`, @by, TRUE)`,
			map[string]any{"id": id, "net": int64(parent.Network), "parent": parent.ID, "seq": parent.HeadSeq, "head": parent.Head[:],
				"by": createdBy}); err != nil {
			return err
		}
		var err error
		r, _, err = repository(ctx, tx, id, "")
		return err
	})
	if unique(err) {
		return r, errors.Join(backend.ErrDuplicate, err)
	}
	return r, err
}

func (x queries) SetParent(ctx context.Context, id, parent int64) error {
	_, err := x.exec(ctx, `UPDATE repository SET parent_id = @parent WHERE repository_id = @id`, map[string]any{"id": id, "parent": parent})
	return err
}

func (x queries) SetParentSeq(ctx context.Context, id, seq int64) error {
	_, err := x.exec(ctx, `UPDATE repository SET parent_seq = @seq WHERE repository_id = @id`, map[string]any{"id": id, "seq": seq})
	return err
}

// repositoryTail are a repository's rows that go with it, in the order they go — what PostgreSQL's
// foreign keys delete with the repository's row, and the rows it deletes itself — each by a
// condition on the repository (@id). A commit's parent edges, a CL's patchsets, an operation log's
// rows, and a workspace's checkpoints go before them, a page at a time like the rest: deleted with
// them, they would count in their statements.
var repositoryTail = []tableRows{
	{"compaction_cursor", "repository_id = @id"},
	{"sequence_pass", "repository_id = @id"},
	{"tree_sizes", "repository_id = @id"},
	{"copy_parent", "repository_id = @id"},
	{"sequence_pending", "repository_id = @id"},
	{"event_pending", "repository_id = @id"},
	{"event", "repository_id = @id"},
	{"candidate", "repository_id = @id"},
	{"patchset", "repository_id = @id"},
	{"changelist", "repository_id = @id"},
	{"changelist_counter", "repository_id = @id"},
	{"head_log", "repository_id = @id"},
	{"commit_path", "repository_id = @id"},
	{"path_index", "repository_id = @id"},
	{"commit_parent", "repository_id = @id"},
	{"commit", "repository_id = @id"},
	{"object", "repository_id = @id"},
	{"operation", "op_log_id IN (SELECT op_log_id FROM op_log WHERE repository_id = @id)"},
	{"view", "op_log_id IN (SELECT op_log_id FROM op_log WHERE repository_id = @id)"},
	{"op_head", "op_log_id IN (SELECT op_log_id FROM op_log WHERE repository_id = @id)"},
	{"op_tombstone", "op_log_id IN (SELECT op_log_id FROM op_log WHERE repository_id = @id)"},
	{"op_log", "repository_id = @id"},
	{"repository_event_counter", "repository_id = @id"},
	{"gc_pass", "repository_id = @id"},
	{"checkpoint", "repository_id = @id"},
	{"workspace", "repository_id = @id"},
	{"dictionary_grant", "repository_id = @id"},
}

func (x queries) DeleteRepository(ctx context.Context, id int64) (backend.Network, bool, error) {
	var net backend.Network
	var found bool
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var n int64
		var err error
		found, err = spannerdata.First(ctx, tx, `SELECT network_id FROM repository WHERE repository_id = @id FOR UPDATE`, map[string]any{"id": id}, &n)
		if err != nil || !found {
			return err
		}
		net = backend.Network(n)
		p := map[string]any{"id": id, "net": n}
		for _, sql := range []string{
			`INSERT OR IGNORE INTO repository_deletion (repository_id, network_id, deleted_at) VALUES (@id, @net, ` + now + `)`,
			// A fork keeps the place it follows up to: its inherited segments are bounded there, and a
			// new upstream (Store.Sync) is taken at it.
			`UPDATE repository SET parent_id = NULL WHERE parent_id = @id`,
			`UPDATE network SET trainer_id = (SELECT r.repository_id FROM repository r WHERE r.network_id = @net AND r.repository_id != @id
				ORDER BY r.created_at, r.repository_id LIMIT 1) WHERE network_id = @net AND trainer_id = @id`,
		} {
			if _, err := spannerdata.Update(ctx, tx, sql, p); err != nil {
				return err
			}
		}
		for _, t := range append(repositoryTail, tableRows{"work", "key = @id"}) {
			if _, err := deleteAll(ctx, tx, t.table, t.cond, p); err != nil {
				return err
			}
		}
		_, err = spannerdata.Update(ctx, tx, `DELETE FROM repository WHERE repository_id = @id`, p)
		return err
	})
	return net, found && err == nil, err
}

// DeleteRepositoryTail deletes the repository's tail but its keyed work, whose lease the deletion
// may be fenced by: that goes with its row.
func (x queries) DeleteRepositoryTail(ctx context.Context, id int64, limit int) (int64, error) {
	var n int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = deleteTail(ctx, tx, repositoryTail, map[string]any{"id": id}, limit)
		return err
	})
	return n, err
}

func (x queries) DeleteContentIndexTail(ctx context.Context, sc backend.Scope, limit int) (int64, error) {
	var n int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = deleteTail(ctx, tx, []tableRows{{"content_log", `scope = @scope AND owner_id = @owner`},
			{"content_log_batch", `scope = @scope AND owner_id = @owner`}},
			scopeParams(sc), limit)
		return err
	})
	return n, err
}

// unfinished are a network's deleted repositories whose deletions are not finished.
const unfinished = `(SELECT repository_id FROM repository_deletion WHERE network_id = @net)`

// networkTail are a network's rows DeleteNetworkTail deletes, by @net.
var networkTail = []tableRows{
	{"content_log", `scope = 0 AND owner_id IN ` + unfinished},
	{"content_log_batch", `scope = 0 AND owner_id IN ` + unfinished},
	{"segment", `network_id = @net`},
	{"content_log", `scope = 1 AND owner_id = @net`},
	{"content_log_batch", `scope = 1 AND owner_id = @net`},
	{"pack", `network_id = @net`},
	{"chunk_release", `network_id = @net`},
	{"dictionary", `network_id = @net`},
}

func (x queries) DeleteNetworkTail(ctx context.Context, net backend.Network, limit int) (int64, error) {
	var n int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		n, err = deleteTail(ctx, tx, networkTail, map[string]any{"net": int64(net)}, limit)
		return err
	})
	return n, err
}

func (x queries) PendingDeletion(ctx context.Context, repo int64) (backend.Deletion, bool, error) {
	var d backend.Deletion
	var ok bool
	err := x.rows(ctx, `SELECT repository_id, network_id FROM repository_deletion WHERE repository_id = @repo`,
		map[string]any{"repo": repo}, func() { ok = false }, func(row *spanner.Row) error {
			var net int64
			if err := row.Columns(&d.Repo, &net); err != nil {
				return err
			}
			d.Net, ok = backend.Network(net), true
			return nil
		})
	return d, ok, err
}

func (x queries) RepositoryIDs(ctx context.Context, after int64, limit int) ([]int64, error) {
	return x.int64s(ctx, `SELECT repository_id FROM repository WHERE repository_id > @after ORDER BY repository_id LIMIT @limit`,
		map[string]any{"after": after, "limit": int64(limit)})
}

func (x queries) RetiredOf(ctx context.Context, net backend.Network) (packs, segments time.Time, err error) {
	err = x.rows(ctx, `SELECT (SELECT MIN(retired_at) FROM pack WHERE network_id = @net AND retired_at IS NOT NULL),
		(SELECT MIN(retired_at) FROM segment WHERE network_id = @net AND retired_at IS NOT NULL)`,
		map[string]any{"net": int64(net)}, func() { packs, segments = time.Time{}, time.Time{} }, func(row *spanner.Row) error {
			var p, s spanner.NullTime
			if err := row.Columns(&p, &s); err != nil {
				return err
			}
			packs, segments = p.Time, s.Time
			return nil
		})
	return packs, segments, err
}

func (x queries) FollowCursors(ctx context.Context, topic string, keys []int64) (map[int64]int64, error) {
	var out map[int64]int64
	err := x.read(func(r spannerdata.Reader) error {
		var err error
		out, err = spannerdata.Cursors(ctx, r, topic, keys)
		return err
	})
	return out, err
}

func (x queries) AddFollower(ctx context.Context, topic, name string) error {
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.AddFollower(ctx, tx, topic, name) })
}

func (x queries) Followers(ctx context.Context, topic string) ([]string, error) {
	var out []string
	err := x.read(func(r spannerdata.Reader) error {
		var err error
		out, err = spannerdata.Followers(ctx, r, topic)
		return err
	})
	return out, err
}

func (x queries) FollowCursor(ctx context.Context, topic string, key int64) (int64, error) {
	var seq int64
	err := x.read(func(r spannerdata.Reader) error {
		var err error
		seq, err = spannerdata.Cursor(ctx, r, topic, key)
		return err
	})
	return seq, err
}

func (x queries) AdvanceFollowCursor(ctx context.Context, topic string, key, seq int64) error {
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return spannerdata.AdvanceCursor(ctx, tx, topic, key, seq)
	})
}

func (x queries) FinishDeletion(ctx context.Context, id int64) error {
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"id": id, "counter": contentLogCounter(0)}
		if _, err := spannerdata.Update(ctx, tx, `UPDATE segment SET retired_at = `+now+` WHERE scope = 0 AND owner_id = @id AND retired_at IS NULL`, p); err != nil {
			return err
		}
		for _, t := range []string{"content_log", "content_log_batch"} {
			if _, err := deleteAll(ctx, tx, t, `scope = 0 AND owner_id = @id`, p); err != nil {
				return err
			}
		}
		for _, sql := range []string{
			`DELETE FROM content_index WHERE scope = 0 AND owner_id = @id`,
			`DELETE FROM counter WHERE name = @counter AND scope_id = @id`,
			`DELETE FROM repository_deletion WHERE repository_id = @id`,
		} {
			if _, err := spannerdata.Update(ctx, tx, sql, p); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteNetwork deletes a network's rows, a page at a time; its packs, dictionaries, chunk
// releases, and deletion records go before its row, as its row's foreign keys and interleaving
// would delete them with it.
func (x queries) DeleteNetwork(ctx context.Context, net backend.Network) error {
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"net": int64(net), "repoLogs": contentLogCounter(0), "netLogs": contentLogCounter(1), "packs": packCounter,
			"ages": ageCounter, "dictionaries": dictionaryCounter}
		// The content indexes of its repositories whose deletions are not finished go too.
		for _, t := range append(networkTail, []tableRows{
			{"content_index", `scope = 0 AND owner_id IN ` + unfinished},
			{"counter", `name = @repoLogs AND scope_id IN ` + unfinished},
			{"content_index", `scope = 1 AND owner_id = @net`},
			{"counter", `(name = @netLogs OR name = @packs OR name = @ages OR name = @dictionaries) AND scope_id = @net`},
			{"repository_deletion", `network_id = @net`},
		}...) {
			if _, err := deleteAll(ctx, tx, t.table, t.cond, p); err != nil {
				return err
			}
		}
		// Its keyed work: its chunk index's folds'.
		if _, err := deleteAll(ctx, tx, "work", `key = @net`, p); err != nil {
			return err
		}
		_, err := spannerdata.Update(ctx, tx, `DELETE FROM network WHERE network_id = @net`, p)
		return err
	})
}

func (x queries) NetworkRepositories(ctx context.Context, net backend.Network) ([]int64, error) {
	return x.int64s(ctx, `SELECT repository_id FROM repository WHERE network_id = @net ORDER BY repository_id`, map[string]any{"net": int64(net)})
}

func (x queries) ReleaseChunks(ctx context.Context, net backend.Network, chunks []encoding.ID) error {
	if len(chunks) == 0 {
		return nil
	}
	_, err := x.execIn(ctx, `INSERT OR IGNORE INTO chunk_release (network_id, chunk_id, released_at)
		SELECT @net, c, `+now+` FROM UNNEST(@ids) AS c`, map[string]any{"net": int64(net), "ids": ids(distinct(chunks))}, []string{"ids"}, nil)
	return err
}

func (x queries) ReleasedChunks(ctx context.Context, net backend.Network, limit int) ([]encoding.ID, error) {
	return x.idList(ctx, `SELECT chunk_id FROM chunk_release WHERE network_id = @net ORDER BY chunk_id LIMIT @limit`,
		map[string]any{"net": int64(net), "limit": int64(limit)})
}

func (x queries) DeleteReleases(ctx context.Context, net backend.Network, chunks []encoding.ID) error {
	_, err := x.execIn(ctx, `DELETE FROM chunk_release WHERE network_id = @net AND chunk_id IN UNNEST(@ids)`,
		map[string]any{"net": int64(net), "ids": ids(chunks)}, []string{"ids"}, nil)
	return err
}

func (x queries) LockRepository(ctx context.Context, id int64) (bool, error) {
	var got int64
	return x.lockOne(ctx, `SELECT repository_id FROM repository WHERE repository_id = @id FOR UPDATE`, map[string]any{"id": id}, &got)
}

func (x queries) Repository(ctx context.Context, id int64) (backend.Repository, bool, error) {
	var r backend.Repository
	var ok bool
	err := x.read(func(rd spannerdata.Reader) error {
		var err error
		r, ok, err = repository(ctx, rd, id, "")
		return err
	})
	return r, ok, err
}

func (x queries) RepositoryExists(ctx context.Context, id int64) (bool, error) {
	var got int64
	return x.one(ctx, `SELECT repository_id FROM repository WHERE repository_id = @id`, map[string]any{"id": id}, &got)
}

func (x queries) Head(ctx context.Context, id int64, lock bool) (encoding.ID, int64, bool, error) {
	var head []byte
	var seq int64
	sql, p := `SELECT head, head_seq FROM repository WHERE repository_id = @id`, map[string]any{"id": id}
	var ok bool
	var err error
	if lock {
		ok, err = x.lockOne(ctx, sql+` FOR UPDATE`, p, &head, &seq)
	} else {
		ok, err = x.one(ctx, sql, p, &head, &seq)
	}
	if err != nil || !ok {
		return encoding.ID{}, 0, false, err
	}
	h, err := toID(head)
	return h, seq, err == nil, err
}

func (x queries) SetHead(ctx context.Context, id int64, head encoding.ID, seq int64) error {
	_, err := x.exec(ctx, `UPDATE repository SET head = @head, head_seq = @seq WHERE repository_id = @id`,
		map[string]any{"id": id, "head": head[:], "seq": seq})
	return err
}

// ─── Events ───

// bump adds n to a repository's counter in table, made at n if it has none, in tx, and returns
// it: the counter's row is held until the transaction ends.
// bump moves a repository's counter in table n on, returning its last: one statement, but for the
// counter's first, which makes its row.
func bump(ctx context.Context, tx *spanner.ReadWriteTransaction, table string, repo, n int64) (int64, error) {
	var last int64
	p := map[string]any{"repo": repo, "n": n}
	found := false
	spannertxn.Counted(tx)
	if err := tx.Query(ctx, spanner.Statement{SQL: `UPDATE ` + table + ` SET last = last + @n WHERE repository_id = @repo THEN RETURN last`,
		Params: p}).Do(func(row *spanner.Row) error {
		found = true
		return row.Columns(&last)
	}); err != nil {
		return 0, err
	}
	if found {
		return last, nil
	}
	_, err := spannerdata.Update(ctx, tx, `INSERT INTO `+table+` (repository_id, last) VALUES (@repo, @n)`, p)
	return n, err
}

func (x queries) ReserveRepositoryEvents(ctx context.Context, repo int64, n int) (int64, error) {
	var last int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		last, err = bump(ctx, tx, "repository_event_counter", repo, int64(n))
		return err
	})
	return last, err
}

// InsertEvents writes the events and their pending marks as mutations: their places, from the
// counter the transaction holds, are theirs alone.
func (x queries) InsertEvents(ctx context.Context, repo int64, evs []backend.LogEvent) error {
	if len(evs) == 0 {
		return nil
	}
	muts := make([]*spanner.Mutation, 0, 2*len(evs))
	for _, e := range evs {
		txn := spanner.NullInt64{Int64: e.Txn, Valid: e.Txn != 0}
		muts = append(muts,
			spanner.Insert("event", []string{"repository_id", "repository_seq", "event_at", "txn", "body"},
				[]any{repo, e.Seq, micros(e.At), txn, e.Body}),
			spanner.Insert("event_pending", []string{"repository_id", "repository_seq"}, []any{repo, e.Seq}))
	}
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error { return tx.BufferWrite(muts) })
}

func (x queries) PendStep(_ context.Context, kind string, key int64, at time.Time, body []byte) error {
	if x.tx == nil {
		return errors.New("spannerstore: a step is pended in a transaction, whose commit it follows")
	}
	return spannerdata.PendStep(x.tx.tx, kind, key, at, body)
}

func (x queries) events(ctx context.Context, sql string, params map[string]any) ([]backend.Event, error) {
	var out []backend.Event
	err := x.rows(ctx, sql, params, func() { out = nil }, func(row *spanner.Row) error {
		var e backend.Event
		if err := row.Columns(&e.Repository, &e.RepositorySeq, &e.Body); err != nil {
			return err
		}
		out = append(out, e)
		return nil
	})
	return out, err
}

func (x queries) LogHeads(ctx context.Context, after int64, limit int) ([][2]int64, error) {
	var out [][2]int64
	err := x.rows(ctx, `SELECT repository_id, last FROM repository_event_counter WHERE repository_id > @after
		ORDER BY repository_id LIMIT @limit`, map[string]any{"after": after, "limit": int64(limit)}, func() { out = nil },
		func(row *spanner.Row) error {
			var h [2]int64
			if err := row.Columns(&h[0], &h[1]); err != nil {
				return err
			}
			out = append(out, h)
			return nil
		})
	return out, err
}

func (x queries) RepositoryEvents(ctx context.Context, repo, after int64, limit int) ([]backend.Event, error) {
	return x.events(ctx, `SELECT repository_id, repository_seq, body FROM event
		WHERE repository_id = @repo AND repository_seq > @after ORDER BY repository_seq LIMIT @limit`,
		map[string]any{"repo": repo, "after": after, "limit": int64(limit)})
}

// ─── The content index ───

// scopeOf is a scope's columns: 0 for a repository's, 1 for a network's, and the owner's ID.
func scopeOf(sc backend.Scope) (int64, int64) {
	if sc.Network {
		return 1, sc.ID
	}
	return 0, sc.ID
}

func scopeParams(sc backend.Scope) map[string]any {
	scope, owner := scopeOf(sc)
	return map[string]any{"scope": scope, "owner": owner}
}

func (x queries) InsertContentIndex(ctx context.Context, sc backend.Scope, gen int64) error {
	p := scopeParams(sc)
	p["gen"] = gen
	_, err := x.exec(ctx, `INSERT INTO content_index (scope, owner_id, generation) VALUES (@scope, @owner, @gen)`, p)
	return err
}

// ShareContentIndex reads the content index's generation in the operation's transaction, which
// holds that cell in share mode until it ends, as every read in a Spanner read-write transaction
// does: writers that read it run beside each other, and one that locks it (LockContentIndex, which
// reads it FOR UPDATE) waits for them. It must be that cell: Spanner locks cells, and a read of the
// row that read no cell a lock takes held nothing — a fold committed beside the transactions that
// relied on the index.
func (x queries) ShareContentIndex(ctx context.Context, sc backend.Scope) (time.Time, int64, bool, error) {
	var today civil.Date
	var gen int64
	ok, err := x.one(ctx, `SELECT CURRENT_DATE('UTC'), generation FROM content_index WHERE scope = @scope AND owner_id = @owner`,
		scopeParams(sc), &today, &gen)
	if err != nil || !ok {
		return time.Time{}, 0, false, err
	}
	return day(today), gen, true, nil
}

func (x queries) LockContentIndex(ctx context.Context, sc backend.Scope) (int64, time.Time, error) {
	var gen int64
	var today civil.Date
	ok, err := x.lockOne(ctx, `SELECT generation, CURRENT_DATE('UTC') FROM content_index WHERE scope = @scope AND owner_id = @owner FOR UPDATE`,
		scopeParams(sc), &gen, &today)
	if err == nil && !ok {
		err = fmt.Errorf("%s has no content index: %w", sc.Name(), errNoRow)
	}
	return gen, day(today), err
}

// logPlace is a read timestamp as a place in a content index's log.
func logPlace(t time.Time) backend.LogPlace {
	b, _ := t.MarshalBinary()
	return b
}

// placeTime is the read timestamp a place is; ok is false for none.
func placeTime(p backend.LogPlace) (t time.Time, ok bool, err error) {
	if p == nil {
		return t, false, nil
	}
	if err := t.UnmarshalBinary(p); err != nil {
		return t, false, fmt.Errorf("a content log place: %w", err)
	}
	return t, true, nil
}

// ContentIndexGeneration and ContentLogSince read unlocked.
func (x queries) ContentIndexGeneration(ctx context.Context, sc backend.Scope) (backend.IndexState, bool, error) {
	var st backend.IndexState
	var folded spanner.NullTime
	var swept spanner.NullInt64
	var ok bool
	err := x.unlocked(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT generation, folded_through, swept_before FROM content_index
			WHERE scope = @scope AND owner_id = @owner`, scopeParams(sc), &st.Generation, &folded, &swept)
		return err
	})
	if folded.Valid {
		st.Folded = logPlace(folded.Time)
	}
	st.SweptBefore = swept.Int64
	return st, ok, err
}

func (x queries) SetContentIndexGeneration(ctx context.Context, sc backend.Scope, gen int64, folded backend.LogPlace) error {
	t, ok, err := placeTime(folded)
	if err != nil {
		return err
	}
	p := scopeParams(sc)
	p["gen"], p["folded"] = gen, spanner.NullTime{Time: t, Valid: ok}
	_, err = x.exec(ctx, `UPDATE content_index SET generation = @gen, folded_through = @folded WHERE scope = @scope AND owner_id = @owner`, p)
	return err
}

func (x queries) SetContentIndexSwept(ctx context.Context, sc backend.Scope, before int64) error {
	p := scopeParams(sc)
	p["before"] = before
	_, err := x.exec(ctx, `UPDATE content_index SET swept_before = @before WHERE scope = @scope AND owner_id = @owner`, p)
	return err
}

// ContentLogSince reads, unlocked — in the snapshot, or in a read-only transaction of its own,
// within a write transaction that holds the index, so no fold commits meanwhile — the rows
// committed after since — a row its transaction wrote by its own commit timestamp, a staged one by
// its batch marker's — in commit order, a transaction's rows in theirs, and returns the read's
// timestamp as the place: everything committed by then it saw, and anything that commits later
// commits after it. A row from before commit timestamps (logged_at NULL) is read only from the
// start.
func (x queries) ContentLogSince(ctx context.Context, sc backend.Scope, since backend.LogPlace) ([]backend.ContentLog, backend.LogPlace, error) {
	var logs []backend.ContentLog
	var place backend.LogPlace
	err := x.unlocked(func(r spannerdata.Reader) error {
		var err error
		logs, place, err = contentLogIn(ctx, r, sc, since)
		return err
	})
	return logs, place, err
}

// ContentLogCurrent is ContentLogSince, the day and the generation read in its read-only
// transaction: one timestamp, and no lock of the read-write transaction it is in.
func (x queries) ContentLogCurrent(ctx context.Context, sc backend.Scope, gen int64, since backend.LogPlace) (time.Time, bool, []backend.ContentLog, backend.LogPlace, error) {
	var today civil.Date
	var current bool
	var logs []backend.ContentLog
	var place backend.LogPlace
	err := x.unlocked(func(r spannerdata.Reader) error {
		var have int64
		ok, err := spannerdata.First(ctx, r, `SELECT CURRENT_DATE('UTC'), generation FROM content_index WHERE scope = @scope AND owner_id = @owner`,
			scopeParams(sc), &today, &have)
		if err != nil || !ok || have != gen {
			return err
		}
		current = true
		logs, place, err = contentLogIn(ctx, r, sc, since)
		return err
	})
	if err != nil || !current {
		return day(today), false, nil, nil, err
	}
	return day(today), true, logs, place, nil
}

// contentLogIn is ContentLogSince's read, in r: a read-only transaction, whose timestamp is the
// place.
func contentLogIn(ctx context.Context, r spannerdata.Reader, sc backend.Scope, since backend.LogPlace) ([]backend.ContentLog, backend.LogPlace, error) {
	after, have, err := placeTime(since)
	if err != nil {
		return nil, nil, err
	}
	type logRow struct {
		at   time.Time
		id   int64
		body []byte
	}
	var rows []logRow
	p := scopeParams(sc)
	direct := `SELECT logged_at, id, body FROM content_log WHERE scope = @scope AND owner_id = @owner AND batch_id IS NULL`
	staged := `SELECT b.logged_at, l.id, l.body FROM content_log_batch b
		JOIN content_log@{FORCE_INDEX=content_log_batched} l ON l.scope = b.scope AND l.owner_id = b.owner_id AND l.batch_id = b.batch_id
		WHERE b.scope = @scope AND b.owner_id = @owner`
	if have {
		// A seek per shard, from after: `shard IN UNNEST(@shards)` read every shard's rows from the
		// index's start — 18,856 rows scanned for 100 returned, on Omni, against 200 this way.
		p["after"], p["shards"] = after, logShardList
		direct = `SELECT l.logged_at, l.id, l.body FROM UNNEST(@shards) AS s
			JOIN@{JOIN_METHOD=APPLY_JOIN} content_log@{FORCE_INDEX=content_log_logged} l
				ON l.scope = @scope AND l.owner_id = @owner AND l.shard = s AND l.logged_at > @after
			WHERE l.batch_id IS NULL`
		staged = `SELECT b.logged_at, l.id, l.body FROM content_log_batch@{FORCE_INDEX=content_log_batch_logged} b
			JOIN content_log@{FORCE_INDEX=content_log_batched} l ON l.scope = b.scope AND l.owner_id = b.owner_id AND l.batch_id = b.batch_id
			WHERE b.scope = @scope AND b.owner_id = @owner AND b.logged_at > @after`
	}
	// One statement, one round trip: every replica reads this each time a write stales its view.
	if err := spannerdata.Each(ctx, r, direct+" UNION ALL "+staged, p, func(row *spanner.Row) error {
		var at spanner.NullTime
		var l logRow
		if err := row.Columns(&at, &l.id, &l.body); err != nil {
			return err
		}
		l.at = at.Time
		rows = append(rows, l)
		return nil
	}); err != nil {
		return nil, nil, err
	}
	read, err := r.(*spanner.ReadOnlyTransaction).Timestamp()
	if err != nil {
		return nil, nil, err
	}
	slices.SortFunc(rows, func(a, b logRow) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return cmp.Compare(a.id, b.id)
	})
	out := make([]backend.ContentLog, len(rows))
	for i, r := range rows {
		out[i] = backend.ContentLog{Body: r.body}
	}
	return out, logPlace(read), nil
}

const insertContentLog = `INSERT INTO content_log (scope, owner_id, id, created_at, body, batch_id, logged_at, shard)
	VALUES (@scope, @owner, @id, ` + now + `, @body, @batch, PENDING_COMMIT_TIMESTAMP(), @shard)`

// logShards is how many shards the content log's commit-timestamp index has (migration 0010): a
// writer's rows take one at random, so writers hold that many pending-timestamp keys between them.
const logShards = 16

// logShardList is every shard, for a reader of the log.
var logShardList = func() []int64 {
	out := make([]int64, logShards)
	for i := range out {
		out[i] = int64(i)
	}
	return out
}()

// logID numbers a transaction's next content log row: from a random base, in order, so its rows
// apply in theirs; a write outside a transaction takes a number of its own.
func (x queries) logID() int64 {
	if x.tx == nil {
		return data.NewID()
	}
	x.tx.mu.Lock()
	defer x.tx.mu.Unlock()
	if x.tx.logID == 0 {
		x.tx.logID = data.NewID() >> 1 // room above for the transaction's rows
	}
	x.tx.logID++
	return x.tx.logID
}

// InsertContentLog records a log row, committed at its transaction's commit timestamp. A
// transaction writes the first stageBytes of its log itself, then stages each further row in a
// transaction of its own, under a batch whose marker it writes itself (content_log_batch): a staged
// row reads only once the transaction commits, as of the marker's commit, as one it wrote would.
// Nothing makes a staged row of a transaction that does not commit read; DeleteFoldedContentLogs
// deletes it. Nothing reads content_log in a transaction after it logs: Spanner refuses to read a
// table a transaction wrote a pending commit timestamp to.
func (x queries) InsertContentLog(ctx context.Context, sc backend.Scope, body []byte) error {
	scope, owner := scopeOf(sc)
	p := map[string]any{"scope": scope, "owner": owner, "id": x.logID(), "body": body, "batch": spanner.NullInt64{},
		"shard": rand.Int64N(logShards)}
	if x.tx == nil {
		_, err := x.exec(ctx, insertContentLog, p)
		return err
	}
	x.tx.mu.Lock()
	stage := x.tx.logBytes+len(body) > x.stageBytes
	if !stage {
		x.tx.logBytes += len(body)
	} else if x.tx.batch == 0 {
		x.tx.batch, x.tx.staged = data.NewID(), map[backend.Scope]bool{}
	}
	batch, marked := x.tx.batch, x.tx.staged[sc]
	x.tx.mu.Unlock()
	if !stage {
		_, err := x.exec(ctx, insertContentLog, p)
		return err
	}
	if !marked {
		if _, err := x.exec(ctx, `INSERT INTO content_log_batch (scope, owner_id, batch_id, logged_at)
			VALUES (@scope, @owner, @batch, PENDING_COMMIT_TIMESTAMP())`, map[string]any{"scope": scope, "owner": owner, "batch": batch}); err != nil {
			return err
		}
		x.tx.mu.Lock()
		x.tx.staged[sc] = true
		x.tx.mu.Unlock()
	}
	p["batch"] = spanner.NullInt64{Int64: batch, Valid: true}
	_, err := spannertxn.ReadWrite(ctx, x.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := spannerdata.Update(ctx, tx, insertContentLog, p)
		return err
	})
	return err
}

// stagedOrphanAge is how long before a fold's place a staged row with no batch marker is taken for
// a transaction that never committed: one that stages its log runs far shorter.
var stagedOrphanAge = time.Hour

// foldedDeletes bounds the log rows one transaction deletes: each is a mutation of the row and one
// of each of its two indexes, of the 80,000 Spanner takes in one commit.
const foldedDeletes = 15_000

// DeleteFoldedContentLogs deletes, up to limit, the rows committed by folded — which the fold that
// read to it holds in its generation — the staged ones of batches whose markers committed by then
// and the markers left with no rows, and the staged rows of batches that never committed. It finds
// them in a read-only transaction and deletes them by key: a folded row changes no more, and only
// the fold that read it, which holds the index's fold claim, deletes it. A delete that read the
// log's ranges in a read-write transaction held them, and every writer appending a row to them —
// at its pending commit timestamp, which the read's range takes in — waited for it: a fold every
// few seconds, at a monorepo's rate, held its writers back that long. A marker goes with the last
// of its rows: the read counts each marker's rows, and the deletes, buffered in the transaction, are
// seen by no read before it commits.
func (x queries) DeleteFoldedContentLogs(ctx context.Context, sc backend.Scope, folded backend.LogPlace, limit int) (int64, error) {
	t, ok, err := placeTime(folded)
	if err != nil || !ok {
		return 0, err
	}
	scope, owner := scopeOf(sc)
	p := scopeParams(sc)
	p["t"], p["orphan"], p["shards"] = t, t.Add(-stagedOrphanAge), logShardList
	limit = min(limit, foldedDeletes)
	var rows, markers []int64
	err = x.unlocked(func(r spannerdata.Reader) error {
		rows, markers = nil, nil
		taken := map[int64]int64{} // a marker's rows this round takes
		for _, q := range []string{
			// Logged directly: a seek per shard of the commit-timestamp index.
			`SELECT l.id, l.batch_id FROM UNNEST(@shards) AS s
				JOIN@{JOIN_METHOD=APPLY_JOIN} content_log@{FORCE_INDEX=content_log_logged} l
					ON l.scope = @scope AND l.owner_id = @owner AND l.shard = s AND l.logged_at <= @t
				WHERE l.batch_id IS NULL LIMIT @page`,
			// From before commit timestamps: a seek per shard, the index holding them first.
			`SELECT l.id, l.batch_id FROM UNNEST(@shards) AS s
				JOIN@{JOIN_METHOD=APPLY_JOIN} content_log@{FORCE_INDEX=content_log_logged} l
					ON l.scope = @scope AND l.owner_id = @owner AND l.shard = s AND l.logged_at IS NULL
				LIMIT @page`,
			// Staged, under markers committed by then.
			`SELECT l.id, l.batch_id FROM content_log_batch@{FORCE_INDEX=content_log_batch_logged} b
				JOIN content_log@{FORCE_INDEX=content_log_batched} l ON l.scope = b.scope AND l.owner_id = b.owner_id AND l.batch_id = b.batch_id
				WHERE b.scope = @scope AND b.owner_id = @owner AND b.logged_at <= @t LIMIT @page`,
			// Staged by transactions that never committed: no marker, and older than any that stages.
			`SELECT l.id, CAST(NULL AS INT64) FROM content_log@{FORCE_INDEX=content_log_batched} l
				WHERE l.scope = @scope AND l.owner_id = @owner AND l.batch_id IS NOT NULL AND l.logged_at <= @orphan
				AND NOT EXISTS (SELECT 1 FROM content_log_batch b WHERE b.scope = l.scope AND b.owner_id = l.owner_id AND b.batch_id = l.batch_id)
				LIMIT @page`,
		} {
			if len(rows) >= limit {
				break
			}
			p["page"] = int64(limit - len(rows))
			if err := spannerdata.Each(ctx, r, q, p, func(row *spanner.Row) error {
				var id int64
				var batch spanner.NullInt64
				if err := row.Columns(&id, &batch); err != nil {
					return err
				}
				rows = append(rows, id)
				if batch.Valid {
					taken[batch.Int64]++
				}
				return nil
			}); err != nil {
				return err
			}
		}
		// Markers by then whose every row this round takes, or none is left.
		p["page"] = int64(limit)
		return spannerdata.Each(ctx, r, `SELECT b.batch_id, (SELECT COUNT(*) FROM content_log@{FORCE_INDEX=content_log_batched} l
				WHERE l.scope = b.scope AND l.owner_id = b.owner_id AND l.batch_id = b.batch_id)
			FROM content_log_batch@{FORCE_INDEX=content_log_batch_logged} b
			WHERE b.scope = @scope AND b.owner_id = @owner AND (b.logged_at <= @t OR b.logged_at IS NULL)
			LIMIT @page`, p, func(row *spanner.Row) error {
			var id, n int64
			if err := row.Columns(&id, &n); err != nil {
				return err
			}
			if n == taken[id] {
				markers = append(markers, id)
			}
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	if err := x.deleteKeys(ctx, "content_log", scope, owner, rows); err != nil {
		return 0, err
	}
	if err := x.deleteKeys(ctx, "content_log_batch", scope, owner, markers); err != nil {
		return 0, err
	}
	return int64(len(rows) + len(markers)), nil
}

// deleteKeys deletes a content index's rows of table by their last key column, ids, in the
// operation's transaction: by key, reading nothing, so it holds no range a writer appends to.
func (x queries) deleteKeys(ctx context.Context, table string, scope, owner int64, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	muts := make([]*spanner.Mutation, len(ids))
	for i, id := range ids {
		muts[i] = spanner.Delete(table, spanner.Key{scope, owner, id})
	}
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error { return tx.BufferWrite(muts) })
}

// ─── Packs ───

// PutPack records a pack under the network's counter's next value, or updates its size if it is
// recorded.
func (x queries) PutPack(ctx context.Context, net backend.Network, id encoding.ID, size int64) (uint64, error) {
	var num int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"net": int64(net), "id": id[:], "size": size}
		ok, err := spannerdata.First(ctx, tx, `SELECT num FROM pack WHERE network_id = @net AND id = @id`, p, &num)
		if err != nil {
			return err
		}
		if ok {
			_, err := spannerdata.Update(ctx, tx, `UPDATE pack SET size = @size WHERE network_id = @net AND id = @id`, p)
			return err
		}
		if num, err = x.packNum(ctx, net); err != nil {
			return err
		}
		p["num"] = num
		_, err = spannerdata.Update(ctx, tx, `INSERT INTO pack (network_id, id, size, created_at, num)
			VALUES (@net, @id, @size, `+now+`, @num)`, p)
		return x.numberTaken(net, err)
	})
	return uint64(num), err
}

func (x queries) InsertPack(ctx context.Context, net backend.Network, id encoding.ID, size int64, dictionary *int64) (uint64, error) {
	num, err := x.packNum(ctx, net)
	if err != nil {
		return 0, err
	}
	dict := spanner.NullInt64{}
	if dictionary != nil {
		dict = spanner.NullInt64{Int64: *dictionary, Valid: true}
	}
	_, err = x.exec(ctx, `INSERT INTO pack (network_id, id, size, created_at, dictionary_id, num)
		VALUES (@net, @id, @size, `+now+`, @dict, @num)`,
		map[string]any{"net": int64(net), "id": id[:], "size": size, "dict": dict, "num": num})
	return uint64(num), x.numberTaken(net, err)
}

// packBlock is how many pack numbers a replica reserves of a network's counter at once: the
// counter is written once a block, not once a pack — every publish writes a pack.
const packBlock = 1024

// numberBlocks are the blocks of pack numbers a replica reserved, by network.
type numberBlocks struct {
	mu sync.Mutex
	// next and end bound each network's block: next is the number it gives next, end past its last.
	next, end map[backend.Network]int64
	// reserving is a network's reservation under way: those that find its block spent meanwhile
	// wait for it, rather than each reserving a block of its own — every publish of that moment
	// contending on the one counter row, nearly all aborted and tried again.
	reserving map[backend.Network]*reservation
}

// reservation is a block of pack numbers being reserved: done is closed once it is, or failed.
type reservation struct {
	done chan struct{}
	err  error
}

// packNum is a network's next pack number: from this replica's block, reserving another when it
// is spent. Numbers are given in no order across replicas, and some are never given; each is
// given once, and a pack's row is refused one another pack holds (pack_num), as a block reserved
// before the network moved away and back would give.
func (x queries) packNum(ctx context.Context, net backend.Network) (int64, error) {
	b := x.packNums
	for {
		b.mu.Lock()
		if b.next[net] < b.end[net] {
			n := b.next[net]
			b.next[net]++
			b.mu.Unlock()
			return n, nil
		}
		if r := b.reserving[net]; r != nil {
			b.mu.Unlock()
			select {
			case <-r.done:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			continue // its block, or — had it failed — a reservation of this one's own
		}
		if b.next == nil {
			b.next, b.end, b.reserving = map[backend.Network]int64{}, map[backend.Network]int64{}, map[backend.Network]*reservation{}
		}
		r := &reservation{done: make(chan struct{})}
		b.reserving[net] = r
		b.mu.Unlock()
		last, err := x.next(ctx, packCounter, int64(net), packBlock)
		b.mu.Lock()
		delete(b.reserving, net)
		if err == nil {
			b.next[net], b.end[net] = last-packBlock+1, last+1
		}
		r.err = err
		close(r.done)
		b.mu.Unlock()
		if err != nil {
			return 0, err
		}
	}
}

// errPackNumberTaken is a pack row refused for a number another pack holds: the transaction runs
// again (Retryable), its next number from a block reserved anew.
var errPackNumberTaken = errors.New("spannerstore: the pack number is another pack's")

// numberTaken is a pack row's insert's error: one refused for a number another pack holds drops
// the network's block — reserved before the network moved away and back — so the transaction run
// again reserves the next, past what the import brought.
func (x queries) numberTaken(net backend.Network, err error) error {
	if spanner.ErrCode(err) != codes.AlreadyExists {
		return err
	}
	b := x.packNums
	b.mu.Lock()
	delete(b.next, net)
	delete(b.end, net)
	b.mu.Unlock()
	return fmt.Errorf("%w: %w", errPackNumberTaken, err)
}

func (x queries) NumberedPacks(ctx context.Context, net backend.Network, nums []uint64) (map[uint64]encoding.ID, error) {
	out := map[uint64]encoding.ID{}
	n := make([]int64, len(nums))
	for i, v := range nums {
		n[i] = int64(v)
	}
	err := x.unlocked(func(r spannerdata.Reader) error {
		clear(out)
		return spannerdata.Each(ctx, r, `SELECT num, id FROM pack@{FORCE_INDEX=pack_num} WHERE network_id = @net AND num IN UNNEST(@nums)`,
			map[string]any{"net": int64(net), "nums": n}, func(row *spanner.Row) error {
				var num int64
				var b []byte
				if err := row.Columns(&num, &b); err != nil {
					return err
				}
				id, err := toID(b)
				if err != nil {
					return err
				}
				out[uint64(num)] = id
				return nil
			})
	})
	return out, err
}

func (x queries) LivePacks(ctx context.Context, net backend.Network) ([]backend.Pack, error) {
	var out []backend.Pack
	err := x.rows(ctx, `SELECT id, num, size, dictionary_id FROM pack WHERE network_id = @net AND retired_at IS NULL ORDER BY created_at, id`,
		map[string]any{"net": int64(net)}, func() { out = nil }, func(row *spanner.Row) error {
			var b []byte
			var num int64
			var dict spanner.NullInt64
			var p backend.Pack
			if err := row.Columns(&b, &num, &p.Size, &dict); err != nil {
				return err
			}
			var err error
			if p.ID, err = toID(b); err != nil {
				return err
			}
			p.Num = uint64(num)
			if dict.Valid {
				d := dict.Int64
				p.Dictionary = &d
			}
			out = append(out, p)
			return nil
		})
	return out, err
}

func (x queries) RetiredPackIDs(ctx context.Context, net backend.Network) ([]encoding.ID, error) {
	return x.idList(ctx, `SELECT id FROM pack WHERE network_id = @net AND retired_at IS NOT NULL`, map[string]any{"net": int64(net)})
}

func (x queries) PackIDs(ctx context.Context, net backend.Network) ([]encoding.ID, error) {
	return x.idList(ctx, `SELECT id FROM pack WHERE network_id = @net`, map[string]any{"net": int64(net)})
}

func (x queries) SetPacksDictionary(ctx context.Context, net backend.Network, packIDs []encoding.ID, dictionary int64) error {
	_, err := x.execIn(ctx, `UPDATE pack SET dictionary_id = @dict WHERE network_id = @net AND id IN UNNEST(@ids)`,
		map[string]any{"net": int64(net), "ids": ids(packIDs), "dict": dictionary}, []string{"ids"}, nil)
	return err
}

func (x queries) RetirePack(ctx context.Context, net backend.Network, id encoding.ID) error {
	_, err := x.exec(ctx, `UPDATE pack SET retired_at = `+now+` WHERE network_id = @net AND id = @id`,
		map[string]any{"net": int64(net), "id": id[:]})
	return err
}

func (x queries) LivePackNumbers(ctx context.Context, net backend.Network, packIDs []encoding.ID) (map[encoding.ID]uint64, error) {
	out := map[encoding.ID]uint64{}
	err := x.rows(ctx, `SELECT id, num FROM pack WHERE network_id = @net AND id IN UNNEST(@ids) AND retired_at IS NULL`,
		map[string]any{"net": int64(net), "ids": ids(packIDs)}, func() { clear(out) }, func(row *spanner.Row) error {
			var b []byte
			var num int64
			if err := row.Columns(&b, &num); err != nil {
				return err
			}
			id, err := toID(b)
			if err != nil {
				return err
			}
			out[id] = uint64(num)
			return nil
		}, "ids")
	return out, err
}

func (x queries) RetirePacks(ctx context.Context, net backend.Network, packIDs []encoding.ID, dry bool) (int, int64, error) {
	p := map[string]any{"net": int64(net), "ids": ids(packIDs)}
	if dry {
		var n, bytes int64
		err := x.rows(ctx, `SELECT COUNT(*), IFNULL(SUM(size), 0) FROM pack WHERE network_id = @net AND id IN UNNEST(@ids) AND retired_at IS NULL`,
			p, func() { n, bytes = 0, 0 }, func(row *spanner.Row) error {
				var c, b int64
				err := row.Columns(&c, &b)
				n, bytes = n+c, bytes+b
				return err
			}, "ids")
		return int(n), bytes, err
	}
	var n int
	var bytes int64
	err := x.returning(ctx, `UPDATE pack SET retired_at = `+now+`
		WHERE network_id = @net AND id IN UNNEST(@ids) AND retired_at IS NULL THEN RETURN size`, p, func() { n, bytes = 0, 0 },
		func(row *spanner.Row) error {
			var size int64
			if err := row.Columns(&size); err != nil {
				return err
			}
			n, bytes = n+1, bytes+size
			return nil
		}, nil, "ids")
	return n, bytes, err
}

// optionalNetwork is a network as a parameter, NULL for every one.
func optionalNetwork(net *backend.Network) spanner.NullInt64 {
	if net == nil {
		return spanner.NullInt64{}
	}
	return spanner.NullInt64{Int64: int64(*net), Valid: true}
}

func (x queries) RetiredPacksBefore(ctx context.Context, t time.Time, net *backend.Network) ([]backend.RetiredPack, error) {
	var out []backend.RetiredPack
	err := x.rows(ctx, `SELECT network_id, id, num FROM pack WHERE retired_at < @t AND (@net IS NULL OR network_id = @net)
		ORDER BY network_id, id`, map[string]any{"t": t, "net": optionalNetwork(net)}, func() { out = nil }, func(row *spanner.Row) error {
		var p backend.RetiredPack
		var n, num int64
		var b []byte
		if err := row.Columns(&n, &b, &num); err != nil {
			return err
		}
		var err error
		if p.ID, err = toID(b); err != nil {
			return err
		}
		p.Net, p.Num = backend.Network(n), uint64(num)
		out = append(out, p)
		return nil
	})
	return out, err
}

func (x queries) DeleteRetiredPack(ctx context.Context, net backend.Network, id encoding.ID) (bool, error) {
	n, err := x.exec(ctx, `DELETE FROM pack WHERE network_id = @net AND id = @id AND retired_at IS NOT NULL`,
		map[string]any{"net": int64(net), "id": id[:]})
	return n > 0, err
}
