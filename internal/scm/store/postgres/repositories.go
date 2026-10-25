package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Repositories ───

const repositoryColumns = `id, network_id, coalesce(parent_id, 0), coalesce(parent_seq, 0), head, head_seq, created_at, created_by`

func scanRepository(row pgx.Row) (backend.Repository, error) {
	var r backend.Repository
	var head []byte
	if err := row.Scan(&r.ID, &r.Network, &r.Parent, &r.ParentSeq, &head, &r.HeadSeq, &r.CreatedAt, &r.CreatedBy); err != nil {
		return r, err
	}
	var err error
	r.Head, err = toID(head)
	return r, err
}

func oneRepository(row pgx.Row) (backend.Repository, bool, error) {
	r, err := scanRepository(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return backend.Repository{}, false, nil
	}
	return r, err == nil, err
}

func (x queries) InsertRepository(ctx context.Context, id int64, net backend.Network, head encoding.ID, createdBy string) (backend.Repository, error) {
	if _, err := x.q.Exec(ctx, `INSERT INTO network (id, trainer_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, net, id); err != nil {
		return backend.Repository{}, err
	}
	r, err := scanRepository(x.q.QueryRow(ctx,
		`INSERT INTO repository (id, network_id, head, created_by, op_content_pinned) VALUES ($1, $2, $3, $4, true) RETURNING `+repositoryColumns,
		id, net, head[:], createdBy))
	if unique(err) {
		return r, errors.Join(backend.ErrDuplicate, err)
	}
	return r, err
}

func (x queries) InsertFork(ctx context.Context, id int64, parent backend.Repository, createdBy string) (backend.Repository, error) {
	r, err := scanRepository(x.q.QueryRow(ctx,
		`INSERT INTO repository (id, network_id, parent_id, parent_seq, head, head_seq, created_by, op_content_pinned)
		VALUES ($1, $2, $3, $5, $4, $5, $6, true) RETURNING `+repositoryColumns,
		id, parent.Network, parent.ID, parent.Head[:], parent.HeadSeq, createdBy))
	if unique(err) {
		return r, errors.Join(backend.ErrDuplicate, err)
	}
	return r, err
}

func (x queries) SetParent(ctx context.Context, id, parent int64) error {
	_, err := x.q.Exec(ctx, `UPDATE repository SET parent_id = $2 WHERE id = $1`, id, parent)
	return err
}

func (x queries) SetParentSeq(ctx context.Context, id, seq int64) error {
	_, err := x.q.Exec(ctx, `UPDATE repository SET parent_seq = $2 WHERE id = $1`, id, seq)
	return err
}

func (x queries) DeleteRepository(ctx context.Context, id int64) (backend.Network, bool, error) {
	var net backend.Network
	err := x.q.QueryRow(ctx, `SELECT network_id FROM repository WHERE id = $1 FOR UPDATE`, id).Scan(&net)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	for _, sql := range []string{
		`INSERT INTO repository_deletion (repository_id, network_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		// A fork keeps the place it follows up to: its inherited segments are bounded there, and a new
		// upstream (Store.Sync) is taken at it.
		`UPDATE repository SET parent_id = NULL WHERE parent_id = $1`,
		`UPDATE network SET trainer_id = (SELECT r.id FROM repository r WHERE r.network_id = $2 AND r.id <> $1
			ORDER BY r.created_at, r.id LIMIT 1) WHERE id = $2 AND trainer_id = $1`,
		// The tables whose rows do not go with the repository's.
		`DELETE FROM tree_sizes WHERE repository_id = $1`,
		`DELETE FROM copy_parent WHERE repository_id = $1`,
		`DELETE FROM event_pending WHERE repository_id = $1`,
		`DELETE FROM event WHERE repository_id = $1`,
		`DELETE FROM work WHERE key = $1`,
		`DELETE FROM repository WHERE id = $1`,
	} {
		args := []any{id}
		if strings.Contains(sql, "$2") {
			args = append(args, net)
		}
		if _, err := x.q.Exec(ctx, sql, args...); err != nil {
			return 0, false, err
		}
	}
	return net, true, nil
}

// tableRows are a table's rows where cond holds, its parameters $1 on.
type tableRows struct{ table, cond string }

// repositoryTail are what DeleteRepositoryTail deletes of a repository ($1), in the order it
// deletes them: children before their parents, which their foreign keys would delete with them.
var repositoryTail = []tableRows{
	{"tree_sizes", "repository_id = $1"},
	{"copy_parent", "repository_id = $1"},
	{"event_pending", "repository_id = $1"},
	{"event", "repository_id = $1"},
	{"candidate", "repository_id = $1"},
	{"patchset", "repository_id = $1"},
	{"changelist", "repository_id = $1"},
	{"changelist_counter", "repository_id = $1"},
	{"head_log", "repository_id = $1"},
	{"commit_path", "repository_id = $1"},
	{"path_index", "repository_id = $1"},
	{"commit_parent", "repository_id = $1"},
	{"commit", "repository_id = $1"},
	{"object", "repository_id = $1"},
	{"operation", "op_log_id IN (SELECT id FROM op_log WHERE repository_id = $1)"},
	{"view", "op_log_id IN (SELECT id FROM op_log WHERE repository_id = $1)"},
	{"op_head", "op_log_id IN (SELECT id FROM op_log WHERE repository_id = $1)"},
	{"op_tombstone", "op_log_id IN (SELECT id FROM op_log WHERE repository_id = $1)"},
	{"op_log", "repository_id = $1"},
	{"repository_event_counter", "repository_id = $1"},
	{"gc_pass", "repository_id = $1"},
	{"checkpoint", "repository_id = $1"},
	{"workspace", "repository_id = $1"},
	{"dictionary_grant", "repository_id = $1"},
}

// deleteTail deletes up to limit of the rows of tables where their conditions hold, in order, a
// table's at a time, and returns how many.
func (x queries) deleteTail(ctx context.Context, tables []tableRows, limit int, args ...any) (int64, error) {
	left := int64(limit)
	for _, t := range tables {
		if left == 0 {
			break
		}
		tag, err := x.q.Exec(ctx, fmt.Sprintf(`DELETE FROM %[1]s WHERE ctid = ANY(ARRAY(SELECT ctid FROM %[1]s WHERE %[2]s LIMIT $%[3]d))`,
			t.table, t.cond, len(args)+1), append(args, left)...)
		if err != nil {
			return int64(limit) - left, err
		}
		left -= tag.RowsAffected()
	}
	return int64(limit) - left, nil
}

// DeleteRepositoryTail deletes the repository's tail but its keyed work, whose lease the deletion
// may be fenced by: that goes with its row.
func (x queries) DeleteRepositoryTail(ctx context.Context, id int64, limit int) (int64, error) {
	return x.deleteTail(ctx, repositoryTail, limit, id)
}

func (x queries) DeleteContentIndexTail(ctx context.Context, sc backend.Scope, limit int) (int64, error) {
	scope, owner := scopeOf(sc)
	return x.deleteTail(ctx, []tableRows{{"content_log", "scope = $1 AND owner_id = $2"}}, limit, scope, owner)
}

func (x queries) DeleteNetworkTail(ctx context.Context, net backend.Network, limit int) (int64, error) {
	const unfinished = `(SELECT repository_id FROM repository_deletion WHERE network_id = $1)`
	return x.deleteTail(ctx, []tableRows{
		{"content_log", "scope = 0 AND owner_id IN " + unfinished},
		{"segment", "network_id = $1"},
		{"content_log", "scope = 1 AND owner_id = $1"},
		{"pack", "network_id = $1"},
		{"chunk_release", "network_id = $1"},
		{"dictionary", "network_id = $1"},
	}, limit, net)
}

func (x queries) PendingDeletions(ctx context.Context, limit int) ([]backend.Deletion, error) {
	rows, err := x.q.Query(ctx, `SELECT repository_id, network_id FROM repository_deletion ORDER BY deleted_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Deletion
	for rows.Next() {
		var d backend.Deletion
		if err := rows.Scan(&d.Repo, &d.Net); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (x queries) FinishDeletion(ctx context.Context, id int64) error {
	for _, sql := range []string{
		`UPDATE segment SET retired_at = now() WHERE scope = 0 AND owner_id = $1 AND retired_at IS NULL`,
		`DELETE FROM content_log WHERE scope = 0 AND owner_id = $1`,
		`DELETE FROM content_index WHERE scope = 0 AND owner_id = $1`,
		`DELETE FROM repository_deletion WHERE repository_id = $1`,
	} {
		if _, err := x.q.Exec(ctx, sql, id); err != nil {
			return err
		}
	}
	return nil
}

func (x queries) DeleteNetwork(ctx context.Context, net backend.Network) error {
	for _, sql := range []string{
		// The content indexes of its repositories whose deletions are not finished.
		`DELETE FROM content_log WHERE scope = 0 AND owner_id IN (SELECT repository_id FROM repository_deletion WHERE network_id = $1)`,
		`DELETE FROM content_index WHERE scope = 0 AND owner_id IN (SELECT repository_id FROM repository_deletion WHERE network_id = $1)`,
		`DELETE FROM segment WHERE network_id = $1`,
		`DELETE FROM content_log WHERE scope = 1 AND owner_id = $1`,
		`DELETE FROM content_index WHERE scope = 1 AND owner_id = $1`,
		// Its keyed work: its chunk index's folds'.
		`DELETE FROM work WHERE key = $1`,
		`DELETE FROM network WHERE id = $1`,
	} {
		if _, err := x.q.Exec(ctx, sql, net); err != nil {
			return err
		}
	}
	return nil
}

func (x queries) NetworkRepositories(ctx context.Context, net backend.Network) ([]int64, error) {
	return int64s(x.q.Query(ctx, `SELECT id FROM repository WHERE network_id = $1 ORDER BY id`, net))
}

func (x queries) ReleaseChunks(ctx context.Context, net backend.Network, chunks []encoding.ID) error {
	_, err := x.q.Exec(ctx, `INSERT INTO chunk_release (network_id, chunk_id) SELECT $1, u FROM unnest($2::bytea[]) AS u
		ON CONFLICT DO NOTHING`, net, ids(chunks))
	return err
}

func (x queries) ReleasedChunks(ctx context.Context, net backend.Network, limit int) ([]encoding.ID, error) {
	rows, err := x.q.Query(ctx, `SELECT chunk_id FROM chunk_release WHERE network_id = $1 ORDER BY chunk_id LIMIT $2`, net, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []encoding.ID
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			return nil, err
		}
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (x queries) DeleteReleases(ctx context.Context, net backend.Network, chunks []encoding.ID) error {
	_, err := x.q.Exec(ctx, `DELETE FROM chunk_release WHERE network_id = $1 AND chunk_id = ANY($2)`, net, ids(chunks))
	return err
}

func (x queries) LockRepository(ctx context.Context, id int64) (bool, error) {
	var got int64
	err := x.q.QueryRow(ctx, `SELECT id FROM repository WHERE id = $1 FOR UPDATE`, id).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (x queries) Repository(ctx context.Context, id int64) (backend.Repository, bool, error) {
	return oneRepository(x.q.QueryRow(ctx, `SELECT `+repositoryColumns+` FROM repository WHERE id = $1`, id))
}

func (x queries) RepositoryExists(ctx context.Context, id int64) (bool, error) {
	var ok bool
	err := x.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM repository WHERE id = $1)`, id).Scan(&ok)
	return ok, err
}

func (x queries) Head(ctx context.Context, id int64, lock bool) (encoding.ID, int64, bool, error) {
	sql := `SELECT head, head_seq FROM repository WHERE id = $1`
	if lock {
		sql += ` FOR UPDATE`
	}
	var head []byte
	var seq int64
	err := x.q.QueryRow(ctx, sql, id).Scan(&head, &seq)
	if errors.Is(err, pgx.ErrNoRows) {
		return encoding.ID{}, 0, false, nil
	}
	if err != nil {
		return encoding.ID{}, 0, false, err
	}
	h, err := toID(head)
	return h, seq, err == nil, err
}

func (x queries) SetHead(ctx context.Context, id int64, head encoding.ID, seq int64) error {
	_, err := x.q.Exec(ctx, `UPDATE repository SET head = $2, head_seq = $3 WHERE id = $1`, id, head[:], seq)
	return err
}

// ─── Events ───

func (x queries) ReserveRepositoryEvents(ctx context.Context, repo int64, n int) (int64, error) {
	var last int64
	err := x.q.QueryRow(ctx, `INSERT INTO repository_event_counter (repository_id, last) VALUES ($1, $2)
		ON CONFLICT (repository_id) DO UPDATE SET last = repository_event_counter.last + EXCLUDED.last RETURNING last`,
		repo, n).Scan(&last)
	return last, err
}

func (x queries) InsertEvents(ctx context.Context, repo int64, evs []backend.LogEvent) error {
	seqs, ats, bodies := make([]int64, len(evs)), make([]time.Time, len(evs)), make([][]byte, len(evs))
	for i, e := range evs {
		seqs[i], ats[i], bodies[i] = e.Seq, e.At, e.Body
	}
	_, err := x.q.Exec(ctx, `WITH pending AS (
			INSERT INTO event_pending (repository_id, repository_seq) SELECT $1, s FROM unnest($2::bigint[]) AS s)
		INSERT INTO event (repository_id, repository_seq, at, body)
		SELECT $1, u.seq, u.at, u.body FROM unnest($2::bigint[], $3::timestamptz[], $4::bytea[]) AS u(seq, at, body)`,
		repo, seqs, ats, bodies)
	return err
}

func events(rows pgx.Rows, err error) ([]backend.Event, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (backend.Event, error) {
		var e backend.Event
		err := r.Scan(&e.Repository, &e.RepositorySeq, &e.Body)
		return e, err
	})
}

func (x queries) LogHeads(ctx context.Context, after int64, limit int) ([][2]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT repository_id, last FROM repository_event_counter WHERE repository_id > $1
		ORDER BY repository_id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) ([2]int64, error) {
		var h [2]int64
		err := r.Scan(&h[0], &h[1])
		return h, err
	})
}

func (x queries) RepositoryEvents(ctx context.Context, repo, after int64, limit int) ([]backend.Event, error) {
	return events(x.q.Query(ctx, `SELECT repository_id, repository_seq, body FROM event
		WHERE repository_id = $1 AND repository_seq > $2 ORDER BY repository_seq LIMIT $3`, repo, after, limit))
}

// ─── The content index ───

// scopeOf is a scope's columns: 0 for a repository's, 1 for a network's, and the owner's ID.
func scopeOf(sc backend.Scope) (int16, int64) {
	if sc.Network {
		return 1, sc.ID
	}
	return 0, sc.ID
}

func (x queries) InsertContentIndex(ctx context.Context, sc backend.Scope, gen int64) error {
	scope, owner := scopeOf(sc)
	_, err := x.q.Exec(ctx, `INSERT INTO content_index (scope, owner_id, generation) VALUES ($1, $2, $3)`, scope, owner, gen)
	return err
}

// ShareContentIndex holds the index's row in share mode and takes the stamp the transaction's log
// rows of it carry: the index's sequence, as it stands while the row is held so.
func (x queries) ShareContentIndex(ctx context.Context, sc backend.Scope) (time.Time, int64, bool, error) {
	scope, owner := scopeOf(sc)
	var today time.Time
	var seq, gen int64
	err := x.q.QueryRow(ctx, `SELECT current_date, log_seq, generation FROM content_index WHERE scope = $1 AND owner_id = $2 FOR SHARE`,
		scope, owner).Scan(&today, &seq, &gen)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, 0, false, nil
	}
	if err == nil {
		x.stamp(sc, seq)
	}
	return today, gen, err == nil, err
}

// LockContentIndex holds the index's row alone: its log rows are stamped past every writer's
// before it, and the sequence moved past them, for every writer after.
func (x queries) LockContentIndex(ctx context.Context, sc backend.Scope) (int64, time.Time, error) {
	scope, owner := scopeOf(sc)
	var gen, seq int64
	var today time.Time
	err := x.q.QueryRow(ctx, `UPDATE content_index SET log_seq = log_seq + 2 WHERE scope = $1 AND owner_id = $2
		RETURNING generation, log_seq - 1, current_date`, scope, owner).Scan(&gen, &seq, &today)
	if err == nil {
		x.stamp(sc, seq)
	}
	return gen, today, err
}

func (x queries) ContentIndexGeneration(ctx context.Context, sc backend.Scope) (backend.IndexState, bool, error) {
	scope, owner := scopeOf(sc)
	var st backend.IndexState
	var folded *string
	var swept *int64
	err := x.q.QueryRow(ctx, `SELECT generation, folded_through::text, swept_before FROM content_index
		WHERE scope = $1 AND owner_id = $2`, scope, owner).Scan(&st.Generation, &folded, &swept)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, false, nil
	}
	if folded != nil {
		st.Folded = backend.LogPlace(*folded)
	}
	if swept != nil {
		st.SweptBefore = *swept
	}
	return st, err == nil, err
}

func (x queries) SetContentIndexGeneration(ctx context.Context, sc backend.Scope, gen int64, folded backend.LogPlace) error {
	scope, owner := scopeOf(sc)
	_, err := x.q.Exec(ctx, `UPDATE content_index SET generation = $3, folded_through = $4::text::pg_snapshot WHERE scope = $1 AND owner_id = $2`,
		scope, owner, gen, snapshotText(folded))
	return err
}

func (x queries) SetContentIndexSwept(ctx context.Context, sc backend.Scope, before int64) error {
	scope, owner := scopeOf(sc)
	_, err := x.q.Exec(ctx, `UPDATE content_index SET swept_before = $3 WHERE scope = $1 AND owner_id = $2`, scope, owner, before)
	return err
}

// snapshotText is a place as the snapshot it is, NULL for none.
func snapshotText(p backend.LogPlace) *string {
	if p == nil {
		return nil
	}
	s := string(p)
	return &s
}

// ContentLogSince reads the rows a snapshot sees that since's did not, by their transactions' IDs,
// in stamp order and a transaction's rows in theirs, and returns the snapshot as the place: one
// statement's, so the rows and the place are one snapshot's whatever the transaction's isolation —
// a write transaction's, which holds the index so no fold commits meanwhile, reads each statement
// at its own.
func (x queries) ContentLogSince(ctx context.Context, sc backend.Scope, since backend.LogPlace) ([]backend.ContentLog, backend.LogPlace, error) {
	scope, owner := scopeOf(sc)
	var snap string
	var own *string
	var bodies [][]byte
	err := x.q.QueryRow(ctx, `SELECT pg_current_snapshot()::text, pg_current_xact_id_if_assigned()::text,
			coalesce((SELECT array_agg(body ORDER BY stamp, id) FROM content_log WHERE scope = $1 AND owner_id = $2
				AND ($3::text IS NULL OR (xid >= pg_snapshot_xmin($3::text::pg_snapshot) AND NOT pg_visible_in_snapshot(xid, $3::text::pg_snapshot)))),
				'{}')`, scope, owner, snapshotText(since)).Scan(&snap, &own, &bodies)
	if err != nil {
		return nil, nil, err
	}
	place, err := readerPlace(snap, own)
	if err != nil {
		return nil, nil, err
	}
	out := make([]backend.ContentLog, len(bodies))
	for i, b := range bodies {
		out[i] = backend.ContentLog{Body: b}
	}
	return out, place, nil
}

// readerPlace is a snapshot as the place of the transaction that took it: with its own ID among
// those under way. A snapshot leaves its own transaction out of them — it sees its own writes
// otherwise — so once a newer transaction put xmax past it, the place would count read what the
// transaction logs after, and its commit, unread.
func readerPlace(snap string, own *string) (backend.LogPlace, error) {
	if own == nil {
		return backend.LogPlace(snap), nil
	}
	parts := strings.SplitN(snap, ":", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("a snapshot %q", snap)
	}
	xmin, err1 := strconv.ParseUint(parts[0], 10, 64)
	xmax, err2 := strconv.ParseUint(parts[1], 10, 64)
	xid, err3 := strconv.ParseUint(*own, 10, 64)
	if err := errors.Join(err1, err2, err3); err != nil {
		return nil, fmt.Errorf("a snapshot %q, transaction %s: %w", snap, *own, err)
	}
	if xid >= xmax {
		return backend.LogPlace(snap), nil // under way to the snapshot already
	}
	xips := []uint64{xid}
	for _, s := range strings.Split(parts[2], ",") {
		if s == "" {
			continue
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("a snapshot %q: %w", snap, err)
		}
		xips = append(xips, n)
	}
	slices.Sort(xips)
	xips = slices.Compact(xips)
	list := make([]string, len(xips))
	for i, n := range xips {
		list[i] = strconv.FormatUint(n, 10)
	}
	return backend.LogPlace(fmt.Sprintf("%d:%d:%s", min(xmin, xid), xmax, strings.Join(list, ","))), nil
}

// InsertContentLog records a log row stamped as its transaction's hold on the index says — the
// index's sequence as it stands, for a transaction that holds none.
func (x queries) InsertContentLog(ctx context.Context, sc backend.Scope, body []byte) error {
	scope, owner := scopeOf(sc)
	if seq, ok := x.stamped(sc); ok {
		_, err := x.q.Exec(ctx, `INSERT INTO content_log (scope, owner_id, body, stamp) VALUES ($1, $2, $3, $4)`, scope, owner, body, seq)
		return err
	}
	_, err := x.q.Exec(ctx, `INSERT INTO content_log (scope, owner_id, body, stamp)
		SELECT $1, $2, $3, log_seq FROM content_index WHERE scope = $1 AND owner_id = $2`, scope, owner, body)
	return err
}

func (x queries) DeleteFoldedContentLogs(ctx context.Context, sc backend.Scope, folded backend.LogPlace, limit int) (int64, error) {
	if folded == nil {
		return 0, nil
	}
	scope, owner := scopeOf(sc)
	tag, err := x.q.Exec(ctx, `DELETE FROM content_log WHERE ctid IN (SELECT ctid FROM content_log WHERE scope = $1 AND owner_id = $2
		AND (xid < pg_snapshot_xmin($3::text::pg_snapshot) OR pg_visible_in_snapshot(xid, $3::text::pg_snapshot)) LIMIT $4)`,
		scope, owner, string(folded), limit)
	return tag.RowsAffected(), err
}

// ─── Packs ───

func (x queries) PutPack(ctx context.Context, net backend.Network, id encoding.ID, size int64) (uint64, error) {
	var num uint64
	err := x.q.QueryRow(ctx, `INSERT INTO pack (network_id, id, size) VALUES ($1, $2, $3)
		ON CONFLICT (network_id, id) DO UPDATE SET size = EXCLUDED.size RETURNING num`,
		net, id[:], size).Scan(&num)
	return num, err
}

func (x queries) InsertPack(ctx context.Context, net backend.Network, id encoding.ID, size int64, dictionary *int64) (uint64, error) {
	var num uint64
	err := x.q.QueryRow(ctx, `INSERT INTO pack (network_id, id, size, dictionary_id) VALUES ($1, $2, $3, $4) RETURNING num`,
		net, id[:], size, dictionary).Scan(&num)
	return num, err
}

func (x queries) NumberedPacks(ctx context.Context, net backend.Network, nums []uint64) (map[uint64]encoding.ID, error) {
	n := make([]int64, len(nums))
	for i, v := range nums {
		n[i] = int64(v)
	}
	rows, err := x.q.Query(ctx, `SELECT num, id FROM pack WHERE network_id = $1 AND num = ANY($2)`, net, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uint64]encoding.ID{}
	for rows.Next() {
		var num uint64
		var b []byte
		if err := rows.Scan(&num, &b); err != nil {
			return nil, err
		}
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		out[num] = id
	}
	return out, rows.Err()
}

func (x queries) LivePacks(ctx context.Context, net backend.Network) ([]backend.Pack, error) {
	rows, err := x.q.Query(ctx, `SELECT id, num, size, dictionary_id FROM pack
		WHERE network_id = $1 AND retired_at IS NULL ORDER BY created_at, id`, net)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Pack
	for rows.Next() {
		var b []byte
		var p backend.Pack
		if err := rows.Scan(&b, &p.Num, &p.Size, &p.Dictionary); err != nil {
			return nil, err
		}
		if p.ID, err = toID(b); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (x queries) RetiredPackIDs(ctx context.Context, net backend.Network) ([]encoding.ID, error) {
	return idList(x.q.Query(ctx, `SELECT id FROM pack WHERE network_id = $1 AND retired_at IS NOT NULL`, net))
}

func (x queries) PackIDs(ctx context.Context, net backend.Network) ([]encoding.ID, error) {
	return idList(x.q.Query(ctx, `SELECT id FROM pack WHERE network_id = $1`, net))
}

func (x queries) SetPacksDictionary(ctx context.Context, net backend.Network, packIDs []encoding.ID, dictionary int64) error {
	_, err := x.q.Exec(ctx, `UPDATE pack SET dictionary_id = $3 WHERE network_id = $1 AND id = ANY($2)`, net, ids(packIDs), dictionary)
	return err
}

func (x queries) RetirePack(ctx context.Context, net backend.Network, id encoding.ID) error {
	_, err := x.q.Exec(ctx, `UPDATE pack SET retired_at = now() WHERE network_id = $1 AND id = $2`, net, id[:])
	return err
}

func (x queries) LivePackNumbers(ctx context.Context, net backend.Network, packIDs []encoding.ID) (map[encoding.ID]uint64, error) {
	rows, err := x.q.Query(ctx, `SELECT id, num FROM pack WHERE network_id = $1 AND id = ANY($2) AND retired_at IS NULL`, net, ids(packIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[encoding.ID]uint64{}
	for rows.Next() {
		var b []byte
		var num uint64
		if err := rows.Scan(&b, &num); err != nil {
			return nil, err
		}
		id, err := toID(b)
		if err != nil {
			return nil, err
		}
		out[id] = num
	}
	return out, rows.Err()
}

func (x queries) RetirePacks(ctx context.Context, net backend.Network, packIDs []encoding.ID, dry bool) (int, int64, error) {
	stmt := `WITH r AS (
			UPDATE pack SET retired_at = now()
			WHERE network_id = $1 AND id = ANY($2) AND retired_at IS NULL RETURNING size
		) SELECT count(*), coalesce(sum(size), 0)::bigint FROM r`
	if dry {
		stmt = `SELECT count(*), coalesce(sum(size), 0)::bigint FROM pack WHERE network_id = $1 AND id = ANY($2) AND retired_at IS NULL`
	}
	var n int
	var bytes int64
	err := x.q.QueryRow(ctx, stmt, net, ids(packIDs)).Scan(&n, &bytes)
	return n, bytes, err
}

func (x queries) RepositoriesRetiringBefore(ctx context.Context, packs, segments time.Time) ([]int64, error) {
	// A network's are its first repository's to delete.
	return int64s(x.q.Query(ctx, `SELECT m FROM (SELECT (SELECT min(r.id) FROM repository r WHERE r.network_id = n.id) AS m FROM network n
			WHERE EXISTS (SELECT 1 FROM pack p WHERE p.network_id = n.id AND p.retired_at < $1)
			   OR EXISTS (SELECT 1 FROM segment g WHERE g.network_id = n.id AND g.retired_at < $2)) t
		WHERE m IS NOT NULL ORDER BY 1`, packs, segments))
}

func (x queries) RetiredPacksBefore(ctx context.Context, t time.Time, net *backend.Network) ([]backend.RetiredPack, error) {
	rows, err := x.q.Query(ctx, `SELECT p.network_id, p.id, p.num FROM pack p
		WHERE p.retired_at < $1 AND ($2::bigint IS NULL OR p.network_id = $2)
		ORDER BY p.network_id, p.id`, t, net)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.RetiredPack
	for rows.Next() {
		var p backend.RetiredPack
		var b []byte
		if err := rows.Scan(&p.Net, &b, &p.Num); err != nil {
			return nil, err
		}
		if p.ID, err = toID(b); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (x queries) DeleteRetiredPack(ctx context.Context, net backend.Network, id encoding.ID) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM pack WHERE network_id = $1 AND id = $2 AND retired_at IS NOT NULL`, net, id[:])
	return tag.RowsAffected() > 0, err
}
