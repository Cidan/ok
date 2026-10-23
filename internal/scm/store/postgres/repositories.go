package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Repositories ───

const repositoryColumns = `id, name, head, head_seq, created_at, created_by`

func scanRepository(row pgx.Row) (backend.Repository, error) {
	var r backend.Repository
	var head []byte
	if err := row.Scan(&r.ID, &r.Name, &head, &r.HeadSeq, &r.CreatedAt, &r.CreatedBy); err != nil {
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

func (x queries) InsertRepository(ctx context.Context, name string, head encoding.ID, createdBy string) (backend.Repository, error) {
	r, err := scanRepository(x.q.QueryRow(ctx,
		`INSERT INTO repository (name, head, created_by) VALUES ($1, $2, $3) RETURNING `+repositoryColumns,
		name, head[:], createdBy))
	if unique(err) {
		return r, errors.Join(backend.ErrDuplicate, err)
	}
	return r, err
}

func (x queries) LockRepositoryName(ctx context.Context, id int64) (string, bool, error) {
	var name string
	err := x.q.QueryRow(ctx, `SELECT name FROM repository WHERE id = $1 FOR UPDATE`, id).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	return name, err == nil, err
}

func (x queries) RenameRepository(ctx context.Context, id int64, name string) (backend.Repository, error) {
	r, err := scanRepository(x.q.QueryRow(ctx, `UPDATE repository SET name = $2 WHERE id = $1 RETURNING `+repositoryColumns, id, name))
	if unique(err) {
		return r, errors.Join(backend.ErrDuplicate, err)
	}
	return r, err
}

func (x queries) Repository(ctx context.Context, id int64) (backend.Repository, bool, error) {
	return oneRepository(x.q.QueryRow(ctx, `SELECT `+repositoryColumns+` FROM repository WHERE id = $1`, id))
}

func (x queries) RepositoryByName(ctx context.Context, name string) (backend.Repository, bool, error) {
	return oneRepository(x.q.QueryRow(ctx, `SELECT `+repositoryColumns+` FROM repository WHERE name = $1`, name))
}

func (x queries) Repositories(ctx context.Context, after string, limit int) ([]backend.Repository, error) {
	rows, err := x.q.Query(ctx, `SELECT `+repositoryColumns+` FROM repository WHERE name > $1 ORDER BY name LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Repository
	for rows.Next() {
		r, err := scanRepository(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
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

func (x queries) InsertEvents(ctx context.Context, repo int64, at time.Time, seqs []int64, bodies [][]byte) error {
	_, err := x.q.Exec(ctx, `WITH pending AS (
			INSERT INTO event_pending (repository_id, repository_seq) SELECT $1, s FROM unnest($3::bigint[]) AS s)
		INSERT INTO event (repository_id, repository_seq, at, body)
		SELECT $1, u.seq, $2, u.body FROM unnest($3::bigint[], $4::bytea[]) AS u(seq, body)`,
		repo, at, seqs, bodies)
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

func (x queries) LogHeads(ctx context.Context, asOf, after int64, limit int) ([][2]int64, error) {
	sql := `SELECT repository_id, last FROM repository_event_counter WHERE repository_id > $2 AND $1 = 0
		ORDER BY repository_id LIMIT $3`
	if asOf > 0 {
		sql = `SELECT repository_id, max(repository_seq) FROM event WHERE seq <= $1 AND repository_id > $2
			GROUP BY repository_id ORDER BY repository_id LIMIT $3`
	}
	rows, err := x.q.Query(ctx, sql, asOf, after, limit)
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

func (x queries) InsertContentIndex(ctx context.Context, repo, gen int64) error {
	_, err := x.q.Exec(ctx, `INSERT INTO content_index (repository_id, generation) VALUES ($1, $2)`, repo, gen)
	return err
}

func (x queries) ShareContentIndex(ctx context.Context, repo int64) (time.Time, bool, error) {
	var today time.Time
	err := x.q.QueryRow(ctx, `SELECT current_date FROM content_index WHERE repository_id = $1 FOR SHARE`, repo).Scan(&today)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	return today, err == nil, err
}

func (x queries) LockContentIndex(ctx context.Context, repo int64) (int64, error) {
	var gen int64
	err := x.q.QueryRow(ctx, `SELECT generation FROM content_index WHERE repository_id = $1 FOR UPDATE`, repo).Scan(&gen)
	return gen, err
}

func (x queries) ContentIndexGeneration(ctx context.Context, repo int64) (int64, bool, error) {
	var gen int64
	err := x.q.QueryRow(ctx, `SELECT generation FROM content_index WHERE repository_id = $1`, repo).Scan(&gen)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return gen, err == nil, err
}

func (x queries) SetContentIndexGeneration(ctx context.Context, repo, gen int64) error {
	_, err := x.q.Exec(ctx, `UPDATE content_index SET generation = $2 WHERE repository_id = $1`, repo, gen)
	return err
}

func (x queries) IndexBlocks(ctx context.Context, repo, gen int64) ([][]byte, error) {
	rows, err := x.q.Query(ctx, `SELECT body FROM index_block WHERE repository_id = $1 AND generation = $2`, repo, gen)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[[]byte])
}

func (x queries) InsertIndexBlocks(ctx context.Context, repo, gen int64, firsts []encoding.ID, bodies [][]byte) error {
	_, err := x.q.Exec(ctx, `INSERT INTO index_block (repository_id, generation, first_id, body)
		SELECT $1, $2, u.f, u.b FROM unnest($3::bytea[], $4::bytea[]) AS u(f, b)`,
		repo, gen, ids(firsts), bodies)
	return err
}

func (x queries) DeleteIndexBlocks(ctx context.Context, repo, gen int64) error {
	_, err := x.q.Exec(ctx, `DELETE FROM index_block WHERE repository_id = $1 AND generation = $2`, repo, gen)
	return err
}

func (x queries) ContentLogIDs(ctx context.Context, repo int64) ([]int64, error) {
	return int64s(x.q.Query(ctx, `SELECT id FROM content_log WHERE repository_id = $1`, repo))
}

func (x queries) ContentLogs(ctx context.Context, repo int64, logIDs []int64) ([]backend.ContentLog, error) {
	rows, err := x.q.Query(ctx, `SELECT id, body FROM content_log WHERE repository_id = $1 AND id = ANY($2) ORDER BY id`, repo, logIDs)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (backend.ContentLog, error) {
		var l backend.ContentLog
		err := r.Scan(&l.ID, &l.Body)
		return l, err
	})
}

func (x queries) InsertContentLog(ctx context.Context, repo int64, body []byte) (int64, error) {
	var id int64
	err := x.q.QueryRow(ctx, `INSERT INTO content_log (repository_id, body) VALUES ($1, $2) RETURNING id`, repo, body).Scan(&id)
	return id, err
}

func (x queries) DeleteContentLogs(ctx context.Context, repo int64, logIDs []int64) error {
	_, err := x.q.Exec(ctx, `DELETE FROM content_log WHERE repository_id = $1 AND id = ANY($2)`, repo, logIDs)
	return err
}

// ─── Packs ───

func (x queries) PutPack(ctx context.Context, repo int64, id encoding.ID, size int64) (uint64, error) {
	var num uint64
	err := x.q.QueryRow(ctx, `INSERT INTO pack (repository_id, id, size) VALUES ($1, $2, $3)
		ON CONFLICT (repository_id, id) DO UPDATE SET size = EXCLUDED.size RETURNING num`,
		repo, id[:], size).Scan(&num)
	return num, err
}

func (x queries) InsertPack(ctx context.Context, repo int64, id encoding.ID, size int64, dictionary *int64) (uint64, error) {
	var num uint64
	err := x.q.QueryRow(ctx, `INSERT INTO pack (repository_id, id, size, dictionary_id) VALUES ($1, $2, $3, $4) RETURNING num`,
		repo, id[:], size, dictionary).Scan(&num)
	return num, err
}

func (x queries) PackNumbers(ctx context.Context, repo int64) (map[uint64]encoding.ID, error) {
	rows, err := x.q.Query(ctx, `SELECT num, id FROM pack WHERE repository_id = $1`, repo)
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

func (x queries) LivePacks(ctx context.Context, repo int64) ([]backend.Pack, error) {
	rows, err := x.q.Query(ctx, `SELECT id, num, size, dictionary_id FROM pack
		WHERE repository_id = $1 AND retired_at IS NULL ORDER BY created_at, id`, repo)
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

func (x queries) RetiredPackIDs(ctx context.Context, repo int64) ([]encoding.ID, error) {
	return idList(x.q.Query(ctx, `SELECT id FROM pack WHERE repository_id = $1 AND retired_at IS NOT NULL`, repo))
}

func (x queries) PackIDs(ctx context.Context, repo int64) ([]encoding.ID, error) {
	return idList(x.q.Query(ctx, `SELECT id FROM pack WHERE repository_id = $1`, repo))
}

func (x queries) SetPacksDictionary(ctx context.Context, repo int64, packIDs []encoding.ID, dictionary int64) error {
	_, err := x.q.Exec(ctx, `UPDATE pack SET dictionary_id = $3 WHERE repository_id = $1 AND id = ANY($2)`, repo, ids(packIDs), dictionary)
	return err
}

func (x queries) RetirePack(ctx context.Context, repo int64, id encoding.ID) error {
	_, err := x.q.Exec(ctx, `UPDATE pack SET retired_at = now() WHERE repository_id = $1 AND id = $2`, repo, id[:])
	return err
}

func (x queries) LivePackNumbers(ctx context.Context, repo int64, packIDs []encoding.ID) (map[encoding.ID]uint64, error) {
	rows, err := x.q.Query(ctx, `SELECT id, num FROM pack WHERE repository_id = $1 AND id = ANY($2) AND retired_at IS NULL`, repo, ids(packIDs))
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

func (x queries) RetirePacks(ctx context.Context, repo int64, packIDs []encoding.ID, dry bool) (int, int64, error) {
	stmt := `WITH r AS (
			UPDATE pack SET retired_at = now()
			WHERE repository_id = $1 AND id = ANY($2) AND retired_at IS NULL RETURNING size
		) SELECT count(*), coalesce(sum(size), 0)::bigint FROM r`
	if dry {
		stmt = `SELECT count(*), coalesce(sum(size), 0)::bigint FROM pack WHERE repository_id = $1 AND id = ANY($2) AND retired_at IS NULL`
	}
	var n int
	var bytes int64
	err := x.q.QueryRow(ctx, stmt, repo, ids(packIDs)).Scan(&n, &bytes)
	return n, bytes, err
}

func (x queries) RetiredPacksBefore(ctx context.Context, t time.Time, repo *int64) ([]backend.RetiredPack, error) {
	rows, err := x.q.Query(ctx, `SELECT p.repository_id, p.id, p.num FROM pack p
		WHERE p.retired_at < $1 AND ($2::bigint IS NULL OR p.repository_id = $2)
		ORDER BY p.repository_id, p.id`, t, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.RetiredPack
	for rows.Next() {
		var p backend.RetiredPack
		var b []byte
		if err := rows.Scan(&p.Repo, &b, &p.Num); err != nil {
			return nil, err
		}
		if p.ID, err = toID(b); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (x queries) DeleteRetiredPack(ctx context.Context, repo int64, id encoding.ID) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM pack WHERE repository_id = $1 AND id = $2 AND retired_at IS NOT NULL`, repo, id[:])
	return tag.RowsAffected() > 0, err
}
