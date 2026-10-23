package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── ChangeLists ───

const changelistColumns = `number, change_id, author, status, abandoned_from, parent_number, current_patchset,
	landed_commit, submitted_by, description, created_at, updated_at, rolls_back, rollback_paths, rollback_left`

func changeLists(rows pgx.Rows, err error) ([]backend.ChangeList, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.ChangeList
	for rows.Next() {
		var c backend.ChangeList
		var landed []byte
		var abandonedFrom *int16
		var parent *int64
		var submittedBy *string
		if err := rows.Scan(&c.Number, &c.ChangeID, &c.Author, &c.Status, &abandonedFrom, &parent, &c.CurrentPatchset,
			&landed, &submittedBy, &c.Description, &c.CreatedAt, &c.UpdatedAt, &c.RollsBack, &c.RollbackPaths, &c.RollbackLeft); err != nil {
			return nil, err
		}
		if abandonedFrom != nil {
			c.AbandonedFrom = *abandonedFrom
		}
		if parent != nil {
			c.Parent = *parent
		}
		if landed != nil {
			id, err := toID(landed)
			if err != nil {
				return nil, err
			}
			c.Landed = &id
		}
		if submittedBy != nil {
			c.SubmittedBy = *submittedBy
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (x queries) ChangeLists(ctx context.Context, repo int64, numbers []int64, lock bool) ([]backend.ChangeList, error) {
	sql := `SELECT ` + changelistColumns + ` FROM changelist WHERE repository_id = $1 AND number = ANY($2) ORDER BY number`
	if lock {
		sql += ` FOR UPDATE`
	}
	return changeLists(x.q.Query(ctx, sql, repo, numbers))
}

func (x queries) ListChangeLists(ctx context.Context, repo, before int64, statuses []int16, author string, limit int) ([]backend.ChangeList, error) {
	return changeLists(x.q.Query(ctx, `SELECT `+changelistColumns+` FROM changelist
		WHERE repository_id = $1 AND ($2 = 0 OR number < $2)
		  AND (cardinality($3::smallint[]) = 0 OR status = ANY($3))
		  AND ($4 = '' OR author = $4)
		ORDER BY number DESC LIMIT $5`, repo, before, statuses, author, limit))
}

func (x queries) ChangeListsOfChanges(ctx context.Context, repo int64, changes [][]byte) ([]backend.ChangeList, error) {
	return changeLists(x.q.Query(ctx, `SELECT `+changelistColumns+` FROM changelist
		WHERE repository_id = $1 AND change_id = ANY($2) ORDER BY number`, repo, changes))
}

func (x queries) Patchsets(ctx context.Context, repo int64, cls []int64) ([]backend.Patchset, error) {
	rows, err := x.q.Query(ctx, `SELECT cl_number, number, commit_id, base_commit_id, coalesce(parent_number, 0), uploaded_by, uploaded_at
		FROM patchset WHERE repository_id = $1 AND cl_number = ANY($2) ORDER BY cl_number, number`, repo, cls)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Patchset
	for rows.Next() {
		var p backend.Patchset
		var commit, base []byte
		if err := rows.Scan(&p.CL, &p.Number, &commit, &base, &p.Parent, &p.UploadedBy, &p.UploadedAt); err != nil {
			return nil, err
		}
		if p.Commit, err = toID(commit); err != nil {
			return nil, err
		}
		if p.Base, err = toID(base); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (x queries) ChangeListStatus(ctx context.Context, repo, number int64) (int16, bool, error) {
	var st int16
	err := x.q.QueryRow(ctx, `SELECT status FROM changelist WHERE repository_id = $1 AND number = $2`, repo, number).Scan(&st)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return st, err == nil, err
}

func (x queries) SetChangeListStatus(ctx context.Context, repo, number int64, status int16, abandonedFrom *int16) error {
	_, err := x.q.Exec(ctx, `UPDATE changelist SET status = $3, abandoned_from = $4, updated_at = now()
		WHERE repository_id = $1 AND number = $2`, repo, number, status, abandonedFrom)
	return err
}

func (x queries) SetLanded(ctx context.Context, repo, number int64, status int16, commit encoding.ID, by string) error {
	_, err := x.q.Exec(ctx, `UPDATE changelist SET status = $3, landed_commit = $4, submitted_by = $5, updated_at = now()
		WHERE repository_id = $1 AND number = $2`, repo, number, status, commit[:], by)
	return err
}

func (x queries) UploadChangeLists(ctx context.Context, repo int64, changes, commits [][]byte) ([]int64, error) {
	return int64s(x.q.Query(ctx, `SELECT number FROM changelist WHERE repository_id = $1 AND change_id = ANY($2)
		UNION SELECT cl_number FROM patchset WHERE repository_id = $1 AND commit_id = ANY($3)`, repo, changes, commits))
}

func (x queries) NextChangeListNumbers(ctx context.Context, repo int64, n int64) (int64, error) {
	var last int64
	err := x.q.QueryRow(ctx, `INSERT INTO changelist_counter (repository_id, last) VALUES ($1, $2)
		ON CONFLICT (repository_id) DO UPDATE SET last = changelist_counter.last + EXCLUDED.last RETURNING last`,
		repo, n).Scan(&last)
	return last, err
}

func nullable[T int16 | int32 | int64](v T) *T {
	if v == 0 {
		return nil
	}
	return &v
}

func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func (x queries) InsertChangeList(ctx context.Context, repo int64, c backend.ChangeList) error {
	_, err := x.q.Exec(ctx, `INSERT INTO changelist (repository_id, number, change_id, author, status, parent_number,
			current_patchset, description, rolls_back, rollback_paths, rollback_left)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		repo, c.Number, c.ChangeID, c.Author, c.Status, nullable(c.Parent), c.CurrentPatchset, c.Description,
		orEmpty(c.RollsBack), orEmpty(c.RollbackPaths), orEmpty(c.RollbackLeft))
	if unique(err) {
		return errors.Join(backend.ErrDuplicate, err)
	}
	return err
}

func (x queries) UpdateChangeList(ctx context.Context, repo, number int64, patchset int32, description string, parent *int64, status int16) error {
	_, err := x.q.Exec(ctx, `UPDATE changelist SET current_patchset = $3, description = $4, parent_number = $5,
			status = $6, updated_at = now() WHERE repository_id = $1 AND number = $2`,
		repo, number, patchset, description, parent, status)
	return err
}

func (x queries) InsertPatchset(ctx context.Context, repo int64, p backend.Patchset) error {
	_, err := x.q.Exec(ctx, `INSERT INTO patchset (repository_id, cl_number, number, commit_id, base_commit_id, parent_number, uploaded_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, repo, p.CL, p.Number, p.Commit[:], p.Base[:], nullable(p.Parent), p.UploadedBy)
	return err
}

func (x queries) RollingBack(ctx context.Context, repo int64, numbers []int64, statuses []int16) (int64, bool, error) {
	var n int64
	err := x.q.QueryRow(ctx, `SELECT number FROM changelist WHERE repository_id = $1 AND rolls_back && $2
		AND status = ANY($3) ORDER BY number LIMIT 1`, repo, numbers, statuses).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return n, err == nil, err
}

func (x queries) StackLandings(ctx context.Context, repo, number int64, submitted int16) ([]backend.Landing, error) {
	rows, err := x.q.Query(ctx, `WITH RECURSIVE above(number) AS (
			SELECT $2::bigint
			UNION
			SELECT p.cl_number FROM patchset p
			JOIN above a ON a.number = p.parent_number
			WHERE p.repository_id = $1
		)
		SELECT h.cl_number, h.seq, h.new_head, h.old_head, c.description, c.author
		FROM above a
		JOIN changelist c ON c.repository_id = $1 AND c.number = a.number AND c.status = $3
		JOIN head_log h ON h.repository_id = $1 AND h.cl_number = a.number
		ORDER BY h.seq`, repo, number, submitted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Landing
	for rows.Next() {
		var l backend.Landing
		var commit, parent []byte
		if err := rows.Scan(&l.Number, &l.Seq, &commit, &parent, &l.Description, &l.Author); err != nil {
			return nil, err
		}
		if l.Commit, err = toID(commit); err != nil {
			return nil, err
		}
		if l.Parent, err = toID(parent); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// ─── Candidates ───

func (x queries) Candidate(ctx context.Context, repo int64, commit encoding.ID) (backend.Candidate, bool, error) {
	var base []byte
	c := backend.Candidate{Commit: commit}
	err := x.q.QueryRow(ctx, `SELECT cl_number, patchset, base, prepared_by, created_at FROM candidate WHERE repository_id = $1 AND commit_id = $2`,
		repo, commit[:]).Scan(&c.Number, &c.Patchset, &base, &c.PreparedBy, &c.PreparedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}
	c.Base, err = toID(base)
	return c, err == nil, err
}

func (x queries) PutCandidate(ctx context.Context, repo int64, c backend.Candidate) (backend.Candidate, error) {
	out := c
	err := x.q.QueryRow(ctx, `WITH added AS (
			INSERT INTO candidate (repository_id, commit_id, cl_number, patchset, base, prepared_by)
			VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING RETURNING prepared_by, created_at)
		SELECT prepared_by, created_at FROM added
		UNION ALL SELECT prepared_by, created_at FROM candidate WHERE repository_id = $1 AND commit_id = $2
		LIMIT 1`, repo, c.Commit[:], c.Number, c.Patchset, c.Base[:], c.PreparedBy).Scan(&out.PreparedBy, &out.PreparedAt)
	return out, err
}

func (x queries) Candidates(ctx context.Context, repo int64) ([]backend.Candidate, error) {
	rows, err := x.q.Query(ctx, `SELECT commit_id, base, cl_number, patchset, prepared_by, created_at FROM candidate
		WHERE repository_id = $1 ORDER BY created_at, commit_id`, repo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backend.Candidate
	for rows.Next() {
		var commit, base []byte
		var c backend.Candidate
		if err := rows.Scan(&commit, &base, &c.Number, &c.Patchset, &c.PreparedBy, &c.PreparedAt); err != nil {
			return nil, err
		}
		if c.Commit, err = toID(commit); err != nil {
			return nil, err
		}
		if c.Base, err = toID(base); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (x queries) DeleteCandidate(ctx context.Context, repo int64, commit encoding.ID) error {
	_, err := x.q.Exec(ctx, `DELETE FROM candidate WHERE repository_id = $1 AND commit_id = $2`, repo, commit[:])
	return err
}

func (x queries) DeleteCandidatesOf(ctx context.Context, repo int64, numbers []int64) error {
	_, err := x.q.Exec(ctx, `DELETE FROM candidate WHERE repository_id = $1 AND cl_number = ANY($2)`, repo, numbers)
	return err
}
