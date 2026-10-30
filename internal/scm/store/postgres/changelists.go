package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── ChangeLists ───

const changelistColumns = `id, number, change_id, author, status, abandoned_from, parent_number, current_patchset,
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
		if err := rows.Scan(&c.ID, &c.Number, &c.ChangeID, &c.Author, &c.Status, &abandonedFrom, &parent, &c.CurrentPatchset,
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

func (x queries) ListChangeLists(ctx context.Context, repo, before int64, statuses []int16, author string, parent int64, limit int) ([]backend.ChangeList, error) {
	return changeLists(x.q.Query(ctx, `SELECT `+changelistColumns+` FROM changelist
		WHERE repository_id = $1 AND number > 0 AND ($2::bigint = 0 OR number < $2)
		  AND (cardinality($3::smallint[]) = 0 OR status = ANY($3))
		  AND ($4 = '' OR author = $4)
		  AND ($6::bigint = 0 OR parent_number = $6)
		ORDER BY number DESC LIMIT $5`, repo, before, statuses, author, limit, parent))
}

func (x queries) ChangeListsOfChanges(ctx context.Context, repo int64, changes [][]byte) ([]backend.ChangeList, error) {
	return changeLists(x.q.Query(ctx, `SELECT `+changelistColumns+` FROM changelist
		WHERE repository_id = $1 AND change_id = ANY($2) ORDER BY number`, repo, changes))
}

func (x queries) Patchsets(ctx context.Context, repo int64, cls []int64) ([]backend.Patchset, error) {
	rows, err := x.q.Query(ctx, `SELECT cl_id, number, commit_id, base_commit_id, coalesce(parent_number, 0), uploaded_by, uploaded_at
		FROM patchset WHERE repository_id = $1 AND cl_id = ANY($2) ORDER BY cl_id, number`, repo, cls)
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

func (x queries) SetChangeListStatus(ctx context.Context, repo, id int64, status int16, abandonedFrom *int16) error {
	_, err := x.q.Exec(ctx, `UPDATE changelist SET status = $3, abandoned_from = $4, updated_at = now()
		WHERE repository_id = $1 AND id = $2`, repo, id, status, abandonedFrom)
	return err
}

func (x queries) SetLanded(ctx context.Context, repo int64, status int16, cls []backend.LandedCL) error {
	if len(cls) == 0 {
		return nil
	}
	idCol, commits, by := make([]int64, len(cls)), make([][]byte, len(cls)), make([]string, len(cls))
	for i, c := range cls {
		idCol[i], commits[i], by[i] = c.ID, c.Commit[:], c.By
	}
	_, err := x.q.Exec(ctx, `UPDATE changelist c SET status = $2, landed_commit = u.commit, submitted_by = u.by, updated_at = now()
		FROM unnest($3::bigint[], $4::bytea[], $5::text[]) AS u(id, commit, by)
		WHERE c.repository_id = $1 AND c.id = u.id`, repo, status, idCol, commits, by)
	return err
}

func (x queries) UploadChangeLists(ctx context.Context, repo int64, changes, commits [][]byte) ([]int64, error) {
	return int64s(x.q.Query(ctx, `SELECT number FROM changelist WHERE repository_id = $1 AND change_id = ANY($2)
		UNION SELECT c.number FROM patchset p JOIN changelist c ON c.repository_id = p.repository_id AND c.id = p.cl_id
			WHERE p.repository_id = $1 AND p.commit_id = ANY($3)`, repo, changes, commits))
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
	_, err := x.q.Exec(ctx, `INSERT INTO changelist (repository_id, id, number, change_id, author, status, parent_number,
			current_patchset, description, rolls_back, rollback_paths, rollback_left)
		VALUES ($1, $12, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		repo, c.Number, c.ChangeID, c.Author, c.Status, nullable(c.Parent), c.CurrentPatchset, c.Description,
		orEmpty(c.RollsBack), orEmpty(c.RollbackPaths), orEmpty(c.RollbackLeft), c.ID)
	if unique(err) {
		return errors.Join(backend.ErrDuplicate, err)
	}
	return err
}

func (x queries) UpdateChangeList(ctx context.Context, repo, id int64, patchset int32, description string, parent *int64, status int16) error {
	_, err := x.q.Exec(ctx, `UPDATE changelist SET current_patchset = $3, description = $4, parent_number = $5,
			status = $6, updated_at = now() WHERE repository_id = $1 AND id = $2`,
		repo, id, patchset, description, parent, status)
	return err
}

func (x queries) InsertPatchset(ctx context.Context, repo int64, p backend.Patchset) error {
	_, err := x.q.Exec(ctx, `INSERT INTO patchset (repository_id, cl_id, number, commit_id, base_commit_id, parent_number, uploaded_by)
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

func (x queries) StackedOn(ctx context.Context, repo int64, parents []int64) ([][2]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT DISTINCT p.parent_number, c.number FROM patchset p
			JOIN changelist c ON c.repository_id = p.repository_id AND c.id = p.cl_id
		WHERE p.repository_id = $1 AND p.parent_number = ANY($2)`, repo, parents)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][2]int64
	for rows.Next() {
		var p [2]int64
		if err := rows.Scan(&p[0], &p[1]); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (x queries) TailLandings(ctx context.Context, repo int64, cls []int64) ([]backend.HeadMove, error) {
	moves, _, err := headMoves(x.q.Query(ctx, `SELECT seq, old_head, new_head, cl_number, patchset, landed_by, landed_at
		FROM head_log WHERE repository_id = $1 AND cl_number = ANY($2) ORDER BY seq`, repo, cls))
	return moves, err
}

// headMoves reads rows of main's moves, and when each was.
func headMoves(rows pgx.Rows, err error) ([]backend.HeadMove, []time.Time, error) {
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var out []backend.HeadMove
	var at []time.Time
	for rows.Next() {
		var h backend.HeadMove
		var old, new []byte
		var t time.Time
		if err := rows.Scan(&h.Seq, &old, &new, &h.CL, &h.Patchset, &h.By, &t); err != nil {
			return nil, nil, err
		}
		if h.Old, err = toID(old); err != nil {
			return nil, nil, err
		}
		if h.New, err = toID(new); err != nil {
			return nil, nil, err
		}
		out = append(out, h)
		at = append(at, t)
	}
	return out, at, rows.Err()
}

func (x queries) SettledChangeLists(ctx context.Context, repo int64, limit int) ([]backend.SettledChangeList, error) {
	cls, err := changeLists(x.q.Query(ctx, `SELECT `+changelistColumns+` FROM changelist
		WHERE repository_id = $1 AND status = $2 AND landed_commit IS NOT NULL ORDER BY number LIMIT $3`,
		repo, settledStatus, limit))
	if err != nil || len(cls) == 0 {
		return nil, err
	}
	numbers := make([]int64, len(cls))
	ids := make([]int64, len(cls))
	for i, c := range cls {
		numbers[i], ids[i] = c.Number, c.ID
	}
	pss, err := x.Patchsets(ctx, repo, ids)
	if err != nil {
		return nil, err
	}
	moves, at, err := headMoves(x.q.Query(ctx, `SELECT seq, old_head, new_head, cl_number, patchset, landed_by, landed_at
		FROM head_log WHERE repository_id = $1 AND cl_number = ANY($2)`, repo, numbers))
	if err != nil {
		return nil, err
	}
	out := make([]backend.SettledChangeList, 0, len(cls))
	byNumber, byID := map[int64]int{}, map[int64]int{}
	for _, c := range cls {
		byNumber[c.Number], byID[c.ID] = len(out), len(out)
		out = append(out, backend.SettledChangeList{ChangeList: c})
	}
	for _, p := range pss {
		o := &out[byID[p.CL]]
		o.Patchsets = append(o.Patchsets, p)
	}
	landed := map[int64]bool{}
	for i, m := range moves {
		o := &out[byNumber[m.CL]]
		o.Landing, o.LandedAt = m, at[i]
		landed[m.CL] = true
	}
	// A CL is settled with its landing: one without (which no landing writes) stays in the tail.
	kept := out[:0]
	for _, o := range out {
		if landed[o.Number] {
			kept = append(kept, o)
		}
	}
	return kept, nil
}

// settledStatus is a landed CL's status (store.StatusSubmitted), which nothing moves a CL out of.
const settledStatus = 3

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

func (x queries) CandidatesOf(ctx context.Context, repo int64, commits []encoding.ID) (map[encoding.ID]backend.Candidate, error) {
	out := map[encoding.ID]backend.Candidate{}
	if len(commits) == 0 {
		return out, nil
	}
	rows, err := x.q.Query(ctx, `SELECT commit_id, cl_number, patchset, base, prepared_by, created_at FROM candidate
		WHERE repository_id = $1 AND commit_id = ANY($2)`, repo, ids(commits))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var c backend.Candidate
		var commit, base []byte
		if err := rows.Scan(&commit, &c.Number, &c.Patchset, &base, &c.PreparedBy, &c.PreparedAt); err != nil {
			return nil, err
		}
		if c.Commit, err = toID(commit); err != nil {
			return nil, err
		}
		if c.Base, err = toID(base); err != nil {
			return nil, err
		}
		out[c.Commit] = c
	}
	return out, rows.Err()
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
