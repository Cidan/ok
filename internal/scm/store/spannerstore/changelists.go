package spannerstore

import (
	"context"
	"errors"
	"slices"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── ChangeLists ───

const changelistColumns = `id, number, change_id, author, status, abandoned_from, parent_number, current_patchset,
	landed_commit, submitted_by, description, created_at, updated_at, rolls_back, rollback_paths, rollback_left`

func changeLists(ctx context.Context, r spannerdata.Reader, sql string, params map[string]any) ([]backend.ChangeList, error) {
	var out []backend.ChangeList
	err := spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
		var c backend.ChangeList
		var status, patchset int64
		var abandonedFrom, parent spanner.NullInt64
		var landed []byte
		var submittedBy spanner.NullString
		if err := row.Columns(&c.ID, &c.Number, &c.ChangeID, &c.Author, &status, &abandonedFrom, &parent, &patchset,
			&landed, &submittedBy, &c.Description, &c.CreatedAt, &c.UpdatedAt, &c.RollsBack, &c.RollbackPaths, &c.RollbackLeft); err != nil {
			return err
		}
		c.Status, c.CurrentPatchset = int16(status), int32(patchset)
		c.AbandonedFrom, c.Parent, c.SubmittedBy = int16(abandonedFrom.Int64), parent.Int64, submittedBy.StringVal
		c.RollsBack, c.RollbackPaths, c.RollbackLeft = orEmpty(c.RollsBack), orEmpty(c.RollbackPaths), orEmpty(c.RollbackLeft)
		if landed != nil {
			id, err := toID(landed)
			if err != nil {
				return err
			}
			c.Landed = &id
		}
		out = append(out, c)
		return nil
	})
	return out, err
}

func (x queries) changeLists(ctx context.Context, sql string, params map[string]any) ([]backend.ChangeList, error) {
	var out []backend.ChangeList
	err := x.read(func(r spannerdata.Reader) error {
		var err error
		out, err = changeLists(ctx, r, sql, params)
		return err
	})
	return out, err
}

func (x queries) ChangeLists(ctx context.Context, repo int64, numbers []int64, lock bool) ([]backend.ChangeList, error) {
	// Through the number index, which a read by number names: a scan of the repository's CLs would
	// lock each in a read-write transaction.
	sql := `SELECT ` + changelistColumns + ` FROM changelist@{FORCE_INDEX=changelist_number}
		WHERE repository_id = @repo AND number IN UNNEST(@numbers) ORDER BY number`
	p := map[string]any{"repo": repo, "numbers": numbers}
	if !lock {
		return x.changeLists(ctx, sql, p)
	}
	var out []backend.ChangeList
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		out, err = changeLists(ctx, tx, sql+` FOR UPDATE`, p)
		return err
	})
	return out, err
}

func int64sOf[T int16 | int32](in []T) []int64 {
	out := make([]int64, len(in))
	for i, v := range in {
		out[i] = int64(v)
	}
	return out
}

func (x queries) ListChangeLists(ctx context.Context, repo, before int64, statuses []int16, author string, parent int64, limit int) ([]backend.ChangeList, error) {
	return x.changeLists(ctx, `SELECT `+changelistColumns+` FROM changelist
		WHERE repository_id = @repo AND number > 0 AND (@before = 0 OR number < @before)
		  AND (ARRAY_LENGTH(@statuses) = 0 OR status IN UNNEST(@statuses))
		  AND (@author = '' OR author = @author)
		  AND (@parent = 0 OR parent_number = @parent)
		ORDER BY number DESC LIMIT @limit`,
		map[string]any{"repo": repo, "before": before, "statuses": int64sOf(statuses), "author": author, "parent": parent, "limit": int64(limit)})
}

func (x queries) ChangeListsOfChanges(ctx context.Context, repo int64, changes [][]byte) ([]backend.ChangeList, error) {
	return x.changeLists(ctx, `SELECT `+changelistColumns+` FROM changelist@{FORCE_INDEX=changelist_change}
		WHERE repository_id = @repo AND change_id IN UNNEST(@changes) ORDER BY number`, map[string]any{"repo": repo, "changes": changes})
}

func patchsets(ctx context.Context, r spannerdata.Reader, repo int64, cls []int64) ([]backend.Patchset, error) {
	var out []backend.Patchset
	err := spannerdata.Each(ctx, r, `SELECT cl_id, number, commit_id, base_commit_id, IFNULL(parent_number, 0), uploaded_by, uploaded_at
		FROM patchset WHERE repository_id = @repo AND cl_id IN UNNEST(@cls) ORDER BY cl_id, number`,
		map[string]any{"repo": repo, "cls": cls}, func(row *spanner.Row) error {
			var p backend.Patchset
			var number int64
			var commit, base []byte
			if err := row.Columns(&p.CL, &number, &commit, &base, &p.Parent, &p.UploadedBy, &p.UploadedAt); err != nil {
				return err
			}
			p.Number = int32(number)
			var err error
			if p.Commit, err = toID(commit); err != nil {
				return err
			}
			if p.Base, err = toID(base); err != nil {
				return err
			}
			out = append(out, p)
			return nil
		})
	return out, err
}

func (x queries) Patchsets(ctx context.Context, repo int64, cls []int64) ([]backend.Patchset, error) {
	var out []backend.Patchset
	err := x.read(func(r spannerdata.Reader) error {
		var err error
		out, err = patchsets(ctx, r, repo, cls)
		return err
	})
	return out, err
}

func (x queries) ChangeListStatus(ctx context.Context, repo, number int64) (int16, bool, error) {
	var st int64
	ok, err := x.one(ctx, `SELECT status FROM changelist@{FORCE_INDEX=changelist_number} WHERE repository_id = @repo AND number = @number`,
		map[string]any{"repo": repo, "number": number}, &st)
	return int16(st), ok, err
}

func (x queries) SetChangeListStatus(ctx context.Context, repo, id int64, status int16, abandonedFrom *int16) error {
	from := spanner.NullInt64{}
	if abandonedFrom != nil {
		from = spanner.NullInt64{Int64: int64(*abandonedFrom), Valid: true}
	}
	_, err := x.exec(ctx, `UPDATE changelist SET status = @status, abandoned_from = @from, updated_at = `+now+`
		WHERE repository_id = @repo AND id = @id`, map[string]any{"repo": repo, "id": id, "status": int64(status), "from": from})
	return err
}

// SetLanded updates each CL by key, every statement in one request.
func (x queries) SetLanded(ctx context.Context, repo int64, status int16, cls []backend.LandedCL) error {
	if len(cls) == 0 {
		return nil
	}
	stmts := make([]spanner.Statement, len(cls))
	for i, c := range cls {
		stmts[i] = spanner.Statement{SQL: `UPDATE changelist SET status = @status, landed_commit = @commit, submitted_by = @by, updated_at = ` + now + `
			WHERE repository_id = @repo AND id = @id`,
			Params: map[string]any{"repo": repo, "id": c.ID, "status": int64(status), "commit": c.Commit[:], "by": c.By}}
	}
	return x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		_, err := spannerdata.BatchUpdate(ctx, tx, stmts)
		return err
	})
}

// UploadChangeLists reads by key alone, through the change and commit indexes, then the CLs of the
// patchsets found by their IDs: in a read-write transaction a scan, or a join that builds on the
// repository's CLs, would lock every one, and wait on any another holds.
func (x queries) UploadChangeLists(ctx context.Context, repo int64, changes, commits [][]byte) ([]int64, error) {
	p := map[string]any{"repo": repo, "changes": changes, "commits": commits}
	numbers, err := x.int64s(ctx, `SELECT number FROM changelist@{FORCE_INDEX=changelist_change}
		WHERE repository_id = @repo AND change_id IN UNNEST(@changes)`, p)
	if err != nil || len(commits) == 0 {
		return numbers, err
	}
	ids, err := x.int64s(ctx, `SELECT DISTINCT cl_id FROM patchset@{FORCE_INDEX=patchset_commit}
		WHERE repository_id = @repo AND commit_id IN UNNEST(@commits)`, p)
	if err != nil || len(ids) == 0 {
		return numbers, err
	}
	byID, err := x.ChangeListNumbers(ctx, repo, ids)
	if err != nil {
		return nil, err
	}
	for _, n := range byID {
		if !slices.Contains(numbers, n) {
			numbers = append(numbers, n)
		}
	}
	return numbers, nil
}

func (x queries) NextChangeListNumbers(ctx context.Context, repo int64, n int64) (int64, error) {
	var last int64
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		last, err = bump(ctx, tx, "changelist_counter", repo, n)
		return err
	})
	return last, err
}

func (x queries) InsertChangeList(ctx context.Context, repo int64, c backend.ChangeList) error {
	_, err := x.exec(ctx, `INSERT INTO changelist (repository_id, id, number, change_id, author, status, parent_number, current_patchset,
			description, created_at, updated_at, rolls_back, rollback_paths, rollback_left)
		VALUES (@repo, @id, @number, @change, @author, @status, @parent, @patchset, @description, `+now+`, `+now+`,
			@rollsBack, @paths, @left)`,
		map[string]any{"repo": repo, "id": c.ID, "number": c.Number, "change": c.ChangeID, "author": c.Author, "status": int64(c.Status),
			"parent": nullable(c.Parent), "patchset": int64(c.CurrentPatchset), "description": c.Description,
			"rollsBack": orEmpty(c.RollsBack), "paths": orEmpty(c.RollbackPaths), "left": orEmpty(c.RollbackLeft)})
	if unique(err) {
		return errors.Join(backend.ErrDuplicate, err)
	}
	return err
}

func (x queries) UpdateChangeList(ctx context.Context, repo, id int64, patchset int32, description string, parent *int64, status int16) error {
	par := spanner.NullInt64{}
	if parent != nil {
		par = spanner.NullInt64{Int64: *parent, Valid: true}
	}
	_, err := x.exec(ctx, `UPDATE changelist SET current_patchset = @patchset, description = @description, parent_number = @parent,
			status = @status, updated_at = `+now+` WHERE repository_id = @repo AND id = @id`,
		map[string]any{"repo": repo, "id": id, "patchset": int64(patchset), "description": description, "parent": par,
			"status": int64(status)})
	return err
}

func (x queries) InsertPatchset(ctx context.Context, repo int64, p backend.Patchset) error {
	_, err := x.exec(ctx, `INSERT INTO patchset (repository_id, cl_id, number, commit_id, base_commit_id, parent_number, uploaded_by,
			uploaded_at)
		VALUES (@repo, @cl, @number, @commit, @base, @parent, @by, `+now+`)`,
		map[string]any{"repo": repo, "cl": p.CL, "number": int64(p.Number), "commit": p.Commit[:], "base": p.Base[:],
			"parent": nullable(p.Parent), "by": p.UploadedBy})
	return err
}

// RollingBack reads unlocked, the statuses' CLs through the status index: in a read-write
// transaction its scan would lock each, and every write of one would wait on the rollback. The
// rollback that asks holds its repository's head, as every rollback does, so one committed before
// it took the head is read here, as of now.
func (x queries) RollingBack(ctx context.Context, repo int64, numbers []int64, statuses []int16) (int64, bool, error) {
	var n int64
	var ok bool
	err := x.unlocked(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT number FROM changelist@{FORCE_INDEX=changelist_status} WHERE repository_id = @repo
			AND status IN UNNEST(@statuses) AND EXISTS (SELECT 1 FROM UNNEST(rolls_back) AS r WHERE r IN UNNEST(@numbers))
			ORDER BY number LIMIT 1`,
			map[string]any{"repo": repo, "numbers": numbers, "statuses": int64sOf(statuses)}, &n)
		return err
	})
	return n, ok, err
}

// StackedOn reads by key alone, as UploadChangeLists does: the patchsets through the parent index,
// then their CLs by ID.
func (x queries) StackedOn(ctx context.Context, repo int64, parents []int64) ([][2]int64, error) {
	var pairs [][2]int64 // (parent, CL ID)
	err := x.rows(ctx, `SELECT DISTINCT parent_number, cl_id FROM patchset@{FORCE_INDEX=patchset_parent}
		WHERE repository_id = @repo AND parent_number IS NOT NULL AND parent_number IN UNNEST(@parents)`,
		map[string]any{"repo": repo, "parents": parents}, func() { pairs = nil }, func(row *spanner.Row) error {
			var p [2]int64
			if err := row.Columns(&p[0], &p[1]); err != nil {
				return err
			}
			pairs = append(pairs, p)
			return nil
		})
	if err != nil || len(pairs) == 0 {
		return nil, err
	}
	ids := make([]int64, len(pairs))
	for i, p := range pairs {
		ids[i] = p[1]
	}
	numbers, err := x.ChangeListNumbers(ctx, repo, ids)
	if err != nil {
		return nil, err
	}
	out := make([][2]int64, len(pairs))
	for i, p := range pairs {
		out[i] = [2]int64{p[0], numbers[p[1]]}
	}
	return out, nil
}

const headMoveColumns = `seq, old_head, new_head, cl_number, patchset, landed_by, landed_at`

// headMoves reads, in r, rows of main's moves, and when each was.
func headMoves(ctx context.Context, r spannerdata.Reader, sql string, params map[string]any) ([]backend.HeadMove, []time.Time, error) {
	var out []backend.HeadMove
	var at []time.Time
	err := spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
		var h backend.HeadMove
		var old, new []byte
		var patchset int64
		var t time.Time
		if err := row.Columns(&h.Seq, &old, &new, &h.CL, &patchset, &h.By, &t); err != nil {
			return err
		}
		h.Patchset = int32(patchset)
		var err error
		if h.Old, err = toID(old); err != nil {
			return err
		}
		if h.New, err = toID(new); err != nil {
			return err
		}
		out, at = append(out, h), append(at, t)
		return nil
	})
	return out, at, err
}

func (x queries) TailLandings(ctx context.Context, repo int64, cls []int64) ([]backend.HeadMove, error) {
	var out []backend.HeadMove
	err := x.read(func(r spannerdata.Reader) error {
		var err error
		out, _, err = headMoves(ctx, r, `SELECT `+headMoveColumns+` FROM head_log WHERE repository_id = @repo AND cl_number IN UNNEST(@cls)
			ORDER BY seq`, map[string]any{"repo": repo, "cls": cls})
		return err
	})
	return out, err
}

func (x queries) SettledChangeLists(ctx context.Context, repo int64, limit int) ([]backend.SettledChangeList, error) {
	var out []backend.SettledChangeList
	err := x.read(func(r spannerdata.Reader) error {
		out = nil
		cls, err := changeLists(ctx, r, `SELECT `+changelistColumns+` FROM changelist
			WHERE repository_id = @repo AND status = @settled AND landed_commit IS NOT NULL ORDER BY number LIMIT @limit`,
			map[string]any{"repo": repo, "settled": int64(settledStatus), "limit": int64(limit)})
		if err != nil || len(cls) == 0 {
			return err
		}
		numbers := make([]int64, len(cls))
		ids := make([]int64, len(cls))
		for i, c := range cls {
			numbers[i], ids[i] = c.Number, c.ID
		}
		pss, err := patchsets(ctx, r, repo, ids)
		if err != nil {
			return err
		}
		moves, at, err := headMoves(ctx, r, `SELECT `+headMoveColumns+` FROM head_log WHERE repository_id = @repo AND cl_number IN UNNEST(@cls)`,
			map[string]any{"repo": repo, "cls": numbers})
		if err != nil {
			return err
		}
		all := make([]backend.SettledChangeList, 0, len(cls))
		byNumber, byID := map[int64]int{}, map[int64]int{}
		for _, c := range cls {
			byNumber[c.Number], byID[c.ID] = len(all), len(all)
			all = append(all, backend.SettledChangeList{ChangeList: c})
		}
		for _, p := range pss {
			o := &all[byID[p.CL]]
			o.Patchsets = append(o.Patchsets, p)
		}
		landed := map[int64]bool{}
		for i, m := range moves {
			o := &all[byNumber[m.CL]]
			o.Landing, o.LandedAt = m, at[i]
			landed[m.CL] = true
		}
		// A CL is settled with its landing: one without (which no landing writes) stays in the tail.
		for _, o := range all {
			if landed[o.Number] {
				out = append(out, o)
			}
		}
		return nil
	})
	return out, err
}

// settledStatus is a landed CL's status (store.StatusSubmitted), which nothing moves a CL out of.
const settledStatus = 3

// ─── Candidates ───

func (x queries) Candidate(ctx context.Context, repo int64, commit encoding.ID) (backend.Candidate, bool, error) {
	c := backend.Candidate{Commit: commit}
	var base []byte
	var patchset int64
	ok, err := x.one(ctx, `SELECT cl_number, patchset, base, prepared_by, created_at FROM candidate WHERE repository_id = @repo AND commit_id = @commit`,
		map[string]any{"repo": repo, "commit": commit[:]}, &c.Number, &patchset, &base, &c.PreparedBy, &c.PreparedAt)
	if err != nil || !ok {
		return c, false, err
	}
	c.Patchset = int32(patchset)
	c.Base, err = toID(base)
	return c, err == nil, err
}

func (x queries) CandidatesOf(ctx context.Context, repo int64, commits []encoding.ID) (map[encoding.ID]backend.Candidate, error) {
	out := map[encoding.ID]backend.Candidate{}
	if len(commits) == 0 {
		return out, nil
	}
	err := x.rows(ctx, `SELECT commit_id, cl_number, patchset, base, prepared_by, created_at FROM candidate
		WHERE repository_id = @repo AND commit_id IN UNNEST(@commits)`, map[string]any{"repo": repo, "commits": ids(commits)},
		func() { out = map[encoding.ID]backend.Candidate{} }, func(row *spanner.Row) error {
			var c backend.Candidate
			var commit, base []byte
			var patchset int64
			if err := row.Columns(&commit, &c.Number, &patchset, &base, &c.PreparedBy, &c.PreparedAt); err != nil {
				return err
			}
			var err error
			if c.Commit, err = toID(commit); err != nil {
				return err
			}
			if c.Base, err = toID(base); err != nil {
				return err
			}
			c.Patchset = int32(patchset)
			out[c.Commit] = c
			return nil
		}, "commits")
	return out, err
}

// PutCandidate records a candidate unless it is recorded, and reads it as recorded.
func (x queries) PutCandidate(ctx context.Context, repo int64, c backend.Candidate) (backend.Candidate, error) {
	out := c
	err := x.write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		p := map[string]any{"repo": repo, "commit": c.Commit[:], "cl": c.Number, "patchset": int64(c.Patchset), "base": c.Base[:],
			"by": c.PreparedBy}
		if _, err := spannerdata.Update(ctx, tx, `INSERT OR IGNORE INTO candidate (repository_id, commit_id, cl_number, patchset, base,
				prepared_by, created_at)
			VALUES (@repo, @commit, @cl, @patchset, @base, @by, `+now+`)`, p); err != nil {
			return err
		}
		_, err := spannerdata.First(ctx, tx, `SELECT prepared_by, created_at FROM candidate WHERE repository_id = @repo AND commit_id = @commit`,
			p, &out.PreparedBy, &out.PreparedAt)
		return err
	})
	return out, err
}

func (x queries) Candidates(ctx context.Context, repo int64) ([]backend.Candidate, error) {
	var out []backend.Candidate
	err := x.rows(ctx, `SELECT commit_id, base, cl_number, patchset, prepared_by, created_at FROM candidate
		WHERE repository_id = @repo ORDER BY created_at, commit_id`, map[string]any{"repo": repo}, func() { out = nil },
		func(row *spanner.Row) error {
			var commit, base []byte
			var patchset int64
			var c backend.Candidate
			if err := row.Columns(&commit, &base, &c.Number, &patchset, &c.PreparedBy, &c.PreparedAt); err != nil {
				return err
			}
			c.Patchset = int32(patchset)
			var err error
			if c.Commit, err = toID(commit); err != nil {
				return err
			}
			if c.Base, err = toID(base); err != nil {
				return err
			}
			out = append(out, c)
			return nil
		})
	return out, err
}

func (x queries) DeleteCandidate(ctx context.Context, repo int64, commit encoding.ID) error {
	_, err := x.exec(ctx, `DELETE FROM candidate WHERE repository_id = @repo AND commit_id = @commit`,
		map[string]any{"repo": repo, "commit": commit[:]})
	return err
}

func (x queries) DeleteCandidatesOf(ctx context.Context, repo int64, numbers []int64) error {
	_, err := x.exec(ctx, `DELETE FROM candidate WHERE repository_id = @repo AND cl_number IN UNNEST(@numbers)`,
		map[string]any{"repo": repo, "numbers": numbers})
	return err
}
