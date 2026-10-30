package spannerstore

import (
	"context"
	"slices"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/data/spannerdata"
)

// A landing's record, a statement for each kind of row whatever the number of its submissions
// (store.RecordLandings).

func (x queries) FinishSubmissions(ctx context.Context, ids []int64, status store.SubmissionStatus, reason, by string) (map[int64]int64, error) {
	out := map[int64]int64{}
	if len(ids) == 0 {
		return out, nil
	}
	err := x.Returning(ctx, `UPDATE submission SET status = @status, reason = @reason, note = '', finished_at = CURRENT_TIMESTAMP(),
		canceled_by = @by WHERE submission_id IN UNNEST(@ids) AND status = @active THEN RETURN submission_id, repository_id`,
		map[string]any{"ids": ids, "active": int64(store.SubmissionActive), "status": int64(status), "reason": reason, "by": nullString(by)},
		func() { out = map[int64]int64{} },
		func(row *spanner.Row) error {
			var id, repo int64
			if err := row.Columns(&id, &repo); err != nil {
				return err
			}
			out[id] = repo
			return nil
		})
	return out, err
}

type attemptOf struct {
	A int64 `spanner:"a"`
	S int64 `spanner:"s"`
}

func (x queries) LandAttempts(ctx context.Context, pairs []store.AttemptOf) error {
	if len(pairs) == 0 {
		return nil
	}
	ps := make([]attemptOf, len(pairs))
	for i, p := range pairs {
		ps[i] = attemptOf{p.Attempt, p.Submission}
	}
	_, err := x.Exec(ctx, `UPDATE attempt SET status = @landed, finished_at = CURRENT_TIMESTAMP()
		WHERE STRUCT<a INT64, s INT64>(attempt_id, submission_id) IN UNNEST(@pairs)`,
		map[string]any{"pairs": ps, "landed": int64(store.AttemptLanded)})
	return err
}

func (x queries) EndActiveAttemptsOf(ctx context.Context, submissions []int64, status store.AttemptStatus, reason string) ([]int64, error) {
	if len(submissions) == 0 {
		return nil, nil
	}
	got, err := x.returningIDs(ctx, `UPDATE attempt SET status = @status, reason = @reason, finished_at = CURRENT_TIMESTAMP()
		WHERE submission_id IN UNNEST(@subs) AND status = @active THEN RETURN attempt_id`,
		map[string]any{"subs": submissions, "active": int64(store.AttemptActive), "status": int64(status), "reason": reason})
	slices.Sort(got)
	return got, err
}

// SetLandedCommits updates each row by key, every statement in one request.
func (x queries) SetLandedCommits(ctx context.Context, rows []store.SubmissionCommit) error {
	if len(rows) == 0 {
		return nil
	}
	stmts := make([]spanner.Statement, len(rows))
	for i, r := range rows {
		stmts[i] = spanner.Statement{SQL: `UPDATE submission_cl SET landed_commit = @commit WHERE submission_id = @sub AND cl_number = @cl`,
			Params: map[string]any{"sub": r.Submission, "cl": r.CLNumber, "commit": r.Commit}}
	}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		_, err := spannerdata.BatchUpdate(ctx, tx, stmts)
		return err
	})
}

func (x queries) InsertLandedChanges(ctx context.Context, repo int64, rows []store.LandedCL) error {
	if len(rows) == 0 {
		return nil
	}
	commits, cls := make([][]byte, len(rows)), make([]int64, len(rows))
	for i, r := range rows {
		commits[i], cls[i] = r.Commit, r.CLNumber
	}
	_, err := x.Exec(ctx, `INSERT OR IGNORE INTO landed_change (repository_id, commit_id, cl_number)
		SELECT @repo, c, @cls[OFFSET(i)] FROM UNNEST(@commits) AS c WITH OFFSET i`,
		map[string]any{"repo": repo, "commits": commits, "cls": cls})
	return err
}

// PutMainCommits records the rows not recorded, then each row's submission, the commit recorded at
// its place kept — every statement in one request.
func (x queries) PutMainCommits(ctx context.Context, repo int64, rows []store.MainCommit) error {
	if len(rows) == 0 {
		return nil
	}
	seqs, commits, subs := make([]int64, len(rows)), make([][]byte, len(rows)), make([]int64, len(rows))
	for i, r := range rows {
		seqs[i], commits[i], subs[i] = r.Seq, r.Commit, r.SubmissionID
	}
	stmts := []spanner.Statement{{SQL: `INSERT OR IGNORE INTO main_commit (repository_id, seq, commit_id, submission_id)
		SELECT @repo, seq, @commits[OFFSET(i)], @subs[OFFSET(i)] FROM UNNEST(@seqs) AS seq WITH OFFSET i`,
		Params: map[string]any{"repo": repo, "seqs": seqs, "commits": commits, "subs": subs}}}
	for _, r := range rows {
		stmts = append(stmts, spanner.Statement{SQL: `UPDATE main_commit SET submission_id = @sub WHERE repository_id = @repo AND seq = @seq`,
			Params: map[string]any{"repo": repo, "seq": r.Seq, "sub": r.SubmissionID}})
	}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		_, err := spannerdata.BatchUpdate(ctx, tx, stmts)
		return err
	})
}

// SetMainCommitSubmissions updates each commit's row, every statement in one request.
func (x queries) SetMainCommitSubmissions(ctx context.Context, repo int64, rows []store.MainCommit) error {
	if len(rows) == 0 {
		return nil
	}
	stmts := make([]spanner.Statement, len(rows))
	for i, r := range rows {
		stmts[i] = spanner.Statement{SQL: `UPDATE main_commit SET submission_id = @sub WHERE repository_id = @repo AND commit_id = @commit`,
			Params: map[string]any{"repo": repo, "commit": r.Commit, "sub": r.SubmissionID}}
	}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		_, err := spannerdata.BatchUpdate(ctx, tx, stmts)
		return err
	})
}
