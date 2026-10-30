package postgres

import (
	"context"

	"github.com/Cidan/ok/internal/automation/store"
)

// A landing's record, a statement for each kind of row whatever the number of its submissions
// (store.RecordLandings).

func (x queries) FinishSubmissions(ctx context.Context, ids []int64, status store.SubmissionStatus, reason, by string) (map[int64]int64, error) {
	out := map[int64]int64{}
	if len(ids) == 0 {
		return out, nil
	}
	// Locked in the order of their IDs, as every writer of several submissions locks them.
	rows, err := x.q.Query(ctx, `WITH locked AS (SELECT id FROM submission WHERE id = ANY($1) ORDER BY id FOR UPDATE)
		UPDATE submission s SET status = $3, reason = $4, note = '', finished_at = now(), canceled_by = nullif($5, '')
		FROM locked WHERE s.id = locked.id AND s.status = $2 RETURNING s.id, s.repository_id`,
		ids, store.SubmissionActive, status, reason, by)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, repo int64
		if err := rows.Scan(&id, &repo); err != nil {
			return nil, err
		}
		out[id] = repo
	}
	return out, rows.Err()
}

func (x queries) LandAttempts(ctx context.Context, pairs []store.AttemptOf) error {
	if len(pairs) == 0 {
		return nil
	}
	attempts, subs := make([]int64, len(pairs)), make([]int64, len(pairs))
	for i, p := range pairs {
		attempts[i], subs[i] = p.Attempt, p.Submission
	}
	_, err := x.q.Exec(ctx, `WITH locked AS (SELECT a.id FROM attempt a JOIN unnest($1::bigint[], $2::bigint[]) AS u(id, sub)
			ON a.id = u.id AND a.submission_id = u.sub ORDER BY a.id FOR UPDATE OF a)
		UPDATE attempt SET status = $3, finished_at = now() WHERE id IN (SELECT id FROM locked)`,
		attempts, subs, store.AttemptLanded)
	return err
}

func (x queries) EndActiveAttemptsOf(ctx context.Context, submissions []int64, status store.AttemptStatus, reason string) ([]int64, error) {
	if len(submissions) == 0 {
		return nil, nil
	}
	got, err := ids(x.q.Query(ctx, `UPDATE attempt SET status = $3, reason = $4, finished_at = now()
		WHERE submission_id = ANY($1) AND status = $2 RETURNING id`, submissions, store.AttemptActive, status, reason))
	if err != nil {
		return nil, err
	}
	sortIDs(got)
	return got, nil
}

func (x queries) SetLandedCommits(ctx context.Context, rows []store.SubmissionCommit) error {
	if len(rows) == 0 {
		return nil
	}
	subs, cls, commits := make([]int64, len(rows)), make([]int64, len(rows)), make([][]byte, len(rows))
	for i, r := range rows {
		subs[i], cls[i], commits[i] = r.Submission, r.CLNumber, r.Commit
	}
	_, err := x.q.Exec(ctx, `UPDATE submission_cl sc SET landed_commit = u.commit
		FROM unnest($1::bigint[], $2::bigint[], $3::bytea[]) AS u(sub, cl, commit)
		WHERE sc.submission_id = u.sub AND sc.cl_number = u.cl`, subs, cls, commits)
	return err
}

func (x queries) InsertLandedChanges(ctx context.Context, repo int64, rows []store.LandedCL) error {
	if len(rows) == 0 {
		return nil
	}
	commits, cls := make([][]byte, len(rows)), make([]int64, len(rows))
	for i, r := range rows {
		commits[i], cls[i] = r.Commit, r.CLNumber
	}
	_, err := x.q.Exec(ctx, `INSERT INTO landed_change (repository_id, commit_id, cl_number)
		SELECT $1, u.commit, u.cl FROM unnest($2::bytea[], $3::bigint[]) AS u(commit, cl) ON CONFLICT DO NOTHING`, repo, commits, cls)
	return err
}

func (x queries) PutMainCommits(ctx context.Context, repo int64, rows []store.MainCommit) error {
	if len(rows) == 0 {
		return nil
	}
	seqs, commits, subs := make([]int64, len(rows)), make([][]byte, len(rows)), make([]int64, len(rows))
	for i, r := range rows {
		seqs[i], commits[i], subs[i] = r.Seq, r.Commit, r.SubmissionID
	}
	_, err := x.q.Exec(ctx, `INSERT INTO main_commit (repository_id, seq, commit_id, submission_id)
		SELECT $1, u.seq, u.commit, u.sub FROM unnest($2::bigint[], $3::bytea[], $4::bigint[]) AS u(seq, commit, sub)
		ON CONFLICT (repository_id, seq) DO UPDATE SET submission_id = EXCLUDED.submission_id`, repo, seqs, commits, subs)
	return err
}

func (x queries) SetMainCommitSubmissions(ctx context.Context, repo int64, rows []store.MainCommit) error {
	if len(rows) == 0 {
		return nil
	}
	commits, subs := make([][]byte, len(rows)), make([]int64, len(rows))
	for i, r := range rows {
		commits[i], subs[i] = r.Commit, r.SubmissionID
	}
	_, err := x.q.Exec(ctx, `UPDATE main_commit mc SET submission_id = u.sub FROM unnest($2::bytea[], $3::bigint[]) AS u(commit, sub)
		WHERE mc.repository_id = $1 AND mc.commit_id = u.commit`, repo, commits, subs)
	return err
}
