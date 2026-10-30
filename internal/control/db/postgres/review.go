package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/pgdata"
)

func (x queries) Reviewers(ctx context.Context, repo, number int64) ([]db.Reviewer, error) {
	rows, err := x.q.Query(ctx, `SELECT principal, added_by, added_at FROM reviewer WHERE repository_id = $1 AND number = $2 ORDER BY added_at, principal`, repo, number)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []db.Reviewer
	for rows.Next() {
		var r db.Reviewer
		if err := rows.Scan(&r.Principal, &r.AddedBy, &r.AddedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (x queries) AddReviewer(ctx context.Context, repo, number int64, principal, by string) (bool, error) {
	tag, err := x.q.Exec(ctx, `INSERT INTO reviewer (repository_id, number, principal, added_by) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, repo, number, principal, by)
	return tag.RowsAffected() > 0, err
}

func (x queries) RemoveReviewer(ctx context.Context, repo, number int64, principal string) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM reviewer WHERE repository_id = $1 AND number = $2 AND principal = $3`, repo, number, principal)
	return tag.RowsAffected() > 0, err
}

func (x queries) Grants(ctx context.Context, repo, number int64) ([]db.Grant, error) {
	return x.GrantsOf(ctx, repo, []int64{number})
}

func (x queries) GrantsOf(ctx context.Context, repo int64, numbers []int64) ([]db.Grant, error) {
	rows, err := x.q.Query(ctx, `SELECT number, principal, label, granted, patchset, updated_at FROM label_grant
		WHERE repository_id = $1 AND number = ANY($2::bigint[]) ORDER BY number, principal, label`, repo, numbers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []db.Grant
	for rows.Next() {
		var g db.Grant
		if err := rows.Scan(&g.Number, &g.Principal, &g.Label, &g.Granted, &g.Patchset, &g.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

func (x queries) SetGrant(ctx context.Context, repo, number int64, principal string, label db.Label, granted bool, patchset int32) (bool, error) {
	if !granted {
		tag, err := x.q.Exec(ctx, `UPDATE label_grant SET granted = false, patchset = $5, updated_at = now()
			WHERE repository_id = $1 AND number = $2 AND principal = $3 AND label = $4 AND granted`,
			repo, number, principal, label, patchset)
		return tag.RowsAffected() > 0, err
	}
	tag, err := x.q.Exec(ctx, `INSERT INTO label_grant (repository_id, number, principal, label, granted, patchset)
		VALUES ($1, $2, $3, $4, true, $5)
		ON CONFLICT (repository_id, number, principal, label) DO UPDATE
			SET granted = true, patchset = EXCLUDED.patchset, updated_at = now()
		WHERE NOT label_grant.granted`, repo, number, principal, label, patchset)
	return tag.RowsAffected() > 0, err
}

const commentColumns = `id, number, patchset, author, coalesce(parent_id, 0), coalesce(file_path, ''), coalesce(line, 0),
	coalesce(side, 0), body, resolved, created_at, updated_at`

func scanComment(row pgx.Row) (db.Comment, error) {
	var c db.Comment
	err := row.Scan(&c.ID, &c.Number, &c.Patchset, &c.Author, &c.ParentID, &c.FilePath, &c.Line, &c.Side, &c.Body, &c.Resolved, &c.CreatedAt, &c.UpdatedAt)
	return c, err
}

func (x queries) Comments(ctx context.Context, repo, number int64) ([]db.Comment, error) {
	rows, err := x.q.Query(ctx, `SELECT `+commentColumns+` FROM comment WHERE repository_id = $1 AND number = $2 ORDER BY created_at, id`, repo, number)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []db.Comment
	for rows.Next() {
		c, err := scanComment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (x queries) GetComment(ctx context.Context, repo, number, id int64) (db.Comment, bool, error) {
	c, err := scanComment(x.q.QueryRow(ctx, `SELECT `+commentColumns+` FROM comment WHERE repository_id = $1 AND number = $2 AND id = $3`, repo, number, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Comment{}, false, nil
	}
	return c, err == nil, err
}

func (x queries) CreateComment(ctx context.Context, repo int64, c db.Comment) (int64, error) {
	var parent *int64
	if c.ParentID != 0 {
		parent = &c.ParentID
	}
	var file *string
	if c.FilePath != "" {
		file = &c.FilePath
	}
	var line *int32
	var side *int16
	if c.Line != 0 {
		line = &c.Line
		s := int16(c.Side)
		side = &s
	} else if c.Side != db.SideNone && c.FilePath != "" {
		s := int16(c.Side)
		side = &s
	}
	var id int64
	err := x.atomically(ctx, func(x queries) error {
		if err := x.q.QueryRow(ctx, `WITH c AS (
				INSERT INTO comment (id, repository_id, number, patchset, author, parent_id, file_path, line, side, body, created_at, updated_at)
				VALUES ($10, $1, $2, $3, $4, $5, $6, $7, $8, $9, clock_timestamp(), clock_timestamp()) RETURNING id
			), ob AS (
				INSERT INTO outbox (repository_id, entry_id, number, queued_at, comment_id) SELECT $1, $11, $2, clock_timestamp(), id FROM c
			)
			SELECT id FROM c`,
			repo, c.Number, c.Patchset, c.Author, parent, file, line, side, c.Body, data.NewID(), data.NewID()).Scan(&id); err != nil {
			return err
		}
		return pgdata.MarkWork(ctx, x.q, db.OutboxWork, repo)
	})
	return id, err
}

func (x queries) SetResolved(ctx context.Context, repo, number, rootID int64, resolved bool) (bool, error) {
	tag, err := x.q.Exec(ctx, `UPDATE comment SET resolved = $4, updated_at = now()
		WHERE repository_id = $1 AND number = $2 AND id = $3 AND parent_id IS NULL AND resolved <> $4`, repo, number, rootID, resolved)
	return tag.RowsAffected() > 0, err
}

func (x queries) DeleteComment(ctx context.Context, repo, number, id int64) error {
	_, err := x.q.Exec(ctx, `DELETE FROM comment WHERE repository_id = $1 AND number = $2 AND id = $3`, repo, number, id)
	return err
}

func (x queries) UnresolvedThreads(ctx context.Context, repo, number int64) (int64, error) {
	var n int64
	err := x.q.QueryRow(ctx, `SELECT count(*) FROM comment WHERE repository_id = $1 AND number = $2
		AND parent_id IS NULL AND file_path IS NOT NULL AND NOT resolved`, repo, number).Scan(&n)
	return n, err
}

func (x queries) RecordEvent(ctx context.Context, repo, number int64, e db.Event) error {
	at := e.At
	if at.IsZero() {
		at = time.Now()
	}
	// The event, its outbox entry, and the outbox's mark, in one transaction: none without the
	// others.
	return x.atomically(ctx, func(x queries) error {
		if _, err := x.q.Exec(ctx, `WITH ev AS (
				INSERT INTO event (id, repository_id, number, at, actor, kind, from_status, to_status, subject, label, patchset,
					commit_id, message, related, recorded_at)
				VALUES ($14, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, clock_timestamp())
				RETURNING id
			)
			INSERT INTO outbox (repository_id, entry_id, number, queued_at, event_id) SELECT $1, $15, $2, clock_timestamp(), id FROM ev`,
			repo, number, at, e.Actor, e.Kind, e.From, e.To, e.Subject, e.Label, e.Patchset, e.Commit, e.Message, e.Related, data.NewID(),
			data.NewID()); err != nil {
			return err
		}
		return pgdata.MarkWork(ctx, x.q, db.OutboxWork, repo)
	})
}

func (x queries) Events(ctx context.Context, repo, number int64) ([]db.Event, error) {
	rows, err := x.q.Query(ctx, `SELECT id, at, actor, kind, from_status, to_status, subject, label, patchset, commit_id, message, related
		FROM event WHERE repository_id = $1 AND number = $2 ORDER BY at, recorded_at, id`, repo, number)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []db.Event
	for rows.Next() {
		var e db.Event
		if err := rows.Scan(&e.ID, &e.At, &e.Actor, &e.Kind, &e.From, &e.To, &e.Subject, &e.Label, &e.Patchset, &e.Commit, &e.Message, &e.Related); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (x queries) LastAbandon(ctx context.Context, repo, number int64) (db.Event, bool, error) {
	var e db.Event
	err := x.q.QueryRow(ctx, `SELECT id, from_status FROM event WHERE repository_id = $1 AND number = $2 AND kind = $3
		ORDER BY at DESC, recorded_at DESC, id DESC LIMIT 1`,
		repo, number, db.EventAbandoned).Scan(&e.ID, &e.From)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Event{}, false, nil
	}
	return e, err == nil, err
}

func (x queries) Favorites(ctx context.Context, principal string) (map[int64]bool, error) {
	rows, err := x.q.Query(ctx, `SELECT repository_id FROM favorite WHERE principal = $1`, principal)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

func (x queries) SetFavorite(ctx context.Context, principal string, repo int64, on bool) error {
	var err error
	if on {
		_, err = x.q.Exec(ctx, `INSERT INTO favorite (principal, repository_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, principal, repo)
	} else {
		_, err = x.q.Exec(ctx, `DELETE FROM favorite WHERE principal = $1 AND repository_id = $2`, principal, repo)
	}
	return err
}
