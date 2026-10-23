package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/work"
)

func (x queries) Outbox(ctx context.Context, repo int64, limit int) ([]db.OutboxEntry, error) {
	rows, err := x.q.Query(ctx, `SELECT entry_id, number, queued_at, event_id, comment_id, head_seq, head FROM outbox
		WHERE repository_id = $1 ORDER BY queued_at, entry_id LIMIT $2`, repo, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (db.OutboxEntry, error) {
		var e db.OutboxEntry
		err := row.Scan(&e.ID, &e.Number, &e.QueuedAt, &e.EventID, &e.CommentID, &e.HeadSeq, &e.Head)
		return e, err
	})
}

func (x queries) DoneOutbox(ctx context.Context, repo int64, ids []int64) error {
	_, err := x.q.Exec(ctx, `DELETE FROM outbox WHERE repository_id = $1 AND entry_id = ANY($2)`, repo, ids)
	return err
}

func (x queries) OutboxRepositories(ctx context.Context) ([]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT DISTINCT repository_id FROM outbox ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func (x queries) GetEvent(ctx context.Context, repo, number, id int64) (db.Event, bool, error) {
	var e db.Event
	err := x.q.QueryRow(ctx, `SELECT id, at, actor, kind, from_status, to_status, subject, label, patchset, commit_id, message, related
		FROM event WHERE repository_id = $1 AND number = $2 AND id = $3`, repo, number, id).Scan(&e.ID, &e.At, &e.Actor, &e.Kind, &e.From,
		&e.To, &e.Subject, &e.Label, &e.Patchset, &e.Commit, &e.Message, &e.Related)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Event{}, false, nil
	}
	return e, err == nil, err
}

// Work is ok control's keyed work's store.
func (s *Store) Work() work.Store { return pgdata.Work(s.pool) }
