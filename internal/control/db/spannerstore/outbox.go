package spannerstore

import (
	"context"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
)

// queueOutbox puts an event or a comment in its repository's outbox, and marks the outbox's work,
// in tx.
func queueOutbox(ctx context.Context, tx *spanner.ReadWriteTransaction, repo, number, event, comment int64) error {
	if _, err := spannerdata.Update(ctx, tx, `INSERT INTO outbox (repository_id, entry_id, number, queued_at, event_id, comment_id)
		VALUES (@repo, @id, @number, CURRENT_TIMESTAMP(), @event, @comment)`,
		map[string]any{"repo": repo, "id": data.NewID(), "number": number, "event": event, "comment": comment}); err != nil {
		return err
	}
	return spannerdata.MarkWork(ctx, tx, db.OutboxWork, repo)
}

func (x queries) Outbox(ctx context.Context, repo int64, limit int) ([]db.OutboxEntry, error) {
	var out []db.OutboxEntry
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT entry_id, number, queued_at, event_id, comment_id, head_seq, head FROM outbox
			WHERE repository_id = @repo ORDER BY queued_at, entry_id LIMIT @limit`, map[string]any{"repo": repo, "limit": int64(limit)},
			func(row *spanner.Row) error {
				var e db.OutboxEntry
				if err := row.Columns(&e.ID, &e.Number, &e.QueuedAt, &e.EventID, &e.CommentID, &e.HeadSeq, &e.Head); err != nil {
					return err
				}
				out = append(out, e)
				return nil
			})
	})
	return out, err
}

func (x queries) DoneOutbox(ctx context.Context, repo int64, ids []int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		ms := make([]*spanner.Mutation, 0, len(ids))
		for _, id := range ids {
			ms = append(ms, spanner.Delete("outbox", spanner.Key{repo, id}))
		}
		return tx.BufferWrite(ms)
	})
}

func (x queries) GetEvent(ctx context.Context, repo, number, id int64) (db.Event, bool, error) {
	var e db.Event
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		ok = false
		return spannerdata.Each(ctx, r, `SELECT event_id, event_at, actor, kind, from_status, to_status, subject, label, patchset, commit_id, message,
				related
			FROM event WHERE repository_id = @repo AND number = @number AND event_id = @id`,
			map[string]any{"repo": repo, "number": number, "id": id}, func(row *spanner.Row) error {
				var kind, from, to, label, patchset int64
				if err := row.Columns(&e.ID, &e.At, &e.Actor, &kind, &from, &to, &e.Subject, &label, &patchset, &e.Commit, &e.Message, &e.Related); err != nil {
					return err
				}
				e.Kind, e.From, e.To, e.Label, e.Patchset = db.EventKind(kind), db.Status(from), db.Status(to), db.Label(label), int32(patchset)
				ok = true
				return nil
			})
	})
	return e, ok, err
}

// Work is ok control's keyed work's store.
func (s *Store) Work() work.Store { return spannerdata.Work(s.Client) }

func (s *Store) Waits() wait.Store { return spannerdata.Waits(s.Client) }
