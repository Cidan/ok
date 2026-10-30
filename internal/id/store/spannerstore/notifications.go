package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/work"
)

// Work is identity's keyed work's store.
func (b *Backend) Work() work.Store { return spannerdata.Work(b.Client) }

func (x queries) AddNotification(ctx context.Context, n store.Notification) (bool, error) {
	var added bool
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		_, err := tx.ReadRow(ctx, "notifications", spanner.Key{n.Principal, n.Source}, []string{"kind"})
		if err == nil {
			added = false
			return nil
		}
		if spanner.ErrCode(err) != codes.NotFound {
			return err
		}
		added = true
		return tx.BufferWrite([]*spanner.Mutation{spanner.Insert("notifications",
			[]string{"principal_id", "source", "kind", "repository_id", "number", "repository", "title", "actor_id", "text", "link", "notified_at", "read"},
			[]any{n.Principal, n.Source, int64(n.Kind), n.Repository, n.Number, n.RepositoryName, n.Title, n.Actor, n.Text, n.Link, n.At, false})})
	})
	return added, err
}

func (x queries) NotificationsOf(ctx context.Context, principal string, unread bool, limit int) ([]store.Notification, error) {
	var out []store.Notification
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT principal_id, source, kind, repository_id, number, repository, title, actor_id, text, link, notified_at, read
			FROM notifications WHERE principal_id = @principal AND (NOT @unread OR NOT read) ORDER BY notified_at DESC, source LIMIT @limit`,
			map[string]any{"principal": principal, "unread": unread, "limit": int64(limit)}, func(row *spanner.Row) error {
				var n store.Notification
				var kind int64
				if err := row.Columns(&n.Principal, &n.Source, &kind, &n.Repository, &n.Number, &n.RepositoryName, &n.Title, &n.Actor, &n.Text,
					&n.Link, &n.At, &n.Read); err != nil {
					return err
				}
				n.Kind = int(kind)
				out = append(out, n)
				return nil
			})
	})
	return out, err
}

func (x queries) UnreadNotifications(ctx context.Context, principal string) (int64, error) {
	var n int64
	err := x.Read(func(r spannerdata.Reader) error {
		_, err := spannerdata.First(ctx, r, `SELECT COUNT(*) FROM notifications WHERE principal_id = @principal AND NOT read`,
			map[string]any{"principal": principal}, &n)
		return err
	})
	return n, err
}

func (x queries) MarkNotificationsRead(ctx context.Context, principal string, sources []string, all bool) error {
	if sources == nil {
		sources = []string{}
	}
	_, err := x.Exec(ctx, `UPDATE notifications SET read = TRUE WHERE principal_id = @principal AND NOT read AND (@all OR source IN UNNEST(@sources))`,
		map[string]any{"principal": principal, "sources": sources, "all": all})
	return err
}

func (x queries) NotificationPrefs(ctx context.Context, principal string) (uint64, bool, error) {
	var m int64
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT mailed FROM notification_prefs WHERE principal_id = @principal`,
			map[string]any{"principal": principal}, &m)
		return err
	})
	return uint64(m), ok, err
}

func (x queries) SetNotificationPrefs(ctx context.Context, principal string, mailed uint64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdate("notification_prefs", []string{"principal_id", "mailed"},
			[]any{principal, int64(mailed)})})
	})
}

var mailColumns = []string{"mail_id", "principal_id", "to_address", "subject", "body", "state", "attempts", "next_try", "last_error", "created_at"}

func (x queries) MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error { return spannerdata.MarkWorkAt(ctx, tx, kind, key, at) })
}

func (x queries) QueueMail(ctx context.Context, m store.Mail) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if err := tx.BufferWrite([]*spanner.Mutation{spanner.Insert("mail_outbox", mailColumns,
			[]any{m.ID, m.Principal, m.To, m.Subject, m.Body, int64(m.State), int64(m.Attempts), m.NextTry, m.LastError, m.CreatedAt})}); err != nil {
			return err
		}
		return spannerdata.MarkWork(ctx, tx, store.MailWork, m.ID)
	})
}

func (x queries) MailByID(ctx context.Context, id int64) (store.Mail, bool, error) {
	var m store.Mail
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var state, attempts int64
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT mail_id, principal_id, to_address, subject, body, state, attempts, next_try, last_error, created_at
			FROM mail_outbox WHERE mail_id = @id`, map[string]any{"id": id}, &m.ID, &m.Principal, &m.To, &m.Subject, &m.Body, &state, &attempts,
			&m.NextTry, &m.LastError, &m.CreatedAt)
		m.State, m.Attempts = int(state), int(attempts)
		return err
	})
	return m, ok, err
}

func (x queries) UpdateMail(ctx context.Context, m store.Mail) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.Update("mail_outbox", []string{"mail_id", "state", "attempts", "next_try", "last_error"},
			[]any{m.ID, int64(m.State), int64(m.Attempts), m.NextTry, m.LastError})})
	})
}
