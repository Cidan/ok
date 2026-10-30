package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/work"
)

// Work is identity's keyed work's store.
func (b *Backend) Work() work.Store { return pgdata.Work(b.pool) }

func (x queries) AddNotification(ctx context.Context, n store.Notification) (bool, error) {
	tag, err := x.q.Exec(ctx, `INSERT INTO notifications (principal_id, source, kind, repository_id, number, repository, title, actor_id, text, link, notified_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (principal_id, source) DO NOTHING`,
		n.Principal, n.Source, int16(n.Kind), n.Repository, n.Number, n.RepositoryName, n.Title, n.Actor, n.Text, n.Link, n.At)
	return tag.RowsAffected() > 0, err
}

func (x queries) NotificationsOf(ctx context.Context, principal string, unread bool, limit int) ([]store.Notification, error) {
	rows, err := x.q.Query(ctx, `SELECT principal_id, source, kind, repository_id, number, repository, title, actor_id, text, link, notified_at, read
		FROM notifications WHERE principal_id = $1 AND (NOT $2 OR NOT read) ORDER BY notified_at DESC, source LIMIT $3`, principal, unread, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Notification, error) {
		var n store.Notification
		var kind int16
		err := row.Scan(&n.Principal, &n.Source, &kind, &n.Repository, &n.Number, &n.RepositoryName, &n.Title, &n.Actor, &n.Text, &n.Link, &n.At, &n.Read)
		n.Kind = int(kind)
		return n, err
	})
}

func (x queries) UnreadNotifications(ctx context.Context, principal string) (int64, error) {
	var n int64
	err := x.q.QueryRow(ctx, `SELECT count(*) FROM notifications WHERE principal_id = $1 AND NOT read`, principal).Scan(&n)
	return n, err
}

func (x queries) MarkNotificationsRead(ctx context.Context, principal string, sources []string, all bool) error {
	_, err := x.q.Exec(ctx, `UPDATE notifications SET read = true WHERE principal_id = $1 AND NOT read AND ($3 OR source = ANY($2))`,
		principal, sources, all)
	return err
}

func (x queries) NotificationPrefs(ctx context.Context, principal string) (uint64, bool, error) {
	var m int64
	err := x.q.QueryRow(ctx, `SELECT mailed FROM notification_prefs WHERE principal_id = $1`, principal).Scan(&m)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	return uint64(m), err == nil, err
}

func (x queries) SetNotificationPrefs(ctx context.Context, principal string, mailed uint64) error {
	_, err := x.q.Exec(ctx, `INSERT INTO notification_prefs (principal_id, mailed) VALUES ($1, $2)
		ON CONFLICT (principal_id) DO UPDATE SET mailed = $2`, principal, int64(mailed))
	return err
}

func (x queries) MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error {
	return pgdata.MarkWorkAt(ctx, x.q, kind, key, at)
}

func (x queries) QueueMail(ctx context.Context, m store.Mail) error {
	if _, err := x.q.Exec(ctx, `INSERT INTO mail_outbox (mail_id, principal_id, to_address, subject, body, state, attempts, next_try, last_error, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`, m.ID, m.Principal, m.To, m.Subject, m.Body, int16(m.State), m.Attempts, m.NextTry,
		m.LastError, m.CreatedAt); err != nil {
		return err
	}
	return pgdata.MarkWork(ctx, x.q, store.MailWork, m.ID)
}

func (x queries) MailByID(ctx context.Context, id int64) (store.Mail, bool, error) {
	var m store.Mail
	var state int16
	err := x.q.QueryRow(ctx, `SELECT mail_id, principal_id, to_address, subject, body, state, attempts, next_try, last_error, created_at
		FROM mail_outbox WHERE mail_id = $1`, id).Scan(&m.ID, &m.Principal, &m.To, &m.Subject, &m.Body, &state, &m.Attempts, &m.NextTry,
		&m.LastError, &m.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Mail{}, false, nil
	}
	m.State = int(state)
	return m, err == nil, err
}

func (x queries) UpdateMail(ctx context.Context, m store.Mail) error {
	_, err := x.q.Exec(ctx, `UPDATE mail_outbox SET state = $2, attempts = $3, next_try = $4, last_error = $5 WHERE mail_id = $1`,
		m.ID, int16(m.State), m.Attempts, m.NextTry, m.LastError)
	return err
}
