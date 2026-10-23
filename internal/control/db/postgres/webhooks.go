package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/data"
)

func (x queries) QueueHeadMoved(ctx context.Context, repo, seq int64, head []byte) error {
	_, err := x.q.Exec(ctx, `WITH ob AS (
			INSERT INTO outbox (repository_id, entry_id, number, queued_at, head_seq, head) VALUES ($1, $2, 0, clock_timestamp(), $3, $4)
		)
		INSERT INTO work (kind, key, dirty) VALUES ($5, $1, true) ON CONFLICT (kind, key) DO UPDATE SET dirty = true, due_at = NULL`,
		repo, data.NewID(), seq, head, db.OutboxWork)
	return err
}

func (x queries) CreateWebhook(ctx context.Context, w db.Webhook) error {
	_, err := x.q.Exec(ctx, `INSERT INTO webhook (repository_id, webhook_id, url, events, secret, created_by, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, w.Repository, w.ID, w.URL, w.Events, w.Secret, w.CreatedBy, w.CreatedAt)
	return err
}

func (x queries) Webhooks(ctx context.Context, repo int64) ([]db.Webhook, error) {
	rows, err := x.q.Query(ctx, `SELECT repository_id, webhook_id, url, events, secret, created_by, created_at FROM webhook
		WHERE repository_id = $1 ORDER BY created_at, webhook_id`, repo)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (db.Webhook, error) {
		var w db.Webhook
		err := row.Scan(&w.Repository, &w.ID, &w.URL, &w.Events, &w.Secret, &w.CreatedBy, &w.CreatedAt)
		return w, err
	})
}

func (x queries) DeleteWebhook(ctx context.Context, repo, id int64) error {
	_, err := x.q.Exec(ctx, `WITH d AS (DELETE FROM webhook_delivery WHERE repository_id = $1 AND webhook_id = $2)
		DELETE FROM webhook WHERE repository_id = $1 AND webhook_id = $2`, repo, id)
	return err
}

func (x queries) QueueDeliveries(ctx context.Context, repo int64, ds []db.Delivery) error {
	if len(ds) == 0 {
		return nil
	}
	ids, hooks, events, payloads, tries, created := make([]int64, len(ds)), make([]int64, len(ds)), make([]string, len(ds)),
		make([][]byte, len(ds)), make([]time.Time, len(ds)), make([]time.Time, len(ds))
	for i, d := range ds {
		ids[i], hooks[i], events[i], payloads[i], tries[i], created[i] = d.ID, d.Webhook, d.Event, d.Payload, d.NextTry, d.CreatedAt
	}
	_, err := x.q.Exec(ctx, `WITH q AS (
			INSERT INTO webhook_delivery (repository_id, delivery_id, webhook_id, event, payload, state, attempts, next_try, last_status, last_error,
				created_at)
			SELECT $1, d, w, e, p, 0, 0, t, 0, '', c FROM unnest($2::bigint[], $3::bigint[], $4::text[], $5::bytea[], $6::timestamptz[],
				$7::timestamptz[]) AS u(d, w, e, p, t, c)
			ON CONFLICT (repository_id, delivery_id) DO NOTHING
		)
		INSERT INTO work (kind, key, dirty) VALUES ($8, $1, true) ON CONFLICT (kind, key) DO UPDATE SET dirty = true, due_at = NULL`,
		repo, ids, hooks, events, payloads, tries, created, db.WebhookWork)
	return err
}

const deliveryColumns = `repository_id, delivery_id, webhook_id, event, payload, state, attempts, next_try, last_status, last_error, created_at`

func collectDeliveries(rows pgx.Rows, err error) ([]db.Delivery, error) {
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (db.Delivery, error) { return scanDelivery(row) })
}

func scanDelivery(row pgx.Row) (db.Delivery, error) {
	var d db.Delivery
	var state int16
	err := row.Scan(&d.Repository, &d.ID, &d.Webhook, &d.Event, &d.Payload, &state, &d.Attempts, &d.NextTry, &d.LastStatus, &d.LastError, &d.CreatedAt)
	d.State = int(state)
	return d, err
}

func (x queries) DueDeliveries(ctx context.Context, repo int64, before time.Time, limit int) ([]db.Delivery, error) {
	return collectDeliveries(x.q.Query(ctx, `SELECT `+deliveryColumns+` FROM webhook_delivery
		WHERE repository_id = $1 AND state = 0 AND next_try <= $2 ORDER BY created_at, delivery_id LIMIT $3`, repo, before, limit))
}

func (x queries) NextDelivery(ctx context.Context, repo int64) (time.Time, error) {
	var at *time.Time
	err := x.q.QueryRow(ctx, `SELECT min(next_try) FROM webhook_delivery WHERE repository_id = $1 AND state = 0`, repo).Scan(&at)
	if err != nil || at == nil {
		return time.Time{}, err
	}
	return *at, nil
}

func (x queries) UpdateDelivery(ctx context.Context, d db.Delivery) error {
	_, err := x.q.Exec(ctx, `UPDATE webhook_delivery SET state = $3, attempts = $4, next_try = $5, last_status = $6, last_error = $7
		WHERE repository_id = $1 AND delivery_id = $2`, d.Repository, d.ID, int16(d.State), d.Attempts, d.NextTry, d.LastStatus, d.LastError)
	return err
}

func (x queries) Deliveries(ctx context.Context, repo, webhook int64, limit int) ([]db.Delivery, error) {
	return collectDeliveries(x.q.Query(ctx, `SELECT `+deliveryColumns+` FROM webhook_delivery
		WHERE repository_id = $1 AND webhook_id = $2 ORDER BY created_at DESC, delivery_id LIMIT $3`, repo, webhook, limit))
}

func (x queries) GetDelivery(ctx context.Context, repo, id int64) (db.Delivery, bool, error) {
	d, err := scanDelivery(x.q.QueryRow(ctx, `SELECT `+deliveryColumns+` FROM webhook_delivery WHERE repository_id = $1 AND delivery_id = $2`, repo, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Delivery{}, false, nil
	}
	return d, err == nil, err
}

func (x queries) PruneDeliveries(ctx context.Context, repo, webhook int64, keep int) error {
	_, err := x.q.Exec(ctx, `DELETE FROM webhook_delivery WHERE repository_id = $1 AND webhook_id = $2 AND state <> 0 AND delivery_id IN (
			SELECT delivery_id FROM webhook_delivery WHERE repository_id = $1 AND webhook_id = $2
			ORDER BY created_at DESC, delivery_id OFFSET $3)`, repo, webhook, keep)
	return err
}

func (x queries) WaitingDeliveries(ctx context.Context) ([]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT DISTINCT repository_id FROM webhook_delivery WHERE state = 0 ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}
