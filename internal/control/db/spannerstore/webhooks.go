package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/data/spannerdata"
)

func (x queries) QueueHeadMoved(ctx context.Context, repo, seq int64, head []byte) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if _, err := spannerdata.Update(ctx, tx, `INSERT INTO outbox (repository_id, entry_id, number, queued_at, event_id, comment_id, head_seq, head)
			VALUES (@repo, @id, 0, CURRENT_TIMESTAMP(), 0, 0, @seq, @head)`,
			map[string]any{"repo": repo, "id": data.NewID(), "seq": seq, "head": head}); err != nil {
			return err
		}
		return spannerdata.MarkWork(ctx, tx, db.OutboxWork, repo)
	})
}

var webhookColumns = []string{"repository_id", "webhook_id", "url", "events", "secret", "created_by", "created_at"}

func (x queries) CreateWebhook(ctx context.Context, w db.Webhook) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.Insert("webhook", webhookColumns,
			[]any{w.Repository, w.ID, w.URL, w.Events, w.Secret, w.CreatedBy, w.CreatedAt})})
	})
}

func (x queries) Webhooks(ctx context.Context, repo int64) ([]db.Webhook, error) {
	var out []db.Webhook
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT repository_id, webhook_id, url, events, secret, created_by, created_at FROM webhook
			WHERE repository_id = @repo ORDER BY created_at, webhook_id`, map[string]any{"repo": repo}, func(row *spanner.Row) error {
			var w db.Webhook
			if err := row.Columns(&w.Repository, &w.ID, &w.URL, &w.Events, &w.Secret, &w.CreatedBy, &w.CreatedAt); err != nil {
				return err
			}
			out = append(out, w)
			return nil
		})
	})
	return out, err
}

func (x queries) DeleteWebhook(ctx context.Context, repo, id int64) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		for _, sql := range []string{
			`DELETE FROM webhook_delivery WHERE repository_id = @repo AND webhook_id = @id`,
			`DELETE FROM webhook WHERE repository_id = @repo AND webhook_id = @id`,
		} {
			if _, err := spannerdata.Update(ctx, tx, sql, map[string]any{"repo": repo, "id": id}); err != nil {
				return err
			}
		}
		return nil
	})
}

var deliveryCols = []string{"repository_id", "delivery_id", "webhook_id", "event", "payload", "state", "attempts", "next_try", "last_status",
	"last_error", "created_at"}

func (x queries) QueueDeliveries(ctx context.Context, repo int64, ds []db.Delivery) error {
	if len(ds) == 0 {
		return nil
	}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		ids := make([]int64, 0, len(ds))
		for _, d := range ds {
			ids = append(ids, d.ID)
		}
		// One queued already — a drain that stopped short, queuing again — stays as it is.
		had := map[int64]bool{}
		if err := spannerdata.Each(ctx, tx, `SELECT delivery_id FROM webhook_delivery WHERE repository_id = @repo AND delivery_id IN UNNEST(@ids)`,
			map[string]any{"repo": repo, "ids": ids}, func(row *spanner.Row) error {
				var id int64
				if err := row.Columns(&id); err != nil {
					return err
				}
				had[id] = true
				return nil
			}); err != nil {
			return err
		}
		ms := make([]*spanner.Mutation, 0, len(ds))
		for _, d := range ds {
			if had[d.ID] {
				continue
			}
			ms = append(ms, spanner.Insert("webhook_delivery", deliveryCols,
				[]any{repo, d.ID, d.Webhook, d.Event, d.Payload, int64(db.DeliveryWaiting), int64(0), d.NextTry, int64(0), "", d.CreatedAt}))
		}
		if err := tx.BufferWrite(ms); err != nil {
			return err
		}
		return spannerdata.MarkWork(ctx, tx, db.WebhookWork, repo)
	})
}

const deliverySelect = `SELECT repository_id, delivery_id, webhook_id, event, payload, state, attempts, next_try, last_status, last_error, created_at
	FROM webhook_delivery `

func (x queries) deliveries(ctx context.Context, sql string, params map[string]any) ([]db.Delivery, error) {
	var out []db.Delivery
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, deliverySelect+sql, params, func(row *spanner.Row) error {
			var d db.Delivery
			var state, attempts, status int64
			if err := row.Columns(&d.Repository, &d.ID, &d.Webhook, &d.Event, &d.Payload, &state, &attempts, &d.NextTry, &status, &d.LastError,
				&d.CreatedAt); err != nil {
				return err
			}
			d.State, d.Attempts, d.LastStatus = int(state), int(attempts), int(status)
			out = append(out, d)
			return nil
		})
	})
	return out, err
}

func (x queries) DueDeliveries(ctx context.Context, repo int64, before time.Time, limit int) ([]db.Delivery, error) {
	return x.deliveries(ctx, `WHERE repository_id = @repo AND state = 0 AND next_try <= @before ORDER BY created_at, delivery_id LIMIT @limit`,
		map[string]any{"repo": repo, "before": before, "limit": int64(limit)})
}

func (x queries) NextDelivery(ctx context.Context, repo int64) (time.Time, error) {
	var at spanner.NullTime
	err := x.Read(func(r spannerdata.Reader) error {
		_, err := spannerdata.First(ctx, r, `SELECT MIN(next_try) FROM webhook_delivery WHERE repository_id = @repo AND state = 0`,
			map[string]any{"repo": repo}, &at)
		return err
	})
	if err != nil || !at.Valid {
		return time.Time{}, err
	}
	return at.Time, nil
}

func (x queries) UpdateDelivery(ctx context.Context, d db.Delivery) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.Update("webhook_delivery",
			[]string{"repository_id", "delivery_id", "state", "attempts", "next_try", "last_status", "last_error"},
			[]any{d.Repository, d.ID, int64(d.State), int64(d.Attempts), d.NextTry, int64(d.LastStatus), d.LastError})})
	})
}

func (x queries) Deliveries(ctx context.Context, repo, webhook int64, limit int) ([]db.Delivery, error) {
	return x.deliveries(ctx, `WHERE repository_id = @repo AND webhook_id = @webhook ORDER BY created_at DESC, delivery_id LIMIT @limit`,
		map[string]any{"repo": repo, "webhook": webhook, "limit": int64(limit)})
}

func (x queries) GetDelivery(ctx context.Context, repo, id int64) (db.Delivery, bool, error) {
	ds, err := x.deliveries(ctx, `WHERE repository_id = @repo AND delivery_id = @id`, map[string]any{"repo": repo, "id": id})
	if err != nil || len(ds) == 0 {
		return db.Delivery{}, false, err
	}
	return ds[0], true, nil
}

func (x queries) PruneDeliveries(ctx context.Context, repo, webhook int64, keep int) error {
	_, err := x.Exec(ctx, `DELETE FROM webhook_delivery WHERE repository_id = @repo AND webhook_id = @webhook AND state != 0 AND delivery_id IN (
			SELECT delivery_id FROM webhook_delivery WHERE repository_id = @repo AND webhook_id = @webhook
			ORDER BY created_at DESC, delivery_id LIMIT 1000000 OFFSET @keep)`,
		map[string]any{"repo": repo, "webhook": webhook, "keep": int64(keep)})
	return err
}

func (x queries) WaitingDeliveries(ctx context.Context) ([]int64, error) {
	var out []int64
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT DISTINCT repository_id FROM webhook_delivery WHERE state = 0 ORDER BY 1`, nil,
			func(row *spanner.Row) error {
				var id int64
				if err := row.Columns(&id); err != nil {
					return err
				}
				out = append(out, id)
				return nil
			})
	})
	return out, err
}
