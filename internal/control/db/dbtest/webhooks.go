package dbtest

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/move"
)

// A repository's webhooks and their deliveries: queued once however often, due by their next try,
// updated, listed latest first, pruned to the latest done, and gone with their webhook; and main's
// moves in the outbox.
func testWebhooks(t *testing.T, s db.Store) {
	ctx := move.WithRepository(context.Background(), 1)
	at := epoch.Truncate(time.Second)
	w := db.Webhook{Repository: 1, ID: 10, URL: "https://hooks.example.com/ok", Events: db.WebhookCL | db.WebhookPush, Secret: []byte{1, 2, 3},
		CreatedBy: alice, CreatedAt: at}
	if err := s.CreateWebhook(ctx, w); err != nil {
		t.Fatal(err)
	}
	if ws, err := s.Webhooks(ctx, 1); err != nil || len(ws) != 1 || ws[0].URL != w.URL || string(ws[0].Secret) != string(w.Secret) || ws[0].Events != 3 {
		t.Fatalf("webhooks: %+v, %v", ws, err)
	}
	queue := func(ids ...int64) {
		t.Helper()
		var ds []db.Delivery
		for i, id := range ids {
			ds = append(ds, db.Delivery{Repository: 1, ID: id, Webhook: 10, Event: "cl", Payload: []byte(`{}`), NextTry: at,
				CreatedAt: at.Add(time.Duration(i) * time.Second)})
		}
		if err := s.QueueDeliveries(ctx, 1, ds); err != nil {
			t.Fatal(err)
		}
	}
	queue(1, 2, 3)
	// Delivered, then queued again: it stays delivered.
	d1, ok, err := s.GetDelivery(ctx, 1, 1)
	if err != nil || !ok {
		t.Fatalf("GetDelivery = %+v, %v, %v", d1, ok, err)
	}
	d1.State, d1.Attempts, d1.LastStatus = db.DeliveryDelivered, 1, 200
	if err := s.UpdateDelivery(ctx, d1); err != nil {
		t.Fatal(err)
	}
	queue(1)
	if d, _, _ := s.GetDelivery(ctx, 1, 1); d.State != db.DeliveryDelivered || d.LastStatus != 200 {
		t.Errorf("a delivered delivery queued again: %+v", d)
	}
	due, err := s.DueDeliveries(ctx, 1, at, 10)
	if err != nil || len(due) != 2 || due[0].ID != 2 {
		t.Errorf("due: %+v, %v", due, err)
	}
	d2 := due[0]
	d2.NextTry, d2.Attempts, d2.LastError = at.Add(time.Hour), 1, "refused"
	if err := s.UpdateDelivery(ctx, d2); err != nil {
		t.Fatal(err)
	}
	if next, err := s.NextDelivery(ctx, 1); err != nil || !next.Equal(at) {
		t.Errorf("the next due: %v, %v", next, err)
	}
	if repos, err := s.WaitingDeliveries(ctx); err != nil || !slices.Equal(repos, []int64{1}) {
		t.Errorf("repositories with deliveries waiting: %v, %v", repos, err)
	}
	if work, err := s.Work().Due(ctx, db.WebhookWork, 10); err != nil || !slices.Equal(work, []int64{1}) {
		t.Errorf("the webhooks' work: %v, %v", work, err)
	}
	ds, err := s.Deliveries(ctx, 1, 10, 10)
	if err != nil || len(ds) != 3 || ds[0].ID != 3 || ds[2].ID != 1 {
		t.Errorf("latest first: %+v, %v", ds, err)
	}
	// Pruned to the latest one: of the done, only the delivered first goes; waiting ones stay.
	if err := s.PruneDeliveries(ctx, 1, 10, 1); err != nil {
		t.Fatal(err)
	}
	if ds, _ := s.Deliveries(ctx, 1, 10, 10); len(ds) != 2 || ds[0].ID != 3 || ds[1].ID != 2 {
		t.Errorf("after pruning: %+v", ds)
	}
	if err := s.DeleteWebhook(ctx, 1, 10); err != nil {
		t.Fatal(err)
	}
	if ws, _ := s.Webhooks(ctx, 1); len(ws) != 0 {
		t.Errorf("a removed webhook: %+v", ws)
	}
	if ds, _ := s.Deliveries(ctx, 1, 10, 10); len(ds) != 0 {
		t.Errorf("a removed webhook's deliveries: %+v", ds)
	}

	if err := s.QueueHeadMoved(ctx, 1, 5, []byte{0xab}); err != nil {
		t.Fatal(err)
	}
	es, err := s.Outbox(ctx, 1, 10)
	if err != nil || len(es) != 1 || es[0].Number != 0 || es[0].HeadSeq != 5 || string(es[0].Head) != "\xab" {
		t.Errorf("main's move in the outbox: %+v, %v", es, err)
	}
}
