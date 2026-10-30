package spannerdata

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannertxn"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
)

// WaitsDDL makes the table a service keeps the live requests waiting on its subjects in
// (internal/wait), as pgdata.WaitsSchema does on PostgreSQL.
var WaitsDDL = []string{`CREATE TABLE waiter (
    subject   STRING(MAX) NOT NULL,
    inbox     STRING(MAX) NOT NULL,
    lapses_at TIMESTAMP   NOT NULL,
) PRIMARY KEY (subject, inbox)`, `CREATE INDEX waiter_inbox ON waiter (inbox)`}

// Waits are the registrations in c's database (wait.Store).
func Waits(c *spanner.Client) wait.Store { return waits{c} }

type waits struct{ c *spanner.Client }

func (w waits) Register(ctx context.Context, inbox string, subjects []string, lapses time.Time) error {
	_, err := spannertxn.ReadWrite(ctx, w.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		if _, err := update(ctx, tx, `INSERT OR UPDATE INTO waiter (subject, inbox, lapses_at) SELECT s, @inbox, @lapses FROM UNNEST(@subjects) AS s`,
			map[string]any{"subjects": subjects, "inbox": inbox, "lapses": lapses}); err != nil {
			return err
		}
		return PendStep(tx, wait.LapseKind, wait.Key(inbox), lapses.Add(time.Second), []byte(inbox))
	})
	return err
}

// deleteOf removes inbox's registrations lapsed at before (all, for the zero time): found through
// the index of inboxes, deleted by key.
func (w waits) deleteOf(ctx context.Context, inbox string, before time.Time) error {
	_, err := spannertxn.ReadWrite(ctx, w.c, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var muts []*spanner.Mutation
		err := tx.Query(ctx, spanner.Statement{SQL: `SELECT subject, lapses_at FROM waiter@{FORCE_INDEX=waiter_inbox} WHERE inbox = @inbox`,
			Params: map[string]any{"inbox": inbox}}).Do(func(r *spanner.Row) error {
			var subject string
			var lapses time.Time
			if err := r.Columns(&subject, &lapses); err != nil {
				return err
			}
			if before.IsZero() || !lapses.After(before) {
				muts = append(muts, spanner.Delete("waiter", spanner.Key{subject, inbox}))
			}
			return nil
		})
		if err != nil || len(muts) == 0 {
			return err
		}
		return tx.BufferWrite(muts)
	})
	return err
}

func (w waits) Unregister(ctx context.Context, inbox string) error {
	return w.deleteOf(ctx, inbox, time.Time{})
}

func (w waits) Waiting(ctx context.Context, subjects []string, now time.Time) ([]string, error) {
	var out []string
	err := w.c.Single().Query(ctx, spanner.Statement{SQL: `SELECT DISTINCT inbox FROM waiter WHERE subject IN UNNEST(@subjects) AND lapses_at > @now`,
		Params: map[string]any{"subjects": subjects, "now": now}}).Do(func(r *spanner.Row) error {
		var in string
		if err := r.Columns(&in); err != nil {
			return err
		}
		out = append(out, in)
		return nil
	})
	return out, err
}

func (w waits) Lapse(ctx context.Context, inbox string, now time.Time) error {
	return w.deleteOf(ctx, inbox, now)
}

func (w waits) Jobs() work.Jobs { return work.JobsOf(w.c) }
