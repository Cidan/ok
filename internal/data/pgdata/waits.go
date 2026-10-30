package pgdata

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
)

// WaitsSchema makes the table a service keeps the live requests waiting on its subjects in
// (internal/wait); a service whose writes say what they changed includes it in its migrations.
const WaitsSchema = `CREATE TABLE waiter (
    subject   text        NOT NULL,
    inbox     text        NOT NULL,
    lapses_at timestamptz NOT NULL,
    PRIMARY KEY (subject, inbox)
);
CREATE INDEX waiter_inbox ON waiter (inbox);`

// Waits are the registrations in pool's database (wait.Store).
func Waits(pool *pgxpool.Pool) wait.Store { return waits{pool} }

type waits struct{ pool *pgxpool.Pool }

func (w waits) Register(ctx context.Context, inbox string, subjects []string, lapses time.Time) error {
	return Txn(ctx, w.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO waiter (subject, inbox, lapses_at) SELECT s, $2, $3 FROM unnest($1::text[]) AS s
			ON CONFLICT (subject, inbox) DO UPDATE SET lapses_at = EXCLUDED.lapses_at`, subjects, inbox, lapses); err != nil {
			return err
		}
		return PendStep(tx, wait.LapseKind, wait.Key(inbox), lapses.Add(time.Second), []byte(inbox))
	})
}

func (w waits) Unregister(ctx context.Context, inbox string) error {
	_, err := w.pool.Exec(ctx, `DELETE FROM waiter WHERE inbox = $1`, inbox)
	return err
}

func (w waits) Waiting(ctx context.Context, subjects []string, now time.Time) ([]string, error) {
	rows, err := w.pool.Query(ctx, `SELECT DISTINCT inbox FROM waiter WHERE subject = ANY($1) AND lapses_at > $2`, subjects, now)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (w waits) Lapse(ctx context.Context, inbox string, now time.Time) error {
	_, err := w.pool.Exec(ctx, `DELETE FROM waiter WHERE inbox = $1 AND lapses_at <= $2`, inbox, now)
	return err
}

func (w waits) Jobs() work.Jobs { return work.JobsOf(w.pool) }
