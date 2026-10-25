package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Cidan/ok/internal/scm/store/backend"
)

// ─── Sequencing ───

// SequencesAfterCommit: on PostgreSQL a transaction takes its repository's CL numbers and places in
// its log from their counters, as the last locks before it commits. A row lock here is waited for,
// never aborted, and the transaction's statements take microseconds, so writers queue on the
// counters briefly; and PostgreSQL has no commit timestamp to order by.
func (b *Backend) SequencesAfterCommit() bool { return false }

// errNoSequencing refuses the sequencing pass's operations, which no PostgreSQL transaction leaves
// work for.
var errNoSequencing = errors.Join(errors.ErrUnsupported, errors.New("postgres: a transaction sequences itself"))

func (x queries) Pend(context.Context, []backend.Pending) error { return errNoSequencing }

func (x queries) PendingRows(context.Context, int64, int) ([]backend.Pending, error) {
	return nil, errNoSequencing
}

func (x queries) PendingHeld(context.Context, int64, []backend.PendingKey) (map[backend.PendingKey]bool, error) {
	return nil, errNoSequencing
}

func (x queries) DeletePending(context.Context, int64, []backend.PendingKey) error {
	return errNoSequencing
}

func (x queries) NumberChangeLists(context.Context, int64, []int64, []int64) error {
	return errNoSequencing
}

func (x queries) TransactionEvents(context.Context, int64, int64) ([]int64, error) {
	return nil, errNoSequencing
}

func (x queries) PendingRepositories(context.Context, time.Time, int) ([]int64, error) {
	return nil, nil
}

func (x queries) ChangeListNumbers(ctx context.Context, repo int64, ids []int64) (map[int64]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT id, number FROM changelist WHERE repository_id = $1 AND id = ANY($2)`, repo, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var id, n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}
