package spannerstore

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) NetworkRepositories(ctx context.Context, network int64) ([]store.Repository, error) {
	return x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE network_id = @net ORDER BY repository_id`,
		map[string]any{"net": network})
}

const moveColumns = `network_id, source, destination, phase, asked_by, asked_at, frozen_at, updated_at, next_at, error`

func scanMove(row *spanner.Row) (store.Move, error) {
	var m store.Move
	var phase string
	var frozen spanner.NullTime
	err := row.Columns(&m.Network, &m.Source, &m.Destination, &phase, &m.AskedBy, &m.AskedAt, &frozen, &m.UpdatedAt, &m.NextAt, &m.Error)
	m.Phase = store.MovePhase(phase)
	if frozen.Valid {
		m.FrozenAt = frozen.Time
	}
	return m, err
}

// moves reads the moves a query selects, in its order.
func moves(ctx context.Context, r spannerdata.Reader, sql string, params map[string]any) ([]store.Move, error) {
	var out []store.Move
	err := spannerdata.Each(ctx, r, sql, params, func(row *spanner.Row) error {
		m, err := scanMove(row)
		if err != nil {
			return err
		}
		out = append(out, m)
		return nil
	})
	return out, err
}

func oneMove(ms []store.Move, err error) (store.Move, error) {
	if err != nil {
		return store.Move{}, err
	}
	if len(ms) == 0 {
		return store.Move{}, store.ErrNotFound
	}
	return ms[0], nil
}

func (x queries) PutMove(ctx context.Context, m store.Move) error {
	frozen := spanner.NullTime{Time: m.FrozenAt, Valid: !m.FrozenAt.IsZero()}
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdate("network_moves",
			[]string{"network_id", "source", "destination", "phase", "asked_by", "asked_at", "frozen_at", "updated_at", "next_at", "error"},
			[]any{m.Network, m.Source, m.Destination, string(m.Phase), m.AskedBy, m.AskedAt, frozen, m.UpdatedAt, m.NextAt, m.Error})})
	})
}

func (x queries) LockMove(ctx context.Context, network int64) (store.Move, error) {
	var out store.Move
	err := x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		var err error
		out, err = oneMove(moves(ctx, tx, `SELECT `+moveColumns+` FROM network_moves WHERE network_id = @net FOR UPDATE`,
			map[string]any{"net": network}))
		return err
	})
	return out, err
}

func (x queries) Move(ctx context.Context, network int64) (store.Move, error) {
	var out store.Move
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		out, err = oneMove(moves(ctx, r, `SELECT `+moveColumns+` FROM network_moves WHERE network_id = @net`, map[string]any{"net": network}))
		return err
	})
	return out, err
}

func (x queries) Moves(ctx context.Context, limit int) ([]store.Move, error) {
	var out []store.Move
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		out, err = moves(ctx, r, `SELECT `+moveColumns+` FROM network_moves ORDER BY asked_at DESC, network_id LIMIT @limit`,
			map[string]any{"limit": int64(limit)})
		return err
	})
	return out, err
}

func (x queries) DueMoves(ctx context.Context, now time.Time, limit int) ([]int64, error) {
	var out []int64
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT network_id FROM network_moves WHERE phase NOT IN ('done', 'abandoned') AND next_at <= @now
			ORDER BY next_at LIMIT @limit`, map[string]any{"now": now, "limit": int64(limit)}, func(row *spanner.Row) error {
			var n int64
			if err := row.Columns(&n); err != nil {
				return err
			}
			out = append(out, n)
			return nil
		})
	})
	return out, err
}

func (x queries) OwnerRegion(ctx context.Context, owner string) (string, error) {
	var region string
	err := x.Read(func(r spannerdata.Reader) error {
		region = ""
		_, err := spannerdata.First(ctx, r, `SELECT region FROM owner_regions WHERE principal_id = @owner`, map[string]any{"owner": owner}, &region)
		return err
	})
	return region, err
}

func (x queries) SetOwnerRegion(ctx context.Context, owner, region string) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		if region == "" {
			return tx.BufferWrite([]*spanner.Mutation{spanner.Delete("owner_regions", spanner.Key{owner})})
		}
		return tx.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdate("owner_regions", []string{"principal_id", "region"}, []any{owner, region})})
	})
}
