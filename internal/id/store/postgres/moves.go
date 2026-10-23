package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) NetworkRepositories(ctx context.Context, network int64) ([]store.Repository, error) {
	return x.repositories(ctx, `SELECT `+repositoryColumns+` FROM repositories WHERE network_id = $1 ORDER BY id`, network)
}

const moveColumns = `network_id, source, destination, phase, asked_by, asked_at, frozen_at, updated_at, next_at, error`

func scanMove(row pgx.Row) (store.Move, error) {
	var m store.Move
	var phase string
	var frozen *time.Time
	err := row.Scan(&m.Network, &m.Source, &m.Destination, &phase, &m.AskedBy, &m.AskedAt, &frozen, &m.UpdatedAt, &m.NextAt, &m.Error)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Move{}, store.ErrNotFound
	}
	m.Phase = store.MovePhase(phase)
	if frozen != nil {
		m.FrozenAt = *frozen
	}
	return m, err
}

func (x queries) PutMove(ctx context.Context, m store.Move) error {
	var frozen *time.Time
	if !m.FrozenAt.IsZero() {
		frozen = &m.FrozenAt
	}
	_, err := x.q.Exec(ctx, `INSERT INTO network_moves (`+moveColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (network_id) DO UPDATE SET source = $2, destination = $3, phase = $4, asked_by = $5, asked_at = $6, frozen_at = $7,
			updated_at = $8, next_at = $9, error = $10`,
		m.Network, m.Source, m.Destination, string(m.Phase), m.AskedBy, m.AskedAt, frozen, m.UpdatedAt, m.NextAt, m.Error)
	return err
}

func (x queries) LockMove(ctx context.Context, network int64) (store.Move, error) {
	return scanMove(x.q.QueryRow(ctx, `SELECT `+moveColumns+` FROM network_moves WHERE network_id = $1 FOR UPDATE`, network))
}

func (x queries) Move(ctx context.Context, network int64) (store.Move, error) {
	return scanMove(x.q.QueryRow(ctx, `SELECT `+moveColumns+` FROM network_moves WHERE network_id = $1`, network))
}

func (x queries) Moves(ctx context.Context, limit int) ([]store.Move, error) {
	rows, err := x.q.Query(ctx, `SELECT `+moveColumns+` FROM network_moves ORDER BY asked_at DESC, network_id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []store.Move
	for rows.Next() {
		m, err := scanMove(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (x queries) DueMoves(ctx context.Context, now time.Time, limit int) ([]int64, error) {
	rows, err := x.q.Query(ctx, `SELECT network_id FROM network_moves WHERE phase NOT IN ('done', 'abandoned') AND next_at <= $1
		ORDER BY next_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func (x queries) OwnerRegion(ctx context.Context, owner string) (string, error) {
	var region string
	err := x.q.QueryRow(ctx, `SELECT region FROM owner_regions WHERE principal_id = $1`, owner).Scan(&region)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return region, err
}

func (x queries) SetOwnerRegion(ctx context.Context, owner, region string) error {
	if region == "" {
		_, err := x.q.Exec(ctx, `DELETE FROM owner_regions WHERE principal_id = $1`, owner)
		return err
	}
	_, err := x.q.Exec(ctx, `INSERT INTO owner_regions (principal_id, region) VALUES ($1, $2)
		ON CONFLICT (principal_id) DO UPDATE SET region = $2`, owner, region)
	return err
}
