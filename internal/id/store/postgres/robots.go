package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) InsertOrganizationRobot(ctx context.Context, robot, org string) error {
	_, err := x.q.Exec(ctx, `INSERT INTO organization_robots (principal_id, organization_id) VALUES ($1, $2)`, robot, org)
	return err
}

func (x queries) RobotOrganization(ctx context.Context, robot string) (string, error) {
	var org string
	err := x.q.QueryRow(ctx, `SELECT organization_id FROM organization_robots WHERE principal_id = $1`, robot).Scan(&org)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrNotFound
	}
	return org, err
}

func (x queries) OrganizationRobots(ctx context.Context, org string) ([]store.Principal, error) {
	rows, err := x.q.Query(ctx, `SELECT `+principalColumns+` FROM principals
		WHERE id IN (SELECT principal_id FROM organization_robots WHERE organization_id = $1) ORDER BY name`, org)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (store.Principal, error) { return scanPrincipal(row) })
}

func (x queries) InsertIssuer(ctx context.Context, org, issuer string, at time.Time) error {
	_, err := x.q.Exec(ctx, `INSERT INTO organization_issuers (organization_id, issuer, created_at) VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`, org, issuer, at)
	return err
}

func (x queries) DeleteIssuer(ctx context.Context, org, issuer string) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM organization_issuers WHERE organization_id = $1 AND issuer = $2`, org, issuer)
	if err != nil {
		return false, err
	}
	if _, err := x.q.Exec(ctx, `DELETE FROM robot_bindings WHERE organization_id = $1 AND issuer = $2`, org, issuer); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (x queries) Issuers(ctx context.Context, org string) ([]string, error) {
	rows, err := x.q.Query(ctx, `SELECT issuer FROM organization_issuers WHERE organization_id = $1 ORDER BY issuer`, org)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (x queries) TrustsIssuer(ctx context.Context, org, issuer string) (bool, error) {
	var ok bool
	err := x.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM organization_issuers WHERE organization_id = $1 AND issuer = $2)`, org, issuer).Scan(&ok)
	return ok, err
}

func (x queries) OrganizationBindings(ctx context.Context, org string) (map[string][]store.Binding, error) {
	rows, err := x.q.Query(ctx, `SELECT principal_id, issuer, subject FROM robot_bindings WHERE organization_id = $1 ORDER BY issuer, subject`, org)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]store.Binding{}
	for rows.Next() {
		var id string
		b := store.Binding{Organization: org}
		if err := rows.Scan(&id, &b.Issuer, &b.Subject); err != nil {
			return nil, err
		}
		out[id] = append(out[id], b)
	}
	return out, rows.Err()
}

func (x queries) DeleteOrganizationBinding(ctx context.Context, robot string, b store.Binding) (bool, error) {
	tag, err := x.q.Exec(ctx, `DELETE FROM robot_bindings WHERE principal_id = $1 AND organization_id = $2 AND issuer = $3 AND subject = $4`,
		robot, b.Organization, b.Issuer, b.Subject)
	return tag.RowsAffected() == 1, err
}
