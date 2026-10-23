package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) PutDeployAgent(ctx context.Context, a store.DeployAgent) error {
	_, err := x.q.Exec(ctx, `INSERT INTO deploy_agents (organization_id, robot_id, set_by, set_at) VALUES ($1, $2, $3, $4)
		ON CONFLICT (organization_id) DO UPDATE SET robot_id = $2, set_by = $3, set_at = $4`, a.Organization, a.Robot, a.SetBy, a.SetAt)
	return err
}

func (x queries) DeleteDeployAgent(ctx context.Context, org string) error {
	_, err := x.q.Exec(ctx, `DELETE FROM deploy_agents WHERE organization_id = $1`, org)
	return err
}

func (x queries) deployAgent(ctx context.Context, where string, arg string) (store.DeployAgent, error) {
	var a store.DeployAgent
	err := x.q.QueryRow(ctx, `SELECT organization_id, robot_id, set_by, set_at FROM deploy_agents WHERE `+where+` = $1`, arg).
		Scan(&a.Organization, &a.Robot, &a.SetBy, &a.SetAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.DeployAgent{}, store.ErrNotFound
	}
	return a, err
}

func (x queries) DeployAgentOf(ctx context.Context, org string) (store.DeployAgent, error) {
	return x.deployAgent(ctx, "organization_id", org)
}

func (x queries) DeployAgentByRobot(ctx context.Context, robot string) (store.DeployAgent, error) {
	return x.deployAgent(ctx, "robot_id", robot)
}

func (x queries) OwnerCells(ctx context.Context, owner string) ([]string, error) {
	rows, err := x.q.Query(ctx, `SELECT DISTINCT cell FROM repositories WHERE owner_id = $1 AND state <> 'creating' ORDER BY cell`, owner)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
