package spannerstore

import (
	"context"

	"cloud.google.com/go/spanner"

	"github.com/Cidan/ok/internal/data/spannerdata"
	"github.com/Cidan/ok/internal/id/store"
)

func (x queries) PutDeployAgent(ctx context.Context, a store.DeployAgent) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdate("deploy_agents", []string{"organization_id", "robot_id", "set_by", "set_at"},
			[]any{a.Organization, a.Robot, a.SetBy, a.SetAt})})
	})
}

func (x queries) DeleteDeployAgent(ctx context.Context, org string) error {
	return x.Write(ctx, func(tx *spanner.ReadWriteTransaction) error {
		return tx.BufferWrite([]*spanner.Mutation{spanner.Delete("deploy_agents", spanner.Key{org})})
	})
}

func (x queries) deployAgent(ctx context.Context, where, arg string) (store.DeployAgent, error) {
	var a store.DeployAgent
	var ok bool
	err := x.Read(func(r spannerdata.Reader) error {
		var err error
		ok, err = spannerdata.First(ctx, r, `SELECT organization_id, robot_id, set_by, set_at FROM deploy_agents WHERE `+where+` = @arg`,
			map[string]any{"arg": arg}, &a.Organization, &a.Robot, &a.SetBy, &a.SetAt)
		return err
	})
	if err == nil && !ok {
		err = store.ErrNotFound
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
	var out []string
	err := x.Read(func(r spannerdata.Reader) error {
		out = nil
		return spannerdata.Each(ctx, r, `SELECT DISTINCT cell FROM repositories WHERE owner_id = @owner AND state != 'creating' ORDER BY cell`,
			map[string]any{"owner": owner}, func(row *spanner.Row) error {
				var c string
				if err := row.Columns(&c); err != nil {
					return err
				}
				out = append(out, c)
				return nil
			})
	})
	return out, err
}
