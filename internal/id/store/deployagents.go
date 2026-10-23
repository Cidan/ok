package store

import (
	"context"
	"errors"
	"time"
)

// Deploy agents (docs/hosted.md, "Deploy agents, as built"): the robot of an organization's own that
// runs in its cluster, through which the deployer rolls its repositories and runs their migrations.

// DeployAgent is an organization's designation of its deploy agent.
type DeployAgent struct {
	Organization string
	Robot        string
	SetBy        string
	SetAt        time.Time
}

// SetDeployAgent designates one of an organization's robots its deploy agent — an active one, of its
// own — or none (robot ""), by a person.
func (s *Store) SetDeployAgent(ctx context.Context, org, robot, by string) error {
	if robot == "" {
		return s.b.DeleteDeployAgent(ctx, org)
	}
	return s.b.Txn(ctx, func(tx Ops) error {
		if _, err := tx.LockOrganization(ctx, org); err != nil {
			return err
		}
		of, err := tx.RobotOrganization(ctx, robot)
		if errors.Is(err, ErrNotFound) || err == nil && of != org {
			return invalid("%s is no robot of this organization", robot)
		}
		if err != nil {
			return err
		}
		p, err := tx.PrincipalByID(ctx, robot)
		if err != nil {
			return err
		}
		if !p.Active() {
			return invalid("%s is not active", p.Name)
		}
		return tx.PutDeployAgent(ctx, DeployAgent{Organization: org, Robot: robot, SetBy: by, SetAt: s.now()})
	})
}

// DeployAgentOf is an organization's agent, an active robot; ErrNotFound when it has none.
func (s *Store) DeployAgentOf(ctx context.Context, org string) (DeployAgent, error) {
	a, err := s.b.DeployAgentOf(ctx, org)
	if err != nil {
		return DeployAgent{}, err
	}
	return s.activeAgent(ctx, a)
}

// AgentAccount is the designation a robot is the agent of, while it is active; ErrNotFound when it
// is none's.
func (s *Store) AgentAccount(ctx context.Context, robot string) (DeployAgent, error) {
	a, err := s.b.DeployAgentByRobot(ctx, robot)
	if err != nil {
		return DeployAgent{}, err
	}
	return s.activeAgent(ctx, a)
}

// activeAgent is a designation whose robot is active: a deleted or suspended one's is none.
func (s *Store) activeAgent(ctx context.Context, a DeployAgent) (DeployAgent, error) {
	p, err := s.b.PrincipalByID(ctx, a.Robot)
	if err != nil {
		return DeployAgent{}, err
	}
	if !p.Active() {
		return DeployAgent{}, ErrNotFound
	}
	return a, nil
}

// OwnerCells are the cells holding an owner's made repositories.
func (s *Store) OwnerCells(ctx context.Context, owner string) ([]string, error) {
	return s.b.OwnerCells(ctx, owner)
}
