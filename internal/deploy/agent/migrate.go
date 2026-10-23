package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/Cidan/ok/internal/checkout"
	"github.com/Cidan/ok/internal/deploy/engine"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Migrations runs deployments' migrations in the agent's cluster, with Bazel in its own environment:
// of its organization's repositories alone, at commits on their main alone — so a deployer, whatever
// it asks, runs no one else's code and nothing unreviewed with the cluster's databases' credentials
// — one at a time, on its checkout.
type Migrations struct {
	// Identity says whose a repository is; SCM reads main, as the agent's robot.
	Identity identitypb.DeployAgentsClient
	SCM      interface {
		scmpb.RepositoriesClient
		scmpb.BrowseClient
	}
	Checkout checkout.Checkout
	Bazel    *engine.Bazel

	mu sync.Mutex
}

// Run runs a migration, saying what it did.
func (m *Migrations) Run(ctx context.Context, req *deploypb.MigrationRequest) *deploypb.MigrationResult {
	repo, commit := req.GetRepositoryId(), req.GetCommitId()
	owned, err := m.Identity.AgentRepository(ctx, &identitypb.RepositoryRef{Id: repo})
	if err != nil {
		return &deploypb.MigrationResult{Error: fmt.Sprintf("asking whose repository %d is: %v", repo, err)}
	}
	if !owned.GetOwned() {
		return &deploypb.MigrationResult{Failed: fmt.Sprintf("repository %d is not this agent's organization's: it runs none of its migrations", repo)}
	}
	r, err := m.SCM.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
	if err != nil {
		return &deploypb.MigrationResult{Error: fmt.Sprintf("reading %s: %v", owned.GetFullName(), err)}
	}
	on, err := m.SCM.IsAncestor(ctx, &scmpb.IsAncestorRequest{RepositoryId: repo, Ancestor: commit, Descendant: r.GetHead()})
	if err != nil {
		return &deploypb.MigrationResult{Error: fmt.Sprintf("reading %s's main: %v", owned.GetFullName(), err)}
	}
	if !on.GetIsAncestor() {
		return &deploypb.MigrationResult{Failed: fmt.Sprintf("%x is not on %s's main: the agent runs only what has landed", commit, owned.GetFullName())}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.Checkout.Sync(ctx, repo, commit, nil); err != nil {
		return &deploypb.MigrationResult{Error: fmt.Sprintf("checking out %x: %v", commit, err)}
	}
	values := map[string][]byte{}
	for _, s := range req.GetSecrets() {
		values[s.GetPath()] = s.GetValue()
	}
	defer func() {
		for _, v := range values {
			clear(v)
		}
	}()
	a, err := m.Bazel.Migrate(ctx, req.GetTarget(), req.GetAllow(), values)
	if err != nil {
		return &deploypb.MigrationResult{Error: err.Error()}
	}
	return &deploypb.MigrationResult{Report: a.Report, Refused: a.Refused, Output: a.Output, Failed: a.Failed}
}
