package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/Cidan/ok/internal/deploy/tunnel"
	deploypb "github.com/Cidan/ok/proto/ok/deploy/v1"
)

// Clusters are where repositories deploy (docs/hosted.md, "Deploy agents, as built").
type Clusters interface {
	// For is the cluster a repository's deployments roll in; an error wrapping tunnel.ErrNoAgent
	// while its owner's agent is not connected — or, in a hosted service, its owner designated none.
	For(ctx context.Context, repo int64) (Cluster, error)
}

// Cluster is where a repository deploys.
type Cluster struct {
	Kube kubernetes.Interface
	// Migrate, when set, runs its migrations in the cluster, by its owner's agent; unset, they run at
	// the deployer, on its checkout.
	Migrate func(ctx context.Context, repo int64, commit []byte, target string, allow []string, secrets map[string][]byte) (Applied, error)
}

// AgentClusters send each repository's deployments to its owner's deploy agent, if its owner
// designated one; to the deployer's own cluster otherwise — in an installation alone: a hosted
// service's repositories wait until their owner's agent connects.
type AgentClusters struct {
	Local kubernetes.Interface
	Hub   *tunnel.Hub
	// Owner is a repository's owner's stable ID ("" before the directory said); Designated whether
	// an account designated a deploy agent.
	Owner      func(ctx context.Context, repo int64) (string, error)
	Designated func(ctx context.Context, account string) (bool, error)
	Hosted     bool

	mu    sync.Mutex
	kubes map[string]kubernetes.Interface
}

func (c *AgentClusters) For(ctx context.Context, repo int64) (Cluster, error) {
	owner, err := c.Owner(ctx, repo)
	if err != nil {
		return Cluster{}, err
	}
	designated := false
	if owner != "" {
		if designated, err = c.Designated(ctx, owner); err != nil {
			return Cluster{}, err
		}
	}
	switch {
	case designated:
	case !c.Hosted:
		return Cluster{Kube: c.Local}, nil
	case owner == "":
		return Cluster{}, fmt.Errorf("%w: the directory has not said who owns repository %d yet", tunnel.ErrNoAgent, repo)
	default:
		return Cluster{}, fmt.Errorf("%w: its owner has designated none", tunnel.ErrNoAgent)
	}
	kube, err := c.kube(owner)
	if err != nil {
		return Cluster{}, err
	}
	return Cluster{Kube: kube, Migrate: func(ctx context.Context, repo int64, commit []byte, target string, allow []string, secrets map[string][]byte) (Applied, error) {
		link, err := c.Hub.Link(ctx, owner)
		if err != nil {
			return Applied{}, err
		}
		req := &deploypb.MigrationRequest{RepositoryId: repo, CommitId: commit, Target: target, Allow: allow}
		for p, v := range secrets {
			req.Secrets = append(req.Secrets, &deploypb.SecretValue{Path: p, Value: v})
		}
		res, err := tunnel.Migrate(ctx, link, req)
		if err != nil {
			return Applied{}, err
		}
		return Applied{Output: res.GetOutput(), Report: res.GetReport(), Refused: res.GetRefused(), Failed: res.GetFailed()}, nil
	}}, nil
}

// kube is the client of an account's cluster, through its agent wherever it connected.
func (c *AgentClusters) kube(account string) (kubernetes.Interface, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if k := c.kubes[account]; k != nil {
		return k, nil
	}
	k, err := kubernetes.NewForConfig(&rest.Config{Host: "https://agent.invalid", QPS: 20, Burst: 40,
		Transport: &tunnel.Transport{Link: func(ctx context.Context) (tunnel.Link, error) {
			link, err := c.Hub.Link(ctx, account)
			if err != nil {
				return nil, fmt.Errorf("%w (%s's)", err, account)
			}
			return link, nil
		}}})
	if err != nil {
		return nil, err
	}
	if c.kubes == nil {
		c.kubes = map[string]kubernetes.Interface{}
	}
	c.kubes[account] = k
	return k, nil
}

// Designations are accounts' deploy agents' designations as the directory says them, each asked
// again after ttl.
type Designations struct {
	Ask func(ctx context.Context, account string) (bool, error)
	TTL time.Duration

	mu   sync.Mutex
	seen map[string]designation
}

type designation struct {
	yes bool
	at  time.Time
}

// Designated reports whether an account designated a deploy agent.
func (d *Designations) Designated(ctx context.Context, account string) (bool, error) {
	d.mu.Lock()
	s, ok := d.seen[account]
	d.mu.Unlock()
	if ok && time.Since(s.at) < d.TTL {
		return s.yes, nil
	}
	yes, err := d.Ask(ctx, account)
	if err != nil {
		return false, err
	}
	d.mu.Lock()
	if d.seen == nil {
		d.seen = map[string]designation{}
	}
	d.seen[account] = designation{yes: yes, at: time.Now()}
	d.mu.Unlock()
	return yes, nil
}
