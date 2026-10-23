package service

import (
	"context"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/automation/cache"
	"github.com/Cidan/ok/internal/automation/store"
)

// RunHeader names the run a remote cache call is for: its worker's pass-through sets it (docs/hosted.md,
// "Hosted automation, as built").
const RunHeader = "x-ok-run"

// namespacesKept is how long a run's namespaces are used before they are worked out again.
const namespacesKept = 30 * time.Second

// Namespaces are the remote cache's calls' namespaces, from the run each names: a cache.Resolve.
// A run's account's namespace for post, deploys, and the pre and manual runs of someone who may
// land on the repository; a pre whose author may not writes <account>/u/<author>, and reads that,
// then the account's. A call for a run that does not run is refused.
type Namespaces struct {
	Store   *store.Store
	Checker *authz.Checker

	mu   sync.Mutex
	kept map[int64]keptNamespaces
}

type keptNamespaces struct {
	ns cache.Namespaces
	at time.Time
}

// Resolve resolves a call's namespaces.
func (n *Namespaces) Resolve(ctx context.Context) (cache.Namespaces, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	vs := md.Get(RunHeader)
	if len(vs) != 1 {
		return cache.Namespaces{}, status.Errorf(codes.InvalidArgument, "a remote cache call names its run (%s)", RunHeader)
	}
	id, err := strconv.ParseInt(vs[0], 10, 64)
	if err != nil {
		return cache.Namespaces{}, status.Errorf(codes.InvalidArgument, "%s is a run's ID", RunHeader)
	}
	n.mu.Lock()
	if k, ok := n.kept[id]; ok && time.Since(k.at) < namespacesKept {
		n.mu.Unlock()
		return k.ns, nil
	}
	n.mu.Unlock()
	run, err := n.Store.Get(ctx, id)
	if err != nil {
		return cache.Namespaces{}, toStatus(err)
	}
	if run.Status != store.StatusRunning {
		return cache.Namespaces{}, status.Errorf(codes.PermissionDenied, "run %d is not running: its cache is closed", id)
	}
	account, err := n.Store.OwnerOf(ctx, run.RepositoryID)
	if err != nil {
		return cache.Namespaces{}, toStatus(err)
	}
	if account == "" {
		account = "_"
	}
	ns := cache.Namespaces{Write: account, Read: []string{account}}
	if run.Origin.Phase == store.PhasePre || run.Origin.Phase == store.PhaseManual {
		d, err := n.Checker.Decide(ctx, run.RequestedBy, nil, run.RepositoryID, authz.Land)
		if err != nil {
			return cache.Namespaces{}, status.Errorf(codes.Unavailable, "deciding who may land on repository %d: %v", run.RepositoryID, err)
		}
		if !d.Allowed {
			own := account + "/u/" + run.RequestedBy
			ns = cache.Namespaces{Write: own, Read: []string{own, account}}
		}
	}
	n.mu.Lock()
	if n.kept == nil {
		n.kept = map[int64]keptNamespaces{}
	}
	now := time.Now()
	for k, v := range n.kept {
		if now.Sub(v.at) >= namespacesKept {
			delete(n.kept, k)
		}
	}
	n.kept[id] = keptNamespaces{ns: ns, at: now}
	n.mu.Unlock()
	return ns, nil
}
