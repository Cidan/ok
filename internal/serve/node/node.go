package node

import (
	"context"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc"
)

// Node is the base construct of a role — anything that participates in
// the dependency graph. A value that is only a Node is a plain library: it
// contributes health and dependencies but nothing served. Nodes are singletons,
// obtained through their package's Get() accessor.
type Node interface {
	// Name is the node's name (e.g. "ok/identity/service"). It is the node's
	// identity — unique within a role — and its vertex key in the graph.
	Name() string
	// Dependencies are the nodes this node depends on, each obtained via its
	// package Get() singleton. The union across the graph must be acyclic.
	Dependencies() []Node
	// Healthy reports whether this node is currently healthy. the runtime propagates
	// health across the graph and gates readiness on it.
	Healthy(ctx context.Context) bool
	// Setup initializes the node's state (open handles, dial deps). the runtime calls
	// it once, after the graph is validated, in dependency order — a node's
	// dependencies are already set up when its Setup runs.
	Setup(ctx context.Context) error
}

// Service is a gRPC service node. Every Service serves both gRPC and REST: the runtime calls
// RegisterRPC for each one, and serves whatever it registers over REST too, as calls
// through the same server — so every interceptor applies to both, and no service
// writes REST code.
type Service interface {
	Node
	// RegisterRPC registers the service on the runtime's shared gRPC server. The generated
	// registration function carries the grpc.ServiceDesc, so it never appears in author
	// code.
	RegisterRPC(ctx context.Context, reg grpc.ServiceRegistrar) error
}

// Application is a long-running worker node. the runtime supervises Start; when it
// returns or errors, the supervisor applies its restart policy.
type Application interface {
	Node
	Start(ctx context.Context) error
}

// WebServer is a node that contributes plain HTTP handlers. the runtime calls Mount
// with a fresh chi router and mounts it at the returned route on the shared
// single-port router.
type WebServer interface {
	Node
	Mount(r *chi.Mux) (route string, err error)
}

// SelfAuthenticated is a WebServer that authenticates its own requests — a protocol with its own
// credentials, such as SCIM's bearer token — so the role's web middleware does not resolve its
// callers.
type SelfAuthenticated interface {
	WebServer
	AuthenticatesItself()
}
