package serve

import (
	"context"
	"log/slog"

	"github.com/Cidan/ok/internal/serve/health"
	"github.com/Cidan/ok/internal/serve/interceptor"
	"github.com/Cidan/ok/internal/serve/node"
	"github.com/Cidan/ok/internal/serve/serving"
)

// Run is how one of ok's roles serves: it walks the node graph rooted at the assembly, validates
// it, sets every node up in dependency order, then serves the graph on one port (gRPC, REST, web,
// and health) until ctx ends, after which it shuts down gracefully. A bad graph, a failed setup,
// or a failed serve is its error: a role comes up whole or not at all.
func Run(ctx context.Context, root node.Node, opts ...Option) error {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	g, err := walk(root)
	if err != nil {
		return err
	}
	observe(root, g)
	if err := g.setup(ctx); err != nil {
		return err
	}
	chain := interceptor.New()
	chain.UseUnary(o.unary...)
	chain.UseStream(o.stream...)
	metricsAddr := o.metricsAddr
	if metricsAddr == "" {
		metricsAddr = "127.0.0.1:0"
	}
	slog.Info("serve: starting", "assembly", root.Name())
	return serving.Serve(ctx, serving.Config{
		Nodes:          g.list(),
		Chain:          chain,
		Health:         health.New(g.healthy),
		Addr:           o.addr,
		Listener:       o.listener,
		Reflection:     o.reflection,
		StopTimeout:    o.stopTimeout,
		MaxRecvMsgSize: o.maxRecv,
		MaxSendMsgSize: o.maxSend,
		MetricsAddr:    metricsAddr,
		TLS:            o.tls,
		Middleware:     o.middleware,
	})
}
