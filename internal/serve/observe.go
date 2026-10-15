package serve

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/serve/node"
)

// The node graph's metrics: what the binary is, how long each node took to set up, and whether
// each is healthy.
var (
	buildInfo = metrics.NewGauge("ok_build_info",
		"1, labeled with the binary's assembly (its root node) and the Go release it was built with.", "assembly", "go_version")
	nodeSetup = metrics.NewGauge("ok_node_setup_seconds",
		"How long the node's Setup took.", "node")
)

// observed is the graph ok_node_healthy samples.
var observed atomic.Pointer[graph]

// healthTimeout bounds how long sampling the nodes' health may take: a node whose Healthy is
// still answering then counts as unhealthy.
const healthTimeout = 2 * time.Second

func init() {
	metrics.NewGaugeFunc("ok_node_healthy", "1 when the node reports itself healthy, 0 when not.", func(report func(float64, ...string)) {
		g := observed.Load()
		if g == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), healthTimeout)
		defer cancel()
		for name, n := range g.nodes {
			report(healthValue(ctx, n), name)
		}
	}, "node")
}

func healthValue(ctx context.Context, n node.Node) float64 {
	done := make(chan bool, 1)
	go func() { done <- n.Healthy(ctx) }()
	select {
	case ok := <-done:
		if ok {
			return 1
		}
		return 0
	case <-ctx.Done():
		return 0
	}
}

// observe records what the binary is and starts sampling its graph's health.
func observe(root node.Node, g *graph) {
	buildInfo.With(root.Name(), runtime.Version()).Set(1)
	observed.Store(g)
}
