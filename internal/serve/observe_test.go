package serve

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/serve/node"
)

// sickNode is a node that reports itself unhealthy.
type sickNode struct{ testNode }

func (*sickNode) Healthy(context.Context) bool { return false }

// The graph's metrics: the binary's assembly, each node's setup time, and each node's health,
// sampled when the metrics are read.
func TestGraphMetrics(t *testing.T) {
	slow := &testNode{name: "//observe/slow", setup: func() { time.Sleep(20 * time.Millisecond) }}
	sick := &sickNode{testNode{name: "//observe/sick"}}
	root := &testNode{name: "//observe/root", deps: []node.Node{slow, sick}}
	g, err := walk(root)
	if err != nil {
		t.Fatal(err)
	}
	observe(root, g)
	if err := g.setup(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := metrics.Default()
	if v := r.Read("ok_build_info", "//observe/root", runtime.Version()); v.Value != 1 {
		t.Errorf("build info: %+v", v)
	}
	if v := r.Read("ok_node_setup_seconds", "//observe/slow"); v.Value < 0.02 {
		t.Errorf("the slow node's setup took %v s, want at least 0.02", v.Value)
	}
	if v := r.Read("ok_node_healthy", "//observe/root"); !v.Found || v.Value != 1 {
		t.Errorf("the root's health: %+v", v)
	}
	if v := r.Read("ok_node_healthy", "//observe/sick"); !v.Found || v.Value != 0 {
		t.Errorf("the sick node's health: %+v", v)
	}
}
