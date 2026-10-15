package serve

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/serve/node"
)

// testNode is a bare node.Node for exercising the walk. Real nodes are pointer
// types obtained via a package Get() singleton; testNode is used the same way.
type testNode struct {
	name  string
	deps  []node.Node
	setup func()
}

func (n *testNode) Name() string                     { return n.name }
func (n *testNode) Dependencies() []node.Node        { return n.deps }
func (n *testNode) Healthy(ctx context.Context) bool { return true }
func (n *testNode) Setup(ctx context.Context) error {
	if n.setup != nil {
		n.setup()
	}
	return nil
}

var _ node.Node = (*testNode)(nil)

func TestWalkDiamondDedup(t *testing.T) {
	// root -> a -> shared, root -> b -> shared. shared is one instance (as a
	// Get() singleton would be), so it must be walked once.
	shared := &testNode{name: "//shared"}
	a := &testNode{name: "//a", deps: []node.Node{shared}}
	b := &testNode{name: "//b", deps: []node.Node{shared}}
	root := &testNode{name: "//root", deps: []node.Node{a, b}}

	g, err := walk(root)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(g.nodes) != 4 {
		t.Fatalf("nodes = %d, want 4 (root,a,b,shared)", len(g.nodes))
	}
	if g.nodes["//shared"] != node.Node(shared) {
		t.Fatal("shared resolved to a different instance than the one referenced")
	}
}

func TestWalkDuplicateNameFatal(t *testing.T) {
	// Two distinct instances claiming the same Name — the singleton-violation the
	// dup check must reject.
	x1 := &testNode{name: "//dup"}
	x2 := &testNode{name: "//dup"}
	root := &testNode{name: "//root", deps: []node.Node{x1, x2}}

	if _, err := walk(root); err == nil {
		t.Fatal("expected a duplicate-node error, got nil")
	}
}

func TestWalkSharedSingletonIsNotDuplicate(t *testing.T) {
	// The SAME instance referenced twice is a legal diamond, not a duplicate.
	shared := &testNode{name: "//shared"}
	a := &testNode{name: "//a", deps: []node.Node{shared}}
	b := &testNode{name: "//b", deps: []node.Node{shared}}
	root := &testNode{name: "//root", deps: []node.Node{a, b}}

	if _, err := walk(root); err != nil {
		t.Fatalf("shared singleton wrongly rejected: %v", err)
	}
}

func TestWalkCycleFatal(t *testing.T) {
	a := &testNode{name: "//a"}
	b := &testNode{name: "//b", deps: []node.Node{a}}
	a.deps = []node.Node{b} // //a <-> //b
	root := &testNode{name: "//root", deps: []node.Node{a}}

	if _, err := walk(root); err == nil {
		t.Fatal("expected a cycle error, got nil")
	}
}

func TestWalkNilDepFatal(t *testing.T) {
	root := &testNode{name: "//root", deps: []node.Node{nil}}
	if _, err := walk(root); err == nil {
		t.Fatal("expected a nil-dependency error, got nil")
	}
}

func TestWalkNilRootFatal(t *testing.T) {
	if _, err := walk(nil); err == nil {
		t.Fatal("expected a nil-root error, got nil")
	}
}

func TestSetupOrderDepsFirst(t *testing.T) {
	var order []string
	rec := func(name string) func() { return func() { order = append(order, name) } }
	shared := &testNode{name: "//shared", setup: rec("//shared")}
	a := &testNode{name: "//a", deps: []node.Node{shared}, setup: rec("//a")}
	b := &testNode{name: "//b", deps: []node.Node{shared}, setup: rec("//b")}
	root := &testNode{name: "//root", deps: []node.Node{a, b}, setup: rec("//root")}

	g, err := walk(root)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if err := g.setup(context.Background()); err != nil {
		t.Fatalf("setup: %v", err)
	}

	if len(order) != 4 {
		t.Fatalf("setup ran %d nodes, want 4: %v", len(order), order)
	}
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	// dependencies before dependents
	if !(pos["//shared"] < pos["//a"] && pos["//shared"] < pos["//b"]) {
		t.Fatalf("shared not set up before a/b: %v", order)
	}
	if !(pos["//a"] < pos["//root"] && pos["//b"] < pos["//root"]) {
		t.Fatalf("a/b not set up before root: %v", order)
	}
}

func TestSetupRunsSharedOnce(t *testing.T) {
	count := 0
	shared := &testNode{name: "//shared", setup: func() { count++ }}
	a := &testNode{name: "//a", deps: []node.Node{shared}}
	b := &testNode{name: "//b", deps: []node.Node{shared}}
	root := &testNode{name: "//root", deps: []node.Node{a, b}}

	g, err := walk(root)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if err := g.setup(context.Background()); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if count != 1 {
		t.Fatalf("shared Setup ran %d times, want 1", count)
	}
}
