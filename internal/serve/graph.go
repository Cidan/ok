package serve

import (
	"context"
	"fmt"
	"time"

	"github.com/Cidan/ok/internal/dag"
	"github.com/Cidan/ok/internal/serve/node"
)

// graph is the resolved result of walking the node tree: the one instance per
// Name, plus a topological ordering (parent before dependency).
type graph struct {
	nodes map[string]node.Node // Name -> the single instance
	order []string             // topological: for edge parent->dep, parent precedes dep
	dg    *dag.Graph[string]
}

// walk discovers every node reachable from root by following Dependencies(),
// deduping by pointer identity and enforcing one instance per Name, then
// validates the graph is acyclic and computes a topological order. It returns an
// error on a nil dependency, a duplicate node (two distinct instances sharing a
// Name), or a cycle.
func walk(root node.Node) (*graph, error) {
	if root == nil {
		return nil, fmt.Errorf("serve: nil root node")
	}
	g := &graph{nodes: map[string]node.Node{}, dg: dag.New[string]()}
	// seen keys on the interface value: two references to the same Get() singleton
	// are the same key (a shared/diamond dep is walked once); two distinct
	// instances are different keys and are caught by the Name check below.
	seen := map[node.Node]bool{}

	var visit func(n node.Node) error
	visit = func(n node.Node) error {
		if seen[n] {
			return nil
		}
		seen[n] = true

		name := n.Name()
		if prev, ok := g.nodes[name]; ok && prev != n {
			return fmt.Errorf("serve: duplicate node %q: two distinct instances "+
				"(share one Get() singleton instead of constructing it twice)", name)
		}
		g.nodes[name] = n
		g.dg.AddVertex(name)

		for _, dep := range n.Dependencies() {
			if dep == nil {
				return fmt.Errorf("serve: node %q has a nil dependency", name)
			}
			g.dg.AddEdge(name, dep.Name()) // parent -> dep
			if err := visit(dep); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(root); err != nil {
		return nil, err
	}

	if err := g.dg.Validate(); err != nil {
		return nil, fmt.Errorf("serve: invalid node graph: %w", err)
	}
	order, err := g.dg.TopologicalSort()
	if err != nil {
		return nil, fmt.Errorf("serve: invalid node graph: %w", err)
	}
	g.order = order
	return g, nil
}

// setup runs Setup on every node in dependency order — dependencies before
// dependents — which is the reverse of the parent->dep topological order.
func (g *graph) setup(ctx context.Context) error {
	for i := len(g.order) - 1; i >= 0; i-- {
		n := g.nodes[g.order[i]]
		start := time.Now()
		if err := n.Setup(ctx); err != nil {
			return fmt.Errorf("serve: setup %s: %w", n.Name(), err)
		}
		nodeSetup.With(n.Name()).Set(time.Since(start).Seconds())
	}
	return nil
}

// list returns every node in topological order (parent before dependency).
func (g *graph) list() []node.Node {
	out := make([]node.Node, 0, len(g.order))
	for _, name := range g.order {
		out = append(out, g.nodes[name])
	}
	return out
}

// healthy reports whether every node in the graph is currently healthy — the
// binary's overall liveness.
func (g *graph) healthy(ctx context.Context) bool {
	for _, n := range g.nodes {
		if !n.Healthy(ctx) {
			return false
		}
	}
	return true
}
