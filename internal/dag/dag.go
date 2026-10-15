// Package dag implements a small, dependency-free directed acyclic graph with
// cycle detection, topological ordering, and breadth-first traversal.
//
// It backs the serving runtime's node graph (package serve) — an assembly at the
// root, fanning out into services, applications, web servers, and their
// libraries — but is generic over any ordered vertex type so it stays reusable
// and, crucially, deterministic: every slice a Graph returns is sorted, so
// traversal order and error reports never depend on Go's map iteration order.
package dag

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// Graph is a directed graph over vertices of type T. The zero value is not
// usable; construct one with New.
type Graph[T cmp.Ordered] struct {
	out map[T]map[T]struct{} // vertex -> set of successors (edges out of it)
	in  map[T]map[T]struct{} // vertex -> set of predecessors (edges into it)
}

// New returns an empty graph.
func New[T cmp.Ordered]() *Graph[T] {
	return &Graph[T]{
		out: make(map[T]map[T]struct{}),
		in:  make(map[T]map[T]struct{}),
	}
}

// AddVertex adds v to the graph. It is idempotent: adding an existing vertex
// leaves its edges untouched.
func (g *Graph[T]) AddVertex(v T) {
	if _, ok := g.out[v]; !ok {
		g.out[v] = make(map[T]struct{})
		g.in[v] = make(map[T]struct{})
	}
}

// AddEdge adds a directed edge from -> to, creating either endpoint if it does
// not already exist. It is idempotent. A self-edge (from == to) is permitted at
// insertion time and is reported as a cycle by Validate.
func (g *Graph[T]) AddEdge(from, to T) {
	g.AddVertex(from)
	g.AddVertex(to)
	g.out[from][to] = struct{}{}
	g.in[to][from] = struct{}{}
}

// HasVertex reports whether v is in the graph.
func (g *Graph[T]) HasVertex(v T) bool {
	_, ok := g.out[v]
	return ok
}

// HasEdge reports whether the edge from -> to exists.
func (g *Graph[T]) HasEdge(from, to T) bool {
	succ, ok := g.out[from]
	if !ok {
		return false
	}
	_, ok = succ[to]
	return ok
}

// Len returns the number of vertices.
func (g *Graph[T]) Len() int { return len(g.out) }

// Vertices returns every vertex, sorted.
func (g *Graph[T]) Vertices() []T {
	if len(g.out) == 0 {
		return nil
	}
	vs := make([]T, 0, len(g.out))
	for v := range g.out {
		vs = append(vs, v)
	}
	slices.Sort(vs)
	return vs
}

// Successors returns the direct successors of v (the vertices v has edges to),
// sorted. It returns nil if v is absent or has none.
func (g *Graph[T]) Successors(v T) []T {
	return sortedKeys(g.out[v])
}

// Predecessors returns the direct predecessors of v (the vertices with edges to
// v), sorted. It returns nil if v is absent or has none.
func (g *Graph[T]) Predecessors(v T) []T {
	return sortedKeys(g.in[v])
}

// Roots returns the vertices with no incoming edges, sorted.
func (g *Graph[T]) Roots() []T {
	var roots []T
	for v := range g.out {
		if len(g.in[v]) == 0 {
			roots = append(roots, v)
		}
	}
	slices.Sort(roots)
	return roots
}

// Leaves returns the vertices with no outgoing edges, sorted.
func (g *Graph[T]) Leaves() []T {
	var leaves []T
	for v := range g.out {
		if len(g.out[v]) == 0 {
			leaves = append(leaves, v)
		}
	}
	slices.Sort(leaves)
	return leaves
}

// CycleError is returned by Validate and TopologicalSort when the graph contains
// a cycle. Cycle is the offending path, beginning and ending on the same vertex
// (e.g. [a, b, c, a]).
type CycleError[T cmp.Ordered] struct {
	Cycle []T
}

func (e *CycleError[T]) Error() string {
	parts := make([]string, len(e.Cycle))
	for i, v := range e.Cycle {
		parts[i] = fmt.Sprint(v)
	}
	return "dag: cycle detected: " + strings.Join(parts, " -> ")
}

// Validate reports whether the graph is acyclic. It returns nil if so, otherwise
// a *CycleError naming one cycle. The cycle reported is deterministic: vertices
// are explored in sorted order.
func (g *Graph[T]) Validate() error {
	const (
		white = 0 // unvisited
		gray  = 1 // on the current DFS stack
		black = 2 // fully explored
	)
	color := make(map[T]int, len(g.out))
	var stack []T

	var visit func(v T) []T
	visit = func(v T) []T {
		color[v] = gray
		stack = append(stack, v)
		for _, w := range g.Successors(v) {
			switch color[w] {
			case white:
				if c := visit(w); c != nil {
					return c
				}
			case gray:
				// w is on the current stack: the path from w to v, plus the
				// edge v -> w, is a cycle.
				idx := slices.Index(stack, w)
				cyc := append([]T{}, stack[idx:]...)
				return append(cyc, w)
			}
		}
		stack = stack[:len(stack)-1]
		color[v] = black
		return nil
	}

	for _, v := range g.Vertices() {
		if color[v] == white {
			if c := visit(v); c != nil {
				return &CycleError[T]{Cycle: c}
			}
		}
	}
	return nil
}

// TopologicalSort returns the vertices in dependency order: for every edge
// from -> to, from precedes to. Among otherwise-unordered vertices the smaller
// one comes first, so the result is deterministic. It returns a *CycleError if
// the graph is not acyclic.
func (g *Graph[T]) TopologicalSort() ([]T, error) {
	indeg := make(map[T]int, len(g.out))
	for v := range g.out {
		indeg[v] = len(g.in[v])
	}

	// ready holds the in-degree-zero frontier, kept sorted so we always emit the
	// smallest eligible vertex next.
	var ready []T
	for v, d := range indeg {
		if d == 0 {
			ready = append(ready, v)
		}
	}
	slices.Sort(ready)

	order := make([]T, 0, len(indeg))
	for len(ready) > 0 {
		v := ready[0]
		ready = ready[1:]
		order = append(order, v)
		for _, w := range g.Successors(v) {
			indeg[w]--
			if indeg[w] == 0 {
				idx, _ := slices.BinarySearch(ready, w)
				ready = slices.Insert(ready, idx, w)
			}
		}
	}

	if len(order) != len(indeg) {
		// Some vertices never reached in-degree zero: a cycle. Validate reports
		// exactly which vertices form it.
		if err := g.Validate(); err != nil {
			return nil, err
		}
		return nil, &CycleError[T]{}
	}
	return order, nil
}

// BreadthFirst visits every vertex reachable from start, start first, in
// breadth-first order, calling visit with each vertex and its distance (in
// edges) from start. Each vertex is visited once, at its shortest distance.
// Successors are explored in sorted order, so the traversal is deterministic. If
// visit returns an error the walk stops and returns it. It returns an error if
// start is not in the graph.
func (g *Graph[T]) BreadthFirst(start T, visit func(v T, depth int) error) error {
	if !g.HasVertex(start) {
		return fmt.Errorf("dag: start vertex %v not in graph", start)
	}
	type item struct {
		v     T
		depth int
	}
	seen := map[T]bool{start: true}
	queue := []item{{start, 0}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if err := visit(cur.v, cur.depth); err != nil {
			return err
		}
		for _, w := range g.Successors(cur.v) {
			if !seen[w] {
				seen[w] = true
				queue = append(queue, item{w, cur.depth + 1})
			}
		}
	}
	return nil
}

func sortedKeys[T cmp.Ordered](m map[T]struct{}) []T {
	if len(m) == 0 {
		return nil
	}
	keys := make([]T, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
