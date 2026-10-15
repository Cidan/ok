package dag

import (
	"cmp"
	"errors"
	"reflect"
	"testing"
)

func TestEmptyGraph(t *testing.T) {
	g := New[string]()
	if g.Len() != 0 {
		t.Fatalf("Len = %d, want 0", g.Len())
	}
	if v := g.Vertices(); v != nil {
		t.Fatalf("Vertices = %v, want nil", v)
	}
	if r := g.Roots(); r != nil {
		t.Fatalf("Roots = %v, want nil", r)
	}
	if err := g.Validate(); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}
	order, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort err = %v", err)
	}
	if len(order) != 0 {
		t.Fatalf("TopologicalSort = %v, want empty", order)
	}
}

func TestAddVertexIdempotent(t *testing.T) {
	g := New[string]()
	g.AddVertex("a")
	g.AddEdge("a", "b")
	g.AddVertex("a") // must not clobber the a->b edge
	if !g.HasEdge("a", "b") {
		t.Fatal("re-adding vertex a dropped edge a->b")
	}
	if !g.HasVertex("a") || !g.HasVertex("b") {
		t.Fatal("expected a and b present")
	}
}

func TestAddEdgeCreatesVertices(t *testing.T) {
	g := New[string]()
	g.AddEdge("a", "b")
	if !g.HasVertex("a") || !g.HasVertex("b") {
		t.Fatal("AddEdge should create both endpoints")
	}
	if !g.HasEdge("a", "b") {
		t.Fatal("edge a->b missing")
	}
	if g.HasEdge("b", "a") {
		t.Fatal("edge b->a should not exist (directed)")
	}
	// Idempotent.
	g.AddEdge("a", "b")
	if got := g.Successors("a"); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("Successors(a) = %v, want [b]", got)
	}
}

func TestSuccessorsPredecessorsSorted(t *testing.T) {
	g := New[string]()
	g.AddEdge("root", "c")
	g.AddEdge("root", "a")
	g.AddEdge("root", "b")
	if got := g.Successors("root"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("Successors(root) = %v, want sorted [a b c]", got)
	}
	g.AddEdge("z", "target")
	g.AddEdge("a", "target")
	g.AddEdge("m", "target")
	if got := g.Predecessors("target"); !reflect.DeepEqual(got, []string{"a", "m", "z"}) {
		t.Fatalf("Predecessors(target) = %v, want sorted [a m z]", got)
	}
	if got := g.Successors("absent"); got != nil {
		t.Fatalf("Successors(absent) = %v, want nil", got)
	}
}

func TestRootsAndLeaves(t *testing.T) {
	g := New[string]()
	g.AddEdge("a", "b")
	g.AddEdge("a", "c")
	g.AddEdge("b", "d")
	g.AddEdge("c", "d")
	g.AddVertex("island")
	if got := g.Roots(); !reflect.DeepEqual(got, []string{"a", "island"}) {
		t.Fatalf("Roots = %v, want [a island]", got)
	}
	if got := g.Leaves(); !reflect.DeepEqual(got, []string{"d", "island"}) {
		t.Fatalf("Leaves = %v, want [d island]", got)
	}
}

func TestValidateAcyclic(t *testing.T) {
	// Diamond: a -> b -> d, a -> c -> d. Acyclic.
	g := New[string]()
	g.AddEdge("a", "b")
	g.AddEdge("a", "c")
	g.AddEdge("b", "d")
	g.AddEdge("c", "d")
	if err := g.Validate(); err != nil {
		t.Fatalf("Validate = %v, want nil for a DAG", err)
	}
}

func TestValidateSimpleCycle(t *testing.T) {
	g := New[string]()
	g.AddEdge("a", "b")
	g.AddEdge("b", "a")
	err := g.Validate()
	var ce *CycleError[string]
	if !errors.As(err, &ce) {
		t.Fatalf("Validate = %v, want *CycleError", err)
	}
	// Exploration starts at the smallest vertex "a": a -> b -> a.
	if !reflect.DeepEqual(ce.Cycle, []string{"a", "b", "a"}) {
		t.Fatalf("Cycle = %v, want [a b a]", ce.Cycle)
	}
	if ce.Error() != "dag: cycle detected: a -> b -> a" {
		t.Fatalf("Error = %q", ce.Error())
	}
}

func TestValidateSelfLoop(t *testing.T) {
	g := New[string]()
	g.AddEdge("a", "a")
	err := g.Validate()
	var ce *CycleError[string]
	if !errors.As(err, &ce) {
		t.Fatalf("Validate = %v, want *CycleError", err)
	}
	if !reflect.DeepEqual(ce.Cycle, []string{"a", "a"}) {
		t.Fatalf("Cycle = %v, want [a a]", ce.Cycle)
	}
}

func TestValidateDeeperCycle(t *testing.T) {
	// a -> b -> c -> d -> b : the cycle is b,c,d,b, reachable from root a.
	g := New[string]()
	g.AddEdge("a", "b")
	g.AddEdge("b", "c")
	g.AddEdge("c", "d")
	g.AddEdge("d", "b")
	err := g.Validate()
	var ce *CycleError[string]
	if !errors.As(err, &ce) {
		t.Fatalf("Validate = %v, want *CycleError", err)
	}
	if !reflect.DeepEqual(ce.Cycle, []string{"b", "c", "d", "b"}) {
		t.Fatalf("Cycle = %v, want [b c d b]", ce.Cycle)
	}
}

func TestTopologicalSortRespectsEdges(t *testing.T) {
	g := New[string]()
	g.AddEdge("a", "b")
	g.AddEdge("a", "c")
	g.AddEdge("b", "d")
	g.AddEdge("c", "d")
	order, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort err = %v", err)
	}
	// Deterministic tie-breaking by smallest vertex: a, b, c, d.
	if !reflect.DeepEqual(order, []string{"a", "b", "c", "d"}) {
		t.Fatalf("order = %v, want [a b c d]", order)
	}
	assertTopoValid(t, g, order)
}

func TestTopologicalSortComplexDeterministic(t *testing.T) {
	g := New[string]()
	g.AddEdge("shirt", "tie")
	g.AddEdge("tie", "jacket")
	g.AddEdge("belt", "jacket")
	g.AddEdge("pants", "belt")
	g.AddEdge("pants", "shoes")
	g.AddEdge("socks", "shoes")
	g.AddVertex("watch")
	order, err := g.TopologicalSort()
	if err != nil {
		t.Fatalf("TopologicalSort err = %v", err)
	}
	assertTopoValid(t, g, order)
	// Running again must yield the identical order.
	order2, _ := g.TopologicalSort()
	if !reflect.DeepEqual(order, order2) {
		t.Fatalf("non-deterministic topo: %v vs %v", order, order2)
	}
}

func TestTopologicalSortCycle(t *testing.T) {
	g := New[string]()
	g.AddEdge("a", "b")
	g.AddEdge("b", "c")
	g.AddEdge("c", "a")
	_, err := g.TopologicalSort()
	var ce *CycleError[string]
	if !errors.As(err, &ce) {
		t.Fatalf("TopologicalSort err = %v, want *CycleError", err)
	}
	if len(ce.Cycle) == 0 {
		t.Fatal("cycle error should name the cycle")
	}
}

func TestBreadthFirstOrderAndDepth(t *testing.T) {
	// root -> a, root -> b; a -> c; b -> c; c -> d.
	g := New[string]()
	g.AddEdge("root", "a")
	g.AddEdge("root", "b")
	g.AddEdge("a", "c")
	g.AddEdge("b", "c")
	g.AddEdge("c", "d")

	type visitRec struct {
		v     string
		depth int
	}
	var got []visitRec
	err := g.BreadthFirst("root", func(v string, depth int) error {
		got = append(got, visitRec{v, depth})
		return nil
	})
	if err != nil {
		t.Fatalf("BreadthFirst err = %v", err)
	}
	want := []visitRec{
		{"root", 0},
		{"a", 1},
		{"b", 1},
		{"c", 2},
		{"d", 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("BFS = %v, want %v", got, want)
	}
}

func TestBreadthFirstVisitsEachOnce(t *testing.T) {
	g := New[int]()
	g.AddEdge(0, 1)
	g.AddEdge(0, 2)
	g.AddEdge(1, 3)
	g.AddEdge(2, 3)
	g.AddEdge(3, 4)
	counts := map[int]int{}
	if err := g.BreadthFirst(0, func(v, _ int) error {
		counts[v]++
		return nil
	}); err != nil {
		t.Fatalf("BreadthFirst err = %v", err)
	}
	for v, c := range counts {
		if c != 1 {
			t.Fatalf("vertex %d visited %d times, want 1", v, c)
		}
	}
	if len(counts) != 5 {
		t.Fatalf("visited %d vertices, want 5", len(counts))
	}
}

func TestBreadthFirstErrorPropagates(t *testing.T) {
	g := New[string]()
	g.AddEdge("root", "a")
	g.AddEdge("a", "b")
	sentinel := errors.New("boom")
	var visited []string
	err := g.BreadthFirst("root", func(v string, _ int) error {
		visited = append(visited, v)
		if v == "a" {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	// Walk must have stopped at a, never reaching b.
	if !reflect.DeepEqual(visited, []string{"root", "a"}) {
		t.Fatalf("visited = %v, want [root a]", visited)
	}
}

func TestBreadthFirstMissingStart(t *testing.T) {
	g := New[string]()
	g.AddVertex("a")
	err := g.BreadthFirst("ghost", func(string, int) error { return nil })
	if err == nil {
		t.Fatal("expected error for missing start vertex")
	}
}

func TestBreadthFirstReachabilityOnly(t *testing.T) {
	// island is unreachable from root and must not be visited.
	g := New[string]()
	g.AddEdge("root", "a")
	g.AddVertex("island")
	var visited []string
	_ = g.BreadthFirst("root", func(v string, _ int) error {
		visited = append(visited, v)
		return nil
	})
	if !reflect.DeepEqual(visited, []string{"root", "a"}) {
		t.Fatalf("visited = %v, want [root a]", visited)
	}
}

// assertTopoValid checks that order lists every vertex exactly once and that no
// edge points backwards in the ordering.
func assertTopoValid[T cmp.Ordered](t *testing.T, g *Graph[T], order []T) {
	t.Helper()
	pos := make(map[T]int, len(order))
	for i, v := range order {
		if _, dup := pos[v]; dup {
			t.Fatalf("vertex %v appears twice in topo order", v)
		}
		pos[v] = i
	}
	if len(pos) != g.Len() {
		t.Fatalf("topo order has %d vertices, graph has %d", len(pos), g.Len())
	}
	for _, from := range g.Vertices() {
		for _, to := range g.Successors(from) {
			if pos[from] >= pos[to] {
				t.Fatalf("edge %v->%v violates topo order (%d >= %d)", from, to, pos[from], pos[to])
			}
		}
	}
}
