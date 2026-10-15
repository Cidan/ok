package store

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
)

func sorted(in ...encoding.ID) []encoding.ID {
	out := append([]encoding.ID(nil), in...)
	sortIDs(out)
	return out
}

func heads(t *testing.T, s *Store, repo int64, principal string) []encoding.ID {
	t.Helper()
	h, err := s.GetOpHeads(context.Background(), repo, principal, testWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// op publishes one operation (with an empty view) whose parents are given, applying the
// heads update.
func op(t *testing.T, s *Store, repo int64, principal, desc string, update *OpHeadsUpdate, parents ...encoding.ID) encoding.ID {
	t.Helper()
	b := newBuilder(t)
	id := b.operation(b.view(), desc, parents...)
	if update != nil {
		update.New = id
	}
	mustPublish(t, s, repo, principal, b, update)
	return id
}

func TestOpHeadsSemantics(t *testing.T) {
	s := testStore(t)
	repo := newRepo(t, s).ID
	root := encoding.RootOperationID

	// Never empty: a fresh log's head is the root operation.
	if got := heads(t, s, repo, "alice"); !reflect.DeepEqual(got, []encoding.ID{root}) {
		t.Fatalf("fresh heads = %v", got)
	}
	a := op(t, s, repo, "alice", "a", &OpHeadsUpdate{Old: []encoding.ID{root}}, root)
	if got := heads(t, s, repo, "alice"); !reflect.DeepEqual(got, []encoding.ID{a}) {
		t.Fatalf("after root->a: %v", got)
	}
	// Idempotent add: adding a head again changes nothing.
	b := newBuilder(t)
	if got := mustPublish(t, s, repo, "alice", b, &OpHeadsUpdate{New: a}); !reflect.DeepEqual(got, []encoding.ID{a}) {
		t.Fatalf("re-adding a: %v", got)
	}
	// Tolerant remove: removing IDs that are not heads is a no-op, and an old ID equal to
	// the new one is kept.
	bid := op(t, s, repo, "alice", "b", &OpHeadsUpdate{Old: []encoding.ID{{0x77}, a}}, a)
	if got := heads(t, s, repo, "alice"); !reflect.DeepEqual(got, []encoding.ID{bid}) {
		t.Fatalf("after a->b with an unknown old id: %v", got)
	}
	if got := mustPublish(t, s, repo, "alice", newBuilder(t), &OpHeadsUpdate{Old: []encoding.ID{bid}, New: bid}); !reflect.DeepEqual(got, []encoding.ID{bid}) {
		t.Fatalf("old == new removed the head: %v", got)
	}
	// Divergence: two updates from b each leave their own head; a merge resolves them.
	c := op(t, s, repo, "alice", "c", &OpHeadsUpdate{Old: []encoding.ID{bid}}, bid)
	d := op(t, s, repo, "alice", "d", &OpHeadsUpdate{Old: []encoding.ID{bid}}, bid)
	if got := heads(t, s, repo, "alice"); !reflect.DeepEqual(got, sorted(c, d)) {
		t.Fatalf("divergent heads = %v", got)
	}
	e := op(t, s, repo, "alice", "e", &OpHeadsUpdate{Old: []encoding.ID{c, d}}, c, d)
	if got := heads(t, s, repo, "alice"); !reflect.DeepEqual(got, []encoding.ID{e}) {
		t.Fatalf("merged heads = %v", got)
	}
	// Logs are per principal.
	if got := heads(t, s, repo, "bob"); !reflect.DeepEqual(got, []encoding.ID{root}) {
		t.Fatalf("bob's heads = %v", got)
	}
}

func opExists(t *testing.T, s *Store, repo int64, principal string, id encoding.ID) bool {
	t.Helper()
	res, err := s.ReadOperations(context.Background(), repo, principal, testWorkspace, []ObjectRef{{encoding.KindOperation, id}})
	if err != nil {
		t.Fatal(err)
	}
	return res[0].Found
}

func TestCollectionNeverTakesACurrentHead(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	root := encoding.RootOperationID
	a := op(t, s, repo, "alice", "a", &OpHeadsUpdate{Old: []encoding.ID{root}}, root)
	b := op(t, s, repo, "alice", "b", &OpHeadsUpdate{Old: []encoding.ID{a}}, a)
	// x is abandoned: the heads move from x to y, whose parent is b, not x.
	x := op(t, s, repo, "alice", "x", &OpHeadsUpdate{Old: []encoding.ID{b}}, b)
	y := op(t, s, repo, "alice", "y", &OpHeadsUpdate{Old: []encoding.ID{x}}, b)
	// z is a head another machine published concurrently: divergent from y.
	z := op(t, s, repo, "alice", "z", &OpHeadsUpdate{Old: nil}, b)
	if got := heads(t, s, repo, "alice"); !reflect.DeepEqual(got, sorted(y, z)) {
		t.Fatalf("heads = %v", got)
	}
	future := time.Now().Add(time.Hour)

	// Whatever the caller passes — nothing, an old operation, an unknown ID — the current
	// heads and their ancestors survive; only the abandoned x goes.
	for _, passed := range [][]encoding.ID{nil, {a}, {{0x42}}} {
		if err := s.CollectOperations(ctx, repo, "alice", testWorkspace, passed, future); err != nil {
			t.Fatal(err)
		}
		for name, id := range map[string]encoding.ID{"a": a, "b": b, "y": y, "z": z} {
			if !opExists(t, s, repo, "alice", id) {
				t.Fatalf("collecting with heads %v took %s", passed, name)
			}
		}
	}
	if opExists(t, s, repo, "alice", x) {
		t.Fatal("the abandoned operation survived collection")
	}
	// keep_newer protects recent operations even when unreachable.
	w := op(t, s, repo, "alice", "w", &OpHeadsUpdate{Old: nil}, b)
	mustPublish(t, s, repo, "alice", newBuilder(t), &OpHeadsUpdate{Old: []encoding.ID{w}, New: y})
	if err := s.CollectOperations(ctx, repo, "alice", testWorkspace, nil, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if !opExists(t, s, repo, "alice", w) {
		t.Fatal("collection took an operation newer than keep_newer")
	}
	// Views still used by a surviving operation stay; the abandoned ones' views go too.
	var views int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM view v JOIN op_log l ON l.id = v.op_log_id
		WHERE l.repository_id = $1 AND NOT EXISTS (SELECT 1 FROM operation o WHERE o.op_log_id = v.op_log_id AND o.view_id = v.id)`, repo).Scan(&views); err != nil {
		t.Fatal(err)
	}
	if views != 0 {
		t.Fatalf("%d unreferenced views survived", views)
	}
}

func TestResolveOperationPrefix(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	repo := newRepo(t, s).ID
	// Publish operations until two share a first hex digit.
	byNibble := map[byte][]encoding.ID{}
	var pair []encoding.ID
	for i := 0; pair == nil; i++ {
		b := newBuilder(t)
		b.seq = i * 10
		id := b.operation(b.view(), "op", encoding.RootOperationID)
		mustPublish(t, s, repo, "alice", b, nil)
		n := id[0] >> 4
		byNibble[n] = append(byNibble[n], id)
		if len(byNibble[n]) == 2 {
			pair = byNibble[n]
		}
	}
	resolve := func(prefix []byte, odd bool) (Resolution, encoding.ID) {
		t.Helper()
		r, id, err := s.ResolveOperationPrefix(ctx, repo, "alice", testWorkspace, prefix, odd)
		if err != nil {
			t.Fatal(err)
		}
		return r, id
	}
	// The shared first digit is ambiguous; each full ID resolves to itself.
	if r, _ := resolve([]byte{pair[0][0] & 0xf0}, true); r != AmbiguousMatch {
		t.Errorf("shared odd-nibble prefix: %v", r)
	}
	for _, id := range pair {
		if r, got := resolve(id[:], false); r != SingleMatch || got != id {
			t.Errorf("full ID %s: %v %s", id, r, got)
		}
		// A long enough prefix is unique.
		if r, got := resolve(id[:8], false); r != SingleMatch || got != id {
			t.Errorf("8-byte prefix of %s: %v %s", id, r, got)
		}
	}
	// The root operation resolves by prefix too, as in jj; another log's IDs do not.
	r, got := resolve([]byte{0, 0, 0, 0}, false)
	if r != SingleMatch || got != encoding.RootOperationID {
		t.Errorf("root prefix: %v %s", r, got)
	}
	if r, _, err := s.ResolveOperationPrefix(ctx, repo, "bob", testWorkspace, pair[0][:8], false); err != nil || r != NoMatch {
		t.Errorf("another principal resolved alice's operation: %v %v", r, err)
	}
	// An all-0xff prefix exercises the open-ended range.
	if r, _ := resolve([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, false); r != NoMatch {
		t.Errorf("0xff prefix: %v", r)
	}
	if _, _, err := s.ResolveOperationPrefix(ctx, repo, "alice", testWorkspace, make([]byte, 33), false); err == nil {
		t.Error("a 33-byte prefix was accepted")
	}
}

// opViewing publishes one operation whose view names a commit of its own, so every operation's
// view differs.
func opViewing(t *testing.T, s *Store, repo int64, principal, desc string, update *OpHeadsUpdate, parents ...encoding.ID) (encoding.ID, encoding.ID) {
	t.Helper()
	b := newBuilder(t)
	c := b.commit(b.tree(fileEntry("f", b.file([]byte(desc)))), desc, encoding.RootCommitID)
	v := b.view(c)
	id := b.operation(v, desc, parents...)
	if update != nil {
		update.New = id
	}
	mustPublish(t, s, repo, principal, b, update)
	return id, v
}

// prefetchOps collects a PrefetchOperations stream, checking each object against its ID.
func prefetchOps(t *testing.T, s *Store, repo int64, principal string, want, have []encoding.ID, limit int) []EncodedObject {
	t.Helper()
	var got []EncodedObject
	if err := s.PrefetchOperations(context.Background(), repo, principal, testWorkspace, want, have, limit, func(o EncodedObject) error {
		if _, err := decodeStored(o.Kind, o.ID, o.Body); err != nil {
			t.Fatalf("%s %s: %v", o.Kind, o.ID, err)
		}
		got = append(got, o)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

// A log's operations stream nearest first from the heads, each followed by its view; have
// leaves out everything it reaches, limit stops the walk, and another person's log is not read.
func TestPrefetchOperations(t *testing.T) {
	s := testStore(t)
	repo := newRepo(t, s).ID
	root := encoding.RootOperationID
	//   root ← a ← b ← c ← m
	//              ↖ d ↙
	a, va := opViewing(t, s, repo, "alice", "a", &OpHeadsUpdate{Old: []encoding.ID{root}}, root)
	b, vb := opViewing(t, s, repo, "alice", "b", &OpHeadsUpdate{Old: []encoding.ID{a}}, a)
	c, vc := opViewing(t, s, repo, "alice", "c", &OpHeadsUpdate{Old: []encoding.ID{b}}, b)
	d, vd := opViewing(t, s, repo, "alice", "d", &OpHeadsUpdate{}, b)
	m, vm := opViewing(t, s, repo, "alice", "m", &OpHeadsUpdate{Old: []encoding.ID{c, d}}, c, d)
	if got := heads(t, s, repo, "alice"); !reflect.DeepEqual(got, []encoding.ID{m}) {
		t.Fatalf("heads = %v", got)
	}
	ref := func(o EncodedObject) ObjectRef { return ObjectRef{Kind: o.Kind, ID: o.ID} }
	refs := func(objs []EncodedObject) []ObjectRef {
		out := make([]ObjectRef, len(objs))
		for i, o := range objs {
			out[i] = ref(o)
		}
		return out
	}
	opRef := func(id encoding.ID) ObjectRef { return ObjectRef{Kind: encoding.KindOperation, ID: id} }
	viewRef := func(id encoding.ID) ObjectRef { return ObjectRef{Kind: encoding.KindView, ID: id} }

	// Everything, from the heads: breadth first, each operation followed by its view.
	want := []ObjectRef{opRef(m), viewRef(vm), opRef(c), viewRef(vc), opRef(d), viewRef(vd), opRef(b), viewRef(vb), opRef(a), viewRef(va)}
	if got := refs(prefetchOps(t, s, repo, "alice", nil, nil, 0)); !reflect.DeepEqual(got, want) {
		t.Fatalf("whole log:\n got %v\nwant %v", got, want)
	}
	// Naming the head is the same as naming nothing.
	if got := refs(prefetchOps(t, s, repo, "alice", []encoding.ID{m}, nil, 0)); !reflect.DeepEqual(got, want) {
		t.Fatalf("from the head:\n got %v\nwant %v", got, want)
	}
	// have: what b reaches (b and a) is held.
	if got := refs(prefetchOps(t, s, repo, "alice", nil, []encoding.ID{b}, 0)); !reflect.DeepEqual(got, want[:6]) {
		t.Fatalf("have b:\n got %v\nwant %v", got, want[:6])
	}
	// have names something the log does not hold: ignored.
	if got := refs(prefetchOps(t, s, repo, "alice", nil, []encoding.ID{{0x55}}, 0)); !reflect.DeepEqual(got, want) {
		t.Fatalf("unknown have:\n got %v\nwant %v", got, want)
	}
	// limit counts operations.
	if got := refs(prefetchOps(t, s, repo, "alice", nil, nil, 2)); !reflect.DeepEqual(got, want[:4]) {
		t.Fatalf("limit 2:\n got %v\nwant %v", got, want[:4])
	}
	// From d alone: d and its ancestors.
	if got := refs(prefetchOps(t, s, repo, "alice", []encoding.ID{d}, nil, 0)); !reflect.DeepEqual(got, []ObjectRef{opRef(d), viewRef(vd), opRef(b), viewRef(vb), opRef(a), viewRef(va)}) {
		t.Fatalf("from d: %v", got)
	}
	// Another person's log is theirs: bob has none, and naming alice's operations reads nothing.
	if got := prefetchOps(t, s, repo, "bob", []encoding.ID{m}, nil, 0); len(got) != 0 {
		t.Fatalf("bob read %d of alice's objects", len(got))
	}
	// Operations sharing a view: the view is sent once, after the first.
	e := op(t, s, repo, "carol", "e", &OpHeadsUpdate{Old: []encoding.ID{root}}, root)
	f := op(t, s, repo, "carol", "f", &OpHeadsUpdate{Old: []encoding.ID{e}}, e)
	got := prefetchOps(t, s, repo, "carol", nil, nil, 0)
	if len(got) != 3 || ref(got[0]) != opRef(f) || got[1].Kind != encoding.KindView || ref(got[2]) != opRef(e) {
		t.Fatalf("a shared view: %v", refs(got))
	}
}
