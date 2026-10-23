package store

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// countingBackend counts the walks of the tail's commits.
type countingBackend struct {
	backend.Backend
	tailWalks atomic.Int32
}

func (c *countingBackend) TailAncestry(ctx context.Context, repo int64, from []encoding.ID, minGen int64) ([]backend.GraphCommit, error) {
	c.tailWalks.Add(1)
	return c.Backend.TailAncestry(ctx, repo, from, minGen)
}

// history is a chain of commits on the root: the first 30 uploaded and compacted into segments,
// the next 5 uploaded and in the tail, pinned, the last 5 published only; and a merge of its tip
// and its eleventh.
type history struct {
	chain []encoding.ID
	merge encoding.ID
}

func buildHistory(t *testing.T, e *clEnv) history {
	t.Helper()
	ctx := context.Background()
	var h history
	parent := encoding.RootCommitID
	for i := range 40 {
		parent = e.commit(alice, newChange(), parent, fmt.Sprint("c", i), map[string]string{"f": fmt.Sprint(i)})
		h.chain = append(h.chain, parent)
		switch i {
		case 29:
			e.mustUpload(alice, Ready, h.chain[:30]...)
			if r, err := e.s.CompactSegments(ctx, e.repo, 1); err != nil || r.Commits != 30 {
				t.Fatalf("compacting the first 30 commits: %+v, %v", r, err)
			}
		case 34:
			e.mustUpload(alice, Ready, h.chain[30:35]...)
		}
	}
	b := newBuilder(t)
	tree := b.tree(fileEntry("m", b.file([]byte("merge\n"))))
	h.merge = b.commit(tree, "merge", h.chain[39], h.chain[10])
	mustPublish(t, e.s, e.repo, alice, b, nil)
	return h
}

// The walk of a history across segments, the tail's pinned commits, and its unpinned ones reaches
// every commit down to its generation bound, with each one's parents and their generations, as one
// walk of the tail would.
func TestAncestryAcrossSegments(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	h := buildHistory(t, e)
	gen := func(i int) int64 { return int64(i + 1) } // the chain's i-th; the root is 0
	got, err := reopen(t, e.s).ancestry(ctx, e.repo, []encoding.ID{h.merge}, gen(5))
	if err != nil {
		t.Fatal(err)
	}
	want := map[encoding.ID]backend.Ancestor{
		h.merge: {ID: h.merge, Parents: []encoding.ID{h.chain[39], h.chain[10]}, Generations: []int64{gen(39), gen(10)}},
	}
	for i := 5; i < 40; i++ {
		want[h.chain[i]] = backend.Ancestor{ID: h.chain[i], Parents: []encoding.ID{h.chain[i-1]}, Generations: []int64{gen(i - 1)}}
	}
	if len(got) != len(want) {
		t.Fatalf("reached %d commits, want %d", len(got), len(want))
	}
	for i, a := range got {
		w, ok := want[a.ID]
		if !ok || !slices.Equal(a.Parents, w.Parents) || !slices.Equal(a.Generations, w.Generations) {
			t.Fatalf("commit %s: %+v, want %+v", a.ID, a, w)
		}
		if i > 0 && compareIDs(got[i-1].ID, a.ID) >= 0 {
			t.Fatal("not in ID order")
		}
	}
	// From the chain's first commit, its parent the root, which no commit row is: no edge.
	first, err := e.s.ancestry(ctx, e.repo, []encoding.ID{h.chain[0]}, 0)
	if err != nil || len(first) != 1 || len(first[0].Parents) != 0 {
		t.Fatalf("the first commit's ancestry: %+v, %v", first, err)
	}
	// A commit the repository lacks is reached, with no edges.
	if none, err := e.s.ancestry(ctx, e.repo, []encoding.ID{{7}}, 0); err != nil || len(none) != 1 || len(none[0].Parents) != 0 {
		t.Fatalf("an absent commit's ancestry: %+v, %v", none, err)
	}
	if g, err := e.s.generation(ctx, e.repo, h.chain[3]); err != nil || g != gen(3) {
		t.Errorf("a moved commit's generation: %d, %v", g, err)
	}
	res, err := reopen(t, e.s).ReadObjects(ctx, e.repo, []ObjectRef{{encoding.KindCommit, h.chain[3]}, {encoding.KindCommit, h.chain[36]}})
	if err != nil || !res[0].Found || !res[1].Found {
		t.Errorf("commit bodies, from a segment and from the tail: %+v, %v", res, err)
	}
	present, err := e.s.existing(ctx, e.s.b, e.repo, tCommit, encoding.KindCommit, []encoding.ID{h.chain[3], h.chain[36], {7}})
	if err != nil || !present[h.chain[3]] || !present[h.chain[36]] || present[encoding.ID{7}] {
		t.Errorf("commits found present: %v, %v", present, err)
	}
}

// A walk through history in segments queries the tail once each time it enters it, not each
// generation.
func TestAncestryEntersTheTailOnce(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	h := buildHistory(t, e)
	cb := &countingBackend{Backend: dbOf(e.s).open(t)}
	s, err := New(cb, e.s.blobs, Config{Names: testNames})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	for _, c := range []struct {
		from  encoding.ID
		walks int32
		what  string
	}{
		{h.chain[29], 1, "from the segments' tip to the root: the root looked for once"},
		{h.chain[39], 2, "from the tail's tip: the tail walked once, then the root"},
	} {
		cb.tailWalks.Store(0)
		got, err := s.ancestry(ctx, e.repo, []encoding.ID{c.from}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n := cb.tailWalks.Load(); n > c.walks {
			t.Errorf("%s: walked the tail %d times over %d commits, want at most %d", c.what, n, len(got), c.walks)
		}
	}
}
