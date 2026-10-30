package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// countingWrites counts, by method, the calls a landing makes to write and read its CLs' rows,
// in transactions and snapshots alike.
type countingWrites struct {
	backend.Backend
	mu    *sync.Mutex
	calls map[string]int
}

func (c countingWrites) count(name string) {
	c.mu.Lock()
	c.calls[name]++
	c.mu.Unlock()
}

func (c countingWrites) Txn(ctx context.Context, fn func(tx backend.Ops) error) error {
	return c.Backend.Txn(ctx, func(tx backend.Ops) error { return fn(countingOps{tx, c}) })
}

func (c countingWrites) Snapshot(ctx context.Context, fn func(tx backend.Ops) error) error {
	return c.Backend.Snapshot(ctx, func(tx backend.Ops) error { return fn(countingOps{tx, c}) })
}

type countingOps struct {
	backend.Ops
	c countingWrites
}

func (o countingOps) InsertCommits(ctx context.Context, repo int64, commits []backend.Commit, by string) ([]encoding.ID, error) {
	o.c.count("InsertCommits")
	return o.Ops.InsertCommits(ctx, repo, commits, by)
}

func (o countingOps) InsertCommitParents(ctx context.Context, repo int64, edges []backend.ParentEdge) error {
	o.c.count("InsertCommitParents")
	return o.Ops.InsertCommitParents(ctx, repo, edges)
}

func (o countingOps) InsertHeadLogs(ctx context.Context, repo int64, moves []backend.HeadMove) error {
	o.c.count("InsertHeadLogs")
	return o.Ops.InsertHeadLogs(ctx, repo, moves)
}

func (o countingOps) SetLanded(ctx context.Context, repo int64, status int16, cls []backend.LandedCL) error {
	o.c.count("SetLanded")
	return o.Ops.SetLanded(ctx, repo, status, cls)
}

func (o countingOps) InsertCommitPaths(ctx context.Context, repo int64, rows []backend.CommitPaths) error {
	o.c.count("InsertCommitPaths")
	return o.Ops.InsertCommitPaths(ctx, repo, rows)
}

func (o countingOps) Candidate(ctx context.Context, repo int64, commit encoding.ID) (backend.Candidate, bool, error) {
	o.c.count("Candidate")
	return o.Ops.Candidate(ctx, repo, commit)
}

func (o countingOps) CandidatesOf(ctx context.Context, repo int64, commits []encoding.ID) (map[encoding.ID]backend.Candidate, error) {
	o.c.count("CandidatesOf")
	return o.Ops.CandidatesOf(ctx, repo, commits)
}

func (o countingOps) ObjectBodies(ctx context.Context, repo int64, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID][]byte, error) {
	o.c.count("ObjectBodies")
	return o.Ops.ObjectBodies(ctx, repo, kind, ids)
}

// A landing writes and reads its CLs' rows in as many calls of each kind for one CL as for
// eight, prepared on one head as the landing queue prepares a batch's: each kind of row in a
// statement of its own, and the trees its merges read a level at a time across all its CLs, from
// a replica that holds none of them.
func TestLandingCallsDoNotGrowWithItsCLs(t *testing.T) {
	ctx := context.Background()
	counts := map[int]map[string]int{}
	for _, n := range []int{1, 8} {
		e := newCLEnv(t)
		base := map[string]string{}
		var paths []string
		for i := range 60 {
			p := fmt.Sprintf("a%d/b%d/c%d/f%d.go", i%2, i%3, i%5, i)
			base[p], paths = fmt.Sprint("base ", i), append(paths, p)
		}
		e.landChange("base", base)
		head := e.head()
		var items []LandItem
		for i := range n {
			files := map[string]string{}
			for k, v := range base {
				files[k] = v
			}
			files[paths[i*7%len(paths)]] = fmt.Sprint("changed by ", i)
			number := e.mustUpload(alice, Ready, e.commit(alice, newChange(), head, fmt.Sprint("cl ", i), files))[0].Number
			cand, err := e.s.PrepareCandidate(ctx, e.repo, number, 1, head, alice)
			if err != nil {
				t.Fatal(err)
			}
			items = append(items, LandItem{Number: number, Patchset: 1, Candidate: &cand.Commit})
		}
		w := countingWrites{Backend: dbOf(e.s).open(t), mu: &sync.Mutex{}, calls: map[string]int{}}
		cold, err := New(w, e.s.blobs, Config{Names: testNames})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cold.Close)
		landed, _, _, err := cold.Land(ctx, e.repo, items, head, alice, false)
		if err != nil || len(landed) != n {
			t.Fatalf("landing %d CLs: %d landed, %v", n, len(landed), err)
		}
		for i, l := range landed {
			if c := e.cl(l.Number); c.Status != StatusSubmitted || c.LandedCommit == nil || *c.LandedCommit != l.Commit {
				t.Fatalf("CL %d of %d: %+v", i, n, c)
			}
		}
		counts[n] = w.calls
	}
	t.Logf("one CL: %v; eight: %v", counts[1], counts[8])
	for name, one := range counts[1] {
		if eight := counts[8][name]; eight > one+1 {
			t.Errorf("%s: %d calls landing one CL, %d landing eight", name, one, eight)
		}
	}
	for _, name := range []string{"InsertHeadLogs", "SetLanded", "InsertCommitPaths", "CandidatesOf"} {
		if counts[8][name] != 1 {
			t.Errorf("%s: %d calls landing eight CLs, want 1", name, counts[8][name])
		}
	}
}

// budgeted is a backend whose transactions hold budget mutations.
type budgeted struct {
	backend.Backend
	budget int
}

func (b budgeted) MutationBudget() int { return b.budget }

// A landing keeps its transaction inside the backend's mutation budget: the path history its rows
// leave no room for is written after it, the first commits' with it; a landing whose other rows
// alone pass the budget is refused, as too large, so its CLs land fewer at once — and one of them
// alone lands.
func TestLandingStaysWithinItsBudget(t *testing.T) {
	ctx := context.Background()
	e := newCLEnv(t)
	e.landChange("base", map[string]string{"keep": "0"})
	head := e.head()
	var items []LandItem
	for i := range 3 {
		files := map[string]string{"keep": "0"}
		for j := range 20 {
			files[fmt.Sprintf("cl%d/d%d/f.go", i, j)] = "new"
		}
		n := e.mustUpload(alice, Ready, e.commit(alice, newChange(), head, fmt.Sprint("cl ", i), files))[0].Number
		items = append(items, LandItem{Number: n, Patchset: 1})
	}
	open := func(budget int) *Store {
		s, err := New(budgeted{dbOf(e.s).open(t), budget}, e.s.blobs, Config{Names: testNames})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		return s
	}
	// Each CL changes 41 paths: twenty files, their directories, and its own.
	rest := landingOverhead + 3*commitMutations + 3*clMutations
	_, _, _, err := open(rest).Land(ctx, e.repo, items, head, alice, false)
	var rej *LandRejection
	if !errors.As(err, &rej) || rej.Reason != LandTooLarge {
		t.Fatalf("a landing whose rows pass the budget: %v, want refused as too large", err)
	}
	// Room besides for the merges' few new trees (each CL's root: up to ten) and about two CLs' paths.
	s := open(rest + 10*treeMutations + 50*pathMutations)
	landed, _, _, err := s.Land(ctx, e.repo, items, head, alice, false)
	if err != nil || len(landed) != 3 {
		t.Fatalf("a landing within the budget: %v, %v", landed, err)
	}
	db := dbOf(e.s)
	if n := db.count(t, "path_index", "repository_id = @repo", map[string]any{"repo": e.repo}); n < 1 || n > 2 {
		t.Errorf("%d landings' path history left for after the landing, want those of the last that did not fit", n)
	}
	if n := db.count(t, "commit_path", "commit_id = @c", map[string]any{"c": landed[0].Commit[:]}); n == 0 {
		t.Error("the first CL's path history was not written with its landing")
	}
	if err := s.finishPathIndexes(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	for _, l := range landed[1:] {
		if n := db.count(t, "commit_path", "commit_id = @c", map[string]any{"c": l.Commit[:]}); n == 0 {
			t.Errorf("CL %d's path history not written after the landing", l.Number)
		}
	}
}
