package queue

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/workflow"
)

// queueGen makes random landing queues, from a small vocabulary so that their submissions share
// paths and targets: files above, at, and under each other's directories, subtrees and the root,
// the repository's settings, targets affected and reached, every target.
type queueGen struct{ r *rand.Rand }

var (
	genFiles = []string{"a/f.go", "a/b/g.go", "a/b/c/h.go", "a/b", "x/y/z.txt", "x/k", "top.md", "x", workflow.SettingsFile}
	genTrees = []string{"a/", "a/b/", "x/", "/", "q/"}
	genLabel = []string{"//a:a", "//a/b:b", "//a/b/c:c", "//x:x", "//x/y:y", "//:root", "//q:q"}
)

func (g queueGen) pick(from []string, n int) []string {
	var out []string
	for range g.r.IntN(n + 1) {
		out = append(out, from[g.r.IntN(len(from))])
	}
	return out
}

func (g queueGen) footprint() *Footprint {
	files := g.pick(genFiles[:len(genFiles)-1], 3)
	if g.r.IntN(10) == 0 {
		files = append(files, genTrees[g.r.IntN(len(genTrees))])
	}
	if g.r.IntN(40) == 0 {
		files = append(files, workflow.SettingsFile)
	}
	affected, upstream := g.pick(genLabel, 2), g.pick(genLabel, 3)
	switch g.r.IntN(40) {
	case 0:
		affected = append(affected, store.AllTargets)
	case 1:
		upstream = append(upstream, store.AllTargets)
	}
	return NewFootprint(affected, upstream, files)
}

// snapshot is a queue of up to n submissions, with main's history since From — gaps, commits landed
// outside the queue, a place held twice — and the submissions that finished.
func (g queueGen) snapshot(n int) Snapshot {
	r := g.r
	head := int64(5 + r.IntN(15))
	s := Snapshot{HeadSeq: head, From: int64(r.IntN(int(head))), Depth: r.IntN(10), Finished: map[int64]Outcome{},
		FinishedCLs: map[int64][]int64{}, Landings: map[int64]*Footprint{}}
	finished := []int64{}
	for k := range 1 + r.IntN(6) {
		id := int64(1000 + k)
		finished = append(finished, id)
		s.Finished[id] = Outcome(1 + r.IntN(2))
		s.FinishedCLs[id] = []int64{int64(900 + k)}
		if r.IntN(5) != 0 {
			s.Landings[id] = g.footprint()
		}
	}
	for seq := s.From + 1; seq <= head; seq++ {
		if r.IntN(10) == 0 {
			continue // a gap
		}
		m := MainCommit{Seq: seq, CL: int64(r.IntN(3)) * 7}
		if r.IntN(6) != 0 {
			m.Submission = finished[r.IntN(len(finished))]
		}
		s.Main = append(s.Main, m)
		if r.IntN(30) == 0 {
			m.Submission = finished[r.IntN(len(finished))]
			s.Main = append(s.Main, m) // the same place again
		}
	}
	nextAttempt := int64(1)
	for i := range 1 + r.IntN(n) {
		id := int64(i + 1)
		e := Submission{ID: id, CLs: []int64{int64(100 + i)}}
		if r.IntN(8) == 0 {
			e.CLs = append(e.CLs, int64(500+i))
		}
		if r.IntN(25) == 0 {
			e.Landed = e.CLs[:1+r.IntN(len(e.CLs))]
		}
		for k := range r.IntN(4) {
			a := Attempt{ID: nextAttempt, BaseSeq: s.From - 1 + int64(r.IntN(int(head-s.From)+2)), Chain: []int64{},
				Active: k == 0 || r.IntN(3) == 0, State: State(r.IntN(5))}
			nextAttempt++
			if a.State >= Passed || r.IntN(3) == 0 {
				a.State = Passed
			}
			if r.IntN(3) == 0 {
				a.State = State(r.IntN(5))
			}
			if a.State != Planning || r.IntN(4) == 0 {
				a.Footprint = g.footprint()
			}
			if a.State == Failed {
				a.Reason = "it failed"
			}
			if r.IntN(3) == 0 {
				for j := range i {
					if r.IntN(3) == 0 {
						a.Chain = append(a.Chain, int64(j+1))
					}
				}
				if r.IntN(4) == 0 {
					a.Chain = append(a.Chain, finished[r.IntN(len(finished))])
				}
			}
			e.Attempts = append(e.Attempts, a)
		}
		s.Submissions = append(s.Submissions, e)
	}
	return s
}

// Decide and Batch decide exactly as the decision they replace did — every submission checked
// against every earlier one, a passed attempt against every commit since it was tested, and a batch
// deciding the whole queue again after each landing (reference_test.go) — over random queues.
func TestDecisionMatchesTheReference(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	g := queueGen{rand.New(rand.NewPCG(seed, 1))}
	lands := 0
	for i := range 20000 {
		s := g.snapshot(12)
		if got, want := Decide(s), oldDecide(s); !reflect.DeepEqual(got, want) {
			t.Fatalf("queue %d: Decide\n got  %v\n want %v\nqueue %s", i, got, want, describe(s))
		}
		max := 1 + g.r.IntN(8)
		got, want := Batch(s, max), oldBatch(s, max)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("queue %d: Batch(%d)\n got  %v\n want %v\nqueue %s", i, max, got, want, describe(s))
		}
		lands += len(got)
	}
	if lands < 1000 {
		t.Errorf("the random queues landed %d submissions in their batches: too few to hold Batch to anything", lands)
	}
}

func describe(s Snapshot) string {
	out := fmt.Sprintf("head %d from %d depth %d main %v finished %v landings %d\n", s.HeadSeq, s.From, s.Depth, s.Main, s.Finished,
		len(s.Landings))
	for _, e := range s.Submissions {
		out += fmt.Sprintf("  %d %v landed %v\n", e.ID, e.CLs, e.Landed)
		for _, a := range e.Attempts {
			out += fmt.Sprintf("    attempt %d base %d chain %v active %v state %d fp %v\n", a.ID, a.BaseSeq, a.Chain, a.Active, a.State,
				a.Footprint != nil)
		}
	}
	return out
}

// herd is a monorepo's queue all at once: n submissions, each changing files of its own in one of
// pods' trees and building on its pod's library and the base, every one passed on main, and main
// holding what landed since — as staging's herd of 3,000 people submitting together left it. One in
// fifty changes the same file as the change before it.
func herd(n, pods, landed int) Snapshot {
	s := Snapshot{HeadSeq: int64(landed), Finished: map[int64]Outcome{}, FinishedCLs: map[int64][]int64{}, Landings: map[int64]*Footprint{}}
	change := func(i int) *Footprint {
		pod := i % pods
		dir := func(i int) string { return fmt.Sprintf("src/pod%d/d%d", i%pods, i) }
		shared := i
		if i%50 == 1 && i > 0 {
			shared = i - 1
		}
		return NewFootprint([]string{fmt.Sprintf("//%s:t%d", dir(i), i)},
			[]string{fmt.Sprintf("//lib/pod%d:lib", pod), "//base:base"},
			[]string{fmt.Sprintf("%s/f%d.go", dir(shared), shared), fmt.Sprintf("%s/g%d.go", dir(i), i)})
	}
	for k := range landed {
		id := int64(100000 + k)
		s.Finished[id], s.Landings[id] = Landed, change(n+k)
		s.Main = append(s.Main, MainCommit{Seq: int64(k + 1), CL: int64(k + 1), Submission: id})
	}
	for i := range n {
		s.Submissions = append(s.Submissions, Submission{ID: int64(i + 1), CLs: []int64{int64(10000 + i)},
			Attempts: []Attempt{{ID: int64(i + 1), BaseSeq: 0, Chain: []int64{}, Active: true, State: Passed, Footprint: change(i)}}})
	}
	return s
}

// A decision on a herd's queue is about as costly as the queue is long, not its square: staging's
// 2,000 submissions held each batch of 32 for minutes; deciding and batching 3,000 here, with 1,000
// commits landed since they were tested, takes well under a second.
func TestDecisionAtScale(t *testing.T) {
	s := herd(3000, 8, 1000)
	t0 := time.Now()
	acts := Decide(s)
	decided := time.Since(t0)
	t0 = time.Now()
	batch := Batch(s, 32)
	batched := time.Since(t0)
	t.Logf("3,000 submissions: decided in %s, a batch of %d in %s", decided, len(batch), batched)
	if len(batch) != 32 || acts[0].Kind != Land {
		t.Fatalf("the herd's batch: %d landings, first action %v", len(batch), acts[0])
	}
	if decided > 5*time.Second || batched > 5*time.Second {
		t.Errorf("deciding took %s and batching %s", decided, batched)
	}
	// The reference, on a tenth of the queue: the same, as slowly as a herd's was.
	small := herd(300, 8, 100)
	t0 = time.Now()
	want := oldBatch(small, 32)
	t.Logf("the reference: a batch of 300 submissions in %s", time.Since(t0))
	if got := Batch(small, 32); !reflect.DeepEqual(got, want) || !reflect.DeepEqual(Decide(small), oldDecide(small)) {
		t.Errorf("a herd of 300: the batch %v, the reference's %v", actions(got), actions(want))
	}
}

func BenchmarkBatchHerd(b *testing.B) {
	s := herd(3000, 8, 1000)
	for b.Loop() {
		Batch(s, 32)
	}
}
