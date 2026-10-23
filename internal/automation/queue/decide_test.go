package queue

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// fp is a footprint: labels are affected, "^label" upstream, anything else a file.
func fp(items ...string) *Footprint {
	var affected, upstream, files []string
	for _, it := range items {
		switch {
		case strings.HasPrefix(it, "^"):
			upstream = append(upstream, it[1:])
		case strings.HasPrefix(it, "//"):
			affected = append(affected, it)
		default:
			files = append(files, it)
		}
	}
	return NewFootprint(affected, upstream, files)
}

func TestConflict(t *testing.T) {
	for name, tc := range map[string]struct {
		a, b          *Footprint
		conflict, pth bool
	}{
		"disjoint":           {fp("//a:a", "a/a.go"), fp("//b:b", "b/b.go"), false, false},
		"both affect":        {fp("//lib:lib", "//app:app", "lib/l.go"), fp("//app:app", "app/main.go"), true, false},
		"one builds on":      {fp("//lib:lib", "lib/l.go"), fp("//app:app", "^//lib:lib", "app/build.yaml"), true, false},
		"other builds on":    {fp("//app:app", "^//lib:lib", "app/build.yaml"), fp("//lib:lib", "lib/l.go"), true, false},
		"both build on":      {fp("//a:a", "^//base:base", "a/a.go"), fp("//b:b", "^//base:base", "b/b.go"), false, false},
		"deletes a package":  {fp("gone/build.yaml"), fp("//app:app", "^//gone:gone", "app/build.yaml"), true, false},
		"same file":          {fp("//a:a", "a/a.go"), fp("//a:a", "a/a.go"), true, true},
		"unowned same file":  {fp("README.md"), fp("README.md"), false, true},
		"file and directory": {fp("docs/x"), fp("docs/x/y.md"), false, true},
		"root package":       {fp("//:root", "BUILD.md"), fp("tools/t.sh"), true, false},
		// The settings decide how every file is checked: an imported repository's change, which
		// affects nothing, conflicts with the change that makes the repository a Bazel workspace.
		"imported beside the settings": {fp("README.md"), fp("//a:a", ".ok/repository.yaml"), true, false},
		"two changes to the settings":  {fp(".ok/repository.yaml"), fp(".ok/repository.yaml"), true, true},
	} {
		if got := Conflict(tc.a, tc.b); got != tc.conflict {
			t.Errorf("%s: Conflict %v, want %v", name, got, tc.conflict)
		}
		if got := Conflict(tc.b, tc.a); got != tc.conflict {
			t.Errorf("%s: Conflict (reversed) %v, want %v", name, got, tc.conflict)
		}
		if got := PathConflict(tc.a, tc.b); got != tc.pth {
			t.Errorf("%s: PathConflict %v, want %v", name, got, tc.pth)
		}
		if got := PathConflict(tc.b, tc.a); got != tc.pth {
			t.Errorf("%s: PathConflict (reversed) %v, want %v", name, got, tc.pth)
		}
	}
	if m := Merge(fp("//a:a", "a/a.go"), fp("//b:b", "b/b.go")); !Conflict(m, fp("//b:b", "b/x.go")) || !PathConflict(m, fp("a/a.go")) {
		t.Error("a merged footprint lost a part")
	}
}

// The footprints of the tests' changes.
var (
	libFP  = fp("//lib:lib", "//app:app", "//app:test", "lib/lib.go")   // a library, and what uses it
	appFP  = fp("//app:app", "//app:test", "^//lib:lib", "app/main.go") // what uses the library
	docsFP = fp("//docs:docs", "docs/guide.md")                         // independent of both
	// hot spots: each changes its own file, and affects everything.
	hot = func(file string) *Footprint {
		return fp("//lib:lib", "//app:app", "//docs:docs", "//other:other", file)
	}
)

// attempt is an attempt in a state, planned when it has a footprint.
func attempt(id int64, baseSeq int64, chain []int64, state State, f *Footprint) Attempt {
	if chain == nil {
		chain = []int64{}
	}
	return Attempt{ID: id, BaseSeq: baseSeq, Chain: chain, Active: true, State: state, Footprint: f}
}

func ended(a Attempt) Attempt { a.Active = false; return a }

func sub(id int64, cls []int64, attempts ...Attempt) Submission {
	return Submission{ID: id, CLs: cls, Attempts: attempts}
}

// actions renders actions compactly, notes left out: "create 2 [1]", "land 2/7", ...
func actions(as []Action) []string {
	var out []string
	for _, a := range as {
		switch a.Kind {
		case Create:
			out = append(out, "create "+itoa(a.Submission)+" "+ids(a.Chain))
		case Release:
			out = append(out, "release "+itoa(a.Submission)+"/"+itoa(a.Attempt))
		case Discard:
			out = append(out, "discard "+itoa(a.Submission)+"/"+itoa(a.Attempt))
		case Land:
			out = append(out, "land "+itoa(a.Submission)+"/"+itoa(a.Attempt))
		case Adopt:
			out = append(out, "adopt "+itoa(a.Submission)+"/"+itoa(a.Attempt))
		case Fail:
			out = append(out, "fail "+itoa(a.Submission))
		case Cancel:
			out = append(out, "cancel "+itoa(a.Submission))
		}
	}
	return out
}

func notes(as []Action) map[int64]string {
	out := map[int64]string{}
	for _, a := range as {
		if a.Kind == Note {
			out[a.Submission] = a.Reason
		}
	}
	return out
}

func reason(as []Action, kind Kind, submission int64) string {
	for _, a := range as {
		if a.Kind == kind && a.Submission == submission {
			return a.Reason
		}
	}
	return ""
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func ids(chain []int64) string {
	s := make([]string, len(chain))
	for i, c := range chain {
		s[i] = itoa(c)
	}
	return "[" + strings.Join(s, " ") + "]"
}

func check(t *testing.T, what string, got []Action, want ...string) {
	t.Helper()
	if want == nil {
		want = []string{}
	}
	g := actions(got)
	if g == nil {
		g = []string{}
	}
	if !reflect.DeepEqual(g, want) {
		t.Errorf("%s: %v, want %v (notes %v)", what, g, want, notes(got))
	}
}

func TestNewSubmissionsArePlannedOnMain(t *testing.T) {
	s := Snapshot{HeadSeq: 4, Submissions: []Submission{sub(1, []int64{11}), sub(2, []int64{12})}}
	got := Decide(s)
	check(t, "two new submissions", got, "create 1 []", "create 2 []")
	if n := notes(got); n[1] != "planning post" || n[2] != "planning post" {
		t.Errorf("notes %v", n)
	}

	// While 1 is planned, 2 waits to be grouped.
	s.Submissions = []Submission{sub(1, []int64{11}, attempt(1, 4, nil, Planning, nil)), sub(2, []int64{12}, attempt(2, 4, nil, Planned, docsFP))}
	got = Decide(s)
	check(t, "2 waits for 1's footprint", got)
	if n := notes(got); n[2] != "waiting for CL 11 ahead of it to be planned" {
		t.Errorf("notes %v", n)
	}
}

func TestIndependentSubmissionsRunAndLandInAnyOrder(t *testing.T) {
	s := Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(1, []int64{11}, attempt(1, 4, nil, Planned, libFP)),
		sub(2, []int64{12}, attempt(2, 4, nil, Planned, docsFP)),
	}}
	check(t, "both planned", Decide(s), "release 1/1", "release 2/2")

	// 2 passes first and lands first: 1 is still testing.
	s.Submissions[0].Attempts[0].State = Testing
	s.Submissions[1].Attempts[0].State = Passed
	check(t, "2 passed", Decide(s), "land 2/2")

	// 1 passes after 2 landed: 2 is independent of it, so it lands without running again.
	s = Snapshot{HeadSeq: 5, From: 4, Submissions: []Submission{sub(1, []int64{11}, attempt(1, 4, nil, Passed, libFP))},
		Main: []MainCommit{{Seq: 5, CL: 12, Submission: 2}}, Landings: map[int64]*Footprint{2: docsFP}}
	check(t, "1 passed after 2 landed", Decide(s), "land 1/1")
}

func TestConflictingSubmissionsSpeculate(t *testing.T) {
	// 2 builds on what 1 changes: once both are planned, 2 is tested on top of 1.
	s := Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(1, []int64{11}, attempt(1, 4, nil, Planned, libFP)),
		sub(2, []int64{12}, attempt(2, 4, nil, Planned, appFP)),
	}}
	got := Decide(s)
	check(t, "2 conflicts with 1", got, "release 1/1", "discard 2/2", "create 2 [1]")
	if r := reason(got, Discard, 2); r != "CL 11, ahead of it, changes what it changes: it is tested on top of it now" {
		t.Errorf("the discard's reason: %q", r)
	}

	// 2's speculative attempt passes before 1's: it waits for 1 to land.
	s.Submissions = []Submission{
		sub(1, []int64{11}, attempt(1, 4, nil, Testing, libFP)),
		sub(2, []int64{12}, ended(attempt(2, 4, nil, Planned, appFP)), attempt(3, 4, []int64{1}, Passed, appFP)),
	}
	got = Decide(s)
	check(t, "2 passed first", got)
	if n := notes(got); n[2] != "passed; waiting for CL 11 to land first" || n[1] != "testing" {
		t.Errorf("notes %v", n)
	}

	// 1 lands; 2's result, which assumed it, holds: 2 lands without running again.
	s = Snapshot{HeadSeq: 5, From: 4, Submissions: []Submission{
		sub(2, []int64{12}, ended(attempt(2, 4, nil, Planned, appFP)), attempt(3, 4, []int64{1}, Passed, appFP)),
	}, Finished: map[int64]Outcome{1: Landed}, Main: []MainCommit{{Seq: 5, CL: 11, Submission: 1}}, Landings: map[int64]*Footprint{1: libFP}}
	check(t, "1 landed", Decide(s), "land 2/3")
}

func TestAFailureAheadRebasesWhatSpeculatedOnIt(t *testing.T) {
	// 1 failed on main alone: it fails. 2, tested on top of it, is tested again without it.
	s := Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(1, []int64{11}, Attempt{ID: 1, BaseSeq: 4, Chain: []int64{}, Active: true, State: Failed, Reason: "CL 11: test //app:test failed", Footprint: libFP}),
		sub(2, []int64{12}, ended(attempt(2, 4, nil, Planned, appFP)), attempt(3, 4, []int64{1}, Passed, appFP)),
	}}
	got := Decide(s)
	check(t, "1 failed", got, "fail 1", "discard 2/3", "create 2 []")
	if r := reason(got, Fail, 1); r != "CL 11: test //app:test failed" {
		t.Errorf("the failure: %q", r)
	}

	// Next, 1 has ended: 2's attempt no longer holds.
	s = Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(2, []int64{12}, ended(attempt(2, 4, nil, Planned, appFP)), attempt(3, 4, []int64{1}, Passed, appFP)),
	}, Finished: map[int64]Outcome{1: Ended}, FinishedCLs: map[int64][]int64{1: {11}}}
	got = Decide(s)
	check(t, "2 after 1 failed", got, "discard 2/3", "create 2 []")
	if r := reason(got, Discard, 2); r != "CL 11, which it was tested on, did not land" {
		t.Errorf("the discard's reason: %q", r)
	}
}

func TestAFailureOnTopOfOthersWaitsForThem(t *testing.T) {
	// 2 failed on top of 1, which is still testing: the failure may be 1's doing.
	s := Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(1, []int64{11}, attempt(1, 4, nil, Testing, libFP)),
		sub(2, []int64{12}, ended(attempt(2, 4, nil, Planned, appFP)),
			Attempt{ID: 3, BaseSeq: 4, Chain: []int64{1}, Active: true, State: Failed, Reason: "CL 12: test //app:test failed", Footprint: appFP}),
	}}
	got := Decide(s)
	check(t, "2 failed on 1", got)
	if n := notes(got); n[2] != "post failed on top of CL 11: it fails if CL 11 lands" {
		t.Errorf("notes %v", n)
	}
	// 1 lands: 2's failure holds for main, and it fails.
	s = Snapshot{HeadSeq: 5, From: 4, Submissions: []Submission{s.Submissions[1]}, Finished: map[int64]Outcome{1: Landed},
		Main: []MainCommit{{Seq: 5, CL: 11, Submission: 1}}, Landings: map[int64]*Footprint{1: libFP}}
	check(t, "1 landed", Decide(s), "fail 2")
}

func TestChainsAreTransitive(t *testing.T) {
	// 3 conflicts with 2 alone, but 2 is tested on 1: 3 is tested on both.
	one := fp("//a:a", "a/a.go")
	two := fp("//b:b", "^//a:a", "b/b.go")
	three := fp("//c:c", "^//b:b", "c/c.go")
	if !Conflict(one, two) || !Conflict(two, three) || Conflict(one, three) {
		t.Fatal("the fixture's footprints")
	}
	s := Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(1, []int64{11}, attempt(1, 4, nil, Testing, one)),
		sub(2, []int64{12}, ended(attempt(2, 4, nil, Planned, two)), attempt(4, 4, []int64{1}, Testing, two)),
		sub(3, []int64{13}, attempt(3, 4, nil, Planned, three)),
	}}
	check(t, "3 conflicts with 2", Decide(s), "discard 3/3", "create 3 [1 2]")
}

func TestPathConflictsBlock(t *testing.T) {
	// 1 and 2 change the same file: at most one lands. 2 does not speculate on 1 — it is
	// tested as if 1 fails — and waits for 1 to finish.
	one := fp("//lib:lib", "lib/lib.go")
	two := fp("//lib:lib", "lib/lib.go", "lib/extra.go")
	s := Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(1, []int64{11}, attempt(1, 4, nil, Planned, one)),
		sub(2, []int64{12}, attempt(2, 4, nil, Planned, two)),
	}}
	check(t, "both planned", Decide(s), "release 1/1", "release 2/2")
	s.Submissions[0].Attempts[0].State = Testing
	s.Submissions[1].Attempts[0].State = Passed
	got := Decide(s)
	check(t, "2 passed while 1 tests", got)
	if n := notes(got); n[2] != "passed; waiting for CL 11, which changes the same files, to finish" {
		t.Errorf("notes %v", n)
	}
	// 1 lands: 2's landing check finds it, conflicting, and 2 is prepared again — which the SCM
	// refuses, failing it with the paths.
	s = Snapshot{HeadSeq: 5, From: 4, Submissions: []Submission{sub(2, []int64{12}, attempt(2, 4, nil, Passed, two))},
		Finished: map[int64]Outcome{1: Landed}, Main: []MainCommit{{Seq: 5, CL: 11, Submission: 1}}, Landings: map[int64]*Footprint{1: one}}
	got = Decide(s)
	check(t, "1 landed", got, "discard 2/2", "create 2 []")
	if r := reason(got, Discard, 2); r != "CL 11 landed on main since it was tested, and their changes interact" {
		t.Errorf("the discard's reason: %q", r)
	}

	// A third conflicting with both is tested on the one predicted to land, not both.
	three := fp("//app:app", "^//lib:lib", "app/main.go")
	s = Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(1, []int64{11}, attempt(1, 4, nil, Testing, one)),
		sub(2, []int64{12}, attempt(2, 4, nil, Testing, two)),
		sub(3, []int64{13}, attempt(3, 4, nil, Planned, three)),
	}}
	check(t, "3 conflicts with both", Decide(s), "discard 3/3", "create 3 [1]")
}

func TestDepth(t *testing.T) {
	h1, h2, h3, h4 := hot("third_party/a/a.go"), hot("third_party/b/b.go"), hot("third_party/c/c.go"), hot("third_party/d/d.go")
	s := Snapshot{HeadSeq: 4, Depth: 2, Submissions: []Submission{
		sub(1, []int64{11}, attempt(1, 4, nil, Testing, h1)),
		sub(2, []int64{12}, ended(attempt(2, 4, nil, Planned, h2)), attempt(5, 4, []int64{1}, Testing, h2)),
		sub(3, []int64{13}, ended(attempt(3, 4, nil, Planned, h3)), attempt(6, 4, []int64{1, 2}, Testing, h3)),
		sub(4, []int64{14}, attempt(4, 4, nil, Planned, h4)),
	}}
	got := Decide(s)
	check(t, "a fourth hot spot", got, "discard 4/4")
	if n := notes(got); n[4] != "waiting: 3 submissions ahead of it conflict with it, and the queue speculates on at most 2" {
		t.Errorf("notes %v", n)
	}
	// Its footprint is known: once 1 lands, it is tested on 2 and 3.
	s = Snapshot{HeadSeq: 5, Depth: 2, From: 4, Submissions: s.Submissions[1:], Finished: map[int64]Outcome{1: Landed},
		Main: []MainCommit{{Seq: 5, CL: 11, Submission: 1}}, Landings: map[int64]*Footprint{1: h1}}
	s.Submissions[2].Attempts[0].Active = false
	check(t, "1 landed", Decide(s), "create 4 [2 3]")
}

func TestLandingChecksWhatLandedSinceItWasTested(t *testing.T) {
	passed := sub(1, []int64{11}, attempt(1, 4, nil, Passed, appFP))
	for name, tc := range map[string]struct {
		main     []MainCommit
		landings map[int64]*Footprint
		want     []string
		reason   string
	}{
		"nothing landed": {nil, nil, []string{"land 1/1"}, ""},
		"an independent change": {[]MainCommit{{Seq: 5, CL: 20, Submission: 7}}, map[int64]*Footprint{7: docsFP},
			[]string{"land 1/1"}, ""},
		"an interacting change": {[]MainCommit{{Seq: 5, CL: 20, Submission: 7}}, map[int64]*Footprint{7: libFP},
			[]string{"discard 1/1", "create 1 []"}, "CL 20 landed on main since it was tested, and their changes interact"},
		"a change landed without the queue": {[]MainCommit{{Seq: 5, CL: 20}}, nil,
			[]string{"discard 1/1", "create 1 []"}, "CL 20 landed on main without the landing queue since it was tested"},
	} {
		head := int64(4 + len(tc.main))
		got := Decide(Snapshot{HeadSeq: head, From: 4, Submissions: []Submission{passed}, Main: tc.main, Landings: tc.landings})
		check(t, name, got, tc.want...)
		if r := reason(got, Discard, 1); r != tc.reason {
			t.Errorf("%s: the discard's reason %q", name, r)
		}
	}
	// History automation has not read yet: it waits.
	got := Decide(Snapshot{HeadSeq: 6, From: 4, Submissions: []Submission{passed}, Main: []MainCommit{{Seq: 5, Submission: 7}},
		Landings: map[int64]*Footprint{7: docsFP}})
	check(t, "main's history behind", got)
	if n := notes(got); n[1] != "passed; reading what landed on main since it was tested" {
		t.Errorf("notes %v", n)
	}
}

func TestStacks(t *testing.T) {
	// A stack is one submission: its footprint is its CLs', and it lands whole.
	s := Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(1, []int64{11, 12, 13}, attempt(1, 4, nil, Passed, Merge(libFP, docsFP))),
		sub(2, []int64{14}, attempt(2, 4, nil, Planned, fp("//docs:docs", "docs/other.md"))),
	}}
	got := Decide(s)
	check(t, "a stack and a submission on its docs", got, "land 1/1", "discard 2/2", "create 2 [1]")
	if r := reason(got, Discard, 2); r != "CLs 11, 12, and 13, ahead of it, change what it changes: it is tested on top of them now" {
		t.Errorf("the discard's reason: %q", r)
	}
}

func TestLandedWithoutTheQueue(t *testing.T) {
	// The queue landed it and lost the record: it adopts the landing.
	s := Snapshot{HeadSeq: 5, Submissions: []Submission{
		{ID: 1, CLs: []int64{11}, Landed: []int64{11}, Attempts: []Attempt{attempt(1, 4, nil, Passed, libFP)}},
	}}
	check(t, "a passed submission whose CL landed", Decide(s), "adopt 1/1")
	// Some other way: canceled.
	s.Submissions[0].Attempts[0].State = Testing
	got := Decide(s)
	check(t, "a testing submission whose CL landed", got, "cancel 1")
	if r := reason(got, Cancel, 1); r != "CL 11 landed without the landing queue" {
		t.Errorf("the reason: %q", r)
	}
}

func TestAnAttemptThatCouldNotBePrepared(t *testing.T) {
	// The SCM refused a candidate on main alone: the submission fails with its words.
	s := Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(1, []int64{11}, Attempt{ID: 1, BaseSeq: 4, Chain: []int64{}, Active: true, State: Failed,
			Reason: "CL 11 conflicts with main in lib/lib.go: run jj sync, resolve the conflicts, and upload a new patchset with jj cl"}),
	}}
	got := Decide(s)
	check(t, "refused on main", got, "fail 1")
	if !strings.Contains(reason(got, Fail, 1), "conflicts with main in lib/lib.go") {
		t.Errorf("the failure: %q", reason(got, Fail, 1))
	}
	// A submission whose attempts all ended without a footprint is planned on main again.
	s.Submissions[0].Attempts[0].Active = false
	check(t, "no footprint", Decide(s), "create 1 []")
}

// A batch lands what deciding again after each landing would: independent submissions together,
// in queue order, up to its bound.
func TestBatchLandsWhatDecidingAgainWould(t *testing.T) {
	s := Snapshot{HeadSeq: 4, Submissions: []Submission{
		sub(1, []int64{11}, attempt(1, 4, nil, Passed, libFP)),
		sub(2, []int64{12, 13}, attempt(2, 4, nil, Passed, docsFP)),
		sub(3, []int64{14}, attempt(3, 4, nil, Passed, fp("//other:other", "other/x.go"))),
	}}
	check(t, "three independent", Batch(s, 8), "land 1/1", "land 2/2", "land 3/3")
	check(t, "bounded", Batch(s, 2), "land 1/1", "land 2/2")

	// After the first two land, the queue reads main moved by their three CLs, each theirs.
	after := s.AfterLanding(1, 1).AfterLanding(2, 2)
	if after.HeadSeq != 7 || len(after.Submissions) != 1 || after.Finished[2] != Landed || after.Landings[2] != docsFP {
		t.Errorf("after two landings: %+v", after)
	}
	if want := []MainCommit{{Seq: 5, CL: 11, Submission: 1}, {Seq: 6, CL: 12, Submission: 2}, {Seq: 7, CL: 13, Submission: 2}}; !reflect.DeepEqual(after.Main, want) {
		t.Errorf("main after two landings: %+v", after.Main)
	}
	if len(s.Main) != 0 || len(s.Submissions) != 3 {
		t.Error("AfterLanding changed the snapshot it was given")
	}

	// Nothing to land, or an adoption first: no batch.
	s.Submissions[0].Attempts[0].State = Testing
	check(t, "the first still testing", Batch(Snapshot{HeadSeq: 4, Submissions: s.Submissions[:1]}, 8))
	adopt := Snapshot{HeadSeq: 5, Submissions: []Submission{
		{ID: 1, CLs: []int64{11}, Landed: []int64{11}, Attempts: []Attempt{attempt(1, 4, nil, Passed, libFP)}},
	}}
	check(t, "an adoption", Batch(adopt, 8))
}

// A landing whose footprint interacts with the next submission ends the batch, though deciding
// on the queue before it would land both: the next is tested again on top of it, as deciding
// after the landing says.
func TestBatchStopsWhereALandingInteracts(t *testing.T) {
	// 1 is grouped by an attempt on main alone that changed only docs; the attempt that passed,
	// on top of 9, changed the library 2's app uses.
	one := sub(1, []int64{11}, ended(attempt(1, 4, nil, Passed, docsFP)), attempt(2, 4, []int64{9}, Passed, libFP))
	two := sub(2, []int64{12}, attempt(3, 5, nil, Passed, appFP))
	s := Snapshot{HeadSeq: 5, From: 4, Submissions: []Submission{one, two},
		Finished: map[int64]Outcome{9: Landed}, FinishedCLs: map[int64][]int64{9: {10}},
		Main: []MainCommit{{Seq: 5, CL: 10, Submission: 9}}, Landings: map[int64]*Footprint{9: docsFP}}
	check(t, "deciding before either lands", Decide(s), "land 1/2", "land 2/3")
	check(t, "the batch", Batch(s, 8), "land 1/2")
	check(t, "deciding after 1 lands", Decide(s.AfterLanding(1, 2)), "discard 2/3", "create 2 []")
}
