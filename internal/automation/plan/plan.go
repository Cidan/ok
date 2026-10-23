// Package plan plans a patchset's pre, or post on a candidate (docs/automation.md, "What
// runs"): from the change's job context, what the Bazel graph says it affects, and the
// repository's workflow files, the jobs to run — the built-in ones (build, tests, lint) and the
// workflow jobs of the phase the change selects — and the workflow jobs left to main. In pre it also
// checks the workflow files the change adds, edits, or breaks (through an include) against the
// build graph at the change.
//
// It is pure: the worker that runs a planning run reads the workflow files, queries Bazel, and
// hands this package what they said.
package plan

import (
	"fmt"
	"maps"
	gopath "path"
	"slices"
	"sort"
	"strings"

	"github.com/Cidan/ok/internal/buildsys"
	"github.com/Cidan/ok/internal/workflow"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// The build system's answers the planner reads (internal/buildsys).
type (
	Affected    = buildsys.Affected
	Target      = buildsys.Target
	Step        = buildsys.Step
	Described   = buildsys.Described
	Description = buildsys.Description
)

// Input is what a plan is made from.
type Input struct {
	Context  *workflow.Context
	Affected *Affected
	// Main is the repository's workflow files on main, whose jobs the change selects, and
	// Change those at the change's revision, whose edits are checked. Each maps a
	// repository-relative path to its content.
	Main, Change map[string][]byte
	// Standing is workflow.Checked or workflow.Graduates: neither of the others is planned.
	Standing workflow.Standing
	// SincePre is, for post, what the Bazel graph says the differences between the commit the CL's pre
	// ran on and the candidate affect — none when they are the same tree. nil when there is no
	// pre to compare with: every job then runs.
	SincePre *Affected
	// Pool is the repository's pool of workers, as main's settings name it ("" for none): its
	// built-in jobs', and its workflow jobs' that name none. Read from main, as the workflow files
	// are, so a change does not choose where it runs.
	Pool string
}

// BuildSincePre is the post job that builds the affected targets that changed since pre; the
// build job builds the rest, which built in pre on the same inputs (docs/automation.md,
// "post").
const BuildSincePre = "build since pre"

// Planner makes a plan in two steps: New selects the jobs and says which targets the graph must
// describe (Targets); Plan composes the jobs from the graph's description.
type Planner struct {
	in       Input
	affected map[string]*Target
	upstream map[string]bool
	// changed are the targets the differences since pre affect, when sincePre is known.
	changed  map[string]bool
	sincePre bool
	selected []selection
	checked  []workflow.Job // the jobs of the workflow files the change edits or breaks
	// deploys are the deploys of those files, whose targets must back them.
	deploys  []workflow.Deploy
	problems []string
	notes    []string
}

// selection is a workflow job on main the change selects, and why.
type selection struct {
	job workflow.Job
	why string
}

// New selects the workflow jobs the change brings in, and checks what of the workflow files it
// edits needs no build graph.
func New(in Input) *Planner {
	p := &Planner{in: in, affected: map[string]*Target{}, upstream: map[string]bool{}}
	if len(in.Affected.Problems) > 0 {
		for _, pr := range in.Affected.Problems {
			p.problems = append(p.problems, "the build graph: "+pr)
		}
		return p
	}
	for i := range in.Affected.Affected {
		t := &in.Affected.Affected[i]
		p.affected[t.Label] = t
	}
	for _, u := range in.Affected.Upstream {
		p.upstream[u] = true
	}
	switch {
	case in.SincePre == nil:
	case len(in.SincePre.Problems) > 0:
		p.notes = append(p.notes, "what changed since pre: the build graph at the candidate has problems, so every job runs")
	case slices.Contains(in.SincePre.Changed, workflow.SettingsFile):
		// They name the linters and Bazel's flags, for files no target holds too.
		p.notes = append(p.notes, "the candidate's "+workflow.SettingsFile+" is not the patchset's, so every job runs")
	default:
		p.sincePre, p.changed = true, map[string]bool{}
		for _, t := range in.SincePre.Affected {
			p.changed[t.Label] = true
		}
	}
	if in.Standing == workflow.Graduates {
		// Nothing checked main's workflow files while the repository was imported: the change's
		// are its first, and pre checks every one of them.
		p.selectJobs(workflow.NewSet(in.Change))
		p.checkEdits()
		return p
	}
	main := workflow.NewSet(in.Main)
	for _, e := range main.Problems() {
		p.notes = append(p.notes, "main's workflow files: "+e.Error())
	}
	p.selectJobs(main)
	p.checkEdits()
	return p
}

// Notes are what the plan's log should say besides its jobs.
func (p *Planner) Notes() []string { return p.notes }

// Problems are what fails the plan so far.
func (p *Planner) Problems() []string { return p.problems }

// selectJobs selects the jobs of main's workflows in the context's phase that the change brings
// in (docs/automation.md, "Workflow jobs"), each once, with the first reason found.
func (p *Planner) selectJobs(main *workflow.Set) {
	phase := p.in.Context.Phase
	seen := map[string]bool{}
	add := func(jobs []workflow.Job, why string) {
		for _, j := range jobs {
			if j.Phase != phase || seen[j.ID()] {
				continue
			}
			seen[j.ID()] = true
			p.selected = append(p.selected, selection{job: j, why: why})
		}
	}
	governed := func(dir string) (string, []workflow.Job, bool) {
		g, ok := main.Governing(dir)
		if !ok {
			return "", nil, false
		}
		return g, main.Jobs(g), true
	}
	for _, f := range p.in.Context.Touched() {
		if g, jobs, ok := governed(dirOf(f)); ok {
			add(jobs, fmt.Sprintf("%s changed, and %s's workflows govern it", f, dirLabel(g)))
		}
	}
	for _, t := range p.in.Affected.Affected {
		if g, jobs, ok := governed(packageOf(t.Label)); ok {
			add(jobs, fmt.Sprintf("%s is affected, and %s's workflows govern it", t.Label, dirLabel(g)))
		}
	}
	for _, u := range p.in.Affected.Upstream {
		if g, jobs, ok := governed(packageOf(u)); ok {
			add(jobs, fmt.Sprintf("the change builds on %s, and %s's workflows govern it", u, dirLabel(g)))
		}
	}
	for _, j := range main.All() {
		if j.Phase != phase || seen[j.ID()] {
			continue
		}
		if _, ok := p.affected[j.Target]; ok {
			seen[j.ID()] = true
			p.selected = append(p.selected, selection{job: j, why: "its target is affected"})
		}
	}
}

// checkEdits checks the workflow files the change adds or edits, and those it breaks through
// an include (a file that includes one it edits or deletes) — every one of them, when the change
// makes an imported repository a Bazel workspace — at the change's revision: their schema and
// includes now, their targets once the graph describes them. pre checks them; post runs only on a
// change whose pre passed.
func (p *Planner) checkEdits() {
	if p.in.Context.Phase != workflow.Pre {
		return
	}
	var touched []string
	for _, f := range p.in.Context.Touched() {
		if workflow.IsWorkflowFile(f) {
			touched = append(touched, f)
		}
	}
	if p.in.Standing == workflow.Graduates {
		touched = slices.Sorted(maps.Keys(p.in.Change))
	}
	if len(touched) == 0 {
		return
	}
	change := workflow.NewSet(p.in.Change)
	check := map[string]bool{}
	for _, f := range touched {
		if _, ok := p.in.Change[f]; ok {
			check[f] = true
		}
	}
	for _, f := range change.Includers(touched) {
		check[f] = true
	}
	src := mapSource(p.in.Change)
	for _, f := range sortedKeys(check) {
		for _, e := range workflow.Validate(f, p.in.Change[f], src) {
			p.problems = append(p.problems, e.Error())
		}
		parsed, _ := workflow.Parse(f, p.in.Change[f])
		p.checked = append(p.checked, parsed.Jobs...)
		p.deploys = append(p.deploys, parsed.Deploys...)
	}
	// A workload has one deploy, across the repository (docs/deploy.md, "The stanza").
	for _, e := range change.DeployProblems() {
		p.problems = append(p.problems, e.Error())
	}
}

// Targets are the targets the graph must describe for the plan: those of the workflow jobs it
// selected and of the workflow files it checks.
func (p *Planner) Targets() []string {
	set := map[string]bool{}
	for _, s := range p.selected {
		set[s.job.Target] = true
	}
	for _, j := range p.checked {
		set[j.Target] = true
	}
	for _, d := range p.deploys {
		for _, m := range d.Migrate {
			set[m.Target] = true
		}
		for _, im := range d.Images {
			set[im.Target] = true
		}
	}
	return sortedKeys(set)
}

// Plan composes the plan from the graph's description of Targets. It returns the problems that
// fail it instead, when there are any.
func (p *Planner) Plan(d *Described) (*automationpb.Plan, []string) {
	problems := append([]string(nil), p.problems...)
	if len(p.in.Affected.Problems) > 0 {
		return nil, problems
	}
	desc := map[string]Description{}
	for _, t := range d.Targets {
		desc[t.Label] = t
	}
	// A job's target must exist at the change, and its kind must be able to back the job.
	backs := func(j workflow.Job, whose string) (Description, bool) {
		t, ok := desc[j.Target]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s:%d: job %s%s: target %s does not exist at this change", j.File, j.Line, j.Name, whose, j.Target))
			return t, false
		case len(j.Args) > 0 && !t.Test && !t.Binary:
			problems = append(problems, fmt.Sprintf("%s:%d: job %s%s: target %s is a %s, which builds and takes no args (a test or a binary does)", j.File, j.Line, j.Name, whose, j.Target, t.Kind))
			return t, false
		}
		return t, true
	}
	for _, j := range p.checked {
		backs(j, "")
	}
	// A deploy's images are rules_oci oci_push targets, and its migrations runnable targets — a
	// *_binary, which speaks ok's migrate protocol — at the change.
	exists := func(d workflow.Deploy, target string, line int, what string) (Description, bool) {
		t, ok := desc[target]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s:%d: deploy %s: %s %s does not exist at this change", d.File, line, d.Name, what, target))
		}
		return t, ok
	}
	for _, d := range p.deploys {
		for _, m := range d.Migrate {
			if t, ok := exists(d, m.Target, m.Line, "migration"); ok && !t.Binary {
				problems = append(problems, fmt.Sprintf("%s:%d: deploy %s: migration %s is a %s, not a binary that speaks the migrate protocol", d.File, m.Line, d.Name, m.Target, t.Kind))
			}
		}
		for _, im := range d.Images {
			what := "container " + im.Container + "'s image"
			if t, ok := exists(d, im.Target, im.Line, what); ok && t.Kind != "oci_push" {
				problems = append(problems, fmt.Sprintf("%s:%d: deploy %s: %s %s is a %s, not oci_push", d.File, im.Line, d.Name, what, im.Target, t.Kind))
			}
		}
	}

	out := &automationpb.Plan{}
	out.Jobs = append(out.Jobs, p.builtins()...)
	for _, s := range p.selected {
		t, ok := backs(s.job, " (on main)")
		if !ok {
			continue
		}
		at, affected := p.affected[s.job.Target]
		if !affected {
			out.Skipped = append(out.Skipped, &automationpb.SkippedJob{Job: s.job.ID(), Target: s.job.Target,
				Why: fmt.Sprintf("%s; its target is not affected, so its result on main stands", s.why)})
			continue
		}
		// The arguments stay as written: the worker that runs the job expands them from the job
		// context. Expanding them here only checks them.
		if _, err := workflow.Expand(s.job.Args, p.in.Context); err != nil {
			problems = append(problems, fmt.Sprintf("%s:%d: job %s: %v", s.job.File, s.job.Line, s.job.Name, err))
			continue
		}
		spec := &automationpb.Job{Targets: []string{s.job.Target}, Pool: workflow.PoolOf(s.job.Pool, p.in.Pool)}
		switch {
		case t.Test:
			spec.Kind, spec.Args = automationpb.Job_KIND_TEST, s.job.Args
		case t.Binary:
			spec.Kind, spec.Args = automationpb.Job_KIND_RUN, s.job.Args
		default:
			spec.Kind = automationpb.Job_KIND_BUILD
		}
		// A test's and a build's actions are cached: only a binary, run in the sandbox, reads secrets.
		if len(s.job.Secrets) > 0 && spec.Kind != automationpb.Job_KIND_RUN {
			problems = append(problems, fmt.Sprintf("%s:%d: job %s names secrets, and %s is no binary: only a binary, run in its sandbox, reads secrets (docs/secrets.md)",
				s.job.File, s.job.Line, s.job.Name, s.job.Target))
			continue
		}
		// A run job reads the job context, which names the commit under test; so does a job whose
		// arguments carry a placeholder.
		unchanged := spec.Kind != automationpb.Job_KIND_RUN && !workflow.HasPlaceholders(s.job.Args) && p.unchanged(s.job.Target)
		out.Jobs = append(out.Jobs, &automationpb.PlannedJob{Job: s.job.ID(), Spec: spec,
			Why: fmt.Sprintf("%s; its target is affected: %s", s.why, Chain(at.Because)), UnchangedSincePre: unchanged})
	}
	if len(problems) > 0 {
		return nil, problems
	}
	return out, nil
}

// builtins are the jobs ok runs on every change (docs/automation.md, "Built-in jobs"):
// the build of every affected target, each affected test, and the lint of the change's files —
// which the lint job does not list: its worker reads them from the SCM.
func (p *Planner) builtins() []*automationpb.PlannedJob {
	var out []*automationpb.PlannedJob
	var roots []string
	for _, t := range p.in.Affected.Affected {
		if t.Root && !t.Test {
			roots = append(roots, t.Label)
		}
	}
	// In post, a root nothing changed for since pre built there on the same inputs, with what it
	// builds on: its build job carries pre's, and the rest build in a job of their own.
	var held, rebuilt []string
	for _, r := range roots {
		if p.unchanged(r) {
			held = append(held, r)
		} else {
			rebuilt = append(rebuilt, r)
		}
	}
	build := func(id string, targets []string, unchanged bool, why string) *automationpb.PlannedJob {
		return &automationpb.PlannedJob{Job: id, Spec: &automationpb.Job{Kind: automationpb.Job_KIND_BUILD, Targets: targets, Pool: p.pool()},
			Why: "every target the change affects must build: " + why, UnchangedSincePre: unchanged}
	}
	switch {
	case len(roots) == 0:
	case len(held) == 0:
		out = append(out, build("build", roots, false, fmt.Sprintf("building %s builds them all", count(len(roots), "target", "targets"))))
	case len(rebuilt) == 0:
		out = append(out, build("build", roots, true, fmt.Sprintf("building %s builds them all, and nothing they build on changed since pre",
			count(len(roots), "target", "targets"))))
	default:
		out = append(out,
			build("build", held, true, fmt.Sprintf("building %s builds those nothing changed for since pre", count(len(held), "target", "targets"))),
			build(BuildSincePre, rebuilt, false, fmt.Sprintf("building %s builds those that changed since pre", count(len(rebuilt), "target", "targets"))))
	}
	for _, label := range p.in.Affected.Tests {
		t := p.affected[label]
		out = append(out, &automationpb.PlannedJob{Job: "test " + label,
			Spec:              &automationpb.Job{Kind: automationpb.Job_KIND_TEST, Targets: []string{label}, Pool: p.pool()},
			Why:               "the test is affected: " + Chain(t.Because),
			UnchangedSincePre: p.unchanged(label)})
	}
	if len(p.in.Affected.LintTools) == 0 {
		// The repository names no linters (its settings' bazel.lint).
		return out
	}
	if p.in.Standing == workflow.Graduates {
		// Nothing in an imported repository was ever linted.
		return append(out, &automationpb.PlannedJob{Job: "lint",
			Spec: &automationpb.Job{Kind: automationpb.Job_KIND_LINT, Targets: []string{"//..."}, Pool: p.pool()},
			Why:  "the change makes an imported repository a Bazel workspace: every file in it is linted"})
	}
	files := p.in.Context.Existing()
	removed := map[string]bool{}
	for _, r := range p.in.Context.Removed() {
		removed[r] = true
	}
	// A target that held a file the change removes can gain a finding without any file of its
	// own changing.
	var owners []string
	for _, t := range p.in.Affected.Affected {
		if t.Direct && len(t.Because) == 1 && removed[t.Because[0].File] {
			owners = append(owners, t.Label)
		}
	}
	if len(files) > 0 || len(owners) > 0 {
		why := fmt.Sprintf("the change's %s", count(len(files), "file", "files"))
		if len(owners) > 0 {
			why += fmt.Sprintf(", and the %s that held what it removes", count(len(owners), "target", "targets"))
		}
		out = append(out, &automationpb.PlannedJob{Job: "lint",
			Spec:              &automationpb.Job{Kind: automationpb.Job_KIND_LINT, Targets: owners, Pool: p.pool()},
			Why:               why,
			UnchangedSincePre: p.lintUnchanged()})
	}
	return out
}

// unchanged reports whether every one of the targets has the same inputs at the candidate as at
// the commit the CL's pre ran on: nothing that differs between them affects it.
func (p *Planner) unchanged(targets ...string) bool {
	if !p.sincePre {
		return false
	}
	for _, t := range targets {
		if p.changed[t] {
			return false
		}
	}
	return true
}

// lintUnchanged reports whether the lint finds at the candidate what it found at the commit pre
// ran on. It lints the change's files, which the candidate holds as the patchset does, and the
// targets that own them, with the repository's linters: its formatter, a target, and its lint
// aspects, each a .bzl file — which no query sees, so a change to anything in an aspect's package
// counts as a change to it. The files' findings depend on their content and the linters, the
// targets' on their closures. The candidate's change touches no path the patchset's does not, so
// its lint is pre's or a part of it.
func (p *Planner) lintUnchanged() bool {
	if !p.sincePre {
		return false
	}
	var targets []string
	for _, tool := range p.in.Affected.LintTools {
		if !strings.HasSuffix(tool, ".bzl") {
			targets = append(targets, tool)
			continue
		}
		pkg, _, _ := strings.Cut(strings.TrimPrefix(tool, "//"), ":")
		for _, f := range p.in.SincePre.Changed {
			if pkg == "" || f == pkg || strings.HasPrefix(f, pkg+"/") {
				return false
			}
		}
	}
	for _, t := range p.in.Affected.Affected {
		if t.Direct {
			targets = append(targets, t.Label)
		}
	}
	return p.unchanged(targets...)
}

// Chain renders why a target is affected: it, each dependency it reaches the change through,
// and the file that changed.
func Chain(because []Step) string {
	var b strings.Builder
	for i, s := range because {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(s.Label)
		switch {
		case s.File != "":
			b.WriteString(", which holds " + s.File)
		case s.Via != "":
			b.WriteString(" —" + s.Via + "→")
		}
	}
	return b.String()
}

// Summary is a plan's jobs and skipped jobs, one line each, for its log.
func Summary(pl *automationpb.Plan) []string {
	var out []string
	for _, j := range pl.GetJobs() {
		line := fmt.Sprintf("job %s: %s", j.GetJob(), j.GetWhy())
		if j.GetUnchangedSincePre() {
			line += " (unchanged since pre: a pass there holds here)"
		}
		out = append(out, line)
	}
	for _, s := range pl.GetSkipped() {
		out = append(out, fmt.Sprintf("not run %s: %s", s.GetJob(), s.GetWhy()))
	}
	return out
}

func count(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", n, many)
}

// dirOf is a repository-relative path's directory ("" for the root's files).
func dirOf(p string) string {
	d := gopath.Dir(p)
	if d == "." {
		return ""
	}
	return d
}

// packageOf is a label's package directory.
func packageOf(label string) string {
	pkg, _, _ := strings.Cut(strings.TrimPrefix(label, "//"), ":")
	return pkg
}

func dirLabel(dir string) string { return "//" + dir }

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// mapSource serves workflow files from a map.
type mapSource map[string][]byte

func (m mapSource) Read(p string) ([]byte, bool, error) {
	d, ok := m[p]
	return d, ok, nil
}

// pool is the pool the repository's built-in jobs run in.
func (p *Planner) pool() string { return workflow.PoolOf("", p.in.Pool) }
