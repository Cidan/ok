// Package plan decides a plan's deployments (docs/deploy.md, "When a deploy runs" and "Waves"):
// which of the deploys on main the commits since the previous plan make due, and the wave each
// runs in. It is pure: the deployer reads the workflow files, asks Bazel's graphs what changed, and
// reads the workloads, and hands this package what they said.
package plan

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/Cidan/ok/internal/buildsys"
	"github.com/Cidan/ok/internal/workflow"
)

// What the Bazel graphs say the changes since the previous plan affect (internal/buildsys).
type (
	Affected = buildsys.Affected
	Target   = buildsys.Target
	Step     = buildsys.Step
)

// TreeTarget is the migration of the built-in deploy that applies ok identity's groups-and-ACLs
// tree (TreeDeploy): not a Bazel target, but the deployer's own step.
const TreeTarget = "ok:identity-tree"

// TreeDeploy is the built-in deploy of the repository that holds ok's groups-and-ACLs tree: it
// applies the tree to ok identity. The repository's settings, which name the tree's folder,
// declare it.
func TreeDeploy() workflow.Deploy {
	return workflow.Deploy{File: workflow.SettingsFile, Name: "identity", Migrate: []workflow.Ref{{Target: TreeTarget}}}
}

// Chain renders why a target is affected.
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

// Input is what a plan is made from.
type Input struct {
	// Deploys are every deploy on main at the plan's head.
	Deploys []workflow.Deploy
	// First reports there was no previous plan: every deploy is due.
	First bool
	// Changed are the files changed since the previous plan's head, and Affected what the Bazel
	// graphs say they affect.
	Changed  []string
	Affected *Affected
	// TreeDir is the folder of ok's groups-and-ACLs tree, when this repository holds it: the plan
	// then has the built-in deploy that applies it (TreeDeploy), due when the tree or the settings
	// change.
	TreeDir string
	// Replicas are each workload's replicas now, by Workload.String(); a workload missing is
	// taken for one replica.
	Replicas map[string]int32
	// Self is the deployer's own workload (Workload.String()): its deploy goes last, in a wave
	// of its own.
	Self string
}

// Deployment is one deploy the plan makes due.
type Deployment struct {
	Deploy workflow.Deploy
	Why    string
	// Wave is 1, 2, or 3; then each deploy that runs alone takes one of its own, and the
	// deployer's own deploy the one after those.
	Wave int
	// Failed is why the deploy cannot run at all (it shares its workload with another); such
	// a deployment is recorded failed and takes no wave.
	Failed string
}

// Make decides the plan's deployments: each due deploy, with why and its wave, and each
// deploy that cannot run, with why.
func Make(in Input) []Deployment {
	affected := map[string]*Target{}
	if in.Affected != nil {
		for i := range in.Affected.Affected {
			affected[in.Affected.Affected[i].Label] = &in.Affected.Affected[i]
		}
	}
	changed := map[string]bool{}
	for _, f := range in.Changed {
		changed[f] = true
	}
	byWorkload := map[string][]string{}
	for _, d := range in.Deploys {
		if d.Workload != nil {
			byWorkload[d.Workload.String()] = append(byWorkload[d.Workload.String()], d.ID())
		}
	}

	var due, failed []Deployment
	if in.TreeDir != "" {
		if why := treeDue(in); why != "" {
			due = append(due, Deployment{Deploy: TreeDeploy(), Why: why})
		}
	}
	for _, d := range in.Deploys {
		why := dueBecause(in.First, d, affected, changed)
		if why == "" {
			continue
		}
		if d.Workload != nil && len(byWorkload[d.Workload.String()]) > 1 {
			failed = append(failed, Deployment{Deploy: d, Why: why, Failed: fmt.Sprintf("%s is rolled by more than one deploy (%s): a workload has one deploy",
				d.Workload, strings.Join(byWorkload[d.Workload.String()], ", "))})
			continue
		}
		due = append(due, Deployment{Deploy: d, Why: why})
	}
	sort.SliceStable(due, func(i, j int) bool {
		a, b := order(due[i].Deploy, in), order(due[j].Deploy, in)
		if a != b {
			return a < b
		}
		return due[i].Deploy.ID() < due[j].Deploy.ID()
	})
	// A deploy that runs alone takes a wave of its own after the rest's, so the plan's other
	// deployments are done before it interrupts what they need. The deployer's own deploy sorts
	// last, and takes a wave of its own after those: it starts only once every other deployment
	// of the plan has succeeded, so its failure stops no one else's, and it never rolls itself
	// while it drives another roll.
	var rest, alone, self []Deployment
	for _, d := range due {
		switch {
		case isSelf(d.Deploy, in):
			self = append(self, d)
		case d.Deploy.Alone:
			alone = append(alone, d)
		default:
			rest = append(rest, d)
		}
	}
	last := 0
	for i := range rest {
		rest[i].Wave = wave(i, len(rest))
		last = max(last, rest[i].Wave)
	}
	for i := range alone {
		last++
		alone[i].Wave = last
	}
	for i := range self {
		self[i].Wave = last + 1
	}
	out := append(append(rest, alone...), self...)
	return append(out, failed...)
}

// isSelf reports whether a deploy rolls the deployer's own workload.
func isSelf(d workflow.Deploy, in Input) bool {
	return d.Workload != nil && in.Self != "" && d.Workload.String() == in.Self
}

// treeDue says why the tree's deploy is due, or "" when it is not.
func treeDue(in Input) string {
	if in.First {
		return "the deployer has not planned this repository before: the groups-and-ACLs tree is applied"
	}
	for _, f := range in.Changed {
		switch {
		case f == workflow.SettingsFile:
			return fmt.Sprintf("the repository's settings %s changed", f)
		case strings.HasPrefix(f, in.TreeDir+"/"):
			return fmt.Sprintf("the groups-and-ACLs tree's %s changed", f)
		}
	}
	return ""
}

// dueBecause says why a deploy is due, or "" when it is not.
func dueBecause(first bool, d workflow.Deploy, affected map[string]*Target, changed map[string]bool) string {
	if first {
		return "the deployer has not planned this repository before: every deploy is due"
	}
	if changed[d.File] {
		return fmt.Sprintf("its workflow file %s changed", d.File)
	}
	for _, im := range d.Images {
		if t := affected[im.Target]; t != nil {
			return fmt.Sprintf("container %s's image %s is affected: %s", im.Container, im.Target, Chain(t.Because))
		}
	}
	for _, m := range d.Migrate {
		if t := affected[m.Target]; t != nil {
			return fmt.Sprintf("its migration %s is affected: %s", m.Target, Chain(t.Because))
		}
	}
	return ""
}

// order places a deploy in its plan (docs/deploy.md, "Waves"): workloads with several
// replicas first — their canaries serve beside healthy replicas — then those that only migrate,
// then single-replica workloads, and the deployer's own last.
func order(d workflow.Deploy, in Input) int {
	switch {
	case d.Workload == nil:
		return 1
	case isSelf(d, in):
		return 3
	}
	n, ok := in.Replicas[d.Workload.String()]
	if ok && n > 1 {
		return 0
	}
	return 2
}

// wave is the wave of the i-th of n deployments: one, then half of the rest (rounded up), then
// the rest.
func wave(i, n int) int {
	if i == 0 {
		return 1
	}
	rest := n - 1
	if i <= (rest+1)/2 {
		return 2
	}
	return 3
}

// Deploys are the deploys of a set of workflow files, the files that do not parse left out.
func Deploys(files map[string][]byte) ([]workflow.Deploy, []workflow.Error) {
	set := workflow.NewSet(files)
	return set.Deploys(), slices.Clip(set.Problems())
}
