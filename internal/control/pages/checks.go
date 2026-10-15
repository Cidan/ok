package pages

import (
	"strconv"
	"strings"
)

// Check kinds: the groups the Checks tab shows a patchset's pre, and a submission's post, in.
const (
	CheckPlan     = "plan"
	CheckBuild    = "build"
	CheckTest     = "test"
	CheckLint     = "lint"
	CheckWorkflow = "workflow"
)

// checkKinds are the groups of runs, in the order the tab shows them.
var checkKinds = []struct{ kind, title string }{
	{CheckPlan, "Plan"},
	{CheckBuild, "Build"},
	{CheckTest, "Test"},
	{CheckLint, "Lint"},
	{CheckWorkflow, "Workflow Jobs"},
}

// runStatuses are a run's statuses, worst first: a group shows its worst, and tallies its runs
// in this order.
var runStatuses = []struct{ kind, label string }{
	{"failed", "Failed"},
	{"error", "Error"},
	{"running", "Running"},
	{"queued", "Queued"},
	{"canceled", "Canceled"},
	{"passed", "Passed"},
}

// checkGroup is one group of the Checks tab: a kind of run, the workflow jobs left unrun, or a
// submission's attempts. It opens to its rows; its header tallies them and shows the worst status.
// Key names its open state, which the tab keeps across its refreshes (clcomments.js).
type checkGroup struct {
	Key         string
	Title       string
	Count       int
	Runs        []CLCheck
	Skipped     []CLSkippedCheck
	Attempts    []CLAttempt
	Tally       string
	StatusLabel string
	StatusKind  string // clCheckTone's key
	Open        bool   // it starts open: one of its runs failed
}

// checkGroups are a set of runs' groups — one for each kind, whether it ran or not, then the
// workflow jobs left unrun — keyed under scope ("pre-3", "post").
func checkGroups(scope string, runs []CLCheck, skipped []CLSkippedCheck) []checkGroup {
	out := make([]checkGroup, 0, len(checkKinds)+1)
	for _, k := range checkKinds {
		g := checkGroup{Key: scope + ":" + k.kind, Title: k.title}
		for _, c := range runs {
			if c.Kind == k.kind {
				g.Runs = append(g.Runs, c)
			}
		}
		g.Count = len(g.Runs)
		g.tally()
		out = append(out, g)
	}
	unaffected := checkGroup{Key: scope + ":unaffected", Title: "Unaffected Workflow Jobs", Skipped: skipped, Count: len(skipped),
		StatusLabel: "None", StatusKind: "none", Tally: "None"}
	if len(skipped) > 0 {
		unaffected.StatusLabel, unaffected.Tally = "Not Run", pluralCount(len(skipped), "job", "jobs")+" whose targets the change does not affect"
	}
	return append(out, unaffected)
}

// tally sets a group of runs' header: how many runs stand where, and the worst status.
func (g *checkGroup) tally() {
	if len(g.Runs) == 0 {
		g.Tally, g.StatusLabel, g.StatusKind = "None", "None", "none"
		return
	}
	var parts []string
	for _, st := range runStatuses {
		n := 0
		for _, c := range g.Runs {
			if c.StatusKind == st.kind {
				n++
			}
		}
		if n == 0 {
			continue
		}
		if g.StatusKind == "" {
			g.StatusLabel, g.StatusKind = st.label, st.kind
		}
		parts = append(parts, strconv.Itoa(n)+" "+st.label)
	}
	g.Tally = strings.Join(parts, " · ")
	g.Open = g.StatusKind == "failed" || g.StatusKind == "error"
}

// attemptsGroup is a submission's attempts at landing, newest first: its header shows the newest's
// status.
func attemptsGroup(scope string, attempts []CLAttempt) checkGroup {
	g := checkGroup{Key: scope + ":attempts", Title: "Attempts", Attempts: attempts, Count: len(attempts),
		Tally: "None", StatusLabel: "None", StatusKind: "none"}
	if len(attempts) > 0 {
		g.Tally = pluralCount(len(attempts), "attempt", "attempts") + " at landing it"
		g.StatusLabel, g.StatusKind = attempts[0].Status, attempts[0].StatusKind
	}
	return g
}

// preScope keys a patchset's pre groups: each patchset's open state is its own.
func preScope(patchset int64) string { return "pre-" + strconv.FormatInt(patchset, 10) }

// checkRowName is a run's name in its group: a test is its target.
func checkRowName(c CLCheck) string {
	if c.Kind == CheckTest {
		return strings.TrimPrefix(c.Name, "test ")
	}
	return c.Name
}
