package review

import (
	"testing"

	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

func run(job string, st automationpb.Run_Status) *automationpb.Run {
	return &automationpb.Run{Origin: &automationpb.Origin{Job: job}, Status: st}
}

func TestPreReason(t *testing.T) {
	passed := automationpb.Run_STATUS_PASSED
	for name, tc := range map[string]struct {
		pre  *automationpb.Pre
		want string
	}{
		"passed":      {&automationpb.Pre{Verdict: automationpb.Pre_VERDICT_PASSED}, ""},
		"not planned": {&automationpb.Pre{Verdict: automationpb.Pre_VERDICT_NONE}, "Checks haven't run on patchset 3 yet"},
		"running":     {&automationpb.Pre{Verdict: automationpb.Pre_VERDICT_PENDING}, "Checks are still running on patchset 3"},
		"plan failed": {&automationpb.Pre{Verdict: automationpb.Pre_VERDICT_FAILED, Plan: run("plan", automationpb.Run_STATUS_FAILED)},
			"Checks failed on patchset 3: plan failed"},
		"a workflow job canceled": {&automationpb.Pre{Verdict: automationpb.Pre_VERDICT_FAILED, Plan: run("plan", passed),
			Jobs: []*automationpb.Run{run("build", passed), run("//.ok/workflow/ci.yaml:pre:e2e", automationpb.Run_STATUS_CANCELED), run("lint", automationpb.Run_STATUS_FAILED)}},
			"Checks failed on patchset 3: e2e was canceled"},
		"an error": {&automationpb.Pre{Verdict: automationpb.Pre_VERDICT_FAILED, Plan: run("plan", passed),
			Jobs: []*automationpb.Run{run("test //a:test", automationpb.Run_STATUS_ERROR)}},
			"Checks failed on patchset 3: test //a:test could not run"},
	} {
		if got := PreReason(tc.pre, 3); got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}
