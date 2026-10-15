package checks

import (
	"testing"

	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

func TestJobName(t *testing.T) {
	for id, want := range map[string][2]string{
		"//apps/x/.ok/workflow/ci.yaml:pre:unit": {"unit", "//apps/x/.ok/workflow/ci.yaml"},
		"build":                                  {"build", "Built In"},
		"//odd":                                  {"//odd", "Built In"},
		"//a:b":                                  {"//a:b", "Built In"},
	} {
		name, source := JobName(id)
		if name != want[0] || source != want[1] {
			t.Errorf("JobName(%q) = %q, %q; want %q, %q", id, name, source, want[0], want[1])
		}
	}
}

func TestCounting(t *testing.T) {
	a := func(id int64, s automationpb.Attempt_Status) *automationpb.Attempt {
		return &automationpb.Attempt{Id: id, Status: s}
	}
	for name, tc := range map[string]struct {
		attempts []*automationpb.Attempt // newest first
		want     int64
	}{
		"none":                   {nil, 0},
		"the one testing":        {[]*automationpb.Attempt{a(3, automationpb.Attempt_STATUS_DISCARDED), a(2, automationpb.Attempt_STATUS_ACTIVE), a(1, automationpb.Attempt_STATUS_FAILED)}, 2},
		"the newest that landed": {[]*automationpb.Attempt{a(3, automationpb.Attempt_STATUS_DISCARDED), a(2, automationpb.Attempt_STATUS_LANDED), a(1, automationpb.Attempt_STATUS_FAILED)}, 2},
		"else the newest":        {[]*automationpb.Attempt{a(2, automationpb.Attempt_STATUS_DISCARDED), a(1, automationpb.Attempt_STATUS_DISCARDED)}, 2},
	} {
		got := Counting(&automationpb.Submission{Attempts: tc.attempts})
		if got.GetId() != tc.want {
			t.Errorf("%s: attempt %d, want %d", name, got.GetId(), tc.want)
		}
	}
}
