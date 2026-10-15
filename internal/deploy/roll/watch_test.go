package roll

import (
	"strings"
	"testing"
	"time"
)

// A stage's pods must be Ready within 3 minutes of starting, and the stage passes 30 seconds
// after they started: a pod Ready at 10 seconds is watched 20 seconds more, and one Ready only
// after 30 seconds passes once it is Ready.
func TestEvaluateTiming(t *testing.T) {
	t0 := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) time.Time { return t0.Add(d) }
	pod := Pod{Name: "p", Started: t0, Ready: true}
	for name, tc := range map[string]struct {
		now  time.Duration
		pods []Pod
		want Verdict
	}{
		"ready, at 29s":                 {29 * time.Second, []Pod{pod}, Waiting},
		"ready, at 30s":                 {30 * time.Second, []Pod{pod}, Passed},
		"ready only at 2m, at 2m":       {2 * time.Minute, []Pod{pod}, Passed},
		"not ready yet at 2m59s":        {2*time.Minute + 59*time.Second, []Pod{{Name: "p", Started: t0}}, Waiting},
		"not ready at 3m01s":            {3*time.Minute + time.Second, []Pod{{Name: "p", Started: t0}}, Failed},
		"one of two settled":            {30 * time.Second, []Pod{pod, {Name: "q", Started: at(10 * time.Second), Ready: true}}, Waiting},
		"one of two pods, at 3m01s":     {3*time.Minute + time.Second, []Pod{pod}, Failed},
		"a pod replaced mid-stage, new": {time.Minute, []Pod{{Name: "r", Started: at(45 * time.Second), Ready: true}}, Waiting},
	} {
		want := 1
		if strings.HasPrefix(name, "one of two") {
			want = 2
		}
		j := Evaluate(DefaultTiming, at(tc.now), t0, want, tc.pods, nil)
		if j.Verdict != tc.want {
			t.Errorf("%s: %v (%s), want %v", name, j.Verdict, j.Reason, tc.want)
		}
	}
	// A pod seen Ready that is not any more fails the stage.
	j := Evaluate(DefaultTiming, at(20*time.Second), t0, 1, []Pod{{Name: "p", Started: t0}}, []string{"p"})
	if j.Verdict != Failed || !strings.Contains(j.Reason, "was Ready and is not any more") {
		t.Errorf("a pod that stopped being Ready: %+v", j)
	}
}
