package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type fake struct {
	plan    Report
	applied int
	err     error
}

func (f *fake) Plan(context.Context) (Report, error) { return f.plan, nil }
func (f *fake) Apply(_ context.Context, r Report) error {
	f.applied++
	return f.err
}

func runFake(t *testing.T, f *fake, args ...string) (int, Report) {
	t.Helper()
	var out bytes.Buffer
	code := Run(context.Background(), args, &out, func(context.Context) (Migrator, error) { return f, nil })
	var r Report
	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatalf("%v: %q is no report: %v", args, out.String(), err)
	}
	return code, r
}

func TestRun(t *testing.T) {
	dropping := Report{Database: "app", Server: "db:5432", Steps: []Step{
		{Description: "0002_drop", SQL: "DROP TABLE old;", Hazards: []Hazard{{Class: DeletesData, Detail: "DROP TABLE old"}, {Class: AccessExclusiveLock}}},
	}}
	// plan says what apply would do, gated or not, and applies nothing.
	f := &fake{plan: dropping}
	code, r := runFake(t, f, "plan")
	if code != 0 || f.applied != 0 || r.Executed || len(r.Steps) != 1 || !r.Steps[0].Hazards[0].Gated || r.Steps[0].Hazards[1].Gated {
		t.Errorf("plan: %d %+v (applied %d)", code, r, f.applied)
	}
	// apply refuses a gated hazard not allowed, applying nothing.
	code, r = runFake(t, f, "apply")
	if code != ExitRefused || f.applied != 0 || len(r.Refused) != 1 || r.Refused[0] != DeletesData || r.Executed || r.Error == "" {
		t.Errorf("apply, not allowed: %d %+v", code, r)
	}
	// Allowed, it applies.
	code, r = runFake(t, f, "apply", "--allow-hazard", DeletesData)
	if code != 0 || f.applied != 1 || !r.Executed || len(r.Refused) != 0 {
		t.Errorf("apply, allowed: %d %+v", code, r)
	}
	// A failure says why.
	f = &fake{plan: Report{Steps: []Step{{Description: "0003"}}}, err: errors.New("relation x exists")}
	if code, r = runFake(t, f, "apply"); code != 1 || r.Executed || r.Error != "relation x exists" {
		t.Errorf("a failure: %d %+v", code, r)
	}
	// Nothing to do: nothing applied, and it succeeded.
	f = &fake{}
	if code, r = runFake(t, f, "apply"); code != 0 || f.applied != 0 || r.Steps == nil {
		t.Errorf("nothing to do: %d %+v", code, r)
	}
	for _, args := range [][]string{nil, {"drop"}, {"plan", "--allow-hazard", "X"}, {"apply", "extra"}} {
		if code, r := runFake(t, &fake{}, args...); code != 1 || r.Error == "" {
			t.Errorf("%q: %d %+v", args, code, r)
		}
	}
}
