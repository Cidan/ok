package load

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// Report is what a run did.
type Report struct {
	Shape        string            `json:"shape"`
	CellFraction float64           `json:"cell_fraction"`
	Rates        Rates             `json:"rates_offered_per_second"`
	Started      time.Time         `json:"started"`
	Offered      Millis            `json:"offered_for_ms"`
	Operations   map[string]Result `json:"operations"`
	Bytes        Bytes             `json:"bytes_each"`
	CLs          CLOutcomes        `json:"cls"`
	Runs         Runs              `json:"runs"`
	Checks       []Check           `json:"checks"`
	Pass         bool              `json:"pass"`
}

// Bytes are what each publish, checkpoint, and CL sent.
type Bytes struct {
	Publish    float64 `json:"publish"`
	Checkpoint float64 `json:"checkpoint"`
	CL         float64 `json:"cl"`
}

// CLOutcomes are how the CLs a run uploaded ended.
type CLOutcomes struct {
	Submitted int            `json:"submitted"`
	Landed    int            `json:"landed"`
	Failed    map[string]int `json:"failed,omitempty"`
	Stuck     []string       `json:"stuck,omitempty"`
}

// Runs are what the synthetic workers did.
type Runs struct {
	Plans    int64 `json:"plans"`
	Jobs     int64 `json:"jobs"`
	Released int64 `json:"released"`
}

// Check is something that must hold however a run went.
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

func (r *run) report(started time.Time, offered time.Duration) *Report {
	rep := &Report{Shape: r.cfg.Shape.String(), CellFraction: r.cfg.Shape.CellFraction(), Rates: r.cfg.Shape.Rates(), Started: started,
		Offered: ms(offered), Operations: map[string]Result{},
		Bytes: Bytes{Publish: float64(r.cfg.PublishBytes), Checkpoint: float64(r.cfg.CheckpointBytes), CL: float64(r.cfg.CLBytes)}}
	for name, st := range r.stats {
		if res := st.result(offered); res.Offered > 0 {
			rep.Operations[name] = res
		}
	}
	r.cls.Lock()
	rep.CLs = CLOutcomes{Submitted: r.cls.submitted, Landed: r.cls.landed, Stuck: append([]string(nil), r.cls.stuck...)}
	if len(r.cls.failed) > 0 {
		rep.CLs.Failed = map[string]int{}
		for k, v := range r.cls.failed {
			rep.CLs.Failed[k] = v
		}
	}
	r.cls.Unlock()
	rep.Runs = Runs{Plans: r.runs.plans.Load(), Jobs: r.runs.jobs.Load(), Released: r.runs.released.Load()}
	rep.judge()
	return rep
}

// judge decides a report's checks.
func (rep *Report) judge() {
	failedSubmitted := 0
	for reason, n := range rep.CLs.Failed {
		if reason == "post failed" {
			failedSubmitted += n
		}
	}
	rep.Checks = []Check{
		{Name: "every submitted CL landed or failed with a reason", OK: rep.CLs.Landed+failedSubmitted == rep.CLs.Submitted,
			Detail: fmt.Sprintf("%d submitted, %d landed, %d failed in post", rep.CLs.Submitted, rep.CLs.Landed, failedSubmitted)},
		{Name: "no CL stuck", OK: len(rep.CLs.Stuck) == 0, Detail: fmt.Sprintf("%d stuck", len(rep.CLs.Stuck))},
		{Name: "every operation offered ran or was shed", OK: accounted(rep.Operations)},
		{Name: "CLs offered landed", OK: rep.CLs.Landed > 0 || rep.Operations["cl.landed"].Offered == 0,
			Detail: fmt.Sprintf("%d of %d offered", rep.CLs.Landed, rep.Operations["cl.landed"].Offered)},
	}
	rep.Pass = true
	for _, c := range rep.Checks {
		rep.Pass = rep.Pass && c.OK
	}
}

// Merge is the reports of a run's pods, which ran at once, as one: their counts summed, their
// latencies' histograms merged, their rates summed over the longest of them.
func Merge(reps []*Report) (*Report, error) {
	if len(reps) == 0 {
		return nil, errors.New("load: no reports to merge")
	}
	out := &Report{Shape: fmt.Sprintf("%d pods: %s", len(reps), reps[0].Shape), Started: reps[0].Started, Operations: map[string]Result{},
		Bytes: reps[0].Bytes, CLs: CLOutcomes{Failed: map[string]int{}}}
	hists := map[string]*histogram{}
	for _, r := range reps {
		out.CellFraction += r.CellFraction
		out.Rates.Publishes += r.Rates.Publishes
		out.Rates.Checkpoints += r.Rates.Checkpoints
		out.Rates.CLs += r.Rates.CLs
		out.Rates.Browses += r.Rates.Browses
		out.Offered = max(out.Offered, r.Offered)
		if r.Started.Before(out.Started) {
			out.Started = r.Started
		}
		for name, o := range r.Operations {
			m := out.Operations[name]
			m.Offered += o.Offered
			m.Shed += o.Shed
			m.OK += o.OK
			for code, n := range o.Errors {
				if m.Errors == nil {
					m.Errors, m.ErrorExamples = map[string]int64{}, map[string]string{}
				}
				m.Errors[code] += n
				if _, ok := m.ErrorExamples[code]; !ok {
					m.ErrorExamples[code] = o.ErrorExamples[code]
				}
			}
			h := hists[name]
			if h == nil {
				h = newHistogram()
				hists[name] = h
			}
			if len(o.Buckets) != len(h.counts) {
				return nil, fmt.Errorf("load: %s's histogram has %d buckets, not %d", name, len(o.Buckets), len(h.counts))
			}
			for i, n := range o.Buckets {
				h.counts[i] += n
			}
			out.Operations[name] = m
		}
		out.CLs.Submitted += r.CLs.Submitted
		out.CLs.Landed += r.CLs.Landed
		out.CLs.Stuck = append(out.CLs.Stuck, r.CLs.Stuck...)
		for k, v := range r.CLs.Failed {
			out.CLs.Failed[k] += v
		}
		out.Runs.Plans += r.Runs.Plans
		out.Runs.Jobs += r.Runs.Jobs
		out.Runs.Released += r.Runs.Released
	}
	over := time.Duration(float64(out.Offered) * float64(time.Millisecond)).Seconds()
	for name, m := range out.Operations {
		h := hists[name]
		m.OfferedRate, m.AchievedRate = float64(m.Offered)/over, float64(m.OK)/over
		m.P50, m.P90, m.P99, m.P999 = ms(h.quantile(0.5)), ms(h.quantile(0.9)), ms(h.quantile(0.99)), ms(h.quantile(0.999))
		m.Buckets = h.counts
		out.Operations[name] = m
	}
	if len(out.CLs.Failed) == 0 {
		out.CLs.Failed = nil
	}
	out.judge()
	return out, nil
}

// accounted reports whether every operation's offers ended: done, failed, or shed.
func accounted(ops map[string]Result) bool {
	for _, o := range ops {
		var errs int64
		for _, n := range o.Errors {
			errs += n
		}
		if o.OK+errs+o.Shed != o.Offered {
			return false
		}
	}
	return true
}

// Summary is a report in lines, for a terminal.
func (rep *Report) Summary() []string {
	out := []string{fmt.Sprintf("%s, for %.0fs", rep.Shape, float64(rep.Offered)/1000)}
	names := make([]string, 0, len(rep.Operations))
	for n := range rep.Operations {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		o := rep.Operations[n]
		var errs int64
		for _, e := range o.Errors {
			errs += e
		}
		out = append(out, fmt.Sprintf("  %-13s %8.2f/s offered %8.2f/s done  shed %d  errors %d  p50 %.1fms p99 %.1fms p99.9 %.1fms",
			n, o.OfferedRate, o.AchievedRate, o.Shed, errs, o.P50, o.P99, o.P999))
	}
	out = append(out, fmt.Sprintf("  CLs: %d submitted, %d landed, %v failed, %d stuck; runs: %d plans, %d jobs", rep.CLs.Submitted, rep.CLs.Landed,
		rep.CLs.Failed, len(rep.CLs.Stuck), rep.Runs.Plans, rep.Runs.Jobs))
	for _, c := range rep.Checks {
		mark := "ok  "
		if !c.OK {
			mark = "FAIL"
		}
		out = append(out, fmt.Sprintf("  %s %s %s", mark, c.Name, c.Detail))
	}
	return out
}
