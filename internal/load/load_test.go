package load

import (
	"math"
	"strings"
	"testing"
	"time"
)

// A shape's rates are the target's per repository, times its speed; peak multiplies publishes.
func TestRates(t *testing.T) {
	s := Shape{Repositories: 100, Speed: 100, CLsPerDay: 10}
	r := s.Rates()
	near := func(got, want float64) bool { return math.Abs(got-want) < want*1e-9 }
	if !near(r.Publishes, 10_000*200/86400.0) || !near(r.Checkpoints, 10_000*0.1/5) || !near(r.CLs, 10_000*10/86400.0) || s.CellFraction() != 1 {
		t.Errorf("a cell's share: %+v, %g", r, s.CellFraction())
	}
	s.Peak = true
	if p := s.Rates(); !near(p.Publishes, r.Publishes*12000/2300) || p.Checkpoints != r.Checkpoints {
		t.Errorf("at peak: %+v", p)
	}
	if !strings.Contains(s.String(), "at peak: 1 of a cell's share") {
		t.Errorf("String() = %q", s.String())
	}
}

// A histogram's quantile is within its buckets' 5% of the true one.
func TestHistogram(t *testing.T) {
	h := newHistogram()
	for i := 1; i <= 1000; i++ {
		h.add(time.Duration(i) * time.Millisecond)
	}
	for q, want := range map[float64]time.Duration{0.5: 500 * time.Millisecond, 0.99: 990 * time.Millisecond, 0.999: 999 * time.Millisecond} {
		got := h.quantile(q)
		if got < want || float64(got) > float64(want)*1.051 {
			t.Errorf("quantile %g = %s, want within 5%% above %s", q, got, want)
		}
	}
	if newHistogram().quantile(0.5) != 0 {
		t.Error("an empty histogram's median")
	}
}

// A cost scales a run's measurements linearly to a cell's full share, and prices each term.
func TestCost(t *testing.T) {
	p := Prices{CoreHour: Price{Price: 0.03}, MemoryGiBHour: Price{Price: 0.004}, MemoryPerCoreGiB: 4, ObjectGiBMonth: Price{Price: 0.02},
		ObjectClassA: Price{Price: 5e-6}, SpannerNodeHour: Price{Price: 0.9}, SpannerGiBMonth: Price{Price: 0.3}, SpannerNodeReads: 22500,
		SpannerNodeWrites: 3500, SpannerCPUCeiling: 0.65}
	rep := &Report{CellFraction: 0.5, Operations: map[string]Result{"publish": {OK: 100}}, Bytes: Bytes{Publish: 5e6}}
	// Over 100 s at half a cell: 1 core, 700 rows written a second (1,400 at a cell's), 10 objects.
	m := Measured{Seconds: 100, OKCoreSeconds: 100, DatabaseRowsWritten: 70_000, DatabaseRowsRead: 1000, ObjectBytes: 5e8, ObjectsWritten: 1000}
	c, err := m.Price(rep, p, Assumptions{Headroom: 0.5, Compression: 2, DatabaseBytesPerRepository: 50e6})
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]Line{}
	for _, l := range c.Lines {
		by[l.What] = l
	}
	if l := by["ok's servers"]; l.Quantity != 4 || math.Abs(l.Monthly-4*(0.03+4*0.004)*730) > 1e-9 {
		t.Errorf("servers: %+v", l)
	}
	if l := by["the cell's Spanner compute"]; l.Quantity != 1 { // 1,400 writes a second: 0.4 of a node, at 65%: one
		t.Errorf("Spanner compute: %+v", l)
	}
	if l := by["the cell's object writes"]; math.Abs(l.Quantity-20*secondsAMonth) > 1 {
		t.Errorf("object writes: %+v", l)
	}
	if !strings.Contains(by["the cell's object storage"].Basis, "stored 1.00 bytes a byte") {
		t.Errorf("object storage's basis: %s", by["the cell's object storage"].Basis)
	}
	var sum float64
	for _, l := range c.Lines {
		sum += l.Monthly
	}
	if math.Abs(sum-c.Cell) > 1e-6 || math.Abs(c.Target-c.Cell*100) > 1e-6 || math.Abs(c.PerAccount-c.Cell/10_000) > 1e-9 {
		t.Errorf("totals: cell %g (lines %g), target %g, account %g", c.Cell, sum, c.Target, c.PerAccount)
	}
	if _, err := (Measured{}).Price(rep, p, Assumptions{Headroom: 0.5, Compression: 2}); err == nil {
		t.Error("a run of no time priced")
	}
}

// A run that offered no CL passes the CL check; one that offered CLs and landed none fails it.
func TestCLCheck(t *testing.T) {
	check := func(offered int64, landed int) bool {
		r := &run{cfg: Config{Shape: Shape{Repositories: 1, Speed: 1, CLsPerDay: 10}}, stats: map[string]*stat{}}
		for _, op := range operations {
			r.stats[op] = newStat()
		}
		for range offered {
			r.stats["cl.landed"].offer()
			r.stats["cl.landed"].done(time.Second, nil)
		}
		r.cls.landed, r.cls.submitted = landed, landed
		for _, c := range r.report(time.Now(), time.Minute).Checks {
			if c.Name == "CLs offered landed" {
				return c.OK
			}
		}
		t.Fatal("no CL check")
		return false
	}
	if !check(0, 0) || check(2, 0) || !check(2, 2) {
		t.Errorf("none offered: %v; two offered, none landed: %v; two landed: %v", check(0, 0), check(2, 0), check(2, 2))
	}
}

// Reports of a run's pods merge as one: their counts and rates summed, their latencies' histograms
// merged, and their checks judged again.
func TestMerge(t *testing.T) {
	report := func(ok int64, lat time.Duration, landed int) *Report {
		st := newStat()
		for range ok {
			st.offer()
			st.done(lat, nil)
		}
		r := &Report{Offered: 60_000, Operations: map[string]Result{"publish": st.result(time.Minute)}, CLs: CLOutcomes{Submitted: landed, Landed: landed}}
		r.judge()
		return r
	}
	m, err := Merge([]*Report{report(100, 10*time.Millisecond, 3), report(300, 100*time.Millisecond, 2)})
	if err != nil {
		t.Fatal(err)
	}
	p := m.Operations["publish"]
	if p.OK != 400 || p.Offered != 400 || math.Abs(p.AchievedRate-400.0/60) > 1e-9 {
		t.Errorf("merged publishes: %+v", p)
	}
	// A quarter at 10 ms, the rest at 100 ms: the median is the slow pod's, within a bucket.
	if p.P50 < 95 || p.P50 > 106 || p.P90 < 95 {
		t.Errorf("merged quantiles: p50 %v, p90 %v", p.P50, p.P90)
	}
	if m.CLs.Landed != 5 || !m.Pass {
		t.Errorf("merged CLs %+v, pass %v: %+v", m.CLs, m.Pass, m.Checks)
	}
	bad := report(1, time.Millisecond, 0)
	bad.CLs.Stuck = []string{"stuck"}
	if m, _ := Merge([]*Report{report(1, time.Millisecond, 1), bad}); m.Pass {
		t.Error("a pod's stuck CL passed the merged run")
	}
}

// The people files name the same people, by handle and address, in the system of record and Dex.
func TestPeopleFiles(t *testing.T) {
	people, dex := PeopleFiles(3, "$2a$10$x")
	for _, want := range []string{"handle: load,", "handle: p00000, email: p00000@example.com", "handle: p00002"} {
		if !strings.Contains(string(people), want) {
			t.Errorf("people.yaml lacks %q:\n%s", want, people)
		}
	}
	if n := strings.Count(string(dex), "hash: \"$2a$10$x\""); n != 4 || !strings.Contains(string(dex), "email: p00001@example.com") {
		t.Errorf("dex-people.yaml: %d users\n%s", n, dex)
	}
}
