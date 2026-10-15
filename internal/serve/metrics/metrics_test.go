package metrics

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustPanic(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		r := recover()
		if r == nil {
			t.Fatalf("no panic, want one saying %q", want)
		}
		if !strings.Contains(fmt.Sprint(r), want) {
			t.Fatalf("panic %q, want one saying %q", r, want)
		}
	}()
	f()
}

func exposition(r *Registry) string {
	var b bytes.Buffer
	r.WritePrometheus(&b)
	return b.String()
}

// A declaration that breaks a rule panics, saying which.
func TestDeclarationsAreChecked(t *testing.T) {
	r := NewRegistry()
	for _, c := range []struct {
		want string
		f    func()
	}{
		{"snake_case", func() { r.NewCounter("Bad_total", "x") }},
		{"snake_case", func() { r.NewCounter("bad__total", "x") }},
		{"snake_case", func() { r.NewGauge("trailing_", "x") }},
		{"must end in _total", func() { r.NewCounter("requests", "x") }},
		{"which only a counter may", func() { r.NewGauge("requests_total", "x") }},
		{"which only a counter may", func() { r.NewHistogram("sizes_total", "x", Count) }},
		{"must end in _seconds", func() { r.NewHistogram("latency", "x", Seconds) }},
		{"must end in _bytes", func() { r.NewHistogram("size_seconds", "x", Bytes) }},
		{"names a unit", func() { r.NewHistogram("files_bytes", "x", Count) }},
		{"one-line description", func() { r.NewGauge("depth", "") }},
		{"one-line description", func() { r.NewGauge("depth", "two\nlines") }},
		{"is reserved", func() { r.NewGauge("depth", "x", "namespace") }},
		{"is reserved", func() { r.NewGauge("depth", "x", "service") }},
		{"is reserved", func() { r.NewHistogram("x_seconds", "x", Seconds, "le") }},
		{"named twice", func() { r.NewGauge("depth", "x", "queue", "queue") }},
		{"not a snake_case name", func() { r.NewGauge("depth", "x", "Queue") }},
		{"must end in _total", func() { r.NewCounterFunc("reads", "x", func(func(float64, ...string)) {}) }},
	} {
		mustPanic(t, c.want, c.f)
	}
	r.NewGauge("depth", "Queue depth.")
	mustPanic(t, "declared twice", func() { r.NewGauge("depth", "Again.") })
}

// Series are written with their family's HELP and TYPE, label values escaped, families sorted by
// name; a family with no series writes nothing.
func TestExposition(t *testing.T) {
	r := NewRegistry()
	reads := r.NewCounter("cache_reads_total", `Reads, by "cache" and result.`, "cache", "result")
	depth := r.NewGauge("queue_depth", "Items waiting.")
	latency := r.NewHistogram("store_request_seconds", "Requests to the object store.", Seconds, "op")
	r.NewGauge("unused", "Never set.", "x")

	reads.With("objects", "hit").Add(3)
	reads.With("objects", "miss").Inc()
	reads.With(`odd "value"`+"\n\\", "hit").Inc()
	depth.Set(7.5)
	latency.With("get").Observe(0.25)
	latency.With("get").Observe(0.5)

	got := exposition(r)
	for _, want := range []string{
		"# HELP cache_reads_total Reads, by \"cache\" and result.\n# TYPE cache_reads_total counter\n",
		`cache_reads_total{cache="objects",result="hit"} 3` + "\n",
		`cache_reads_total{cache="objects",result="miss"} 1` + "\n",
		`cache_reads_total{cache="odd \"value\"\n\\",result="hit"} 1` + "\n",
		"# TYPE queue_depth gauge\nqueue_depth 7.5\n",
		"# TYPE store_request_seconds histogram\n",
		`store_request_seconds_count{op="get"} 2` + "\n",
		`store_request_seconds_sum{op="get"} 0.75` + "\n",
		`store_request_seconds_bucket{op="get",le="0.1"} 0` + "\n",
		`store_request_seconds_bucket{op="get",le="0.25"} 1` + "\n",
		`store_request_seconds_bucket{op="get",le="0.5"} 2` + "\n",
		`store_request_seconds_bucket{op="get",le="+Inf"} 2` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the exposition lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "unused") {
		t.Errorf("a family with no series was written:\n%s", got)
	}
	if !(strings.Index(got, "cache_reads_total") < strings.Index(got, "queue_depth") && strings.Index(got, "queue_depth") < strings.Index(got, "store_request_seconds")) {
		t.Errorf("families are not sorted by name:\n%s", got)
	}

	if v := r.Read("cache_reads_total", "objects", "hit"); !v.Found || v.Value != 3 {
		t.Errorf("Read counter = %+v", v)
	}
	if v := r.Read("store_request_seconds", "get"); !v.Found || v.Count != 2 || v.Sum != 0.75 {
		t.Errorf("Read histogram = %+v", v)
	}
	if v := r.Read("queue_depth"); !v.Found || v.Value != 7.5 {
		t.Errorf("Read gauge = %+v", v)
	}
	if v := r.Read("cache_reads_total", "chunks", "hit"); v.Found {
		t.Errorf("Read found a series never written: %+v", v)
	}
	if v := r.Read("no_such_metric"); v.Found {
		t.Errorf("Read found an undeclared metric")
	}

	mustPanic(t, "takes 2 label values", func() { reads.With("objects") })
	mustPanic(t, "takes 0 label values", func() { depth.With("x") })
	mustPanic(t, "not in seconds", func() {
		r.NewHistogram("sizes_bytes", "Sizes.", Bytes).With().Since(time.Now())
	})
}

// Past MaxSeries label combinations, a metric records in its folded series, and says how often.
func TestSeriesAreFoldedPastTheCap(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("paths_total", "Paths.", "path")
	for i := range MaxSeries + 5 {
		c.With(fmt.Sprintf("/p/%d", i)).Inc()
	}
	c.With("/p/0").Inc()
	if v := r.Read("paths_total", "/p/0"); v.Value != 2 {
		t.Fatalf("an existing series is %v, want 2", v.Value)
	}
	if v := r.Read("paths_total", Other); v.Value != 5 {
		t.Fatalf("the folded series is %v, want 5", v.Value)
	}
	if v := r.Read("ok_metrics_folded_total", "paths_total"); v.Value != 5 {
		t.Fatalf("folds counted %v, want 5", v.Value)
	}
	if n := strings.Count(exposition(r), "\npaths_total{"); n != MaxSeries+1 {
		t.Fatalf("%d series written, want %d", n, MaxSeries+1)
	}
}

// A sampled metric reads its series when written: reports of one series add up, a wrong number
// of label values panics, and past MaxSeries the rest are folded.
func TestSampledMetrics(t *testing.T) {
	r := NewRegistry()
	held := map[string]float64{"objects": 10, "chunks": 20}
	var mu sync.Mutex
	r.NewGaugeFunc("cache_bytes", "Bytes held, by cache.", func(report func(float64, ...string)) {
		mu.Lock()
		defer mu.Unlock()
		for name, n := range held {
			report(n, name)
		}
		report(1, "chunks")
	}, "cache")
	r.NewCounterFunc("cache_evictions_total", "Evictions.", func(report func(float64, ...string)) { report(4) })

	got := exposition(r)
	for _, want := range []string{
		"# TYPE cache_bytes gauge\ncache_bytes{cache=\"chunks\"} 21\ncache_bytes{cache=\"objects\"} 10\n",
		"# TYPE cache_evictions_total counter\ncache_evictions_total 4\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the exposition lacks %q:\n%s", want, got)
		}
	}
	mu.Lock()
	held["diffs"] = 3
	mu.Unlock()
	if v := r.Read("cache_bytes", "diffs"); v.Value != 3 {
		t.Errorf("a new value was not sampled: %+v", v)
	}

	r.NewGaugeFunc("broken", "Reports the wrong labels.", func(report func(float64, ...string)) { report(1) }, "a")
	mustPanic(t, "takes 1 label values", func() { exposition(r) })

	r2 := NewRegistry()
	r2.NewGaugeFunc("many", "Many.", func(report func(float64, ...string)) {
		for i := range MaxSeries + 3 {
			report(1, fmt.Sprint(i))
		}
	}, "i")
	if v := r2.Read("many", Other); v.Value != 3 {
		t.Errorf("the folded sampled series is %v, want 3", v.Value)
	}
}

// The handler serves the registry and the process's metrics at /metrics, and nothing else.
func TestHandler(t *testing.T) {
	r := NewRegistry()
	r.NewCounter("served_total", "Served.").Inc()
	h := r.Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain; version=0.0.4") {
		t.Fatalf("GET /metrics: %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	for _, want := range []string{"served_total 1\n", "go_goroutines ", "process_resident_memory_bytes "} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	for _, c := range []struct {
		method, path string
		code         int
	}{
		{http.MethodGet, "/", http.StatusNotFound},
		{http.MethodGet, "/metrics/x", http.StatusNotFound},
		{http.MethodPost, "/metrics", http.StatusMethodNotAllowed},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(c.method, c.path, nil))
		if rec.Code != c.code {
			t.Errorf("%s %s: %d, want %d", c.method, c.path, rec.Code, c.code)
		}
	}
}

// Series are safe to use from many goroutines at once, and none are lost.
func TestConcurrentUse(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounter("hits_total", "Hits.", "shard")
	h := r.NewHistogram("work_seconds", "Work.", Seconds, "shard")
	g := r.NewGauge("open", "Open.", "shard")
	var wg sync.WaitGroup
	for w := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			shard := fmt.Sprint(w % 4)
			for range 1000 {
				c.With(shard).Inc()
				h.With(shard).Observe(0.001)
				g.With(shard).Inc()
				g.With(shard).Dec()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 50 {
			exposition(r)
		}
	}()
	wg.Wait()
	for shard := range 4 {
		s := fmt.Sprint(shard)
		if v := r.Read("hits_total", s); v.Value != 4000 {
			t.Errorf("shard %s counted %v, want 4000", s, v.Value)
		}
		if v := r.Read("work_seconds", s); v.Count != 4000 {
			t.Errorf("shard %s observed %d, want 4000", s, v.Count)
		}
		if v := r.Read("open", s); v.Value != 0 {
			t.Errorf("shard %s gauge %v, want 0", s, v.Value)
		}
	}
}
