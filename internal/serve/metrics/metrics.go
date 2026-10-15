// Package metrics is how ok's roles report what they do: counters, gauges, and histograms, each
// declared once — a package variable — with its name, what it measures, and its label names, and
// written in the Prometheus text format on the role's metrics port. The serving runtime reports
// every role's calls, routes, process, and node graph itself; a service declares only what is its
// own:
//
//	var lookups = metrics.NewCounter("ok_scm_cache_lookups_total",
//		"Object lookups, by where they were answered.", "tier")
//
//	lookups.With("memory").Inc()
//
// A declaration that breaks a rule — a name that is not snake_case, a counter not ending in
// _total, a histogram not ending in its unit, a reserved or repeated label — panics: declarations
// run when their package loads, so its tests fail. A metric holds at most MaxSeries label
// combinations; values past them are folded into one series.
package metrics

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	vm "github.com/VictoriaMetrics/metrics"
)

// buckets are each unit's histogram buckets' upper bounds: a millisecond to five minutes, 64 bytes
// to a gibibyte in steps of four, and one to a million.
var buckets = map[Unit][]float64{
	Seconds: {.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	Bytes:   {64, 256, 1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20, 16 << 20, 64 << 20, 256 << 20, 1 << 30},
	Count:   {1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 100000, 1000000},
}

// Unit is what a histogram observes. Its name ends in it: _seconds, _bytes.
type Unit uint8

const (
	// Count is a quantity with no unit: messages, files, items.
	Count Unit = iota
	// Seconds is a duration.
	Seconds
	// Bytes is a size.
	Bytes
)

// MaxSeries is how many label combinations one metric holds. Values past it are recorded in one
// series whose every label is Other, and counted by ok_metrics_folded_total.
const MaxSeries = 1000

// Other is every label of a metric's folded series.
const Other = "_other"

// reserved are the labels the scrape adds to every series (the target's job, instance, namespace,
// pod, container, Service, and endpoint) and those the exposition gives meaning to.
var reserved = map[string]bool{
	"le": true, "vmrange": true, "quantile": true,
	"job": true, "instance": true, "namespace": true, "pod": true, "container": true,
	"service": true, "endpoint": true,
}

var identRE = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

type kind uint8

const (
	counterKind kind = iota
	gaugeKind
	histogramKind
	counterFuncKind
	gaugeFuncKind
)

func (k kind) exposed() string {
	switch k {
	case counterKind, counterFuncKind:
		return "counter"
	case gaugeKind, gaugeFuncKind:
		return "gauge"
	default:
		return "histogram"
	}
}

// Registry holds metrics and writes them. A binary reports through Default; a test that needs
// metrics of its own makes a Registry with NewRegistry.
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
	folded   *Counter
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	r := &Registry{families: map[string]*family{}}
	r.folded = r.NewCounter("ok_metrics_folded_total",
		"Observations recorded in a metric's folded series: its label combinations were already MaxSeries.", "metric")
	return r
}

var defaultRegistry = NewRegistry()

// Default is the role's registry: what it serves on its metrics port.
func Default() *Registry { return defaultRegistry }

// family is one metric: its declaration and its series.
type family struct {
	r      *Registry
	name   string
	help   string
	kind   kind
	unit   Unit
	labels []string
	set    *vm.Set

	mu     sync.RWMutex
	series map[string]any // by joined label values: *vm.Counter, *vm.Gauge, or *vm.PrometheusHistogram

	sample func(report func(value float64, labelValues ...string)) // the func kinds
}

func (r *Registry) declare(name, help string, k kind, unit Unit, labels []string) *family {
	if !identRE.MatchString(name) {
		panic(fmt.Sprintf("metrics: %q is not a snake_case name", name))
	}
	isTotal := strings.HasSuffix(name, "_total")
	switch {
	case (k == counterKind || k == counterFuncKind) && !isTotal:
		panic(fmt.Sprintf("metrics: counter %q must end in _total", name))
	case k != counterKind && k != counterFuncKind && isTotal:
		panic(fmt.Sprintf("metrics: %q ends in _total, which only a counter may", name))
	}
	if k == histogramKind {
		seconds, bytes := strings.HasSuffix(name, "_seconds"), strings.HasSuffix(name, "_bytes")
		switch {
		case unit == Seconds && !seconds:
			panic(fmt.Sprintf("metrics: histogram %q is in seconds and must end in _seconds", name))
		case unit == Bytes && !bytes:
			panic(fmt.Sprintf("metrics: histogram %q is in bytes and must end in _bytes", name))
		case unit == Count && (seconds || bytes):
			panic(fmt.Sprintf("metrics: histogram %q counts things, but its name names a unit", name))
		}
	}
	if help == "" || strings.ContainsAny(help, "\n\r") {
		panic(fmt.Sprintf("metrics: %q needs a one-line description", name))
	}
	for i, l := range labels {
		switch {
		case !identRE.MatchString(l):
			panic(fmt.Sprintf("metrics: %q: label %q is not a snake_case name", name, l))
		case reserved[l]:
			panic(fmt.Sprintf("metrics: %q: label %q is reserved: the scrape or the format gives it its own meaning", name, l))
		case slices.Contains(labels[:i], l):
			panic(fmt.Sprintf("metrics: %q: label %q is named twice", name, l))
		}
	}
	f := &family{
		r: r, name: name, help: help, kind: k, unit: unit,
		labels: slices.Clone(labels), set: vm.NewSet(), series: map[string]any{},
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.families[name]; ok {
		panic(fmt.Sprintf("metrics: %q is declared twice", name))
	}
	r.families[name] = f
	return f
}

// get returns the series for the label values, creating it. Past MaxSeries, it is the folded
// series.
func (f *family) get(values []string) any {
	if len(values) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s takes %d label values (%s), not %d", f.name, len(f.labels), strings.Join(f.labels, ", "), len(values)))
	}
	key := strings.Join(values, "\xff")
	f.mu.RLock()
	s, ok := f.series[key]
	f.mu.RUnlock()
	if ok {
		return s
	}
	f.mu.Lock()
	if s, ok := f.series[key]; ok {
		f.mu.Unlock()
		return s
	}
	folded := len(f.series) >= MaxSeries
	if folded {
		values = others(len(values))
		key = strings.Join(values, "\xff")
	}
	s, ok = f.series[key]
	if !ok {
		name := seriesName(f.name, f.labels, values)
		switch f.kind {
		case counterKind:
			s = f.set.NewCounter(name)
		case gaugeKind:
			s = f.set.NewGauge(name, nil)
		case histogramKind:
			s = f.set.NewPrometheusHistogramExt(name, buckets[f.unit])
		}
		f.series[key] = s
	}
	f.mu.Unlock()
	if folded && f != f.r.folded.f {
		f.r.folded.With(f.name).Inc()
	}
	return s
}

func others(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = Other
	}
	return out
}

// seriesName is a series in the text format: name{label="value",…}, values escaped.
func seriesName(name string, labels, values []string) string {
	if len(labels) == 0 {
		return name
	}
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l)
		b.WriteString(`="`)
		b.WriteString(escapeValue(values[i]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

var valueEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escapeValue(v string) string {
	if !utf8.ValidString(v) {
		v = strings.ToValidUTF8(v, "�")
	}
	return valueEscaper.Replace(v)
}

// Counter counts events: requests, bytes sent, errors. It only goes up.
type Counter struct{ f *family }

// NewCounter declares a counter in the binary's registry. Its name ends in _total.
func NewCounter(name, help string, labels ...string) *Counter {
	return defaultRegistry.NewCounter(name, help, labels...)
}

// NewCounter declares a counter in r.
func (r *Registry) NewCounter(name, help string, labels ...string) *Counter {
	return &Counter{r.declare(name, help, counterKind, Count, labels)}
}

// With is the counter's series for the label values, one per label, in their declared order.
// Keep it to count the same series again without looking it up.
func (c *Counter) With(labelValues ...string) CounterSeries {
	return CounterSeries{c.f.get(labelValues).(*vm.Counter)}
}

// Inc adds one to a counter declared without labels.
func (c *Counter) Inc() { c.With().Inc() }

// Add adds n to a counter declared without labels.
func (c *Counter) Add(n uint64) { c.With().Add(n) }

// CounterSeries is one series of a counter.
type CounterSeries struct{ c *vm.Counter }

// Inc adds one.
func (s CounterSeries) Inc() { s.c.Inc() }

// Add adds n.
func (s CounterSeries) Add(n uint64) { s.c.AddInt64(int64(n)) }

// Gauge is a value that goes up and down: queue length, bytes held, connections open.
type Gauge struct{ f *family }

// NewGauge declares a gauge in the binary's registry.
func NewGauge(name, help string, labels ...string) *Gauge {
	return defaultRegistry.NewGauge(name, help, labels...)
}

// NewGauge declares a gauge in r.
func (r *Registry) NewGauge(name, help string, labels ...string) *Gauge {
	return &Gauge{r.declare(name, help, gaugeKind, Count, labels)}
}

// With is the gauge's series for the label values.
func (g *Gauge) With(labelValues ...string) GaugeSeries {
	return GaugeSeries{g.f.get(labelValues).(*vm.Gauge)}
}

// Set sets a gauge declared without labels.
func (g *Gauge) Set(v float64) { g.With().Set(v) }

// GaugeSeries is one series of a gauge.
type GaugeSeries struct{ g *vm.Gauge }

// Set sets the value.
func (s GaugeSeries) Set(v float64) { s.g.Set(v) }

// Add adds v (negative to subtract).
func (s GaugeSeries) Add(v float64) { s.g.Add(v) }

// Inc adds one.
func (s GaugeSeries) Inc() { s.g.Inc() }

// Dec subtracts one.
func (s GaugeSeries) Dec() { s.g.Dec() }

// Histogram is the distribution of observations: latencies, sizes. Its buckets are Prometheus'
// (le), fixed by its unit, so any Prometheus-compatible system computes its quantiles.
type Histogram struct{ f *family }

// NewHistogram declares a histogram in the binary's registry. Its name ends in its unit.
func NewHistogram(name, help string, unit Unit, labels ...string) *Histogram {
	return defaultRegistry.NewHistogram(name, help, unit, labels...)
}

// NewHistogram declares a histogram in r.
func (r *Registry) NewHistogram(name, help string, unit Unit, labels ...string) *Histogram {
	return &Histogram{r.declare(name, help, histogramKind, unit, labels)}
}

// With is the histogram's series for the label values.
func (h *Histogram) With(labelValues ...string) HistogramSeries {
	return HistogramSeries{h.f.get(labelValues).(*vm.PrometheusHistogram), h.f}
}

// Observe records v in a histogram declared without labels.
func (h *Histogram) Observe(v float64) { h.With().Observe(v) }

// Since records the time since start in a histogram declared without labels, in seconds.
func (h *Histogram) Since(start time.Time) { h.With().Since(start) }

// HistogramSeries is one series of a histogram.
type HistogramSeries struct {
	h *vm.PrometheusHistogram
	f *family
}

// Observe records v, in the histogram's unit.
func (s HistogramSeries) Observe(v float64) { s.h.Update(v) }

// Since records the time since start, for a histogram in seconds.
func (s HistogramSeries) Since(start time.Time) {
	if s.f.unit != Seconds {
		panic(fmt.Sprintf("metrics: Since records seconds, and %s is not in seconds", s.f.name))
	}
	s.h.Update(time.Since(start).Seconds())
}

// NewCounterFunc declares, in the binary's registry, a counter whose series are read when the
// metrics are written: sample reports each series' value, with its label values. It is for a
// count something else already keeps.
func NewCounterFunc(name, help string, sample func(report func(value float64, labelValues ...string)), labels ...string) {
	defaultRegistry.NewCounterFunc(name, help, sample, labels...)
}

// NewCounterFunc declares a sampled counter in r.
func (r *Registry) NewCounterFunc(name, help string, sample func(report func(value float64, labelValues ...string)), labels ...string) {
	r.declare(name, help, counterFuncKind, Count, labels).sample = sample
}

// NewGaugeFunc declares, in the binary's registry, a gauge whose series are read when the
// metrics are written: sample reports each series' value, with its label values.
func NewGaugeFunc(name, help string, sample func(report func(value float64, labelValues ...string)), labels ...string) {
	defaultRegistry.NewGaugeFunc(name, help, sample, labels...)
}

// NewGaugeFunc declares a sampled gauge in r.
func (r *Registry) NewGaugeFunc(name, help string, sample func(report func(value float64, labelValues ...string)), labels ...string) {
	r.declare(name, help, gaugeFuncKind, Count, labels).sample = sample
}
