package metrics

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"

	vm "github.com/VictoriaMetrics/metrics"
)

var helpEscaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`)

// WritePrometheus writes every metric in r in the Prometheus text format, each with its HELP and
// TYPE, sorted by name.
func (r *Registry) WritePrometheus(w io.Writer) {
	r.mu.Lock()
	fams := make([]*family, 0, len(r.families))
	for _, f := range r.families {
		fams = append(fams, f)
	}
	r.mu.Unlock()
	slices.SortFunc(fams, func(a, b *family) int { return strings.Compare(a.name, b.name) })
	var bb bytes.Buffer
	for _, f := range fams {
		f.write(&bb)
	}
	_, _ = w.Write(bb.Bytes())
}

// write writes the family: its HELP and TYPE, then its series. A family with no series yet writes
// nothing.
func (f *family) write(w *bytes.Buffer) {
	var body bytes.Buffer
	if f.sample != nil {
		f.writeSampled(&body)
	} else {
		f.set.WritePrometheus(&body)
	}
	if body.Len() == 0 {
		return
	}
	fmt.Fprintf(w, "# HELP %s %s\n", f.name, helpEscaper.Replace(f.help))
	fmt.Fprintf(w, "# TYPE %s %s\n", f.name, f.kind.exposed())
	w.Write(body.Bytes())
}

// writeSampled asks a func family's sample for its series and writes them, past MaxSeries summed
// into the folded series.
func (f *family) writeSampled(w *bytes.Buffer) {
	type point struct {
		values []string
		value  float64
	}
	var points []point
	index := map[string]int{}
	f.sample(func(value float64, labelValues ...string) {
		if len(labelValues) != len(f.labels) {
			panic(fmt.Sprintf("metrics: %s takes %d label values (%s), not %d", f.name, len(f.labels), strings.Join(f.labels, ", "), len(labelValues)))
		}
		key := strings.Join(labelValues, "\xff")
		if i, ok := index[key]; ok {
			points[i].value += value
			return
		}
		if len(points) >= MaxSeries {
			f.r.folded.With(f.name).Inc()
			labelValues = others(len(labelValues))
			key = strings.Join(labelValues, "\xff")
			if i, ok := index[key]; ok {
				points[i].value += value
				return
			}
		}
		index[key] = len(points)
		points = append(points, point{slices.Clone(labelValues), value})
	})
	slices.SortFunc(points, func(a, b point) int { return slices.Compare(a.values, b.values) })
	for _, p := range points {
		fmt.Fprintf(w, "%s %s\n", seriesName(f.name, f.labels, p.values), formatValue(p.value))
	}
}

func formatValue(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

// Handler serves the binary's metrics: Default's, and the process's and the Go runtime's.
func Handler() http.Handler { return defaultRegistry.Handler() }

// Handler serves r's metrics and the process's at /metrics, and nothing else.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/metrics" {
			http.NotFound(w, req)
			return
		}
		if req.Method != http.MethodGet && req.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		var bb bytes.Buffer
		r.WritePrometheus(&bb)
		vm.WriteGoMetrics(&bb)
		vm.WriteProcMetrics(&bb)
		_, _ = w.Write(bb.Bytes())
	})
}

// Reading is a series as last written: a counter's or gauge's Value; a histogram's Count and Sum.
type Reading struct {
	Found bool
	Value float64
	Count uint64
	Sum   float64
}

// Read reads one series of a metric, for tests: by its label values, in their declared order.
func (r *Registry) Read(name string, labelValues ...string) Reading {
	r.mu.Lock()
	f, ok := r.families[name]
	r.mu.Unlock()
	if !ok {
		return Reading{}
	}
	if len(labelValues) != len(f.labels) {
		panic(fmt.Sprintf("metrics: %s takes %d label values, not %d", f.name, len(f.labels), len(labelValues)))
	}
	var bb bytes.Buffer
	f.write(&bb)
	labels := seriesName("", f.labels, labelValues)
	var out Reading
	sc := bufio.NewScanner(&bb)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		series, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			continue
		}
		switch series {
		case name + labels:
			out.Found, out.Value = true, v
		case name + "_count" + labels:
			out.Found, out.Count = true, uint64(v)
		case name + "_sum" + labels:
			out.Found, out.Sum = true, v
		}
	}
	return out
}
