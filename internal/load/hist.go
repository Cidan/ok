package load

import (
	"math"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/status"
)

// histogram counts durations in buckets 5% apart, from 10µs to an hour: a quantile it reports is
// within 5% of the true one.
type histogram struct {
	counts []int64
}

const (
	histMin   = 10 * time.Microsecond
	histRatio = 1.05
)

var histBuckets = int(math.Ceil(math.Log(float64(time.Hour)/float64(histMin))/math.Log(histRatio))) + 1

func newHistogram() *histogram { return &histogram{counts: make([]int64, histBuckets)} }

func (h *histogram) add(d time.Duration) {
	i := 0
	if d > histMin {
		i = min(int(math.Ceil(math.Log(float64(d)/float64(histMin))/math.Log(histRatio))), histBuckets-1)
	}
	h.counts[i]++
}

// quantile is the upper bound of the bucket holding the q-th quantile.
func (h *histogram) quantile(q float64) time.Duration {
	var total int64
	for _, c := range h.counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	rank := int64(math.Ceil(q * float64(total)))
	var seen int64
	for i, c := range h.counts {
		if seen += c; seen >= rank {
			return time.Duration(float64(histMin) * math.Pow(histRatio, float64(i)))
		}
	}
	return time.Hour
}

// stat is one operation's measurements.
type stat struct {
	mu       sync.Mutex
	offered  int64
	shed     int64 // offered, not started: too many under way
	ok       int64
	errs     map[string]int64
	hist     *histogram
	examples map[string]string
}

func newStat() *stat {
	return &stat{errs: map[string]int64{}, hist: newHistogram(), examples: map[string]string{}}
}

func (s *stat) offer() {
	s.mu.Lock()
	s.offered++
	s.mu.Unlock()
}

func (s *stat) dropped() {
	s.mu.Lock()
	s.shed++
	s.mu.Unlock()
}

// done records an operation that ran: its time, and its error's code.
func (s *stat) done(d time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		s.ok++
		s.hist.add(d)
		return
	}
	c := status.Code(err)
	s.errs[c.String()]++
	if _, ok := s.examples[c.String()]; !ok {
		s.examples[c.String()] = err.Error()
	}
}

// Result is one operation's part of a report.
type Result struct {
	Offered       int64             `json:"offered"`
	OfferedRate   float64           `json:"offered_per_second"`
	Shed          int64             `json:"shed"`
	OK            int64             `json:"ok"`
	AchievedRate  float64           `json:"achieved_per_second"`
	Errors        map[string]int64  `json:"errors,omitempty"`
	ErrorExamples map[string]string `json:"error_examples,omitempty"`
	P50           Millis            `json:"p50_ms"`
	P90           Millis            `json:"p90_ms"`
	P99           Millis            `json:"p99_ms"`
	P999          Millis            `json:"p999_ms"`
	// Buckets are its latencies' histogram's counts, which reports of a run's pods merge by.
	Buckets []int64 `json:"buckets"`
}

// Millis is a duration in milliseconds, as a report gives it.
type Millis float64

func ms(d time.Duration) Millis { return Millis(float64(d) / float64(time.Millisecond)) }

func (s *stat) result(over time.Duration) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := Result{Offered: s.offered, Shed: s.shed, OK: s.ok, OfferedRate: float64(s.offered) / over.Seconds(),
		AchievedRate: float64(s.ok) / over.Seconds(), P50: ms(s.hist.quantile(0.5)), P90: ms(s.hist.quantile(0.9)),
		P99: ms(s.hist.quantile(0.99)), P999: ms(s.hist.quantile(0.999)), Buckets: append([]int64(nil), s.hist.counts...)}
	if len(s.errs) > 0 {
		r.Errors, r.ErrorExamples = map[string]int64{}, map[string]string{}
		keys := make([]string, 0, len(s.errs))
		for k := range s.errs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			r.Errors[k], r.ErrorExamples[k] = s.errs[k], s.examples[k]
		}
	}
	return r
}
