package store

import (
	"testing"
	"time"
)

// An account is admitted while its running runs and month's seconds, here and elsewhere together,
// are under its quota; zero is unlimited.
func TestQuotaAdmits(t *testing.T) {
	for _, c := range []struct {
		q                Quota
		running, seconds int64
		want             bool
	}{
		{Quota{}, 1000, 1 << 40, true},
		{Quota{Concurrent: 3}, 2, 0, true},
		{Quota{Concurrent: 3}, 3, 0, false},
		{Quota{Concurrent: 3, RunningElsewhere: 1}, 2, 0, false},
		{Quota{Seconds: 60}, 0, 59, true},
		{Quota{Seconds: 60, SecondsElsewhere: 30}, 0, 30, false},
	} {
		if got := c.q.admits(c.running, c.seconds); got != c.want {
			t.Errorf("%+v admits %d running, %d seconds: %v, want %v", c.q, c.running, c.seconds, got, c.want)
		}
	}
}

// A dispatch job is on its pool's queue under the ID its run names, its body the run and when the
// job was made; each new job's ID is its own.
func TestBusJob(t *testing.T) {
	made := time.Now()
	j := busJob(dispatchJob{id: "j1", run: 7, repo: 3, pool: "gpu"}, time.Time{})
	if j.Queue != "automation.dispatch.gpu" || j.ID != "j1" || j.Key != 3 || !j.NotBefore.IsZero() {
		t.Fatalf("a dispatch job: %+v", j)
	}
	run, at, ok := parseJobBody(j.Body)
	if !ok || run != 7 || at.Before(made.Add(-time.Second)) || at.After(time.Now().Add(time.Second)) {
		t.Fatalf("its body: %d, %v, %v", run, at, ok)
	}
	if d := busJob(dispatchJob{id: "j2", run: 8}, time.Time{}); d.Queue != "automation.dispatch.default" {
		t.Errorf("a job of no pool's: %s", d.Queue)
	}
	if newDispatchJob() == newDispatchJob() {
		t.Error("two jobs' IDs are one")
	}
	if _, _, ok := parseJobBody([]byte("short")); ok {
		t.Error("a body of five bytes parsed")
	}
}
