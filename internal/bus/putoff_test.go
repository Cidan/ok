package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/bus"
)

// A job put off comes back as a copy of its own — base~n, its body as it was — after the delay;
// the delivery put off is gone, and no copy spends an attempt.
func TestPutOff(t *testing.T) {
	ctx := context.Background()
	q := bus.NewLocalQueue()
	if err := q.Enqueue(ctx, bus.Job{ID: "j", Queue: "q", Key: 7, Body: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	ls, err := q.Lease(ctx, "q", bus.LeaseOptions{Max: 10, Visibility: time.Minute, Wait: time.Second})
	if err != nil || len(ls) != 1 {
		t.Fatalf("the job: leased %d, %v", len(ls), err)
	}
	l := ls[0]
	for want := 1; want <= 3; want++ {
		if err := bus.PutOff(ctx, q, l, 30*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if ls, _ := q.Lease(ctx, "q", bus.LeaseOptions{Max: 10, Visibility: time.Minute}); len(ls) != 0 {
			t.Fatalf("copy %d came back before its delay", want)
		}
		time.Sleep(50 * time.Millisecond)
		ls, err := q.Lease(ctx, "q", bus.LeaseOptions{Max: 10, Visibility: time.Minute})
		if err != nil || len(ls) != 1 {
			t.Fatalf("copy %d: leased %d, %v; the delivery put off is gone, the copy alone back", want, len(ls), err)
		}
		l = ls[0]
		j := l.Job()
		if bus.JobBase(j.ID) != "j" || bus.Copies(j.ID) != want || j.Key != 7 || string(j.Body) != "b" || l.Attempt() != 1 {
			t.Fatalf("copy %d: %+v, attempt %d", want, j, l.Attempt())
		}
	}
	if bus.JobBase("j") != "j" || bus.Copies("j") != 0 {
		t.Error("a job's own ID")
	}
	if d := bus.Backoff(3, 20*time.Millisecond, time.Second); d != 160*time.Millisecond {
		t.Errorf("the third copy's wait: %v", d)
	}
	if d := bus.Backoff(40, 20*time.Millisecond, time.Second); d != time.Second {
		t.Errorf("a late copy's wait: %v", d)
	}
}
