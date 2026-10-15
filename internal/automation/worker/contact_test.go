package worker

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/checkout"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// A worker is ready only once automation has answered it, and only as long as the answer is
// recent.
func TestContact(t *testing.T) {
	var c Contact
	t0 := time.Unix(1_790_000_000, 0)
	if c.Within(t0, ReadyWithin) {
		t.Fatal("ready before automation ever answered")
	}
	c.heard(t0)
	if !c.Within(t0, ReadyWithin) || !c.Within(t0.Add(ReadyWithin), ReadyWithin) {
		t.Fatal("not ready right after automation answered")
	}
	if c.Within(t0.Add(ReadyWithin+time.Second), ReadyWithin) {
		t.Fatal("still ready once the answer is old")
	}
	var none *Contact
	none.heard(t0) // a worker configured without one records nothing
}

// leases answers each Lease in turn, then ends the worker.
type leases struct {
	automationpb.WorkClient
	mu      sync.Mutex
	answers []error // nil: an empty lease
	contact *Contact
	ready   []bool // whether the worker was ready as each Lease came in
	stop    context.CancelFunc
}

func (l *leases) Lease(context.Context, *automationpb.LeaseRequest, ...grpc.CallOption) (*automationpb.LeaseResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ready = append(l.ready, l.contact.Within(time.Now(), ReadyWithin))
	if len(l.answers) == 0 {
		l.stop()
		return nil, status.Error(codes.Canceled, "the test is over")
	}
	err := l.answers[0]
	l.answers = l.answers[1:]
	if err != nil {
		return nil, err
	}
	return &automationpb.LeaseResponse{}, nil
}

// A lease automation refuses or cannot answer is no contact; an empty lease is.
func TestRunRecordsContact(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	contact := &Contact{}
	work := &leases{answers: []error{status.Error(codes.Unavailable, "automation is down"), nil}, contact: contact, stop: cancel}
	err := Run(ctx, Config{Name: "w", Work: work, Checkouts: make([]checkout.Checkout, 1), Bases: make([]checkout.Checkout, 1), Scratch: t.TempDir(),
		OutputBases: t.TempDir(), Graphs: t.TempDir(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Contact: contact})
	if err != nil {
		t.Fatal(err)
	}
	if want := []bool{false, false, true}; len(work.ready) != len(want) || work.ready[0] != want[0] || work.ready[1] != want[1] || work.ready[2] != want[2] {
		t.Fatalf("ready as each lease came in: %v, want %v", work.ready, want)
	}
}
