package queue

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/automation/store"
	"github.com/Cidan/ok/internal/automation/store/postgres"
	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/work"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// landing is the SCM's candidates of a repository: one prepared long ago, one just now.
type landing struct {
	scmpb.LandingClient
	mu        sync.Mutex
	discarded [][]byte
}

var (
	oldCandidate   = []byte("old")
	youngCandidate = []byte("young")
)

func (l *landing) ListCandidates(context.Context, *scmpb.ListCandidatesRequest, ...grpc.CallOption) (*scmpb.ListCandidatesResponse, error) {
	return &scmpb.ListCandidatesResponse{Candidates: []*scmpb.Candidate{
		{CommitId: oldCandidate, PreparedAt: timestamppb.New(time.Now().Add(-time.Hour))},
		{CommitId: youngCandidate, PreparedAt: timestamppb.New(time.Now())},
	}}, nil
}

func (l *landing) DiscardCandidate(_ context.Context, in *scmpb.DiscardCandidateRequest, _ ...grpc.CallOption) (*scmpb.DiscardCandidateResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.discarded = append(l.discarded, in.GetCommitId())
	return &scmpb.DiscardCandidateResponse{}, nil
}

// A pass that finds nothing left to do books its repository's collection, once however many such
// passes come; the collection holds the queue's claim — put off while a pass holds it — and
// discards the candidates no attempt holds once they are past the grace, the queue marked for when
// the rest come of age. No replica keeps when it last collected.
func TestCollection(t *testing.T) {
	ctx := context.Background()
	b, err := postgres.Open(ctx, testdb.Pool(t))
	if err != nil {
		t.Fatal(err)
	}
	blobs, err := blob.NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st := store.New(b, blobs, store.DefaultConfig)
	l := &landing{}
	r := New(st, SCM{Landing: l}, Config{Grace: time.Minute}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w := st.Work()
	const repo = 7

	for range 3 {
		if err := r.bookCollection(ctx, repo); err != nil {
			t.Fatal(err)
		}
	}
	if p, err := w.Peek(ctx, store.CollectWork, repo); err != nil || !p.Marked {
		t.Fatalf("the collection, booked: %+v, %v", p, err)
	}

	// A pass holds another repository's queue: its collection is put off — booked again — and
	// nothing is discarded.
	const other = 8
	_, release, ok, err := work.Claim(ctx, w, store.QueueWork, other, "a pass", time.Minute)
	if err != nil || !ok {
		t.Fatalf("the pass's claim: %v, %v", ok, err)
	}
	if err := r.collectHeld(ctx, other); err != nil {
		t.Fatal(err)
	}
	if p, err := w.Peek(ctx, store.CollectWork, other); err != nil || !p.Marked || len(l.discarded) != 0 {
		t.Fatalf("a collection while a pass holds the queue: %+v, %v, discarded %q", p, err, l.discarded)
	}
	release()

	if err := r.collectHeld(ctx, other); err != nil {
		t.Fatal(err)
	}
	if len(l.discarded) != 1 || !bytes.Equal(l.discarded[0], oldCandidate) {
		t.Errorf("discarded %q, want the old candidate alone", l.discarded)
	}
	if p, err := w.Peek(ctx, store.QueueWork, other); err != nil || !p.Marked {
		t.Errorf("the queue, marked for when the young candidate comes of age: %+v, %v", p, err)
	}
}
