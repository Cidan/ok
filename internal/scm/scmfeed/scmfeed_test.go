package scmfeed_test

import (
	"context"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/busconfig"
	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/data/pgdata"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/id/idtest"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/scm/scmtest"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	"github.com/Cidan/ok/internal/work"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// scmEnv is a development SCM, beside a development identity, over a database and bucket of its
// own, publishing its events on the bus beside its database.
type scmEnv struct {
	events scmpb.EventsClient // as ok control calls it
	review scmpb.ReviewClient
	repos  scmpb.RepositoriesClient // as ada calls them
	store  scmpb.StoreClient
	cls    scmpb.ChangeListsClient
	queue  bus.Queue
	ada    string // her stable ID
	bumped map[int64]int
}

func devSCM(t *testing.T) *scmEnv {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	id := idtest.Start(t, "ada")
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: " + idtest.Roles + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgdb.Open(ctx, testdb.Postgres(t), role.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	cfg.Bus = bustest.Config(t)
	bb, err := busconfig.Open(ctx, cfg.Bus)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bb.Close)
	scm, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: pool, Blobs: testobjects.Store(t),
		Identity: id.As("scm.ok.robot.example.com")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scm.Close)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, scm.Root, serve.WithAuthn(scm.Resolver), serve.WithListener(lis)) }()
	t.Cleanup(func() { cancel(); <-done })
	as := func(p string) *grpc.ClientConn {
		conn, err := dial.Dial(lis.Addr().String(), dial.Options{DevPrincipal: p})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	control, ada := as("control.ok.robot.example.com"), as("ada.human.example.com")
	return &scmEnv{events: scmpb.NewEventsClient(control), review: scmpb.NewReviewClient(control), repos: scmpb.NewRepositoriesClient(ada),
		store: scmpb.NewStoreClient(ada), cls: scmpb.NewChangeListsClient(ada), queue: bb.Queue, ada: id.ID("ada.human.example.com"), bumped: map[int64]int{}}
}

// bump writes one event to a repository's log: ada uploads a CL, a draft, then ok control asks
// for its review, and returns it to draft, in turn.
func (e *scmEnv) bump(t *testing.T, repo int64) {
	t.Helper()
	ctx := context.Background()
	n := e.bumped[repo]
	e.bumped[repo]++
	if n == 0 {
		b := scmtest.NewObjects(t)
		tree := b.Add(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: "f", Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: b.File([]byte("one"))}}}})
		commit := b.Commit(tree, "one", encoding.RootCommitID)
		if _, _, err := b.Publish(ctx, e.store, repo, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := e.cls.Upload(ctx, &scmpb.UploadRequest{RepositoryId: repo, CommitIds: [][]byte{commit[:]}, Readiness: scmpb.UploadRequest_READINESS_WIP}); err != nil {
			t.Fatal(err)
		}
		return
	}
	kind := scmpb.TransitionRequest_KIND_REQUEST_REVIEW
	if n%2 == 0 {
		kind = scmpb.TransitionRequest_KIND_RETURN_TO_DRAFT
	}
	if _, err := e.review.Transition(ctx, &scmpb.TransitionRequest{RepositoryId: repo, Number: 1, Kind: kind, ActorId: e.ada}); err != nil {
		t.Fatal(err)
	}
}

// applier applies in memory, telling changed when it does.
type applier struct {
	mu      sync.Mutex
	cursors map[int64]int64
	applied map[int64][]int64
	changed chan struct{}
}

func (a *applier) Cursors(_ context.Context, repos []int64) (map[int64]int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := map[int64]int64{}
	for _, r := range repos {
		out[r] = a.cursors[r]
	}
	return out, nil
}

func (a *applier) Apply(_ context.Context, e bus.Event) error {
	ev, err := scmfeed.Decode(e)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if e.Seq <= a.cursors[e.Key] {
		return nil
	}
	if ev.GetKind() == nil {
		panic("an event of no kind")
	}
	a.cursors[e.Key] = e.Seq
	a.applied[e.Key] = append(a.applied[e.Key], e.Seq)
	select {
	case a.changed <- struct{}{}:
	default:
	}
	return nil
}

// await waits until repo's events applied are want.
func (a *applier) await(t *testing.T, repo int64, want []int64) {
	t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		a.mu.Lock()
		got := slices.Clone(a.applied[repo])
		a.mu.Unlock()
		if slices.Equal(got, want) {
			return
		}
		select {
		case <-a.changed:
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatalf("repository %d applied %v, want %v", repo, got, want)
		}
	}
}

// A follower of the SCM's events — ok control's, as it calls — catches up the repositories whose
// logs went past it, relayed before it registered, and applies what is relayed next, once each.
func TestFollowTheSCM(t *testing.T) {
	ctx := context.Background()
	e := devSCM(t)
	create := func(name string) int64 {
		r, err := e.repos.CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		return r.GetId()
	}
	ra, rb := create("a"), create("b")
	e.bump(t, ra)
	e.bump(t, rb)
	rc := create("c")

	a := &applier{cursors: map[int64]int64{}, applied: map[int64][]int64{}, changed: make(chan struct{}, 1)}
	// Its keyed work, in a database of its own, on the bus's queue as control's.
	p := testdb.Pool(t)
	if _, err := p.Exec(ctx, pgdata.WorkSchema); err != nil {
		t.Fatal(err)
	}
	w := pgdata.Work(p)
	if _, err := work.RegisterStore(w, work.Jobs{Queue: e.queue, Prefix: "control"}); err != nil {
		t.Fatal(err)
	}
	f := &follow.Follower{Topic: scmfeed.Topic, Log: scmfeed.Log{Events: e.events}, Apply: a, Work: w, Register: scmfeed.Register(e.events)}
	fctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- f.Run(fctx) }()
	defer func() { stop(); <-done }()

	a.await(t, ra, []int64{1, 2})
	a.await(t, rb, []int64{1, 2})
	a.await(t, rc, []int64{1})
	// Then what is written as it follows.
	e.bump(t, rc)
	e.bump(t, ra)
	a.await(t, rc, []int64{1, 2})
	a.await(t, ra, []int64{1, 2, 3})
}
