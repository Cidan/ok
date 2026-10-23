package scmfeed_test

import (
	"context"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/bus/pgbus"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/dial"
	"github.com/Cidan/ok/internal/pgdb"
	"github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/testdb/testobjects"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// devSCM serves a development SCM over a database and bucket of its own, publishing its events on
// the bus beside its database, and returns its Events as ok control calls them, its Repositories
// as ada does, the bus, and its database.
func devSCM(t *testing.T) (scmpb.EventsClient, scmpb.RepositoriesClient, *pgbus.Bus, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: {control: control.ok}\n"))
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgdb.Open(ctx, testdb.Postgres(t), role.Schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	busPool, err := pgdb.Beside(ctx, pool, pgbus.Schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(busPool.Close)
	b, err := pgbus.Open(ctx, busPool, pgbus.Options{Poll: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	scm, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: pool, Blobs: testobjects.Store(t), Stream: b})
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
	as := func(p string) *dialed {
		conn, err := dial.Dial(lis.Addr().String(), dial.Options{DevPrincipal: p})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return &dialed{events: scmpb.NewEventsClient(conn), repos: scmpb.NewRepositoriesClient(conn)}
	}
	return as("control.ok.robot.example.com").events, as("ada.human.example.com").repos, b, pool
}

type dialed struct {
	events scmpb.EventsClient
	repos  scmpb.RepositoriesClient
}

// applier applies in memory, telling changed when it does; legacy is its place in the SCM's feed.
type applier struct {
	mu      sync.Mutex
	cursors map[int64]int64
	applied map[int64][]int64
	legacy  int64
	changed chan struct{}
}

func (a *applier) Cursor(_ context.Context, repo int64) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cursors[repo], nil
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

func (a *applier) LegacyCursor(context.Context) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.legacy, nil
}
func (a *applier) Seed(_ context.Context, repo, seq int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.cursors[repo]; !ok {
		a.cursors[repo] = seq
	}
	return nil
}
func (a *applier) LeaveLegacy(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.legacy = 0
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

// A follower of the SCM's events leaves its place in the SCM's feed for each repository's log, catches
// up the repositories whose logs went past it — none of which the stream brought, their events
// published before its group was made — and applies what the stream brings next, once each.
func TestFollowTheSCM(t *testing.T) {
	ctx := context.Background()
	events, repos, b, pool := devSCM(t)
	create := func(name string) int64 {
		r, err := repos.CreateRepository(ctx, &scmpb.CreateRepositoryRequest{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		return r.GetId()
	}
	rename := func(id int64, name string) {
		if _, err := repos.RenameRepository(ctx, &scmpb.RenameRepositoryRequest{RepositoryId: id, Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	ra, rb := create("a"), create("b")
	rename(ra, "a2")
	// These three were written while the old feed numbered every repository's events together,
	// and the legacy follower had applied it through a's rename.
	for i, ev := range [][2]int64{{ra, 1}, {rb, 1}, {ra, 2}} {
		if _, err := pool.Exec(ctx, `UPDATE event SET seq = $3 WHERE repository_id = $1 AND repository_seq = $2`, ev[0], ev[1], i+1); err != nil {
			t.Fatal(err)
		}
	}
	const legacy = 3
	rename(rb, "b2")
	rc := create("c")

	a := &applier{cursors: map[int64]int64{}, applied: map[int64][]int64{}, legacy: legacy, changed: make(chan struct{}, 1)}
	f := &follow.Follower{Stream: b, Topic: scmfeed.Topic, Group: "test", Log: scmfeed.Log{Events: events}, Apply: a}
	fctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- f.Run(fctx) }()
	defer func() { stop(); <-done }()

	a.await(t, rb, []int64{2})
	a.await(t, rc, []int64{1})
	a.mu.Lock()
	seeded, alone, left := a.cursors[ra], len(a.applied[ra]) == 0, a.legacy == 0
	a.mu.Unlock()
	if seeded != 2 || !alone || !left {
		t.Errorf("a's cursor %d, none applied %v, the legacy feed left %v: want 2, true, and true", seeded, alone, left)
	}
	// Then what is written as it follows.
	rename(rc, "c2")
	rename(ra, "a3")
	a.await(t, rc, []int64{1, 2})
	a.await(t, ra, []int64{3})
}
