package move_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/Cidan/ok/internal/config"
	edgerole "github.com/Cidan/ok/internal/edge/role"
	"github.com/Cidan/ok/internal/id/idtest"
	idstore "github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/scmtest"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/testdb/roledb"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// startEdge serves a development edge of a region in front of the directory's cells, its rate
// limits past any load's.
func startEdge(t *testing.T, ctx context.Context, id *idtest.Identity, region string) string {
	t.Helper()
	cfg, err := config.Parse([]byte("domain: example.com\npublic_url: http://127.0.0.1:1\nroles: " + idtest.Roles +
		"\nservices: {identity: " + id.Addr + "}\nedge: {region: " + region +
		// A load test's rate: the limits are the edge's tests'.
		", limits: {principal: {per_second: 100000, burst: 100000}, address: {per_second: 100000, burst: 100000}}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := edgerole.Build(ctx, cfg, edgerole.Options{Dev: true, Identity: id.As("edge.ok.robot.example.com")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Close)
	l := listen(t)
	done := make(chan error, 1)
	go func() { done <- serve.Run(ctx, r.Root, serve.WithListener(l)) }()
	t.Cleanup(func() { <-done })
	return l.Addr().String()
}

// load publishes to a repository through an edge until stopped, each commit the last's child,
// asking a publish again while its repository's network is frozen; it returns the commits it
// published, how often it asked again, and any other failure.
type load struct {
	repo    int64
	commits []encoding.ID
	retries atomic.Int64
	failed  error
}

func (l *load) run(t *testing.T, ctx context.Context, edge, who string, stop <-chan struct{}) {
	store := scmpb.NewStoreClient(dialAs(t, edge, who))
	named := metadata.AppendToOutgoingContext(ctx, "ok-repository", fmt.Sprint(l.repo))
	parent := encoding.RootCommitID
	for n := 0; ; n++ {
		select {
		case <-stop:
			return
		default:
		}
		b := scmtest.NewObjects(t)
		tree := b.Add(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: "f", Value: encoding.TreeValue{Kind: encoding.TreeValueFile,
			ID: b.File([]byte(fmt.Sprint("repository ", l.repo, " version ", n, "\n")))}}}})
		commit := b.Commit(tree, fmt.Sprint("change ", n), parent)
		for {
			_, _, err := b.Publish(named, store, l.repo, nil)
			if err == nil {
				break
			}
			if !move.IsFrozen(err) {
				l.failed = fmt.Errorf("publishing change %d: %w", n, err)
				return
			}
			l.retries.Add(1)
			time.Sleep(50 * time.Millisecond)
		}
		l.commits = append(l.commits, commit)
		parent = commit
	}
}

// readAll reports the commits of loads' a repository lacks, read through an edge.
func readAll(t *testing.T, ctx context.Context, edge, who string, l *load) []encoding.ID {
	t.Helper()
	store := scmpb.NewStoreClient(dialAs(t, edge, who))
	named := metadata.AppendToOutgoingContext(ctx, "ok-repository", fmt.Sprint(l.repo))
	req := &scmpb.ReadObjectsRequest{RepositoryId: l.repo}
	for _, c := range l.commits {
		req.Refs = append(req.Refs, &scmpb.ObjectRef{Kind: scmpb.ObjectKind_OBJECT_KIND_COMMIT, Id: c[:]})
	}
	stream, err := store.ReadObjects(named, req)
	if err != nil {
		t.Fatal(err)
	}
	var lacking []encoding.ID
	for {
		r, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return lacking
		}
		if err != nil {
			t.Fatalf("reading repository %d's commits: %v", l.repo, err)
		}
		if nf := r.GetNotFound(); nf != nil {
			lacking = append(lacking, encoding.ID(nf.GetId()))
		}
	}
}

// Three cells in two regions — us-1 and us-2 on PostgreSQL, eu-1 on Spanner — one directory, and an
// edge in each region serve one namespace: repositories are placed by their owners' regions, made
// through the edges, and their networks move between the cells — a PostgreSQL cell's to the Spanner
// cell, and the Spanner cell's to a PostgreSQL cell — under a load of publishes through the edges,
// none failing but those asked again while frozen, every history intact where it lands
// (docs/hosted.md, "Cells, as built").
func TestCellsUnderLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	names := []string{"us-1", "us-2", "eu-1"}
	region := map[string]string{"us-1": "us", "us-2": "us", "eu-1": "eu"}
	scmLis, ctlLis := map[string]net.Listener{}, map[string]net.Listener{}
	var list []config.IdentityCell
	for _, n := range names {
		scmLis[n], ctlLis[n] = listen(t), listen(t)
		list = append(list, config.IdentityCell{Name: n, Region: region[n], SCM: scmLis[n].Addr().String(), Control: ctlLis[n].Addr().String()})
	}
	id := idtest.StartWithCells(t, list, "ada", "bea")
	dbs := map[string]*roledb.DBs{"us-1": roledb.NewPostgres(t), "us-2": roledb.NewPostgres(t), "eu-1": roledb.NewSpanner(t)}
	for _, n := range names {
		startCellOn(t, ctx, id, n, dbs[n], scmLis[n], ctlLis[n])
	}
	edges := map[string]string{"us": startEdge(t, ctx, id, "us"), "eu": startEdge(t, ctx, id, "eu")}
	for who, r := range map[string]string{"ada": "eu", "bea": "us"} {
		if err := id.Store.SetOwnerRegion(ctx, id.ID(who+".human.example.com"), r); err != nil {
			t.Fatal(err)
		}
	}

	// Each person makes repositories through their region's edge, in their region's cells.
	create := func(who, edge, name string) int64 {
		t.Helper()
		r, err := scmpb.NewRepositoriesClient(dialAs(t, edges[edge], who+".human.example.com")).CreateRepository(ctx,
			&scmpb.CreateRepositoryRequest{Name: name})
		if err != nil {
			t.Fatalf("%s making %s through the %s edge: %v", who, name, edge, err)
		}
		return r.GetId()
	}
	euApp, usApp, usLib := create("ada", "eu", "ada/app"), create("bea", "us", "bea/app"), create("bea", "eu", "bea/lib")
	cellOf := func(repo int64) string {
		t.Helper()
		e, err := id.Store.Repository(ctx, repo)
		if err != nil {
			t.Fatal(err)
		}
		return e.Cell
	}
	if got := cellOf(euApp); got != "eu-1" {
		t.Errorf("ada's repository is in %s, want eu-1, her region's", got)
	}
	for _, r := range []int64{usApp, usLib} {
		if got := cellOf(r); region[got] != "us" {
			t.Errorf("bea's repository %d is in %s, want a cell of her region, us, whichever edge she asked", r, got)
		}
	}

	// A load on each, through the other region's edge for one of them.
	loads := []*load{{repo: euApp}, {repo: usApp}, {repo: usLib}}
	who := map[int64]string{euApp: "ada.human.example.com", usApp: "bea.human.example.com", usLib: "bea.human.example.com"}
	via := map[int64]string{euApp: edges["eu"], usApp: edges["us"], usLib: edges["eu"]}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, l := range loads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.run(t, ctx, via[l.repo], who[l.repo], stop)
		}()
	}
	time.Sleep(time.Second)

	// bea's app goes from PostgreSQL to the Spanner cell, ada's from it to PostgreSQL's us-2.
	dest := map[int64]string{usApp: "eu-1", euApp: "us-2"}
	var networks []int64
	for repo, cell := range dest {
		m, err := id.Store.AskMove(ctx, repo, cell, id.ID("ada.human.example.com"))
		if err != nil {
			t.Fatalf("moving repository %d to %s: %v", repo, cell, err)
		}
		networks = append(networks, m.Network)
	}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		id.Mover.Pass(ctx)
		done := 0
		for _, n := range networks {
			m, err := id.Store.GetMove(ctx, n)
			if err != nil {
				t.Fatal(err)
			}
			if m.Phase == idstore.MoveDone {
				done++
			} else if m.Phase.Finished() {
				t.Fatalf("network %d's move: %+v", n, m)
			}
		}
		if done == len(networks) {
			break
		}
		if time.Now().After(deadline) {
			for _, n := range networks {
				m, _ := id.Store.GetMove(ctx, n)
				t.Logf("network %d: %+v", n, m)
			}
			t.Fatal("the moves did not finish")
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(time.Second) // publishing on, to where they went
	close(stop)
	wg.Wait()

	for _, l := range loads {
		if l.failed != nil {
			t.Errorf("repository %d's load: %v", l.repo, l.failed)
		}
		if len(l.commits) < 2 {
			t.Errorf("repository %d's load published %d commits", l.repo, len(l.commits))
		}
		if cell, ok := dest[l.repo]; ok && cellOf(l.repo) != cell {
			t.Errorf("repository %d is in %s, want %s", l.repo, cellOf(l.repo), cell)
		}
		if lacking := readAll(t, ctx, via[l.repo], who[l.repo], l); len(lacking) > 0 {
			t.Errorf("repository %d lacks %d of its %d commits where it is", l.repo, len(lacking), len(l.commits))
		}
		t.Logf("repository %d: %d commits, %d publishes asked again while frozen", l.repo, len(l.commits), l.retries.Load())
	}
}
