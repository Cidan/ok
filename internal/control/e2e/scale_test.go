package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	autorole "github.com/Cidan/ok/internal/automation/role"
	"github.com/Cidan/ok/internal/bus/busconfig"
	"github.com/Cidan/ok/internal/control/role"
	"github.com/Cidan/ok/internal/deploy/engine"
	deployrole "github.com/Cidan/ok/internal/deploy/role"
	deploystore "github.com/Cidan/ok/internal/deploy/store"
	deploypostgres "github.com/Cidan/ok/internal/deploy/store/postgres"
	deployspanner "github.com/Cidan/ok/internal/deploy/store/spannerstore"
	"github.com/Cidan/ok/internal/scm/encoding"
	scmrole "github.com/Cidan/ok/internal/scm/role"
	"github.com/Cidan/ok/internal/scm/scmfeed"
	"github.com/Cidan/ok/internal/scm/scmtest"
	"github.com/Cidan/ok/internal/serve"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/spannerdb"
	"github.com/Cidan/ok/internal/testdb/roledb"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// chaosEvery is how often TestScale stops a replica.
const chaosEvery = 150 * time.Millisecond

// scaleCount is a count TestScale reads from the environment variable name, or def when it is
// unset.
func scaleCount(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		t.Fatalf("%s=%q is not a count", name, v)
	}
	return n
}

// TestScale is phase 2's proof (docs/hosted.md, "The plan"): many repositories at once, each
// through upload, review, pre, submission, post, and landing, across three replicas each of the
// SCM, ok automation, and ok control and three followers of the deployer, while one replica after
// another is stopped and another started in its place. Every repository's main ends at the commit
// its CL landed as, the landing queue's doing; every follower has applied every repository's
// every event, once.
//
// A role's replicas share one database pool and one bus, as a test's PostgreSQL allows only so
// many connections: what a replica's stopping takes with it is the rest — its server and its
// connections, its follower and cursors in flight, its relay, its keyed work and leases, its
// listeners.
func TestScale(t *testing.T) {
	// How many repositories it drives, and how many of them at once. It holds some 70 connections
	// to its database server, so it runs when asked, as CI's ok:test target asks: beside every
	// other package's tests, a developer's server would run out of them.
	if os.Getenv("OK_TEST_SCALE_REPOSITORIES") == "" {
		t.Skip("OK_TEST_SCALE_REPOSITORIES is unset: set it to how many repositories to drive")
	}
	n, atOnce := scaleCount(t, "OK_TEST_SCALE_REPOSITORIES", 0), scaleCount(t, "OK_TEST_SCALE_AT_ONCE", 32)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute+time.Duration(n)*time.Second)
	defer cancel()
	ctlProxy := newProxy(t)
	base := "http://" + ctlProxy.addr()
	cfg := testConfig(t, base)
	// The roles' databases (roledb, as OK_TEST_DATABASE names them): on Spanner each replica opens
	// its role's as cfg says, and no pool; the checks read them through clients of their own.
	dbs := roledb.New(t)
	dbs.Configure(cfg)
	pool := func(schema string) *pgxpool.Pool {
		t.Helper()
		return dbs.Pool(t, schema, 6)
	}
	tablesOf := func(role string, p *pgxpool.Pool) tables {
		t.Helper()
		if !dbs.OnSpanner() {
			return tables{pg: p}
		}
		sdb, err := spannerdb.Open(ctx, cfg.Database.Spanner, role)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(sdb.Close)
		return tables{sp: sdb.Client}
	}
	open := func() *busconfig.Bus {
		t.Helper()
		b, err := busconfig.Open(ctx, cfg.Bus)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.Close)
		return b
	}
	idAddr := startIdentity(t, ctx, cfg, func(schema string) *pgxpool.Pool { return pool(schema) })
	packs, logs := bucket(t), bucket(t)

	scmPool, autoPool, ctlPool, deployPool := pool(scmrole.Schema), pool(autorole.Schema), pool(role.Schema), pool(deployrole.Schema)
	scmProxy, autoProxy := newProxy(t), newProxy(t)
	_, deployAddr := startDeployer(t)
	idSCM, idAuto, idCtl := as(t, idAddr, scmSelf), as(t, idAddr, automation), as(t, idAddr, control)
	scmAsAuto, scmAsCtl, scmAsDeploy := as(t, scmProxy.addr(), automation), as(t, scmProxy.addr(), control), as(t, scmProxy.addr(), deployBot)
	autoAsCtl, deployAsCtl := as(t, autoProxy.addr(), control), as(t, deployAddr, control)

	scms := &replicaSet{name: "the SCM", proxy: scmProxy, start: func() (string, func(), error) {
		r, err := scmrole.Build(ctx, cfg, scmrole.Options{Dev: true, Pool: scmPool, Blobs: packs, Identity: idSCM})
		if err != nil {
			return "", nil, err
		}
		addr, stop := serveRole(t, ctx, nil, r.Root, serve.WithAuthn(r.Resolver))
		return addr, func() { stop(); r.Close() }, nil
	}}
	autos := &replicaSet{name: "ok automation", proxy: autoProxy, start: func() (string, func(), error) {
		r, err := autorole.Build(ctx, cfg, autorole.Options{Dev: true, Pool: autoPool, Logs: logs, Identity: idAuto, SCM: scmAsAuto})
		if err != nil {
			return "", nil, err
		}
		addr, stop := serveRole(t, ctx, nil, r.Root, serve.WithAuthn(r.Resolver))
		return addr, func() { stop(); r.Close() }, nil
	}}
	ctls := &replicaSet{name: "ok control", proxy: ctlProxy, start: func() (string, func(), error) {
		r, err := role.Build(ctx, cfg, role.Options{Dev: true, Pool: ctlPool, Identity: idCtl, SCM: scmAsCtl,
			Automation: autoAsCtl, Deploy: deployAsCtl})
		if err != nil {
			return "", nil, err
		}
		addr, stop := serveRole(t, ctx, nil, r.Root, serve.WithAuthn(r.Resolver))
		return addr, func() { stop(); r.Close() }, nil
	}}
	ctlTables, autoTables, deployTables := tablesOf("control", ctlPool), tablesOf("automation", autoPool), tablesOf("deploy", deployPool)
	var deployBackend deploystore.Backend
	if dbs.OnSpanner() {
		sdb, err := spannerdb.Open(ctx, cfg.Database.Spanner, "deploy")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(sdb.Close)
		if deployBackend, err = deployspanner.Open(ctx, sdb.Admin, sdb.Client); err != nil {
			t.Fatal(err)
		}
	} else {
		b, err := deploypostgres.Open(ctx, deployPool)
		if err != nil {
			t.Fatal(err)
		}
		deployBackend = b
	}
	deployStore := deploystore.New(deployBackend)
	// Its keyed work's jobs, the relay's among them, on the bus's queue as the deployer's.
	if err := open().RegisterWork(deployStore.Work(), "deploy"); err != nil {
		t.Fatal(err)
	}
	deployEvents := scmpb.NewEventsClient(scmAsDeploy)
	quiet := slog.New(slog.DiscardHandler)
	follows := &replicaSet{name: "the deployer's follower", start: func() (string, func(), error) {
		fctx, stop := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() { defer close(done); engine.Follow(fctx, deployStore, deployEvents, quiet) }()
		return "", func() { stop(); <-done }, nil
	}}
	sets := []*replicaSet{scms, autos, ctls, follows}
	for _, s := range sets {
		for range 3 {
			if err := s.add(); err != nil {
				t.Fatalf("starting %s: %v", s.name, err)
			}
		}
	}

	// Workers that pass every run, its plan planning nothing.
	work := automationpb.NewWorkClient(as(t, autoProxy.addr(), worker))
	var workers sync.WaitGroup
	for w := range 8 {
		workers.Go(func() { standIn(ctx, work, fmt.Sprintf("stand-in-%d", w)) })
	}

	// One replica after another stopped, of each role in turn, a replica of the role's at random,
	// and another started in its place.
	chaos, stopChaos := context.WithCancel(ctx)
	chaosDone := make(chan struct{})
	go func() {
		defer close(chaosDone)
		rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
		for turn := 0; ; turn++ {
			select {
			case <-chaos.Done():
				return
			case <-time.After(chaosEvery):
			}
			s := sets[turn%len(sets)]
			s.kill(rng)
			if err := s.add(); err != nil {
				t.Errorf("starting %s again: %v", s.name, err)
				return
			}
		}
	}()

	c := &scaleClient{
		ctx: ctx, control: base, http: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		repos:  scmpb.NewRepositoriesClient(as(t, scmProxy.addr(), alice)),
		store:  scmpb.NewStoreClient(as(t, scmProxy.addr(), alice)),
		cls:    scmpb.NewChangeListsClient(as(t, scmProxy.addr(), alice)),
		landed: func(repo int64) (bool, error) { return submittedOnMain(ctx, autoTables, repo) },
	}
	repos := make([]int64, n)
	start := time.Now()
	sem := make(chan struct{}, atOnce)
	var drivers sync.WaitGroup
	var failedMu sync.Mutex
	var failed []int64
	for i := range n {
		sem <- struct{}{}
		drivers.Go(func() {
			defer func() { <-sem }()
			id, err := c.drive(t, fmt.Sprintf("scale-%d", i))
			if err != nil {
				t.Errorf("repository %d: %v", i, err)
				failedMu.Lock()
				failed = append(failed, id)
				failedMu.Unlock()
			}
			repos[i] = id
		})
	}
	drivers.Wait()
	if autoTables.pg != nil {
		for _, id := range failed[:min(len(failed), 4)] {
			dumpLanding(t, autoTables.pg, id)
		}
	}
	stopChaos()
	<-chaosDone
	var killed []string
	for _, s := range sets {
		killed = append(killed, fmt.Sprintf("%d of %s's", s.kills, s.name))
		if s.kills < 2 {
			t.Errorf("%s lost only %d replicas while the repositories landed", s.name, s.kills)
		}
	}
	t.Logf("%d repositories landed in %s; replicas stopped and started meanwhile: %s", n, time.Since(start).Round(time.Second), strings.Join(killed, ", "))
	if t.Failed() {
		return
	}

	// Every follower catches up with every repository's log.
	heads := map[int64]int64{}
	events := scmpb.NewEventsClient(scmAsCtl)
	if err := retry(ctx, "reading the logs' heads", func() error {
		for after := int64(0); ; {
			resp, err := events.Heads(ctx, &scmpb.HeadsRequest{AfterRepositoryId: after})
			if err != nil {
				return err
			}
			for _, h := range resp.GetHeads() {
				heads[h.GetRepositoryId()] = h.GetRepositorySeq()
				after = h.GetRepositoryId()
			}
			if len(resp.GetHeads()) == 0 {
				return nil
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	for name, d := range map[string]tables{"ok control": ctlTables, "ok automation": autoTables, "the deployer": deployTables} {
		if err := retry(ctx, name+" catching up", func() error {
			cursors, err := cursorsOf(ctx, d)
			if err != nil {
				return err
			}
			for _, id := range repos {
				if cursors[id] != heads[id] {
					return fmt.Errorf("repository %d's cursor is %d, its log %d long", id, cursors[id], heads[id])
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Each event applied once: ok control's change log, which nothing but its follower writes
	// uploads and landings to, holds one of each per repository, and automation planned each
	// patchset's pre once.
	check := func(what string, d tables, table, where string) {
		t.Helper()
		const pg, gsql = `repository_id = ANY(@repos)`, `repository_id IN UNNEST(@repos)`
		q := `SELECT repository_id, count(*) FROM ` + table + ` WHERE %s AND ` + where + ` GROUP BY repository_id`
		counts, err := d.pairs(ctx, fmt.Sprintf(q, pg), fmt.Sprintf(q, gsql), map[string]any{"repos": repos})
		if err != nil {
			t.Fatal(err)
		}
		if len(counts) != n {
			t.Errorf("%s: %d repositories of %d", what, len(counts), n)
		}
		for repo, k := range counts {
			if k != 1 {
				t.Errorf("%s: repository %d's is there %d times", what, repo, k)
			}
		}
	}
	check("the uploads in ok control's change log", ctlTables, "event", `kind = 1`)
	check("the landings in ok control's change log", ctlTables, "event", `kind = 6`)
	check("automation's pre plans", autoTables, "plan", `phase = 1`)

	// Every main moved once, to its CL's commit, by the landing queue; the deployer saw it there.
	repoClient := scmpb.NewRepositoriesClient(scmAsCtl)
	clClient := scmpb.NewChangeListsClient(scmAsCtl)
	for _, id := range repos {
		r, err := repoClient.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: id}})
		if err != nil {
			t.Fatal(err)
		}
		cl, err := clClient.GetChangeList(ctx, &scmpb.GetChangeListRequest{RepositoryId: id, Number: 1})
		if err != nil {
			t.Fatal(err)
		}
		if r.GetHeadSeq() != 1 || cl.GetStatus() != scmpb.ChangeListStatus_CHANGE_LIST_STATUS_SUBMITTED || !bytes.Equal(r.GetHead(), cl.GetLandedCommit()) {
			t.Errorf("repository %d: main %x at %d, its CL %s as %x", id, r.GetHead(), r.GetHeadSeq(), cl.GetStatus(), cl.GetLandedCommit())
		}
		var deployHead []byte
		var deploySeq int64
		if err := deployTables.row(ctx, `SELECT head, head_seq FROM repository WHERE id = @id`,
			`SELECT head, head_seq FROM repository WHERE repository_id = @id`, map[string]any{"id": id}, &deployHead, &deploySeq); err != nil ||
			deploySeq != 1 || !bytes.Equal(deployHead, r.GetHead()) {
			t.Errorf("the deployer's repository %d: %x at %d (%v)", id, deployHead, deploySeq, err)
		}
	}
	var byQueue int64
	if err := autoTables.row(ctx, `SELECT count(*) FROM main_commit WHERE repository_id = ANY(@repos) AND seq = 1 AND submission_id IS NOT NULL`,
		`SELECT count(*) FROM main_commit WHERE repository_id IN UNNEST(@repos) AND seq = 1 AND submission_id IS NOT NULL`,
		map[string]any{"repos": repos}, &byQueue); err != nil || byQueue != int64(n) {
		t.Errorf("%d of %d mains moved by the landing queue (%v)", byQueue, n, err)
	}
	cancel()
	workers.Wait()
}

// standIn plays a worker that passes every run it takes, a planning run with a plan of nothing,
// until ctx ends.
func standIn(ctx context.Context, work automationpb.WorkClient, name string) {
	for ctx.Err() == nil {
		resp, err := work.Lease(ctx, &automationpb.LeaseRequest{Worker: name, WaitSeconds: 1})
		if err != nil || resp.GetRun() == nil {
			if err != nil {
				time.Sleep(50 * time.Millisecond)
			}
			continue
		}
		req := &automationpb.FinishRequest{RunId: resp.GetRun().GetId(), Worker: name, Status: automationpb.Run_STATUS_PASSED}
		if resp.GetRun().GetJob().GetKind() == automationpb.Job_KIND_PLAN {
			req.Plan = &automationpb.Plan{}
		}
		finish(ctx, work, req)
	}
}

// finish finishes a run as a worker does (worker.retry): again, while the replica it reached stops
// or another has not started, until the answer is final. A run whose lease answer was lost with a
// replica is still its lease's to time out, and goes to another worker; one whose every attempt
// lost its finish would end in error, which no worker's does.
func finish(ctx context.Context, work automationpb.WorkClient, req *automationpb.FinishRequest) {
	for ctx.Err() == nil {
		_, err := work.Finish(ctx, req)
		switch status.Code(err) {
		case codes.OK, codes.FailedPrecondition, codes.InvalidArgument, codes.PermissionDenied, codes.NotFound,
			codes.ResourceExhausted:
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// scaleClient is alice, driving repositories through the SCM's API and ok control's pages.
type scaleClient struct {
	ctx     context.Context
	control string
	http    *http.Client
	repos   scmpb.RepositoriesClient
	store   scmpb.StoreClient
	cls     scmpb.ChangeListsClient
	// landed reports whether the landing queue has landed a repository's CL on its main.
	landed func(repo int64) (bool, error)
}

// drive creates a repository, uploads a CL to it, approves it, and submits it, until it lands,
// retrying each step a stopped replica failed, and returns the repository's ID.
func (c *scaleClient) drive(t *testing.T, name string) (int64, error) {
	var id int64
	err := retry(c.ctx, "creating "+name, func() error {
		r, err := c.repos.CreateRepository(c.ctx, &scmpb.CreateRepositoryRequest{Name: name})
		if status.Code(err) == codes.AlreadyExists {
			// A creation whose answer was lost with a replica.
			r, err = c.repos.GetRepository(c.ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Name{Name: name}})
		}
		id = r.GetId()
		return err
	})
	if err != nil {
		return 0, err
	}
	b := scmtest.NewObjects(t)
	tree := b.Add(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: "f", Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: b.File([]byte(name + "\n"))}}}})
	commit := b.Commit(tree, "a first change to "+name, encoding.RootCommitID)
	if err := retry(c.ctx, "publishing to "+name, func() error {
		_, _, err := b.Publish(c.ctx, c.store, id, nil)
		return err
	}); err != nil {
		return id, err
	}
	if err := retry(c.ctx, "uploading to "+name, func() error {
		_, err := c.cls.Upload(c.ctx, &scmpb.UploadRequest{RepositoryId: id, CommitIds: [][]byte{commit[:]}, Readiness: scmpb.UploadRequest_READINESS_READY})
		if status.Code(err) == codes.FailedPrecondition {
			// An upload whose answer was lost with a replica, as jj cl takes it: nothing new.
			if cl, gerr := c.cls.GetChangeList(c.ctx, &scmpb.GetChangeListRequest{RepositoryId: id, Number: 1}); gerr == nil &&
				bytes.Equal(cl.GetPatchsets()[len(cl.GetPatchsets())-1].GetCommitId(), commit[:]) {
				return nil
			}
		}
		return err
	}); err != nil {
		return id, err
	}
	path := clPath(name, 1)
	// The reply renders once ok control has projected the CL from the SCM's events.
	if err := retry(c.ctx, "approving "+path, func() error {
		_, err := c.post(path+"/reply", grant("LGTM", "Approval"))
		return err
	}); err != nil {
		return id, err
	}
	// Submit is refused until pre passes and the approval is derived; once taken, the queue lands
	// it — again after a submission a stopped replica's lost run failed.
	return id, retry(c.ctx, "landing "+path, func() error {
		if ok, err := c.landed(id); err != nil || ok {
			return err
		}
		p, err := c.post(path+"/submit", nil)
		if err != nil {
			return err
		}
		if n := notice(p); n != "" {
			return errors.New(n)
		}
		return errors.New("not landed yet")
	})
}

// post posts a form to ok control as alice, an error unless the page renders.
func (c *scaleClient) post(path string, form url.Values) (page, error) {
	if form == nil {
		form = url.Values{}
	}
	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.control+path, strings.NewReader(form.Encode()))
	if err != nil {
		return page{}, err
	}
	req.Header.Set(authn.DevPrincipalKey, alice)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.http.Do(req)
	if err != nil {
		return page{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return page{}, err
	}
	p := page{status: resp.StatusCode, body: html.UnescapeString(string(body)), header: resp.Header}
	if p.status != http.StatusOK && p.status != http.StatusSeeOther && p.status != http.StatusNoContent {
		return p, fmt.Errorf("POST %s: %d", path, p.status)
	}
	return p, nil
}

// submittedOnMain reports whether the landing queue landed a repository's CL 1 on its main.
func submittedOnMain(ctx context.Context, d tables, repo int64) (bool, error) {
	const q = `SELECT EXISTS (SELECT 1 FROM submission s JOIN submission_cl sc ON sc.submission_id = s.%s
		WHERE sc.repository_id = @repo AND sc.cl_number = 1 AND s.status = 2)`
	var ok bool
	err := d.row(ctx, fmt.Sprintf(q, "id"), fmt.Sprintf(q, "submission_id"), map[string]any{"repo": repo}, &ok)
	return ok, err
}

// cursorsOf are a role's followers' places in each repository's log of the SCM's events.
func cursorsOf(ctx context.Context, d tables) (map[int64]int64, error) {
	const q = `SELECT key, seq FROM follow_cursor WHERE topic = @topic`
	return d.pairs(ctx, q, q, map[string]any{"topic": scmfeed.Topic})
}

// tables are a role's tables as the checks read them: through its pool on PostgreSQL, or a client
// of its database on Spanner, each query in its database's SQL, its parameters named.
type tables struct {
	pg *pgxpool.Pool
	sp *spanner.Client
}

// pairs are a query's rows of two numbers, the first to the second.
func (d tables) pairs(ctx context.Context, pgSQL, spannerSQL string, args map[string]any) (map[int64]int64, error) {
	if d.sp == nil {
		rows, err := d.pg.Query(ctx, pgSQL, pgx.NamedArgs(args))
		if err != nil {
			return nil, err
		}
		return pgxCollectPairs(rows)
	}
	out := map[int64]int64{}
	err := d.sp.Single().Query(ctx, spanner.Statement{SQL: spannerSQL, Params: args}).Do(func(r *spanner.Row) error {
		var k, v int64
		if err := r.Columns(&k, &v); err != nil {
			return err
		}
		out[k] = v
		return nil
	})
	return out, err
}

// row scans a query's one row into dest.
func (d tables) row(ctx context.Context, pgSQL, spannerSQL string, args map[string]any, dest ...any) error {
	if d.sp == nil {
		return d.pg.QueryRow(ctx, pgSQL, pgx.NamedArgs(args)).Scan(dest...)
	}
	found := false
	err := d.sp.Single().Query(ctx, spanner.Statement{SQL: spannerSQL, Params: args}).Do(func(r *spanner.Row) error {
		found = true
		return r.Columns(dest...)
	})
	if err == nil && !found {
		err = errors.New("no row")
	}
	return err
}

func pgxCollectPairs(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close()
}) (map[int64]int64, error) {
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var k, v int64
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

// retry runs fn until it succeeds or ctx ends.
func retry(ctx context.Context, what string, fn func() error) error {
	for {
		err := fn()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w (last: %v)", what, ctx.Err(), err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// replicaSet is a role's replicas, which a test stops and starts.
type replicaSet struct {
	name string
	// proxy is what dials the replicas reach them through (nil: none dials them).
	proxy *proxy
	start func() (addr string, stop func(), err error)
	mu    sync.Mutex
	live  []replica
	kills int
}

type replica struct {
	addr string
	stop func()
}

// add starts a replica.
func (s *replicaSet) add() error {
	addr, stop, err := s.start()
	if err != nil {
		return err
	}
	if s.proxy != nil {
		s.proxy.add(addr)
	}
	s.mu.Lock()
	s.live = append(s.live, replica{addr: addr, stop: stop})
	s.mu.Unlock()
	return nil
}

// kill stops a replica at random: its connections are cut, then it stops.
func (s *replicaSet) kill(rng *rand.Rand) {
	s.mu.Lock()
	i := rng.IntN(len(s.live))
	r := s.live[i]
	s.live = slices.Delete(s.live, i, i+1)
	s.kills++
	s.mu.Unlock()
	if s.proxy != nil {
		s.proxy.remove(r.addr)
	}
	r.stop()
}

// proxy forwards each connection to one of a role's live replicas at random, as a load balancer
// in front of them does; a replica taken away has its connections closed.
type proxy struct {
	lis      net.Listener
	mu       sync.Mutex
	backends map[string]map[net.Conn]bool
}

func newProxy(t *testing.T) *proxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{lis: lis, backends: map[string]map[net.Conn]bool{}}
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go p.forward(c)
		}
	}()
	t.Cleanup(func() { lis.Close() })
	return p
}

func (p *proxy) addr() string { return p.lis.Addr().String() }

func (p *proxy) add(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.backends[addr] = map[net.Conn]bool{}
}

func (p *proxy) remove(addr string) {
	p.mu.Lock()
	conns := slices.Collect(maps.Keys(p.backends[addr]))
	delete(p.backends, addr)
	p.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

func (p *proxy) forward(c net.Conn) {
	defer c.Close()
	var addr string
	for range 100 {
		p.mu.Lock()
		addrs := slices.Collect(maps.Keys(p.backends))
		p.mu.Unlock()
		if len(addrs) > 0 {
			addr = addrs[rand.IntN(len(addrs))]
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if addr == "" {
		return
	}
	b, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer b.Close()
	p.mu.Lock()
	conns, ok := p.backends[addr]
	if ok {
		conns[c], conns[b] = true, true
	}
	p.mu.Unlock()
	if !ok {
		return // taken away meanwhile
	}
	done := make(chan struct{}, 2)
	go func() { io.Copy(b, c); done <- struct{}{} }()
	go func() { io.Copy(c, b); done <- struct{}{} }()
	<-done
	p.mu.Lock()
	delete(conns, c)
	delete(conns, b)
	p.mu.Unlock()
}

// dumpLanding logs what ok automation holds of a repository that did not land, and the bus's jobs
// about it, live and dead: on PostgreSQL, where the bus is beside the role's tables.
func dumpLanding(t *testing.T, pool *pgxpool.Pool, repo int64) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []struct{ what, sql string }{
		{"submissions", `SELECT id, status, note, reason FROM submission WHERE repository_id = $1 ORDER BY id`},
		{"attempts", `SELECT a.id, a.status, a.released, a.reason FROM attempt a JOIN submission s ON s.id = a.submission_id
			WHERE s.repository_id = $1 ORDER BY a.id`},
		{"runs", `SELECT id, phase, kind, status, attempt, coalesce(worker, ''), coalesce(dispatch_job, ''), reason, queued_at, started_at, finished_at
			FROM run WHERE repository_id = $1 ORDER BY id`},
		{"work", `SELECT kind, key, dirty, due_at, holder, lease_until FROM work WHERE key = $1`},
		{"jobs", `SELECT queue, id, attempts, not_before, created_at FROM bus.queue_job WHERE key = $1 ORDER BY created_at`},
		{"dead", `SELECT queue, id, attempts FROM bus.queue_dead WHERE key = $1`},
	} {
		rows, err := pool.Query(ctx, q.sql, repo)
		if err != nil {
			t.Logf("repository %d's %s: %v", repo, q.what, err)
			continue
		}
		n := 0
		for rows.Next() {
			vs, err := rows.Values()
			if err != nil {
				t.Logf("repository %d's %s: %v", repo, q.what, err)
				break
			}
			n++
			t.Logf("repository %d's %s: %v", repo, q.what, vs)
		}
		rows.Close()
		t.Logf("repository %d's %s: %d", repo, q.what, n)
	}
	t.Logf("now %v", time.Now().UTC())
}
