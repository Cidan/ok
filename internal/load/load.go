package load

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/scmtest"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Clients are the services a run calls: as a person, and as a worker.
type Clients struct {
	Repositories scmpb.RepositoriesClient
	Store        scmpb.StoreClient
	Upload       scmpb.ChangeListsClient
	Workspaces   scmpb.WorkspacesClient
	Control      controlpb.ChangeListsClient
	Browse       scmpb.BrowseClient
	// Work, WorkerBrowse: as a worker.
	Work         automationpb.WorkClient
	WorkerBrowse scmpb.BrowseClient
}

// Person is someone a run acts as, on a connection of their own.
type Person struct {
	Handle string
	Clients
}

// Repo is a repository a run drives.
type Repo struct {
	ID   int64
	Name string // its full name
}

// Config is a run.
type Config struct {
	// Clients are the run's one person's and its workers', unless People and Workers' clients are
	// set.
	Clients
	// People are those the run acts as, each working in one of Repos — the i-th in the i-th modulo
	// their number, so one repository is everyone's monorepo — and reviewing each other's CLs.
	People []Person
	// WorkerClients are the connections the synthetic workers spread over: Clients' unless set.
	WorkerClients []Clients
	Repos         []Repo
	Shape         Shape
	Duration      time.Duration
	// Drain is how long after Duration the run waits for CLs under way to finish.
	Drain time.Duration
	// MaxInFlight bounds each operation's requests under way: one offered past it is shed.
	MaxInFlight int
	// PublishBytes, CheckpointBytes, and CLBytes are what each writes, in files of at most a MiB.
	PublishBytes, CheckpointBytes, CLBytes int
	// Workers are the synthetic workers; RunTime how long a job of theirs takes; Read whether it
	// reads its commit's files through the SCM, as a cache's miss would.
	Workers int
	RunTime time.Duration
	Read    bool
	// WorkerPrefix tells this run's workers from others' — another pod's of the same run.
	WorkerPrefix string
	// SeedFiles, when set, has each repository seeded with a tree of that many files shaped as a
	// monorepo's, each of SeedFileBytes; checkpoints, publishes, and CLs change files of it, and
	// publishes carry jj's operations and views (monorepo.go). SeedLands: this pod lands the
	// seeds, which others wait for.
	SeedFiles, SeedFileBytes int
	SeedLands                bool
	// Pod and Pods: this pod of the run's, whose share of the seed's files its CLs change.
	Pod, Pods int
	Log       *slog.Logger
}

// run is a run under way.
type run struct {
	cfg        Config
	stats      map[string]*stat
	repos      []*repoState
	people     []*Person
	workspaces []*workspace

	cls struct {
		sync.Mutex
		submitted, landed int
		failed            map[string]int
		stuck             []string
	}
	runs struct{ plans, jobs, released atomic.Int64 }
	// mono and trees: a run with SeedFiles' seed, and the trees it read and wrote.
	mono  *seeded
	trees *trees
}

// repoState is what a run keeps of a repository.
type repoState struct {
	Repo
	mu     sync.Mutex
	head   []byte
	headAt time.Time
}

// workspace is an active workspace, checkpointing every CheckpointEvery as okfs does: the target's
// tenth of accounts active, so a run at speed S has S times its repositories' tenth, several to a
// repository past S = 10.
type workspace struct {
	repo      *repoState
	owner     *Person
	name      string
	mu        sync.Mutex
	lease     *scmpb.Lease
	lastCheck int64
	// With a seed: main's head when it began, its working copy's tree, the files it works on, its
	// working-copy commit, and its operation log's heads.
	base    []byte
	tree    encoding.ID
	paths   []string
	wc      encoding.ID
	opHeads [][]byte
}

// operations a run measures.
var operations = []string{"publish", "checkpoint", "browse", "cl.upload", "cl.review", "cl.submit", "cl.landed", "worker.lease", "worker.read"}

// Run drives the repositories for the duration, then waits for CLs under way, and reports.
func Run(ctx context.Context, cfg Config) (*Report, error) {
	if len(cfg.Repos) == 0 {
		return nil, errors.New("load: no repositories to drive")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	r := &run{cfg: cfg, stats: map[string]*stat{}}
	r.cls.failed = map[string]int{}
	for _, op := range operations {
		r.stats[op] = newStat()
	}
	for _, repo := range cfg.Repos {
		r.repos = append(r.repos, &repoState{Repo: repo})
	}
	if len(cfg.People) == 0 {
		cfg.People = []Person{{Clients: cfg.Clients}}
	}
	for i := range cfg.People {
		r.people = append(r.people, &cfg.People[i])
	}
	if len(cfg.WorkerClients) == 0 {
		cfg.WorkerClients = []Clients{cfg.Clients}
	}
	r.cfg = cfg
	// The active workspaces: a tenth of the units, each a person's — the i-th's in their repository —
	// several to a person past one per person.
	active := max(1, int(math.Ceil(float64(cfg.Shape.Repositories)*cfg.Shape.Speed*ActiveAtPeak)))
	for i := range active {
		p := i % len(r.people)
		r.workspaces = append(r.workspaces, &workspace{repo: r.repoOf(p), owner: r.people[p],
			name: fmt.Sprintf("load-%s%d", r.people[p].Handle, i/len(r.people))})
	}
	rates := cfg.Shape.Rates()
	wctx, stopWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	for i := range cfg.Workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			r.worker(wctx, fmt.Sprintf("load-%s-%d", cfg.WorkerPrefix, i), cfg.WorkerClients[i%len(cfg.WorkerClients)])
		}()
	}
	if cfg.SeedFiles > 0 {
		r.trees = newTrees()
		r.mono = newSeeded(cfg.SeedFiles, cfg.Pod, cfg.Pods)
		seeded := time.Now()
		for _, rs := range r.repos {
			if err := r.seedRepo(ctx, rs); err != nil {
				stopWorkers()
				workers.Wait()
				return nil, fmt.Errorf("load: seeding: %w", err)
			}
		}
		cfg.Log.Info("load: seeded", "repositories", len(r.repos), "files", cfg.SeedFiles, "took", time.Since(seeded).Round(time.Millisecond))
	}
	offerCtx, stopOffering := context.WithTimeout(ctx, cfg.Duration)
	defer stopOffering()
	started := time.Now()
	var ops, cls sync.WaitGroup
	offer := func(name string, rate float64, do func(context.Context) error, group *sync.WaitGroup) {
		defer ops.Done()
		if rate <= 0 {
			return
		}
		st := r.stats[name]
		inFlight := make(chan struct{}, cfg.MaxInFlight)
		next := time.Now()
		for {
			next = next.Add(time.Duration(mrand.ExpFloat64() / rate * float64(time.Second)))
			select {
			case <-offerCtx.Done():
				return
			case <-time.After(time.Until(next)):
			}
			st.offer()
			select {
			case inFlight <- struct{}{}:
			default:
				st.dropped()
				continue
			}
			group.Add(1)
			go func() {
				defer group.Done()
				defer func() { <-inFlight }()
				t0 := time.Now()
				err := do(ctx)
				st.done(time.Since(t0), err)
			}()
		}
	}
	var plain sync.WaitGroup
	ops.Add(4)
	go offer("publish", rates.Publishes, r.publish, &plain)
	go offer("checkpoint", rates.Checkpoints, r.checkpoint, &plain)
	go offer("browse", rates.Browses, r.browse, &plain)
	go offer("cl.landed", rates.CLs, r.cl, &cls)
	ops.Wait()
	offered := time.Since(started)
	plain.Wait()
	drained := make(chan struct{})
	go func() { cls.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(cfg.Drain):
	}
	stopWorkers()
	workers.Wait()
	return r.report(started, offered), nil
}

// repoOf is the repository the i-th person works in.
func (r *run) repoOf(i int) *repoState { return r.repos[i%len(r.repos)] }

// pick is a person, at random, and the repository they work in.
func (r *run) pick() (*Person, *repoState) {
	i := mrand.IntN(len(r.people))
	return r.people[i], r.repoOf(i)
}

// reviewer is someone other than the author, at random: the author, when they are the run's one
// person.
func (r *run) reviewer(author *Person) *Person {
	if len(r.people) == 1 {
		return author
	}
	for {
		if p := r.people[mrand.IntN(len(r.people))]; p != author {
			return p
		}
	}
}

// mainHead is a repository's main, read again after a while.
func (r *run) mainHead(ctx context.Context, p *Person, rs *repoState) ([]byte, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.head != nil && time.Since(rs.headAt) < 30*time.Second {
		return rs.head, nil
	}
	got, err := p.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: rs.ID}})
	if err != nil {
		return nil, err
	}
	rs.head, rs.headAt = got.GetHead(), time.Now()
	return rs.head, nil
}

// maxFile bounds a file a run writes.
const maxFile = 1 << 20

// files stages a tree of fresh files of size bytes in all under dir, returning it.
func (r *run) files(b *scmtest.Objects, dir string, size int) encoding.ID {
	var entries []encoding.TreeEntry
	for i := 0; size > 0 || i == 0; i++ {
		data := make([]byte, min(max(size, 1), maxFile))
		size -= len(data)
		rand.Read(data)
		entries = append(entries, encoding.TreeEntry{Name: fmt.Sprintf("f%03d.bin", i), Value: encoding.TreeValue{Kind: encoding.TreeValueFile, ID: b.File(data)}})
	}
	sub := b.Add(&encoding.Tree{Entries: entries})
	return b.Add(&encoding.Tree{Entries: []encoding.TreeEntry{{Name: dir, Value: encoding.TreeValue{Kind: encoding.TreeValueTree, ID: sub}}}})
}

// commit stages a commit of tree on parent, a change of its own.
func commit(b *scmtest.Objects, tree encoding.ID, desc string, parent []byte) encoding.ID {
	var change encoding.ChangeID
	rand.Read(change[:])
	sig := encoding.Signature{Name: "ok load", Email: "load@example.com", Time: encoding.Timestamp{Millis: time.Now().UnixMilli()}}
	var p encoding.ID
	copy(p[:], parent)
	return b.Add(&encoding.Commit{Parents: []encoding.ID{p}, RootTree: encoding.Merge[encoding.ID]{tree}, ConflictLabels: encoding.Merge[string]{""},
		ChangeID: change, Description: desc, Author: sig, Committer: sig})
}

func name() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// publish sends a commit of fresh files on a repository's main, as a person's jj publishes a
// transaction.
func (r *run) publish(ctx context.Context) error {
	if r.mono != nil {
		ws := r.workspaces[mrand.IntN(len(r.workspaces))]
		ws.mu.Lock()
		defer ws.mu.Unlock()
		if err := r.begin(ctx, ws); err != nil {
			return err
		}
		return r.publishTransaction(ctx, ws)
	}
	p, rs := r.pick()
	head, err := r.mainHead(ctx, p, rs)
	if err != nil {
		return err
	}
	b := scmtest.Builder()
	commit(b, r.files(b, "ws-"+name(), r.cfg.PublishBytes), "a load publish", head)
	_, _, err = b.Publish(ctx, p.Store, rs.ID, nil)
	return err
}

// browse reads a repository's main as a person browsing it does: its root's listing.
func (r *run) browse(ctx context.Context) error {
	p, rs := r.pick()
	head, err := r.mainHead(ctx, p, rs)
	if err != nil {
		return err
	}
	_, err = p.Browse.ReadTree(ctx, &scmpb.ReadTreeRequest{RepositoryId: rs.ID, CommitId: head, LastCommits: true})
	return err
}

// checkpoint records an active workspace's checkpoint of a tree it publishes, as okfs does.
func (r *run) checkpoint(ctx context.Context) error {
	ws := r.workspaces[mrand.IntN(len(r.workspaces))]
	rs, p := ws.repo, ws.owner
	head, err := r.mainHead(ctx, p, rs)
	if err != nil {
		return err
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if err := r.begin(ctx, ws); err != nil {
		return err
	}
	b := scmtest.Builder()
	var tree encoding.ID
	base := head
	if r.mono != nil {
		var err error
		if tree, err = r.editWorkspace(ctx, ws, b, 1, r.cfg.CheckpointBytes); err != nil {
			return err
		}
		base = ws.base
	} else {
		tree = r.files(b, "ws", r.cfg.CheckpointBytes)
	}
	if _, _, err := b.Publish(ctx, p.Store, rs.ID, nil); err != nil {
		return err
	}
	cp, err := p.Workspaces.RecordCheckpoint(ctx, &scmpb.RecordCheckpointRequest{Lease: ws.lease, ParentId: ws.lastCheck, TreeIds: [][]byte{tree[:]},
		BaseCommitId: base})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition || status.Code(err) == codes.PermissionDenied {
			ws.lease = nil // lapsed: attach again next time
		}
		return err
	}
	ws.lastCheck = cp.GetId()
	if r.mono != nil {
		ws.tree = tree
	}
	return nil
}

// cl has a person upload a CL of fresh files, another review and approve it, and its author submit
// it once its pre passed and wait for it to land — each step measured, the whole as cl.landed.
func (r *run) cl(ctx context.Context) error {
	p, rs := r.pick()
	head, err := r.mainHead(ctx, p, rs)
	if err != nil {
		return err
	}
	b := scmtest.Builder()
	var tree encoding.ID
	if r.mono != nil {
		root, err := r.trees.rootOf(ctx, p, rs.ID, head)
		if err != nil {
			return err
		}
		changes := map[string]encoding.ID{}
		for _, path := range r.mono.forCL() {
			changes[path] = fresh(b, r.cfg.CLBytes/clFiles)
		}
		if tree, err = r.trees.edit(ctx, p, rs.ID, b, root, changes); err != nil {
			return err
		}
	} else {
		tree = r.files(b, "cl-"+name(), r.cfg.CLBytes)
	}
	c := commit(b, tree, "a load CL", head)
	step := func(name string, do func() error) error {
		t0 := time.Now()
		err := do()
		r.stats[name].offer()
		r.stats[name].done(time.Since(t0), err)
		return err
	}
	var number int64
	if err := step("cl.upload", func() error {
		if _, _, err := b.Publish(ctx, p.Store, rs.ID, nil); err != nil {
			return err
		}
		resp, err := p.Upload.Upload(ctx, &scmpb.UploadRequest{RepositoryId: rs.ID, CommitIds: [][]byte{c[:]},
			Readiness: scmpb.UploadRequest_READINESS_READY})
		if err != nil {
			return err
		}
		if len(resp.GetResults()) != 1 {
			return fmt.Errorf("the upload made %d CLs", len(resp.GetResults()))
		}
		number = resp.GetResults()[0].GetNumber()
		return nil
	}); err != nil {
		return err
	}
	ref := &controlpb.CLRef{Repository: rs.Name, Number: number}
	if err := step("cl.review", func() error {
		_, err := r.reviewer(p).Control.Reply(ctx, &controlpb.ReplyRequest{Cl: ref, Grants: []*controlpb.ReplyRequest_Grant{
			{Label: controlpb.Label_LABEL_LGTM, Granted: true}, {Label: controlpb.Label_LABEL_APPROVAL, Granted: true}}})
		return err
	}); err != nil {
		return err
	}
	stage, err := r.until(ctx, p, ref, controlpb.Stage_STAGE_SUBMITTABLE, controlpb.Stage_STAGE_PRE_FAILED)
	if err != nil {
		return r.outcome(ref, "", err)
	}
	if stage == controlpb.Stage_STAGE_PRE_FAILED {
		return r.outcome(ref, "pre failed", nil)
	}
	if err := step("cl.submit", func() error {
		_, err := p.Control.Submit(ctx, &controlpb.SubmitRequest{Cl: ref})
		return err
	}); err != nil {
		return err
	}
	r.cls.Lock()
	r.cls.submitted++
	r.cls.Unlock()
	stage, err = r.until(ctx, p, ref, controlpb.Stage_STAGE_LANDED, controlpb.Stage_STAGE_POST_FAILED)
	if err != nil {
		return r.outcome(ref, "", err)
	}
	if stage == controlpb.Stage_STAGE_POST_FAILED {
		return r.outcome(ref, "post failed", nil)
	}
	rs.mu.Lock()
	rs.head = nil // main moved
	rs.mu.Unlock()
	return r.outcome(ref, "landed", nil)
}

// outcome records how a CL ended: landed, failed for a reason, or stuck (err).
func (r *run) outcome(ref *controlpb.CLRef, what string, err error) error {
	r.cls.Lock()
	defer r.cls.Unlock()
	switch {
	case err != nil:
		r.cls.stuck = append(r.cls.stuck, fmt.Sprintf("%s CL %d: %v", ref.GetRepository(), ref.GetNumber(), err))
		return err
	case what == "landed":
		r.cls.landed++
		return nil
	}
	r.cls.failed[what]++
	return fmt.Errorf("CL %s/%d: %s", ref.GetRepository(), ref.GetNumber(), what)
}

// until watches a CL until it reaches a stage at or past want, or fails (failed); it reports
// which.
func (r *run) until(ctx context.Context, p *Person, ref *controlpb.CLRef, want, failed controlpb.Stage) (controlpb.Stage, error) {
	deadline := time.Now().Add(r.cfg.Duration + r.cfg.Drain)
	for time.Now().Before(deadline) {
		wctx, cancel := context.WithDeadline(ctx, deadline)
		stream, err := p.Control.Watch(wctx, ref)
		if err == nil {
			for {
				st, err := stream.Recv()
				if err != nil {
					break
				}
				switch s := st.GetStage(); {
				case s == failed:
					cancel()
					return s, nil
				case s >= want && s != controlpb.Stage_STAGE_ABANDONED && s != controlpb.Stage_STAGE_PRE_FAILED && s != controlpb.Stage_STAGE_POST_FAILED:
					cancel()
					return s, nil
				}
			}
		}
		cancel()
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		time.Sleep(time.Second) // the watch broke: a replica stopped
	}
	return 0, fmt.Errorf("not %s after %s", want, r.cfg.Duration+r.cfg.Drain)
}

// worker is a synthetic worker: it leases runs, answers a plan with one build job, and passes a
// job after RunTime, reading its commit's files first when Read.
func (r *run) worker(ctx context.Context, name string, c Clients) {
	for ctx.Err() == nil {
		t0 := time.Now()
		got, err := c.Work.Lease(ctx, &automationpb.LeaseRequest{Worker: name, WaitSeconds: 5})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			r.stats["worker.lease"].offer()
			r.stats["worker.lease"].done(time.Since(t0), err)
			time.Sleep(time.Second)
			continue
		}
		run := got.GetRun()
		if run == nil {
			continue
		}
		r.stats["worker.lease"].offer()
		r.stats["worker.lease"].done(time.Since(t0), nil)
		r.do(ctx, c, name, run, time.Duration(max(got.GetHeartbeatSeconds(), 1))*time.Second)
	}
}

func (r *run) do(ctx context.Context, c Clients, worker string, run *automationpb.Run, heartbeat time.Duration) {
	finish := func(req *automationpb.FinishRequest) {
		req.RunId, req.Worker = run.GetId(), worker
		if _, err := c.Work.Finish(context.WithoutCancel(ctx), req); err != nil {
			r.cfg.Log.Warn("load: finishing a run", "run", run.GetId(), "err", err)
		}
	}
	if run.GetJob().GetKind() == automationpb.Job_KIND_PLAN {
		r.runs.plans.Add(1)
		finish(&automationpb.FinishRequest{Status: automationpb.Run_STATUS_PASSED, Plan: &automationpb.Plan{Jobs: []*automationpb.PlannedJob{{
			Job: "build", Spec: &automationpb.Job{Kind: automationpb.Job_KIND_BUILD, Targets: []string{"//..."}}, Why: "ok load"}}}})
		return
	}
	r.runs.jobs.Add(1)
	// The job reads its commit's files while its run's time passes, its lease kept by heartbeats
	// throughout, as a worker's checkout and build are: a large tree's read may outlast a lease.
	read := make(chan struct{})
	if r.cfg.Read {
		go func() {
			defer close(read)
			t0 := time.Now()
			err := r.read(ctx, c, run)
			r.stats["worker.read"].offer()
			r.stats["worker.read"].done(time.Since(t0), err)
		}()
	} else {
		close(read)
	}
	end := time.Now().Add(r.cfg.RunTime)
	for read != nil || time.Now().Before(end) {
		wait := heartbeat
		if read == nil {
			wait = min(heartbeat, time.Until(end))
		}
		select {
		case <-ctx.Done():
			if read != nil {
				<-read
			}
			if _, err := c.Work.Release(context.WithoutCancel(ctx), &automationpb.ReleaseRequest{RunId: run.GetId(), Worker: worker}); err == nil {
				r.runs.released.Add(1)
			}
			return
		case <-read:
			read = nil // read: the run's time decides now
			continue
		case <-time.After(wait):
		}
		if read == nil && !time.Now().Before(end) {
			break
		}
		hb, err := c.Work.Heartbeat(ctx, &automationpb.HeartbeatRequest{RunId: run.GetId(), Worker: worker})
		if err == nil && hb.GetStop() != automationpb.HeartbeatResponse_STOP_UNSPECIFIED {
			if hb.GetStop() == automationpb.HeartbeatResponse_STOP_CANCELED {
				finish(&automationpb.FinishRequest{Status: automationpb.Run_STATUS_CANCELED})
			}
			return
		}
	}
	finish(&automationpb.FinishRequest{Status: automationpb.Run_STATUS_PASSED})
}

// read reads a run's commit's files, as a CI checkout's misses would.
func (r *run) read(ctx context.Context, c Clients, run *automationpb.Run) error {
	stream, err := c.WorkerBrowse.Export(ctx, &scmpb.ExportRequest{RepositoryId: run.GetRepositoryId(), CommitId: run.GetCommitId()})
	if err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}
