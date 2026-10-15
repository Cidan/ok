package store

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/testdb"
	"github.com/Cidan/ok/internal/workflow"
)

const self = "deploy.ok.robot.example.com"

// newStore opens a store on a database of this test's own.
func newStore(t *testing.T) *Store {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), testdb.Postgres(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s, err := Open(context.Background(), pool)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func commit(b byte) []byte {
	c := make([]byte, 32)
	c[0] = b
	return c
}

func web() workflow.Deploy {
	return workflow.Deploy{File: "web/.ok/workflow/w.yaml", Name: "web",
		Migrate:  []workflow.Ref{{Target: "//web/schema:apply"}},
		Images:   []workflow.Image{{Container: "web", Target: "//web:push"}},
		Workload: &workflow.Workload{Namespace: "web", Kind: workflow.Deployment, Name: "web"}}
}

func data() workflow.Deploy {
	return workflow.Deploy{File: "data/.ok/workflow/d.yaml", Name: "data", Migrate: []workflow.Ref{{Target: "//data:apply"}}}
}

// moveMain has the feed create repository 1 and move its main to commits 1..n.
func moveMain(t *testing.T, s *Store, n int) {
	t.Helper()
	ctx := context.Background()
	seq, err := s.Cursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if seq == 0 {
		seq++
		if err := s.Apply(ctx, FeedEvent{Seq: seq, Repository: 1, Name: "svc", Created: true}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i <= n; i++ {
		seq++
		if err := s.Apply(ctx, FeedEvent{Seq: seq, Repository: 1, Head: commit(byte(i)), HeadSeq: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
}

// The feed moves main once per event, however often an event is applied, and a repository
// whose main moved past its plan is unplanned.
func TestFeed(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	moveMain(t, s, 3)
	// Applying an event again changes nothing.
	if err := s.Apply(ctx, FeedEvent{Seq: 2, Repository: 1, Head: commit(9), HeadSeq: 9}); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, FeedEvent{Seq: 5, Repository: 1, Name: "svc2"}); err != nil {
		t.Fatal(err)
	}
	rs, err := s.Unplanned(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 1 || rs[0].Name != "svc2" || rs[0].HeadSeq != 3 || rs[0].Head[0] != 3 || rs[0].PlannedSeq != 0 {
		t.Fatalf("unplanned %+v", rs)
	}
	if _, err := s.RecordPlan(ctx, PlanResult{Repository: 1, Head: commit(3), HeadSeq: 3, By: self}); err != nil {
		t.Fatal(err)
	}
	if rs, _ := s.Unplanned(ctx); len(rs) != 0 {
		t.Errorf("planned, yet unplanned: %+v", rs)
	}
	var conflictErr *ConflictError
	if _, err := s.RecordPlan(ctx, PlanResult{Repository: 1, Head: commit(3), HeadSeq: 3, By: self}); !errors.As(err, &conflictErr) {
		t.Errorf("planning a head twice: %v", err)
	}
}

// A plan records its deployments with their images and migrations; a newer plan's deployment
// replaces a waiting one of the same deploy; each covers its range of main.
func TestPlans(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	moveMain(t, s, 2)
	if cov, planned, err := s.Covering(ctx, 1, commit(2)); err != nil || planned || len(cov) != 0 {
		t.Errorf("a commit no plan has reached: %d covering, planned %v (%v)", len(cov), planned, err)
	}
	p1, err := s.RecordPlan(ctx, PlanResult{Repository: 1, Head: commit(2), HeadSeq: 2, BaseSeq: 0, By: self, Notes: []string{"a note"},
		Deployments: []NewDeployment{{Deploy: web(), Why: "its image is affected", Wave: 1}, {Deploy: data(), Why: "first", Wave: 2},
			{Deploy: workflow.Deploy{File: "x/.ok/workflow/x.yaml", Name: "x"}, Why: "first", FailedWhy: "two deploys roll it"}}})
	if err != nil {
		t.Fatal(err)
	}
	open, err := s.Unfinished(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 2 {
		t.Fatalf("%d unfinished", len(open))
	}
	w := open[0]
	if w.Deploy != "//web/.ok/workflow/w.yaml:deploy:web" || w.PlanID != p1 || w.Status != Waiting || w.Stage != Queued ||
		w.FromSeq != 0 || w.ToSeq != 2 || w.Workload.String() != "deployment web/web" || w.RequestedBy != self ||
		!reflect.DeepEqual(w.Images, []Image{{Container: "web", Target: "//web:push"}}) ||
		len(w.Migrations) != 1 || w.Migrations[0].Target != "//web/schema:apply" || w.Migrations[0].Status != MigrationPending {
		t.Errorf("web %+v", w)
	}
	if open[1].Workload != nil || open[1].Wave != 2 {
		t.Errorf("data %+v", open[1])
	}
	statuses, err := s.PlanStatuses(ctx, p1)
	if err != nil || !reflect.DeepEqual(statuses, map[int][]Status{1: {Waiting}, 2: {Waiting}, 0: {Failed}}) {
		t.Errorf("plan statuses %v (%v)", statuses, err)
	}

	// Commit 1 and 2 are both covered.
	for _, c := range []byte{1, 2} {
		cov, planned, err := s.Covering(ctx, 1, commit(c))
		if err != nil || len(cov) != 3 || !planned {
			t.Errorf("commit %d is covered by %d, planned %v (%v)", c, len(cov), planned, err)
		}
	}
	if cov, planned, _ := s.Covering(ctx, 1, commit(7)); len(cov) != 0 || planned {
		t.Errorf("a commit not on main is covered, or planned")
	}

	// A newer plan's web replaces the waiting one.
	moveMain(t, s, 0)
	if err := s.Apply(ctx, FeedEvent{Seq: 10, Repository: 1, Head: commit(3), HeadSeq: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordPlan(ctx, PlanResult{Repository: 1, Head: commit(3), HeadSeq: 3, BaseSeq: 2, By: self,
		Deployments: []NewDeployment{{Deploy: web(), Why: "again", Wave: 1}}}); err != nil {
		t.Fatal(err)
	}
	old, err := s.Get(ctx, w.ID)
	if err != nil || old.Status != Canceled || !strings.Contains(old.Reason, "replaced by the deployment at 03") {
		t.Errorf("the replaced deployment %+v (%v)", old, err)
	}
}

// A deployment runs through its stages and finishes; its plan stops when it fails.
func TestLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	moveMain(t, s, 1)
	p, err := s.RecordPlan(ctx, PlanResult{Repository: 1, Head: commit(1), HeadSeq: 1, By: self,
		Deployments: []NewDeployment{{Deploy: web(), Why: "w", Wave: 1}, {Deploy: data(), Why: "d", Wave: 2},
			{Deploy: workflow.Deploy{File: "z/.ok/workflow/z.yaml", Name: "z", Migrate: []workflow.Ref{{Target: "//z:apply"}}}, Why: "z", Wave: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	open, _ := s.Unfinished(ctx)
	w, d, z := open[0], open[1], open[2]
	if err := s.Start(ctx, w.ID, ""); err != nil {
		t.Fatal(err)
	}
	var conflictErr *ConflictError
	if err := s.Start(ctx, w.ID, ""); !errors.As(err, &conflictErr) {
		t.Errorf("starting twice: %v", err)
	}
	if err := s.SetPushRun(ctx, w.ID, "web", 41); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPushed(ctx, w.ID, "web", "reg/web@sha256:ab"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStage(ctx, w.ID, Migrate); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMigration(ctx, w.ID, 0, MigrationApplied, "applied", []byte(`{"steps":[]}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRoll(ctx, w.ID, []byte(`{"stage":"canary"}`)); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, w.ID)
	if got.Status != Running || got.Stage != Migrate || got.Images[0].RunID != 41 || got.Images[0].Reference != "reg/web@sha256:ab" ||
		got.Migrations[0].Status != MigrationApplied || string(got.Roll) != `{"stage": "canary"}` || got.StartedAt.IsZero() {
		t.Errorf("running %+v", got)
	}

	// z starts; web fails: the plan's waiting deployments are canceled, its running ones halted.
	if err := s.Start(ctx, z.ID, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, w.ID, Failed, "the canary crash-looped"); err != nil {
		t.Fatal(err)
	}
	if err := s.StopPlan(ctx, p, w.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, d.ID); got.Status != Canceled || got.Reason != "deployment "+itoa(w.ID)+" of its plan failed" {
		t.Errorf("the waiting deployment of the failed plan %+v", got)
	}
	if got, _ := s.Get(ctx, z.ID); got.Status != Running || got.HaltBy != "deployment "+itoa(w.ID)+" of its plan failed" {
		t.Errorf("the running deployment of the failed plan %+v", got)
	}
	evs, err := s.Events(ctx, w.ID)
	if err != nil || len(evs) < 3 || !strings.HasPrefix(evs[0].Message, "planned at 01") || evs[len(evs)-1].Message != "failed: the canary crash-looped" {
		t.Errorf("web's log %+v (%v)", evs, err)
	}
	if err := s.Finish(ctx, w.ID, Succeeded, ""); !errors.As(err, &conflictErr) {
		t.Errorf("finishing twice: %v", err)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// People retry what failed, halt what runs, and allow what a migration refused.
func TestActions(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	moveMain(t, s, 1)
	if _, err := s.RecordPlan(ctx, PlanResult{Repository: 1, Head: commit(1), HeadSeq: 1, By: self,
		Deployments: []NewDeployment{{Deploy: web(), Why: "w", Wave: 1}}}); err != nil {
		t.Fatal(err)
	}
	open, _ := s.Unfinished(ctx)
	w := open[0]
	var conflictErr *ConflictError

	if _, err := s.Retry(ctx, w.ID, "alice.human.example.com"); !errors.As(err, &conflictErr) {
		t.Errorf("retrying a waiting deployment: %v", err)
	}
	if _, err := s.Allow(ctx, w.ID, "alice.human.example.com"); !errors.As(err, &conflictErr) {
		t.Errorf("allowing what nothing refused: %v", err)
	}
	// A migration refuses a hazard: the deployment waits; an allowance sends it back to migrate.
	if err := s.Start(ctx, w.ID, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SetMigration(ctx, w.ID, 0, MigrationWaiting, "refusing", nil, []string{"DELETES_DATA"}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetStage(ctx, w.ID, Hazard); err != nil {
		t.Fatal(err)
	}
	got, err := s.Allow(ctx, w.ID, "alice.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	m := got.Migrations[0]
	if got.Stage != Migrate || m.Status != MigrationPending || !reflect.DeepEqual(m.Allowed, []string{"DELETES_DATA"}) || m.AllowedBy != "alice.human.example.com" {
		t.Errorf("after the allowance %+v", got)
	}
	// Halting a running deployment asks its driver; it then fails, and is retried.
	if got, err := s.Halt(ctx, w.ID, "bob.human.example.com"); err != nil || got.HaltBy != "bob.human.example.com" || got.Status != Running {
		t.Errorf("halting a running deployment %+v (%v)", got, err)
	}
	if err := s.Finish(ctx, w.ID, Halted, "halted by bob.human.example.com"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Halt(ctx, w.ID, "bob.human.example.com"); !errors.As(err, &conflictErr) {
		t.Errorf("halting a halted deployment: %v", err)
	}
	r, err := s.Retry(ctx, w.ID, "alice.human.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if r.RetryOf != w.ID || r.PlanID != 0 || r.Status != Waiting || r.Deploy != w.Deploy || r.RequestedBy != "alice.human.example.com" ||
		!reflect.DeepEqual(r.Commit, w.Commit) || r.Workload.String() != w.Workload.String() ||
		len(r.Images) != 1 || r.Images[0].RunID != 0 || len(r.Migrations) != 1 || r.Migrations[0].Status != MigrationPending {
		t.Errorf("the retry %+v", r)
	}
	if _, err := s.Retry(ctx, w.ID, "alice.human.example.com"); !errors.As(err, &conflictErr) || !strings.Contains(err.Error(), "is newer") {
		t.Errorf("retrying what a newer deployment superseded: %v", err)
	}
	// Halting a waiting deployment cancels it at once.
	if got, err := s.Halt(ctx, r.ID, "bob.human.example.com"); err != nil || got.Status != Canceled {
		t.Errorf("halting a waiting deployment %+v (%v)", got, err)
	}
	if _, err := s.Get(ctx, 9999); !errors.Is(err, ErrNotFound) {
		t.Errorf("a deployment that does not exist: %v", err)
	}
}

// A succeeded self deployment whose roll still says to retire its old copy is found.
func TestRetiring(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	moveMain(t, s, 1)
	if _, err := s.RecordPlan(ctx, PlanResult{Repository: 1, Head: commit(1), HeadSeq: 1, By: self,
		Deployments: []NewDeployment{{Deploy: web(), Why: "w", Wave: 1, Self: true}}}); err != nil {
		t.Fatal(err)
	}
	open, _ := s.Unfinished(ctx)
	w := open[0]
	if err := s.Start(ctx, w.ID, "blue"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRoll(ctx, w.ID, []byte(`{"stage":"done","retire":true}`)); err != nil {
		t.Fatal(err)
	}
	if rs, _ := s.Retiring(ctx); len(rs) != 0 {
		t.Errorf("a running deployment is retiring")
	}
	if err := s.Finish(ctx, w.ID, Succeeded, ""); err != nil {
		t.Fatal(err)
	}
	rs, err := s.Retiring(ctx)
	if err != nil || len(rs) != 1 || !rs[0].Self || rs[0].Driver != "blue" {
		t.Fatalf("retiring %+v (%v)", rs, err)
	}
	if err := s.SetRoll(ctx, w.ID, []byte(`{"stage":"done"}`)); err != nil {
		t.Fatal(err)
	}
	if rs, _ := s.Retiring(ctx); len(rs) != 0 {
		t.Errorf("a retired deployment is retiring")
	}
}

// A lock is one holder's at a time, and free again once released: the leader's while it leads,
// and each deployment's, apart from every other, past the context it was taken in until it is
// released.
func TestLock(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	err := s.Lead(ctx, func(ctx context.Context) error {
		if err := s.Lead(ctx, func(context.Context) error { t.Error("two leaders"); return nil }); !errors.Is(err, lock.ErrNotAcquired) {
			t.Errorf("a second leader: %v, want lock.ErrNotAcquired", err)
		}
		if err := s.LeadFeed(ctx, func(context.Context) error { return nil }); err != nil {
			t.Errorf("the feed's lock while leading: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("leading: %v", err)
	}
	led := false
	if err := s.Lead(ctx, func(context.Context) error { led = true; return nil }); err != nil || !led {
		t.Errorf("leading once the first gave up: %v", err)
	}

	pass, endPass := context.WithCancel(ctx)
	release, ok, err := s.LockDeployment(pass, 7)
	if err != nil || !ok {
		t.Fatalf("the first take: %v %v", ok, err)
	}
	endPass()
	time.Sleep(2 * time.Second)
	if _, ok, err := s.LockDeployment(ctx, 7); err != nil || ok {
		t.Errorf("a second take while held, after the pass that took it ended: %v %v", ok, err)
	}
	if r, ok, err := s.LockDeployment(ctx, 8); err != nil || !ok {
		t.Errorf("another deployment's lock: %v %v", ok, err)
	} else {
		r()
	}
	release()
	r, ok, err := s.LockDeployment(ctx, 7)
	if err != nil || !ok {
		t.Errorf("taking it once released: %v %v", ok, err)
	}
	r()
}

// A deployment that replaces a waiting one takes its place — its gates and its wave in its
// plan — and a failure of that plan stops it too: waiting, it is canceled; running in a place at
// or after the failed wave, it is asked to halt.
func TestGates(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	moveMain(t, s, 1)
	p1, err := s.RecordPlan(ctx, PlanResult{Repository: 1, Head: commit(1), HeadSeq: 1, By: self,
		Deployments: []NewDeployment{{Deploy: data(), Why: "d", Wave: 1}, {Deploy: web(), Why: "w", Wave: 3}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, FeedEvent{Seq: 10, Repository: 1, Head: commit(2), HeadSeq: 2}); err != nil {
		t.Fatal(err)
	}
	p2, err := s.RecordPlan(ctx, PlanResult{Repository: 1, Head: commit(2), HeadSeq: 2, BaseSeq: 1, By: self,
		Deployments: []NewDeployment{{Deploy: web(), Why: "w2", Wave: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, FeedEvent{Seq: 11, Repository: 1, Head: commit(3), HeadSeq: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecordPlan(ctx, PlanResult{Repository: 1, Head: commit(3), HeadSeq: 3, BaseSeq: 2, By: self,
		Deployments: []NewDeployment{{Deploy: web(), Why: "w3", Wave: 2}}}); err != nil {
		t.Fatal(err)
	}
	open, _ := s.Unfinished(ctx)
	if len(open) != 2 {
		t.Fatalf("%d unfinished", len(open))
	}
	w3 := open[1]
	if want := []Gate{{Plan: p1, Wave: 3}, {Plan: p2, Wave: 1}}; !reflect.DeepEqual(w3.Gates, want) {
		t.Errorf("web at 3's gates %+v, want %+v", w3.Gates, want)
	}
	// data (plan 1, wave 1) fails: web at 3, in plan 1's wave 3, is canceled with it.
	d := open[0]
	if err := s.Start(ctx, d.ID, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, d.ID, Failed, "x"); err != nil {
		t.Fatal(err)
	}
	if err := s.StopPlan(ctx, p1, d.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, w3.ID); got.Status != Canceled {
		t.Errorf("web at 3 after plan 1 failed: %s", got.Status)
	}
}
