// Package store is ok automation's storage core (docs/automation.md, "Execution"): runs — one
// job on one commit — their queue and the leases workers hold while they run them, plans,
// submissions and their attempts, and runs' logs: appended to the database while a run is in
// progress, moved to the bucket as one object once it finishes. Store holds the rules; a Backend
// (postgres) is the database it keeps them in.
//
// Every state change is one operation or one transaction, so any number of the service's
// replicas can serve workers at once: a queued run is claimed by one worker, and a lease is
// renewed, used, and ended only by the worker that holds it.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/workflow"
)

// Kind is what a job runs.
type Kind int16

const (
	KindBuild Kind = 1 // `bazel build` of the targets
	KindTest  Kind = 2 // `bazel test` of the targets
	KindRun   Kind = 3 // the lone target, a binary, built and run in a sandbox
	KindLint  Kind = 4 // the repository's linters over the files and targets
	KindPlan  Kind = 5 // plan a patchset's pre or a candidate's post: only automation starts one
	KindPush  Kind = 7 // the lone target, an oci_push, built and pushed: only the deployer starts one
)

// Phase is why a run exists.
type Phase int16

const (
	PhasePre    Phase = 1
	PhasePost   Phase = 2
	PhaseManual Phase = 3
	PhaseDeploy Phase = 4 // a deployment's push; Origin.Job is the deploy's ID
)

// Status is where a run is.
type Status int16

const (
	StatusQueued   Status = 1
	StatusRunning  Status = 2
	StatusPassed   Status = 3
	StatusFailed   Status = 4
	StatusCanceled Status = 5
	StatusError    Status = 6
)

// Finished reports whether a run in this status is over.
func (s Status) Finished() bool { return s >= StatusPassed }

// Job is Bazel, run on a commit's checkout, that must succeed.
type Job struct {
	Kind    Kind
	Targets []string
	Args    []string
	// Pool is the pool of workers it runs in (docs/hosted.md, "Hosted automation, as built"): ""
	// for the default one.
	Pool string
}

// Origin is why a run exists: its phase, and the CL, patchset, and job it runs for.
type Origin struct {
	Phase    Phase
	CLNumber int64 // 0: none
	Patchset int32
	Job      string
}

// Run is one job on one commit.
type Run struct {
	ID           int64
	RepositoryID int64
	CommitID     []byte
	Job          Job
	Origin       Origin
	Status       Status
	RequestedBy  string // a principal's stable ID
	// CanceledBy is the stable ID of who canceled it, or asked its worker to stop it: "" for no
	// one (the system), or a run not canceled.
	CanceledBy     string
	RerunOf        int64
	Attempt        int32
	Worker         string
	ExitCode       int32
	Reason         string
	LogSize        int64
	LogStored      bool
	TimeoutSeconds int32
	// PlanID is the plan it runs for (0: none): its planning run, or a job the plan selected,
	// and Why what selected it.
	PlanID     int64
	Why        string
	QueuedAt   time.Time
	StartedAt  time.Time // zero until it starts
	FinishedAt time.Time // zero until it finishes
	// Pushed is what a push run that passed pushed (Get and List carry it).
	Pushed []Pushed
	// CarriedFrom is, for a post run that never ran, the pre run whose pass it carries (0: it
	// ran).
	CarriedFrom int64
	// LeaseKey is the key the claim that leased it minted, for its worker alone (docs/secrets.md,
	// "Readers that are not people"): set by Lease, never stored — the run holds its hash.
	LeaseKey []byte
}

// Pushed is an image a push run pushed: where it went, under which tags, and its manifest's
// digest.
type Pushed struct {
	Target     string // the oci_push
	Registry   string
	Repository string
	Digest     string // sha256:<hex>
	Tags       []string
}

// priorityOf is a run's priority among its account's, by its phase: post's runs, which main waits
// on, and deploys', which carry what landed into the cluster, first; then pre's; then manual runs.
func priorityOf(p Phase) int16 {
	switch p {
	case PhasePost, PhaseDeploy:
		return 2
	case PhasePre:
		return 1
	}
	return 0
}

// Config tunes the store.
type Config struct {
	// Heartbeat is how often a worker renews its lease; a lease lapses after three missed
	// renewals.
	Heartbeat time.Duration
	// DefaultTimeout is a run's timeout when its request names none, and MaxTimeout the
	// longest a request may name.
	DefaultTimeout, MaxTimeout time.Duration
	// Attempts is how many times a run is started before a lost worker makes it an error.
	Attempts int32
	// MaxLog is the most log a run keeps; a longer log ends with a note saying so.
	MaxLog int64
	// Poll bounds every wait on a signal, so a missed one delays and never strands.
	Poll time.Duration
	// Signals carry the store's wake-ups between replicas (docs/hosted.md, "Signals"): the bus's.
	// Nil is within the process (bus.NewLocal).
	Signals bus.Signals
	// PlanPool is the pool of workers planning runs run in: "" for the default one.
	PlanPool string
	// Quotas reads accounts' quotas of CI, for admission; nil admits every account, each of
	// weight 1.
	Quotas Quotas
}

// DefaultConfig is the service's configuration.
var DefaultConfig = Config{
	Heartbeat:      10 * time.Second,
	DefaultTimeout: 2 * time.Hour,
	MaxTimeout:     24 * time.Hour,
	Attempts:       2,
	MaxLog:         64 << 20,
	Poll:           2 * time.Second,
}

// Errors, which the service maps onto gRPC codes.
var (
	ErrNotFound  = errors.New("no such run")
	ErrLeaseLost = errors.New("the run's lease is not this worker's")
	ErrLogFull   = errors.New("the run's log is full")
	ErrSlotTaken = errors.New("another run holds the account's slot")
)

// InvalidError is a request the store refuses as malformed.
type InvalidError struct{ Message string }

func (e *InvalidError) Error() string { return e.Message }

func invalid(format string, args ...any) error {
	return &InvalidError{Message: fmt.Sprintf(format, args...)}
}

// ConflictError is a request the run's state does not allow.
type ConflictError struct{ Message string }

func (e *ConflictError) Error() string { return e.Message }

// Store is ok automation's storage core.
type Store struct {
	b      Backend
	blobs  blob.Store
	cfg    Config
	notify *notifier
	leases *leaseLines
}

// New builds a store over a backend and the logs' bucket. Run Listen for prompt wake-ups;
// without it every wait polls.
func New(b Backend, blobs blob.Store, cfg Config) *Store {
	if cfg.Signals == nil {
		cfg.Signals = bus.NewLocal()
	}
	return &Store{b: b, blobs: blobs, cfg: cfg, notify: newNotifier(), leases: newLeaseLines()}
}

// txOps are a transaction's operations as the store hands them on: the backend's, and the signals
// to send once it commits.
type txOps struct {
	Ops
	pending *bus.Pending
}

// A transaction's locks of rows about a repository read the repository's move gate (Ops.Gate)
// first: one about a repository whose network moves between cells is refused before it writes
// (docs/hosted.md, "Cells, as built").

func (t txOps) LockRun(ctx context.Context, id int64) (*Run, error) {
	r, err := t.Ops.LockRun(ctx, id)
	if err != nil {
		return r, err
	}
	return r, t.Gate(ctx, r.RepositoryID)
}

func (t txOps) LockPlan(ctx context.Context, id int64) (*Plan, error) {
	p, err := t.Ops.LockPlan(ctx, id)
	if err != nil {
		return p, err
	}
	return p, t.Gate(ctx, p.RepositoryID)
}

func (t txOps) LockSubmission(ctx context.Context, id int64) (*Submission, error) {
	s, err := t.Ops.LockSubmission(ctx, id)
	if err != nil {
		return s, err
	}
	return s, t.Gate(ctx, s.RepositoryID)
}

func (t txOps) LockAttempt(ctx context.Context, id int64) (*Attempt, error) {
	a, err := t.Ops.LockAttempt(ctx, id)
	if err != nil {
		return a, err
	}
	subs, err := t.Submissions(ctx, []int64{a.SubmissionID})
	if err != nil {
		return a, err
	}
	if len(subs) == 0 {
		return a, fmt.Errorf("attempt %d's submission %d: %w", id, a.SubmissionID, ErrNotFound)
	}
	return a, t.Gate(ctx, subs[0].RepositoryID)
}

func (t txOps) LockCL(ctx context.Context, repo, cl int64) error {
	if err := t.Gate(ctx, repo); err != nil {
		return err
	}
	return t.Ops.LockCL(ctx, repo, cl)
}

// Every run is queued here, at its phase's priority (priorityOf), in its job's pool or the default.
func (t txOps) InsertRun(ctx context.Context, r NewRun) (*Run, error) {
	if err := t.Gate(ctx, r.RepositoryID); err != nil {
		return nil, err
	}
	return t.Ops.InsertRun(ctx, queuedAs(r))
}

func (t txOps) InsertRuns(ctx context.Context, rs []NewRun) error {
	gated := map[int64]bool{}
	out := make([]NewRun, len(rs))
	for i, r := range rs {
		if !gated[r.RepositoryID] {
			if err := t.Gate(ctx, r.RepositoryID); err != nil {
				return err
			}
			gated[r.RepositoryID] = true
		}
		out[i] = queuedAs(r)
	}
	return t.Ops.InsertRuns(ctx, out)
}

// queuedAs is a run as it is queued: its phase's priority, and its pool named.
func queuedAs(r NewRun) NewRun {
	r.Priority = priorityOf(r.Origin.Phase)
	if r.Job.Pool == "" {
		r.Job.Pool = workflow.DefaultPool
	}
	return r
}

func (t txOps) FinishSubmission(ctx context.Context, id int64, status SubmissionStatus, reason, by string) (int64, bool, error) {
	if _, err := t.LockSubmission(ctx, id); errors.Is(err, ErrNotFound) {
		return 0, false, nil
	} else if err != nil {
		return 0, false, err
	}
	return t.Ops.FinishSubmission(ctx, id, status, reason, by)
}

func (t txOps) LockMain(ctx context.Context, repo int64) error {
	if err := t.Gate(ctx, repo); err != nil {
		return err
	}
	return t.Ops.LockMain(ctx, repo)
}

// txn runs fn in a read-write transaction (Backend.Txn), and sends the signals its committed
// attempt recorded once it has committed.
func (s *Store) txn(ctx context.Context, fn func(tx Ops) error) error {
	var p bus.Pending
	err := s.b.Txn(ctx, func(tx Ops) error {
		p.Reset()
		return fn(txOps{Ops: tx, pending: &p})
	})
	if err == nil {
		p.Send(ctx, s.cfg.Signals)
	}
	return err
}

// leaseTTL is how long a lease lasts without renewal.
func (s *Store) leaseTTL() time.Duration { return 3 * s.cfg.Heartbeat }

// Get returns a run.
func (s *Store) Get(ctx context.Context, id int64) (*Run, error) {
	r, err := s.b.Run(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := s.loadPushed(ctx, []*Run{r}); err != nil {
		return nil, err
	}
	return r, nil
}

// loadPushed fills in what each push run pushed.
func (s *Store) loadPushed(ctx context.Context, runs []*Run) error {
	byID := map[int64]*Run{}
	var ids []int64
	for _, r := range runs {
		if r.Job.Kind == KindPush {
			byID[r.ID] = r
			ids = append(ids, r.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	pushed, err := s.b.Pushed(ctx, ids)
	if err != nil {
		return err
	}
	for id, ps := range pushed {
		byID[id].Pushed = ps
	}
	return nil
}

// ListQuery selects runs: a CL's (Patchset 0: every patchset's), or a commit's.
type ListQuery struct {
	RepositoryID int64
	CLNumber     int64
	Patchset     int32
	CommitID     []byte
	Limit        int
	BeforeID     int64 // 0: from the newest
}

// List returns the runs a query selects, newest first.
func (s *Store) List(ctx context.Context, q ListQuery) ([]*Run, error) {
	if q.RepositoryID <= 0 {
		return nil, invalid("a repository is required")
	}
	if (q.CLNumber > 0) == (len(q.CommitID) > 0) {
		return nil, invalid("name a CL or a commit, not both")
	}
	if len(q.CommitID) > 0 && len(q.CommitID) != 32 {
		return nil, invalid("a commit ID is 32 bytes")
	}
	switch {
	case q.Limit <= 0:
		q.Limit = 100
	case q.Limit > 1000:
		q.Limit = 1000
	}
	before := max(q.BeforeID, 0)
	var out []*Run
	var err error
	if q.CLNumber > 0 {
		out, err = s.b.RunsOfCL(ctx, q.RepositoryID, q.CLNumber, q.Patchset, before, q.Limit)
	} else {
		out, err = s.b.RunsOfCommit(ctx, q.RepositoryID, q.CommitID, before, q.Limit)
	}
	if err != nil {
		return nil, err
	}
	if err := s.loadPushed(ctx, out); err != nil {
		return nil, err
	}
	return out, nil
}

// validJob checks a job's shape: each kind's targets and arguments.
func validJob(j Job) error {
	for _, t := range j.Targets {
		if !strings.HasPrefix(t, "//") || strings.ContainsAny(t, " \t\n") {
			return invalid("%q is not a Bazel label", t)
		}
	}
	switch j.Kind {
	case KindBuild:
		if len(j.Targets) == 0 {
			return invalid("a build names its targets")
		}
		if len(j.Args) > 0 {
			return invalid("a build takes no arguments")
		}
	case KindTest:
		if len(j.Targets) == 0 {
			return invalid("a test names its targets")
		}
		if len(j.Args) > 0 && len(j.Targets) != 1 {
			return invalid("arguments go to one test target, not %d", len(j.Targets))
		}
	case KindRun:
		if len(j.Targets) != 1 {
			return invalid("a run names one target, not %d", len(j.Targets))
		}
	case KindLint:
		// A lint's files are its change's, which its worker reads.
		if len(j.Args) > 0 {
			return invalid("a lint takes no arguments")
		}
	case KindPlan:
		return invalid("only automation plans a patchset's pre")
	case KindPush:
		if len(j.Targets) != 1 {
			return invalid("a push names one oci_push, not %d targets", len(j.Targets))
		}
		if len(j.Args) > 0 {
			return invalid("a push takes no arguments")
		}
	default:
		return invalid("unknown job kind %d", j.Kind)
	}
	return nil
}

// validOrigin checks an origin: a pre or post run is a CL's patchset's.
func validOrigin(o Origin) error {
	switch o.Phase {
	case PhasePre, PhasePost:
		if o.CLNumber <= 0 || o.Patchset <= 0 {
			return invalid("a pre or post run names its CL and patchset")
		}
	case PhaseManual:
		if o.CLNumber < 0 || o.Patchset < 0 || (o.Patchset > 0 && o.CLNumber == 0) {
			return invalid("a patchset belongs to a CL")
		}
	case PhaseDeploy:
		if o.Job == "" || o.CLNumber != 0 || o.Patchset != 0 {
			return invalid("a deployment's push names its deploy, and no CL")
		}
	default:
		return invalid("unknown phase %d", o.Phase)
	}
	return nil
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
