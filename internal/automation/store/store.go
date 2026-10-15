// Package store is ok automation's storage core (docs/automation.md, "Execution"):
// runs — one job on one commit — in its schema, their queue and the leases
// workers hold while they run them, and their logs: appended to the database while a run is in
// progress, moved to the bucket as one object once it finishes.
//
// Every state change is one statement or one transaction, so any number of the service's
// replicas can serve workers at once: a queued run is claimed with FOR UPDATE SKIP LOCKED, and
// a lease is renewed, used, and ended only by the worker that holds it.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Cidan/ok/internal/blob"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/migrate"
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
	ID             int64
	RepositoryID   int64
	CommitID       []byte
	Job            Job
	Origin         Origin
	Status         Status
	RequestedBy    string
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

// deployPriority is a deployment's pushes' priority: they carry what landed into the cluster,
// and go with post's before pre's.
const deployPriority = 1

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
	// Poll bounds every wait on a notification, so a missed one delays and never strands.
	Poll time.Duration
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
	pool   *pgxpool.Pool
	blobs  blob.Store
	cfg    Config
	notify *notifier
	locks  *lock.Lock
}

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations are ok automation's schema.
var Migrations = migrate.MustLoad(migrations, "migrations")

// Open brings the database behind pool to ok automation's schema and builds a store over it and
// the logs' bucket. Run Listen for prompt wake-ups; without it every wait polls.
func Open(ctx context.Context, pool *pgxpool.Pool, blobs blob.Store, cfg Config) (*Store, error) {
	if _, err := migrate.Apply(ctx, pool, "automation", Migrations); err != nil {
		return nil, err
	}
	return &Store{pool: pool, blobs: blobs, cfg: cfg, notify: newNotifier(), locks: lock.New(pool)}, nil
}

// leaseTTL is how long a lease lasts without renewal.
func (s *Store) leaseTTL() time.Duration { return 3 * s.cfg.Heartbeat }

// runColumns are a run's columns, in scanRun's order.
const runColumns = `id, repository_id, commit_id, kind, targets, args, phase, cl_number, patchset, job,
	requested_by, rerun_of, status, attempt, coalesce(worker, ''), coalesce(exit_code, 0), reason, log_size,
	log_stored, timeout_seconds, coalesce(plan_id, 0), why, queued_at, started_at, finished_at, coalesce(carried_from, 0)`

func scanRun(row pgx.Row) (*Run, error) {
	var r Run
	var cl, rerun *int64
	var patchset *int32
	var started, finished *time.Time
	err := row.Scan(&r.ID, &r.RepositoryID, &r.CommitID, &r.Job.Kind, &r.Job.Targets, &r.Job.Args,
		&r.Origin.Phase, &cl, &patchset, &r.Origin.Job, &r.RequestedBy, &rerun, &r.Status, &r.Attempt, &r.Worker,
		&r.ExitCode, &r.Reason, &r.LogSize, &r.LogStored, &r.TimeoutSeconds, &r.PlanID, &r.Why, &r.QueuedAt, &started, &finished,
		&r.CarriedFrom)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if cl != nil {
		r.Origin.CLNumber = *cl
	}
	if patchset != nil {
		r.Origin.Patchset = *patchset
	}
	if rerun != nil {
		r.RerunOf = *rerun
	}
	if started != nil {
		r.StartedAt = *started
	}
	if finished != nil {
		r.FinishedAt = *finished
	}
	return &r, nil
}

// Get returns a run.
func (s *Store) Get(ctx context.Context, id int64) (*Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM run WHERE id = $1`, id))
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
	rows, err := s.pool.Query(ctx, `SELECT run_id, target, registry, repository, digest, tags FROM pushed_image
		WHERE run_id = ANY($1) ORDER BY run_id, position`, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var p Pushed
		if err := rows.Scan(&id, &p.Target, &p.Registry, &p.Repository, &p.Digest, &p.Tags); err != nil {
			return err
		}
		byID[id].Pushed = append(byID[id].Pushed, p)
	}
	return rows.Err()
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
	before := q.BeforeID
	if before <= 0 {
		before = 1<<63 - 1
	}
	var rows pgx.Rows
	var err error
	if q.CLNumber > 0 {
		rows, err = s.pool.Query(ctx, `SELECT `+runColumns+` FROM run
			WHERE repository_id = $1 AND cl_number = $2 AND ($3 = 0 OR patchset = $3) AND id < $4
			ORDER BY id DESC LIMIT $5`, q.RepositoryID, q.CLNumber, q.Patchset, before, q.Limit)
	} else {
		rows, err = s.pool.Query(ctx, `SELECT `+runColumns+` FROM run
			WHERE repository_id = $1 AND commit_id = $2 AND id < $3
			ORDER BY id DESC LIMIT $4`, q.RepositoryID, q.CommitID, before, q.Limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
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

// nullable is a positive number, or SQL NULL.
func nullable[T int32 | int64](v T) *T {
	if v <= 0 {
		return nil
	}
	return &v
}

func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
