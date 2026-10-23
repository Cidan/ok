package store

import (
	"context"
	"time"

	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/workflow"
)

// Backend is the deployer's database: what each database it runs on implements (postgres), and
// the conformance suite in storetest checks. Its operations are the database's steps; the rules
// they make up — planning, replacing, retrying, halting, allowing — are Store's, written once.
type Backend interface {
	Ops
	// Txn runs fn in one read-write transaction, again if the database aborts it to break a
	// deadlock or a conflict; fn has no effects outside the transaction.
	Txn(ctx context.Context, fn func(tx Ops) error) error
	// Lead runs fn holding the lock named role ("leader", "observer") that one replica
	// holds at a time, with a context that ends if the lock is lost, giving it up when fn returns.
	// It returns lock.ErrNotAcquired when another replica holds it, and otherwise fn's error.
	Lead(ctx context.Context, role string, fn lock.LockFn) error
	// TryLockDeployment takes a deployment's lock, if no one holds it, until release; it is held
	// past ctx, refreshed until released or until this process dies.
	TryLockDeployment(ctx context.Context, id int64) (release func(), ok bool, err error)
}

// Ops are the database's operations. Outside a transaction each is its own; inside Txn they are
// the transaction's, and a Lock… read holds what it read against other writers until it ends.
type Ops interface {
	// ─── The feed ───

	// LegacyCursor is the last event of the SCM's feed applied before the deployer followed each
	// repository's log (0: none); ForgetLegacyCursor forgets it.
	LegacyCursor(ctx context.Context) (int64, error)
	ForgetLegacyCursor(ctx context.Context) error
	// FollowCursor is the last place in key's log on topic applied (0: none); LockFollowCursor is
	// FollowCursor, held; SetFollowCursor records it; SeedFollowCursor sets it if there is none.
	FollowCursor(ctx context.Context, topic string, key int64) (int64, error)
	LockFollowCursor(ctx context.Context, topic string, key int64) (int64, error)
	SetFollowCursor(ctx context.Context, topic string, key, seq int64) error
	SeedFollowCursor(ctx context.Context, topic string, key, seq int64) error
	// PutRepository records a repository by its ID, with name, whether or not it was recorded.
	PutRepository(ctx context.Context, id int64, name string) error
	// RenameRepository renames a recorded repository.
	RenameRepository(ctx context.Context, id int64, name string) error
	// EnsureRepository records a repository with no name, unless it is recorded.
	EnsureRepository(ctx context.Context, id int64) error
	// AdvanceHead moves a repository's head forward to head at seq: a seq at or before its head's
	// changes nothing.
	AdvanceHead(ctx context.Context, id int64, head []byte, seq int64) error
	// AddMainCommit records the commit at seq of a repository's main, once.
	AddMainCommit(ctx context.Context, repo, seq int64, commit []byte) error
	// MainCommit finds a commit of a repository's main: its seq, and whether a plan has reached
	// it; ok is false when main never had it.
	MainCommit(ctx context.Context, repo int64, commit []byte) (seq int64, planned, ok bool, err error)
	// Unplanned are the repositories whose head moved past their planned head, by ID.
	Unplanned(ctx context.Context) ([]Repository, error)

	// ─── Plans ───

	// LockPlannedSeq is a repository's planned seq, held.
	LockPlannedSeq(ctx context.Context, repo int64) (int64, error)
	// InsertPlan records a plan and returns its ID.
	InsertPlan(ctx context.Context, repo int64, head []byte, headSeq, baseSeq int64, notes []string) (int64, error)
	// SetPlanned moves a repository's planned head.
	SetPlanned(ctx context.Context, repo int64, head []byte, seq int64) error
	// Plans are a repository's plans (0: every repository's), oldest first.
	Plans(ctx context.Context, repo int64) ([]Plan, error)
	// RecordServices records the services a repository's main declares at seq: each deploy that
	// rolls a workload. Its services not among them are no longer declared.
	RecordServices(ctx context.Context, repo, seq int64, deploys []workflow.Deploy) error

	// ─── Deployments ───

	// InsertDeployment records a deployment, its images, and its migrations, and returns its ID.
	InsertDeployment(ctx context.Context, d DeploymentRow) (int64, error)
	// SetGates records the places in earlier plans a deployment took.
	SetGates(ctx context.Context, id int64, gates []Gate) error
	// CancelWaitingOf finishes, as canceled with reason, the deployments of a repository's
	// deploy that wait, returning each one's plan (0: none), wave, and gates.
	CancelWaitingOf(ctx context.Context, repo int64, deploy, reason string) ([]Replaced, error)
	// CancelWaitingIn finishes, as canceled with reason, the deployments that wait in a plan or
	// in a place in it they took, except one, returning their IDs.
	CancelWaitingIn(ctx context.Context, plan, except int64, reason string) ([]int64, error)
	// HaltRunningIn asks to halt, with reason, the running deployments not yet asked, except one,
	// that are in a plan or in a place in it at or after wave.
	HaltRunningIn(ctx context.Context, plan, except int64, wave int, reason string) error

	// Deployments reads deployments by ID, with their images and migrations, in no order.
	Deployments(ctx context.Context, ids []int64) ([]*Deployment, error)
	// LockDeployment reads a deployment, held; nil when there is none.
	LockDeployment(ctx context.Context, id int64) (*Deployment, error)
	// OpenDeployments are the waiting and running deployments, oldest first.
	OpenDeployments(ctx context.Context) ([]*Deployment, error)
	// RetiringDeployments are the self deployments that succeeded whose roll says the old copy is
	// still to retire (its JSON's "retire"), oldest first.
	RetiringDeployments(ctx context.Context) ([]*Deployment, error)
	// CoveringDeployments are a repository's deployments whose range covers seq, newest first.
	CoveringDeployments(ctx context.Context, repo, seq int64) ([]*Deployment, error)
	// LatestDeployments are a repository's newest deployments, newest first.
	LatestDeployments(ctx context.Context, repo int64, limit int) ([]*Deployment, error)
	// DeployDeployments are a repository's deploy's newest deployments, newest first.
	DeployDeployments(ctx context.Context, repo int64, deploy string, limit int) ([]*Deployment, error)
	// NewerOf is the newest deployment of a repository's deploy after id, if any.
	NewerOf(ctx context.Context, repo int64, deploy string, id int64) (int64, bool, error)
	// PlanStatuses are the statuses of a plan's deployments, by wave.
	PlanStatuses(ctx context.Context, plan int64) (map[int][]Status, error)

	// AppendEvent adds a line to a deployment's log.
	AppendEvent(ctx context.Context, id int64, message string) error
	// Events are a deployment's log, oldest first.
	Events(ctx context.Context, id int64) ([]Event, error)

	// StartDeployment moves a waiting deployment to running at its first stage, reporting
	// whether it was waiting.
	StartDeployment(ctx context.Context, id int64, driver string) (bool, error)
	// SetStage moves a running deployment to a stage.
	SetStage(ctx context.Context, id int64, stage Stage) error
	// SetRoll stores a deployment's roll state.
	SetRoll(ctx context.Context, id int64, roll []byte) error
	// SetPushRun records the run that pushes a deployment's container's image.
	SetPushRun(ctx context.Context, id int64, container string, run int64) error
	// SetPushed records the image a container's push pushed.
	SetPushed(ctx context.Context, id int64, container, reference string) error
	// SetMigration records a migration's outcome.
	SetMigration(ctx context.Context, id int64, position int, status MigrationStatus, output string, report []byte, refused []string) error
	// FinishDeployment ends a waiting or running deployment, reporting whether it was.
	FinishDeployment(ctx context.Context, id int64, status Status, reason string) (bool, error)
	// SetHaltBy asks a deployment to halt, unless it was asked already.
	SetHaltBy(ctx context.Context, id int64, by string) error
	// WaitingMigration is the migration of a deployment that waits for an allowance, if any.
	WaitingMigration(ctx context.Context, id int64) (position int, refused []string, ok bool, err error)
	// AllowMigration allows a migration's refused hazards, as by, setting it pending again.
	AllowMigration(ctx context.Context, id int64, position int, allowed []string, by string) error

	// ─── Services ───

	// Services are the services of a repository (0: every repository), by repository name,
	// name, and ID, each with its latest and its latest succeeded deployment's IDs.
	Services(ctx context.Context, repo int64) ([]ServiceRow, error)
	// Service reads a service; ok is false when there is none.
	Service(ctx context.Context, id int64) (ServiceRow, bool, error)
	// RecordObservation stores what the observer read of a service's workload.
	RecordObservation(ctx context.Context, service int64, o Observation) error
	// Observations are the latest observations of services, by service.
	Observations(ctx context.Context, ids []int64) (map[int64]*Observation, error)
}

// Plan is a plan the deployer made at a repository's head.
type Plan struct {
	ID         int64
	Repository int64
	Head       []byte
	HeadSeq    int64
	BaseSeq    int64
	Notes      []string
	CreatedAt  time.Time
}

// DeploymentRow is a deployment as it is first recorded.
type DeploymentRow struct {
	Repository  int64
	Plan        int64 // 0: none (a retry)
	RetryOf     int64
	Deploy      workflow.Deploy
	Commit      []byte
	FromSeq     int64
	ToSeq       int64
	Why         string
	Wave        int
	RequestedBy string
	Self        bool
	Status      Status
	Stage       Stage
	Reason      string
}

// Replaced is a waiting deployment a newer one replaced: its plan (0: none), its wave, and the
// places it had taken.
type Replaced struct {
	Plan  int64
	Wave  int
	Gates []Gate
}

// ServiceRow is a service, with its latest and its latest succeeded deployment's IDs (0: none).
type ServiceRow struct {
	Service
	LatestID, DeployedID int64
}
