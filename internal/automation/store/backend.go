package store

import (
	"context"
	"time"

	"github.com/Cidan/ok/internal/lock"
)

// Backend is automation's database: what each database it runs on implements (postgres), and
// the conformance suite in storetest checks. Its operations are the database's steps; the rules
// they make up — a run claimed by one worker and renewed, used, and ended only by it, a lost
// worker's run queued again once, a log taken once per byte, a patchset planned once, a
// submission with one active attempt, a post job unchanged since pre carrying pre's pass — are
// Store's, written once.
type Backend interface {
	Ops
	// Txn runs fn in one read-write transaction, again if the database aborts it to break a
	// deadlock or a conflict; fn has no effects outside the transaction. Notifications fn sends
	// are delivered when it commits.
	Txn(ctx context.Context, fn func(tx Ops) error) error
	// Snapshot runs fn in one read-only transaction that sees the database as of one moment.
	Snapshot(ctx context.Context, fn func(tx Ops) error) error
	// Lead runs fn holding the lock named role ("buildcache") that one replica
	// holds at a time, with a context that ends if the lock is lost, giving it up when fn returns.
	// It returns lock.ErrNotAcquired when another replica holds it, and otherwise fn's error.
	Lead(ctx context.Context, role string, fn lock.LockFn) error
	// Listen delivers every notification sent on channels, from any replica, to deliver, until
	// ctx ends or the connection fails (its error); ready is called each time it starts listening,
	// after which nothing sent is missed until it returns.
	Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error
}

// Ops are the database's operations. Outside a transaction each is its own; inside Txn they are
// the transaction's, and a Lock… read holds what it read against other writers until it ends.
type Ops interface {
	// Notify sends a notification on channel: at once outside a transaction, at commit inside.
	Notify(ctx context.Context, channel, payload string) error

	// ─── Runs ───

	// InsertRun records a run and returns it.
	InsertRun(ctx context.Context, r NewRun) (*Run, error)
	// InsertRuns records runs.
	InsertRuns(ctx context.Context, rs []NewRun) error
	// Run reads a run; ErrNotFound when none.
	Run(ctx context.Context, id int64) (*Run, error)
	// LockRun is Run, held.
	LockRun(ctx context.Context, id int64) (*Run, error)
	// RunsOfCL are a repository's CL's runs (patchset 0: every patchset's) below before, newest
	// first, at most limit.
	RunsOfCL(ctx context.Context, repo, cl int64, patchset int32, before int64, limit int) ([]*Run, error)
	// RunsOfCommit are a repository's commit's runs below before, newest first, at most limit.
	RunsOfCommit(ctx context.Context, repo int64, commit []byte, before int64, limit int) ([]*Run, error)
	// PlanRuns are every run of plans, by ID.
	PlanRuns(ctx context.Context, plans []int64) ([]*Run, error)
	// Pushed are what push runs pushed, by run, each in order.
	Pushed(ctx context.Context, runs []int64) (map[int64][]Pushed, error)
	// InsertPushed records what a push run pushed, in order.
	InsertPushed(ctx context.Context, run int64, pushed []Pushed) error

	// ClaimRun leases the queued run of the highest priority, the oldest of it, to worker for
	// ttl, starting it now; nil when none is queued. Two claims never take one run.
	ClaimRun(ctx context.Context, worker string, ttl time.Duration) (*Run, error)
	// RenewLease extends a worker's lease on a running run by ttl from now; ok is false when the
	// run is not running or not the worker's.
	RenewLease(ctx context.Context, id int64, worker string, ttl time.Duration) (l Lease, ok bool, err error)
	// LapsedRuns are the running runs whose leases have lapsed, held; a run another sweep holds
	// is left to it.
	LapsedRuns(ctx context.Context) ([]Lapsed, error)
	// RequeueRun queues a running run again with reason: its worker, lease, start, stop request,
	// and log cleared, the log's bytes deleted — and its attempt counted when nextAttempt.
	RequeueRun(ctx context.Context, id int64, reason string, nextAttempt bool) error
	// FinishRun ends a run with status, exit code, and reason, now, its lease and stop request
	// cleared.
	FinishRun(ctx context.Context, id int64, status Status, exitCode int32, reason string) error
	// CancelQueuedRun ends a queued run canceled with reason, now, its (empty) log stored,
	// reporting whether it was queued.
	CancelQueuedRun(ctx context.Context, id int64, reason string) (bool, error)
	// RequestStop asks a running run's worker to stop it, unless it was asked already.
	RequestStop(ctx context.Context, id int64, reason string) error
	// StopRequest is whether a run's worker is asked to stop it, and why.
	StopRequest(ctx context.Context, id int64) (stopping bool, reason string, err error)

	// ─── Logs ───

	// AppendLogChunk adds a run's log's bytes at offset at, which is its size, and makes its size
	// at plus their length.
	AppendLogChunk(ctx context.Context, id, at int64, data []byte) error
	// LogChunks are a run's log's chunks that reach past from, in order.
	LogChunks(ctx context.Context, id, from int64) ([]LogChunk, error)
	// MarkLogStored records a run's log stored in the bucket and deletes its chunks.
	MarkLogStored(ctx context.Context, id int64) error
	// UnstoredLogs are up to limit finished runs whose logs are not stored, by ID.
	UnstoredLogs(ctx context.Context, limit int) ([]int64, error)

	// ─── The feed, and the build cache's commits ───

	// LegacyCursor is the last event of the SCM's feed applied before automation followed each
	// repository's log (0: none); ForgetLegacyCursor forgets it.
	LegacyCursor(ctx context.Context) (int64, error)
	ForgetLegacyCursor(ctx context.Context) error
	// FollowCursor is the last place in key's log on topic applied (0: none); AdvanceFollowCursor
	// moves it to seq, never back; SeedFollowCursor sets it if there is none.
	FollowCursor(ctx context.Context, topic string, key int64) (int64, error)
	AdvanceFollowCursor(ctx context.Context, topic string, key, seq int64) error
	SeedFollowCursor(ctx context.Context, topic string, key, seq int64) error
	// MainCommits are each repository's newest n commits on main, newest first.
	MainCommits(ctx context.Context, n int) (map[int64][][]byte, error)
	// RunCommits are the commits of every run queued or running, or finished since since, by
	// repository.
	RunCommits(ctx context.Context, since time.Time) (map[int64][][]byte, error)

	// ─── Plans ───

	// LockCL, in a transaction, serializes it with every other that plans or cancels a CL's
	// runs until it ends.
	LockCL(ctx context.Context, repo, cl int64) error
	// InsertPlan records a plan and returns it.
	InsertPlan(ctx context.Context, p NewPlan) (*Plan, error)
	// Plan reads a plan; ErrNotFound when none.
	Plan(ctx context.Context, id int64) (*Plan, error)
	// LockPlan is Plan, held.
	LockPlan(ctx context.Context, id int64) (*Plan, error)
	// Plans reads plans, by ID.
	Plans(ctx context.Context, ids []int64) ([]*Plan, error)
	// PrePlan is a CL's patchset's pre plan; ErrNotFound when none.
	PrePlan(ctx context.Context, repo, cl int64, patchset int32) (*Plan, error)
	// MaxPrePatchset is the newest patchset of a CL with a pre plan (0: none).
	MaxPrePatchset(ctx context.Context, repo, cl int64) (int32, error)
	// SetPlanned records what a plan's planning run found.
	SetPlanned(ctx context.Context, id int64, r PlanResult) error
	// CancelQueuedPre ends canceled, with reason, the queued pre runs of a CL's patchsets below
	// below, returning their IDs.
	CancelQueuedPre(ctx context.Context, repo, cl int64, below int32, reason string) ([]int64, error)
	// StopRunningPre asks the workers of the running pre runs of a CL's patchsets below below to
	// stop them, unless they were asked already.
	StopRunningPre(ctx context.Context, repo, cl int64, below int32, reason string) error
	// ClearPreStops withdraws the stop requests of a patchset's running pre runs.
	ClearPreStops(ctx context.Context, repo, cl int64, patchset int32) error

	// ─── Submissions ───

	// ActiveSubmissionOf finds an active submission of a repository holding any of cls: the CL
	// and the submission, the lowest submission's; ok is false when none.
	ActiveSubmissionOf(ctx context.Context, repo int64, cls []int64) (cl, submission int64, ok bool, err error)
	// InsertSubmission records an active submission of cls, in order, and returns its ID.
	InsertSubmission(ctx context.Context, repo int64, by string, cls []SubmittedCL) (int64, error)
	// Submissions reads submissions with their CLs, by ID.
	Submissions(ctx context.Context, ids []int64) ([]*Submission, error)
	// ActiveSubmissions are a repository's active submissions, by ID.
	ActiveSubmissions(ctx context.Context, repo int64) ([]*Submission, error)
	// SubmissionsOfCL are the submissions holding a repository's CL, newest first, at most limit.
	SubmissionsOfCL(ctx context.Context, repo, cl int64, limit int) ([]*Submission, error)
	// LockSubmission reads a submission, held; ErrNotFound when none.
	LockSubmission(ctx context.Context, id int64) (*Submission, error)
	// FinishSubmission ends an active submission with status and reason, now, its note cleared,
	// returning its repository; ok is false when it was not active.
	FinishSubmission(ctx context.Context, id int64, status SubmissionStatus, reason string) (repo int64, ok bool, err error)
	// ActiveSubmissionsOfCL are the active submissions holding a repository's CL at a patchset
	// below below (0: any), by ID.
	ActiveSubmissionsOfCL(ctx context.Context, repo, cl int64, below int32) ([]int64, error)
	// SetNote records an active submission's note.
	SetNote(ctx context.Context, id int64, note string) error

	// Attempts are the attempts of submissions — those with status, or every one for 0 — by ID,
	// with their candidates in order, without plans or runs.
	Attempts(ctx context.Context, submissions []int64, status AttemptStatus) ([]*Attempt, error)
	// ActiveAttempt is a submission's active attempt; ok is false when none.
	ActiveAttempt(ctx context.Context, submission int64) (id int64, ok bool, err error)
	// LockAttempt reads an attempt, held, with its candidates in order (without plans or runs);
	// ErrNotFound when none.
	LockAttempt(ctx context.Context, id int64) (*Attempt, error)
	// InsertAttempt records an active attempt and returns its ID.
	InsertAttempt(ctx context.Context, a NewAttemptRow) (int64, error)
	// InsertCandidate records an attempt's candidate at position.
	InsertCandidate(ctx context.Context, attempt int64, position int, repo int64, c AttemptCandidate) error
	// SetReleased records an attempt's plans' jobs queued.
	SetReleased(ctx context.Context, id int64) error
	// SetAttemptStatus moves an attempt to status with reason, now — from from only, unless from
	// is 0 — reporting whether it moved.
	SetAttemptStatus(ctx context.Context, id int64, from, to AttemptStatus, reason string) (bool, error)
	// EndActiveAttempts moves a submission's active attempts to status with reason, now,
	// returning their IDs.
	EndActiveAttempts(ctx context.Context, submission int64, status AttemptStatus, reason string) ([]int64, error)
	// UnfinishedRunsOfAttempts are the queued and running runs of attempts' plans, by ID.
	UnfinishedRunsOfAttempts(ctx context.Context, attempts []int64) ([]int64, error)
	// ReusableCandidate is a candidate an active attempt prepared from a CL's patchset on base,
	// the newest attempt's.
	ReusableCandidate(ctx context.Context, repo int64, base []byte, cl int64, patchset int32) ([]byte, bool, error)
	// CandidatesInUse are the candidates a repository's active attempts hold, by commit ID.
	CandidatesInUse(ctx context.Context, repo int64) (map[string]bool, error)
	// MarkWork, TakeWork, RenewWork, ReleaseWork, and DueWork are automation's keyed work's
	// (internal/work's Store).
	MarkWork(ctx context.Context, kind string, key int64) error
	TakeWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	RenewWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	ReleaseWork(ctx context.Context, kind string, key int64, holder string) (bool, error)
	DueWork(ctx context.Context, kind string, limit int) ([]int64, error)
	// QueueRepositories are the repositories with active submissions, or a submission finished
	// within recent by the database's clock, by ID.
	QueueRepositories(ctx context.Context, recent time.Duration) ([]int64, error)

	// SetLandedCommit records the commit a submission's CL landed as.
	SetLandedCommit(ctx context.Context, submission, cl int64, commit []byte) error
	// InsertLandedChange records the CL a landed commit is, once.
	InsertLandedChange(ctx context.Context, repo int64, commit []byte, cl int64) error
	// InsertMainCommit records the commit at seq of a repository's main, once.
	InsertMainCommit(ctx context.Context, repo, seq int64, commit []byte) error
	// PutMainCommit records the commit at seq of a repository's main and the submission that
	// landed it, over what was recorded.
	PutMainCommit(ctx context.Context, repo, seq int64, commit []byte, submission int64) error
	// SetMainCommitSubmission records the submission that landed a commit of main.
	SetMainCommitSubmission(ctx context.Context, repo int64, commit []byte, submission int64) error
	// MainCommitsAfter is a repository's main after from, with each commit's CL and submission.
	MainCommitsAfter(ctx context.Context, repo, from int64) ([]MainCommit, error)
	// LandedCLs are the commits a repository's CLs landed as, by CL.
	LandedCLs(ctx context.Context, repo int64, cls []int64) (map[int64][]byte, error)
}

// NewRun is a run as it is first recorded.
type NewRun struct {
	RepositoryID   int64
	CommitID       []byte
	Job            Job
	Origin         Origin
	PlanID         int64
	Why            string
	RequestedBy    string
	RerunOf        int64
	Status         Status
	Reason         string
	TimeoutSeconds int32
	Priority       int16
	// Started and Finished set its start and end now; LogStored that its log is stored.
	Started, Finished bool
	LogStored         bool
	CarriedFrom       int64
}

// Lease is a renewed lease: whether the run's worker is asked to stop it and why, and whether
// it has run past its timeout.
type Lease struct {
	Stopping       bool
	StopReason     string
	Overdue        bool
	TimeoutSeconds int32
}

// Lapsed is a running run whose lease lapsed.
type Lapsed struct {
	ID         int64
	Attempt    int32
	Worker     string
	Stopping   bool
	StopReason string
}

// LogChunk is a part of a run's log: its bytes at their offset.
type LogChunk struct {
	At   int64
	Data []byte
}

// NewPlan is a plan as it is first recorded.
type NewPlan struct {
	RepositoryID    int64
	CLNumber        int64
	Patchset        int32
	Phase           Phase
	CommitID        []byte
	WorkflowsCommit []byte
	Context         []byte
	Touched         []string
	PreCommit       []byte
}

// NewAttemptRow is an attempt as it is first recorded.
type NewAttemptRow struct {
	SubmissionID int64
	Base         []byte
	BaseSeq      int64
	Chain        []int64
	PrepareError string
}
