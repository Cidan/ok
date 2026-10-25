package store

import (
	"context"
	"time"

	"github.com/Cidan/ok/internal/move"
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
	// Moves are its rows about repositories, for a move between cells.
	Moves() move.Rows
	// FencedWritesBesideEachOther reports whether transactions that rest on one keyed-work lease
	// and mark its work row may run beside each other. On Spanner they may: the fence reads the
	// holder's cell, which a mark does not write. On PostgreSQL they may not: the fence holds the
	// work row in share mode, which a mark of it then upgrades, so beside each other they deadlock
	// on it and hold off the lease's renewal.
	FencedWritesBesideEachOther() bool
}

// Ops are the database's operations. Outside a transaction each is its own; inside Txn they are
// the transaction's, and a Lock… read holds what it read against other writers until it ends.
type Ops interface {
	// Gate reads a repository's move gate in the operation's transaction, refusing it while the
	// repository's network moves between cells (move.Frozen) or once it moved (cells.Moved): what a
	// transaction about a repository it learns from a row it locks runs first
	// (docs/hosted.md, "Cells, as built").
	Gate(ctx context.Context, repo int64) error
	// RunningRuns are the runs of repositories that workers hold, by ID.
	RunningRuns(ctx context.Context, repos []int64) ([]int64, error)

	// ─── Purging ───

	// RunIDsOf are a repository's runs' IDs after after, up to limit, in order.
	RunIDsOf(ctx context.Context, repo, after int64, limit int) ([]int64, error)
	// DeleteRepository deletes every row of a repository: its main's commits and landed changes,
	// its submissions (their CLs, attempts, and candidates), its runs (their images and log
	// chunks), and its plans.
	DeleteRepository(ctx context.Context, repo int64) error

	// ─── Runs ───

	// InsertRun records a run and returns it.
	InsertRun(ctx context.Context, r NewRun) (*Run, error)
	// InsertRuns records runs.
	InsertRuns(ctx context.Context, rs []NewRun) error
	// Run reads a run; ErrNotFound when none.
	Run(ctx context.Context, id int64) (*Run, error)
	// LockRun is Run, held.
	LockRun(ctx context.Context, id int64) (*Run, error)
	// RunsOfCL are a repository's CL's runs (patchset 0: every patchset's) queued before the run
	// before (0: the newest), newest first, at most limit.
	RunsOfCL(ctx context.Context, repo, cl int64, patchset int32, before int64, limit int) ([]*Run, error)
	// RunsOfCommit are a repository's commit's runs queued before the run before (0: the newest),
	// newest first, at most limit.
	RunsOfCommit(ctx context.Context, repo int64, commit []byte, before int64, limit int) ([]*Run, error)
	// PlanRuns are every run of plans, in the order they were queued.
	PlanRuns(ctx context.Context, plans []int64) ([]*Run, error)
	// Pushed are what push runs pushed, by run, each in order.
	Pushed(ctx context.Context, runs []int64) (map[int64][]Pushed, error)
	// InsertPushed records what a push run pushed, in order.
	InsertPushed(ctx context.Context, run int64, pushed []Pushed) error

	// Admission and fair share (docs/hosted.md, "Hosted automation, as built"): a run's account is
	// its repository's owner, "" for a repository the directory's events have not named.
	//
	// QueuedHeads are, for each account with runs queued in a pool — of repositories no move holds
	// — and each priority among them, the oldest one's queue time.
	QueuedHeads(ctx context.Context, pool string) ([]Head, error)
	// RunningByAccount are each account's running runs: in a pool, and in every pool.
	RunningByAccount(ctx context.Context, pool string) (map[string]Running, error)
	// MonthSeconds are accounts' run seconds in a month (YYYY-MM, UTC).
	MonthSeconds(ctx context.Context, month string, accounts []string) (map[string]int64, error)
	// ClaimCandidates are an account's next queued runs in a pool — its highest priority's oldest
	// first — of repositories no move holds, at most limit: read outside any transaction, so a
	// claim locks only the run it takes (ClaimRun).
	ClaimCandidates(ctx context.Context, account, pool string, limit int) ([]Queued, error)
	// ClaimRun leases a queued run of a pool to worker for ttl, holding a slot of its account's
	// (slot < 0: none), the lease's key's hash recorded; nil when it is no longer queued there.
	ClaimRun(ctx context.Context, id int64, pool, worker string, ttl time.Duration, account string, slot int64, keyHash []byte) (*Run, error)
	// LeaseKeyHolds reports whether a run is running under the lease whose key hashes to keyHash.
	LeaseKeyHolds(ctx context.Context, id int64, keyHash []byte) (bool, error)
	// Slots are an account's slots that running runs hold: an account with a concurrent quota
	// runs one run in each of its slots, at most its quota of them (docs/hosted.md, "Hosted
	// automation, as built"). TakeSlot holds one for a run, ErrSlotTaken when another run holds
	// it; a run's slot is let go as it stops running (RequeueRun, FinishRun) or is deleted.
	Slots(ctx context.Context, account string) ([]int64, error)
	TakeSlot(ctx context.Context, account string, slot, run int64) error
	// SetOwner records a repository's account, as the directory's events name it; DropOwner
	// forgets it.
	SetOwner(ctx context.Context, repo int64, account string) error
	DropOwner(ctx context.Context, repo int64) error
	// OwnerOf is a repository's account; "" for one the directory's events have not named.
	OwnerOf(ctx context.Context, repo int64) (string, error)
	// MonthUsage is each account's month that ran or runs: its seconds, and its running runs.
	MonthUsage(ctx context.Context, month string) ([]Usage, error)
	// QueuedByAccount are each account's queued runs in a pool, of repositories no move holds, and
	// when the oldest of them all was queued (zero for none).
	QueuedByAccount(ctx context.Context, pool string) (map[string]int64, time.Time, error)
	// RunningOnWorkers are the runs each worker holds in a pool, by worker name.
	RunningOnWorkers(ctx context.Context, pool string) (map[string]int64, error)
	// RenewLease extends a worker's lease on a running run by ttl from now; ok is false when the
	// run is not running or not the worker's.
	RenewLease(ctx context.Context, id int64, worker string, ttl time.Duration) (l Lease, ok bool, err error)
	// LapsedRunIDs are the running runs whose leases have lapsed, of repositories no move holds:
	// read outside any transaction. LapsedRun holds one of them in the transaction, if it is still
	// lapsed and its repository's network still free.
	LapsedRunIDs(ctx context.Context) ([]int64, error)
	LapsedRun(ctx context.Context, id int64) (Lapsed, bool, error)
	// RequeueRun queues a running run again with reason: its worker, lease, start, stop request,
	// and log cleared, the log's bytes deleted — and its attempt counted when nextAttempt.
	// FinishRun ends a run with status, exit code, and reason, now, its lease and stop request
	// cleared. Each charges a run that was running its seconds since it started, by the database's
	// clock, to its account's month (MonthSeconds).
	RequeueRun(ctx context.Context, id int64, reason string, nextAttempt bool) error
	FinishRun(ctx context.Context, id int64, status Status, exitCode int32, reason string) error
	// CancelQueuedRun ends a queued run canceled with reason, now, by the principal by (a stable
	// ID; "" for no one), its (empty) log stored, reporting whether it was queued.
	CancelQueuedRun(ctx context.Context, id int64, reason, by string) (bool, error)
	// RequestStop asks a running run's worker to stop it, unless it was asked already, recording
	// who asked (by: a stable ID, or "").
	RequestStop(ctx context.Context, id int64, reason, by string) error
	// StopRequest is whether a run's worker is asked to stop it, and why.
	StopRequest(ctx context.Context, id int64) (stopping bool, reason string, err error)

	// ─── Logs ───

	// AdvanceLog moves a run's log size from from to to, if the run is running, leased to worker
	// since started, and its log is from long; it reports whether it did.
	AdvanceLog(ctx context.Context, id int64, worker string, started time.Time, from, to int64) (bool, error)
	// MarkLogStored records a run's log stored in the bucket.
	MarkLogStored(ctx context.Context, id int64) error
	// UnstoredLogs are up to limit finished runs whose logs are not stored, by ID.
	UnstoredLogs(ctx context.Context, limit int) ([]int64, error)

	// ─── The feed, and the build cache's commits ───

	// FollowCursor is the last place in key's log on topic applied (0: none); AdvanceFollowCursor
	// moves it to seq, never back.
	FollowCursor(ctx context.Context, topic string, key int64) (int64, error)
	// FollowCursors are FollowCursor's of keys, at once; a key with none is absent.
	FollowCursors(ctx context.Context, topic string, keys []int64) (map[int64]int64, error)
	AdvanceFollowCursor(ctx context.Context, topic string, key, seq int64) error
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
	// FinishSubmission ends an active submission with status and reason, now, by the principal by
	// (a stable ID; "" for no one), its note cleared, returning its repository; ok is false when it
	// was not active.
	FinishSubmission(ctx context.Context, id int64, status SubmissionStatus, reason, by string) (repo int64, ok bool, err error)
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
	// MarkWork, TakeWork, RenewWork, ReleaseWork, LetWork, and DueWork are automation's keyed work's
	// (internal/work's Store).
	MarkWork(ctx context.Context, kind string, key int64) error
	MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error
	TakeWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	RenewWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	ReleaseWork(ctx context.Context, kind string, key int64, holder string) (bool, error)
	LetWork(ctx context.Context, kind string, key int64, holder string) error
	DueWork(ctx context.Context, kind string, limit int) ([]int64, error)

	// SetLandedCommit records the commit a submission's CL landed as.
	SetLandedCommit(ctx context.Context, submission, cl int64, commit []byte) error
	// InsertLandedChange records the CL a landed commit is, once.
	InsertLandedChange(ctx context.Context, repo int64, commit []byte, cl int64) error
	// LockMain, in a transaction, serializes it with every other that records a repository's main
	// — the SCM's event of a move, the queue's landing — until it ends: each finds what the other
	// wrote only once it has committed.
	LockMain(ctx context.Context, repo int64) error
	// InsertMainCommit records the commit at seq of a repository's main, once, with the submission
	// whose CL landed as it, if one did.
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
	// Started and Finished set its start and end now; LogStored that its log is stored, and
	// LogSize how long it is (a carried run's, which the store makes).
	Started, Finished bool
	LogStored         bool
	LogSize           int64
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
// Head is an account's queued runs in a pool at a priority: the oldest one's queue time.
type Head struct {
	Account  string
	Priority int16
	Oldest   time.Time
}

// Running are an account's running runs: in a pool, and in every pool.
type Running struct{ InPool, Total int64 }

// Usage is an account's month: its run seconds, and its running runs.
type Usage struct {
	Account string
	Seconds int64
	Running int64
}

type Lapsed struct {
	ID           int64
	RepositoryID int64
	Phase        Phase
	Attempt      int32
	Worker       string
	Stopping     bool
	StopReason   string
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
