// Package backend is the SCM's database (internal/scm/store): the interface each database it runs
// on implements (internal/scm/store/postgres), and the rows its operations carry. The rules the
// operations make up — verification, closure, freshness and pinning walks, landing, the content
// index's log, collection — are the store's, written once; a backend holds rows.
//
// It is a package of its own, apart from the store, so that the store's tests, which reach into
// its internals, can open a backend without an import cycle.
package backend

import (
	"context"
	"errors"
	"time"

	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/scm/encoding"
)

// ErrDuplicate is a write a uniqueness constraint refused: a repository's name, a CL's number.
var ErrDuplicate = errors.New("backend: duplicate")

// Backend is the SCM's database.
type Backend interface {
	Ops
	// Txn runs fn in one read-write transaction, once: the store runs it again when Retryable
	// says so. Notifications fn sends are delivered when it commits.
	Txn(ctx context.Context, fn func(tx Ops) error) error
	// Retryable reports a transaction the database aborted to break a deadlock or a conflict,
	// which running again may commit.
	Retryable(err error) bool
	// Snapshot runs fn in one read-only transaction that sees the database as of one moment.
	Snapshot(ctx context.Context, fn func(tx Ops) error) error
	// Lead runs fn holding the lock named role ("gc") that one replica holds at a time, with a
	// context that ends if the lock is lost, giving it up when fn returns. It returns
	// lock.ErrNotAcquired when another replica holds it, and otherwise fn's error.
	Lead(ctx context.Context, role string, fn lock.LockFn) error
	// Listen delivers every notification sent on channels, from any replica, to deliver, until
	// ctx ends or the connection fails (its error); ready is called each time it starts listening,
	// after which nothing sent is missed until it returns.
	Listen(ctx context.Context, channels []string, ready func(), deliver func(channel, payload string)) error
	// Ping reports whether the database answers.
	Ping(ctx context.Context) error
	// RelayEvents hands publish up to limit events no relay has published, by repository and
	// place in its log, then forgets them — unless publish fails, when they stay to be published
	// again. Relays beside it take other events. It returns how many it published.
	RelayEvents(ctx context.Context, limit int, publish func([]Event) error) (int, error)
}

// Walk is what a freshness walk does to each row it reaches (the store's touch, refresh, and
// pin).
type Walk int

const (
	// TouchWalk sets an unpinned row's day to today, if it is older.
	TouchWalk Walk = iota
	// RefreshWalk sets every unpinned row's day to today.
	RefreshWalk
	// PinWalk pins an unpinned row.
	PinWalk
)

// Ops are the database's operations. Outside a transaction each is its own; inside Txn they are
// the transaction's, and a Lock… read holds what it read against other writers until it ends.
// "Today" is the database's current day.
type Ops interface {
	// Notify sends a notification on channel: at once outside a transaction, at commit inside.
	Notify(ctx context.Context, channel, payload string) error

	// ─── Repositories ───

	// InsertRepository records a repository whose main is head; ErrDuplicate for a name taken.
	InsertRepository(ctx context.Context, name string, head encoding.ID, createdBy string) (Repository, error)
	// LockRepositoryName reads a repository's name, held; ok is false when there is none.
	LockRepositoryName(ctx context.Context, id int64) (name string, ok bool, err error)
	// RenameRepository renames a repository; ErrDuplicate for a name taken.
	RenameRepository(ctx context.Context, id int64, name string) (Repository, error)
	// Repository and RepositoryByName read a repository; ok is false when there is none.
	Repository(ctx context.Context, id int64) (Repository, bool, error)
	RepositoryByName(ctx context.Context, name string) (Repository, bool, error)
	// Repositories are up to limit repositories named after after, by name.
	Repositories(ctx context.Context, after string, limit int) ([]Repository, error)
	// RepositoryExists reports whether a repository is recorded.
	RepositoryExists(ctx context.Context, id int64) (bool, error)
	// Head is a repository's main and its place in main's history — held, with lock; ok is false
	// when there is no repository.
	Head(ctx context.Context, id int64, lock bool) (head encoding.ID, seq int64, ok bool, err error)
	// SetHead moves a repository's main.
	SetHead(ctx context.Context, id int64, head encoding.ID, seq int64) error

	// ─── Events ───

	// ReserveRepositoryEvents takes a repository's next n places in its log, holding its counter
	// until the transaction ends, and returns the last.
	ReserveRepositoryEvents(ctx context.Context, repo int64, n int) (int64, error)
	// InsertEvents records a repository's events, encoded, at their places in its log, each for
	// the relay to publish.
	InsertEvents(ctx context.Context, repo int64, at time.Time, seqs []int64, bodies [][]byte) error
	// RepositoryEvents are up to limit of a repository's events after place after in its log, in
	// order.
	RepositoryEvents(ctx context.Context, repo, after int64, limit int) ([]Event, error)
	// LogHeads are, for up to limit repositories with IDs above after, by ID, the last place in
	// each one's log — now, or (asOf > 0) as of the old feed's sequence number asOf; a repository
	// with no event by then is left out.
	LogHeads(ctx context.Context, asOf, after int64, limit int) ([][2]int64, error)

	// ─── The content index ───

	// InsertContentIndex records a repository's content index at generation gen.
	InsertContentIndex(ctx context.Context, repo, gen int64) error
	// ShareContentIndex holds a repository's content index in share mode — writers that commute
	// with each other run beside it; a sweep or a compaction does not — and returns today. ok is
	// false when the repository has no content index.
	ShareContentIndex(ctx context.Context, repo int64) (today time.Time, ok bool, err error)
	// LockContentIndex holds a repository's content index alone and returns its generation.
	LockContentIndex(ctx context.Context, repo int64) (gen int64, err error)
	// ContentIndexGeneration is a repository's content index's generation; ok is false when it
	// has none.
	ContentIndexGeneration(ctx context.Context, repo int64) (gen int64, ok bool, err error)
	// SetContentIndexGeneration moves a repository's content index to generation gen.
	SetContentIndexGeneration(ctx context.Context, repo, gen int64) error
	// IndexBlocks are a generation's blocks' bodies.
	IndexBlocks(ctx context.Context, repo, gen int64) ([][]byte, error)
	// InsertIndexBlocks records a generation's blocks, each by its first entry's ID.
	InsertIndexBlocks(ctx context.Context, repo, gen int64, firsts []encoding.ID, bodies [][]byte) error
	// DeleteIndexBlocks deletes a generation's blocks.
	DeleteIndexBlocks(ctx context.Context, repo, gen int64) error
	// ContentLogIDs are a repository's content log rows' IDs.
	ContentLogIDs(ctx context.Context, repo int64) ([]int64, error)
	// ContentLogs are a repository's content log rows among ids, by ID.
	ContentLogs(ctx context.Context, repo int64, ids []int64) ([]ContentLog, error)
	// InsertContentLog records a content log row and returns its ID.
	InsertContentLog(ctx context.Context, repo int64, body []byte) (int64, error)
	// DeleteContentLogs deletes a repository's content log rows among ids.
	DeleteContentLogs(ctx context.Context, repo int64, ids []int64) error

	// ─── Packs ───

	// PutPack records a pack a publish wrote — again, its size updated, if it is recorded — and
	// returns its number.
	PutPack(ctx context.Context, repo int64, id encoding.ID, size int64) (uint64, error)
	// InsertPack records a pack compaction wrote, with the dictionary it considered (nil: none),
	// and returns its number.
	InsertPack(ctx context.Context, repo int64, id encoding.ID, size int64, dictionary *int64) (uint64, error)
	// PackNumbers are a repository's packs' IDs, retired ones too, by number.
	PackNumbers(ctx context.Context, repo int64) (map[uint64]encoding.ID, error)
	// LivePacks are a repository's packs not retired, oldest first.
	LivePacks(ctx context.Context, repo int64) ([]Pack, error)
	// RetiredPackIDs are a repository's retired packs' IDs.
	RetiredPackIDs(ctx context.Context, repo int64) ([]encoding.ID, error)
	// PackIDs are every one of a repository's packs' IDs.
	PackIDs(ctx context.Context, repo int64) ([]encoding.ID, error)
	// SetPacksDictionary records the dictionary that considered packs.
	SetPacksDictionary(ctx context.Context, repo int64, ids []encoding.ID, dictionary int64) error
	// RetirePack retires a pack, now.
	RetirePack(ctx context.Context, repo int64, id encoding.ID) error
	// LivePackNumbers are the numbers of the packs among ids not retired, by ID.
	LivePackNumbers(ctx context.Context, repo int64, ids []encoding.ID) (map[encoding.ID]uint64, error)
	// RetirePacks retires the packs among ids not retired, now — or, dry, only counts them — and
	// returns how many and their total size.
	RetirePacks(ctx context.Context, repo int64, ids []encoding.ID, dry bool) (int, int64, error)
	// RetiredPacksBefore are the packs retired before t, of one repository or (repo nil) every
	// one, by repository and ID.
	RetiredPacksBefore(ctx context.Context, t time.Time, repo *int64) ([]RetiredPack, error)
	// DeleteRetiredPack deletes a retired pack's row, reporting whether it did.
	DeleteRetiredPack(ctx context.Context, repo int64, id encoding.ID) (bool, error)

	// ─── Objects and commits ───

	// InsertObjects records trees, symlinks, and copy histories, in ID order, skipping those
	// recorded, and returns the IDs it recorded.
	InsertObjects(ctx context.Context, repo int64, objects []Object) ([]encoding.ID, error)
	// InsertCopyParents records copy histories' parent edges, once each.
	InsertCopyParents(ctx context.Context, repo int64, copies, parents []encoding.ID) error
	// InsertCommits records commits, in ID order, written by writtenBy, skipping those recorded,
	// and returns the IDs it recorded.
	InsertCommits(ctx context.Context, repo int64, commits []Commit, writtenBy string) ([]encoding.ID, error)
	// InsertCommitParents records commits' parent edges.
	InsertCommitParents(ctx context.Context, repo int64, edges []ParentEdge) error
	// ExistingObjects are which of ids the repository holds as objects of kind.
	ExistingObjects(ctx context.Context, repo int64, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID]bool, error)
	// ExistingCommits are which of ids the repository holds as commits.
	ExistingCommits(ctx context.Context, repo int64, ids []encoding.ID) (map[encoding.ID]bool, error)
	// ObjectBodies are the bodies of the objects of kind among ids, by ID.
	ObjectBodies(ctx context.Context, repo int64, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID][]byte, error)
	// CommitBodies are the bodies of the commits among ids, by ID.
	CommitBodies(ctx context.Context, repo int64, ids []encoding.ID) (map[encoding.ID][]byte, error)
	// CommitGenerations are the generations of the commits among ids, by ID.
	CommitGenerations(ctx context.Context, repo int64, ids []encoding.ID) (map[encoding.ID]int64, error)
	// WalkObjects applies walk to the objects of kind among ids it applies to — unpinned and
	// older than today for a touch, unpinned for a refresh or a pin — and returns their bodies.
	WalkObjects(ctx context.Context, repo int64, kind encoding.Kind, ids []encoding.ID, walk Walk) (map[encoding.ID][]byte, error)
	// WalkCommits is WalkObjects for commits.
	WalkCommits(ctx context.Context, repo int64, ids []encoding.ID, walk Walk) (map[encoding.ID][]byte, error)
	// StaleObjectIDs and StaleCommitIDs are the unpinned objects or commits not touched since
	// cutoff, by ID.
	StaleObjectIDs(ctx context.Context, repo int64, cutoff time.Time) ([]encoding.ID, error)
	StaleCommitIDs(ctx context.Context, repo int64, cutoff time.Time) ([]encoding.ID, error)
	// StaleObjects are the objects among ids unpinned and not touched since cutoff.
	StaleObjects(ctx context.Context, repo int64, ids []encoding.ID, cutoff time.Time) ([]Object, error)
	// StaleAncestry is the unpinned commits not touched since cutoff among roots, and their
	// ancestors through such commits, with their bodies.
	StaleAncestry(ctx context.Context, repo int64, roots []encoding.ID, cutoff time.Time) (map[encoding.ID][]byte, error)
	// SweepObjects and SweepCommits delete the objects or commits among ids still unpinned and
	// not touched since cutoff — or, dry, count them — and return how many.
	SweepObjects(ctx context.Context, repo int64, ids []encoding.ID, cutoff time.Time, dry bool) (int64, error)
	SweepCommits(ctx context.Context, repo int64, ids []encoding.ID, cutoff time.Time, dry bool) (int64, error)
	// CopyRelatives are the copy histories related to id — its ancestors, and everything
	// descending from one of them — with their bodies.
	CopyRelatives(ctx context.Context, repo int64, id encoding.ID) (map[encoding.ID][]byte, error)
	// Ancestry is the parent edges of from and of every ancestor down to generation minGen: for
	// each commit reached, its parents in order and their generations.
	Ancestry(ctx context.Context, repo int64, from []encoding.ID, minGen int64) ([]Ancestor, error)
	// AllObjects, AllCopyParents, AllCommits, and AllCommitParents are a repository's every row of
	// each, for fsck: objects by ID, commit parents by commit and order.
	AllObjects(ctx context.Context, repo int64) ([]Object, error)
	AllCopyParents(ctx context.Context, repo int64) ([][2]encoding.ID, error)
	AllCommits(ctx context.Context, repo int64) ([]StoredCommit, error)
	AllCommitParents(ctx context.Context, repo int64) ([][2]encoding.ID, error)
	// TreeSizes are the sizes records kept of trees among ids, by tree.
	TreeSizes(ctx context.Context, repo int64, ids []encoding.ID) (map[encoding.ID][]byte, error)
	// InsertTreeSizes keeps trees' sizes records, once each.
	InsertTreeSizes(ctx context.Context, repo int64, ids []encoding.ID, bodies [][]byte) error

	// ─── Main's history ───

	// InsertHeadLog records one move of main.
	InsertHeadLog(ctx context.Context, repo int64, h HeadMove) error
	// MainLog is up to limit of main's heads at seqs from to to, each with its seq as its
	// generation, newest first unless ascending.
	MainLog(ctx context.Context, repo, from, to int64, ascending bool, limit int) ([]GenCommit, error)
	// HeadSeqOf is the place in main's history where main moved to commit; ok is false when it
	// never did.
	HeadSeqOf(ctx context.Context, repo int64, commit encoding.ID) (seq int64, ok bool, err error)
	// InsertCommitPaths records the paths a commit on main at generation gen changed, once each.
	InsertCommitPaths(ctx context.Context, repo, gen int64, commit encoding.ID, paths []string) error
	// PathCommits are up to limit of the commits of main that changed any of paths, at
	// generations from to to, once each, newest first unless ascending.
	PathCommits(ctx context.Context, repo int64, paths []string, from, to int64, ascending bool, limit int) ([]GenCommit, error)
	// LastPathCommits are, for each of paths, the newest commit of main at or below generation gen
	// that changed it (absent: none), by its index in paths.
	LastPathCommits(ctx context.Context, repo, gen int64, paths []string) (map[int]encoding.ID, error)
	// PathHistory is the commits of main at or below generation gen that changed path, newest
	// first.
	PathHistory(ctx context.Context, repo int64, path string, gen int64) ([]encoding.ID, error)

	// ─── Operation logs ───

	// OpLogID is the operation log of a principal's workspace; ok is false when it has none.
	OpLogID(ctx context.Context, repo int64, principal, workspace string) (int64, bool, error)
	// InsertOpLog makes a workspace's operation log, unless it exists — waiting for one being
	// made — and returns its ID and whether it made it.
	InsertOpLog(ctx context.Context, repo int64, principal, workspace string) (int64, bool, error)
	// LockOpLog holds an operation log.
	LockOpLog(ctx context.Context, log int64) error
	// OpHeads are an operation log's heads, by ID.
	OpHeads(ctx context.Context, log int64) ([]encoding.ID, error)
	// AddOpHead makes id a head, once.
	AddOpHead(ctx context.Context, log int64, id encoding.ID) error
	// DeleteOpHeads removes heads.
	DeleteOpHeads(ctx context.Context, log int64, ids []encoding.ID) error
	// OperationIDsFrom are up to limit of an operation log's operations' IDs at or above lo and
	// (hi non-nil) below hi, by ID.
	OperationIDsFrom(ctx context.Context, log int64, lo, hi []byte, limit int) ([]encoding.ID, error)
	// OpBodies are the bodies of the operations or views (kind) among ids, by ID.
	OpBodies(ctx context.Context, log int64, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID][]byte, error)
	// Operations are every operation of a log, with whether each was last touched before before.
	Operations(ctx context.Context, log int64, before time.Time) ([]Operation, error)
	// Views are every view of a log.
	Views(ctx context.Context, log int64) ([]Object, error)
	// OlderThan reports whether any of a log's operations or views was last touched before t.
	OlderThan(ctx context.Context, log int64, t time.Time) (bool, error)
	// PutOperations records operations, in ID order, touching those recorded; PutViews records
	// views so.
	PutOperations(ctx context.Context, log int64, ops []Operation) error
	PutViews(ctx context.Context, log int64, views []Object) error
	// TouchOps touches the operations or views (kind) among ids, now.
	TouchOps(ctx context.Context, log int64, kind encoding.Kind, ids []encoding.ID) error
	// CollectOps deletes a log's operations among doomed last touched before before, then its
	// views last touched before before that no operation names — or, dry, counts what it would
	// delete — and returns how many of each.
	CollectOps(ctx context.Context, log int64, doomed []encoding.ID, before time.Time, dry bool) (ops, views int64, err error)
	// WorkspaceOpLogs are a repository's workspaces' operation logs, by ID.
	WorkspaceOpLogs(ctx context.Context, repo int64) ([]int64, error)
	// DeleteOrphanOpLogs deletes a repository's operation logs of no workspace, and
	// DeleteOrphanWorkspaces its workspaces with no operation log — or, dry, counts them.
	DeleteOrphanOpLogs(ctx context.Context, repo int64, dry bool) (int64, error)
	DeleteOrphanWorkspaces(ctx context.Context, repo int64, dry bool) (int64, error)
	// RepositoryOperations and RepositoryViews are every operation and view of a repository's
	// operation logs.
	RepositoryOperations(ctx context.Context, repo int64) ([]Object, error)
	RepositoryViews(ctx context.Context, repo int64) ([]Object, error)

	// ─── ChangeLists ───

	// ChangeLists are a repository's CLs numbered numbers, by number — held, with lock.
	ChangeLists(ctx context.Context, repo int64, numbers []int64, lock bool) ([]ChangeList, error)
	// ListChangeLists are up to limit of a repository's CLs numbered below before (0: any), in
	// statuses (none: any), by author ("": any), newest first.
	ListChangeLists(ctx context.Context, repo, before int64, statuses []int16, author string, limit int) ([]ChangeList, error)
	// ChangeListsOfChanges are a repository's CLs of change IDs, by number.
	ChangeListsOfChanges(ctx context.Context, repo int64, changes [][]byte) ([]ChangeList, error)
	// Patchsets are the patchsets of a repository's CLs, by CL and number.
	Patchsets(ctx context.Context, repo int64, cls []int64) ([]Patchset, error)
	// ChangeListStatus is a CL's status; ok is false when there is no CL.
	ChangeListStatus(ctx context.Context, repo, number int64) (int16, bool, error)
	// SetChangeListStatus moves a CL to status, with the status an abandoned one returns to.
	SetChangeListStatus(ctx context.Context, repo, number int64, status int16, abandonedFrom *int16) error
	// SetLanded records a CL submitted, landed as commit by by.
	SetLanded(ctx context.Context, repo, number int64, status int16, commit encoding.ID, by string) error
	// UploadChangeLists are the CLs of changes, and those whose patchsets are among commits.
	UploadChangeLists(ctx context.Context, repo int64, changes, commits [][]byte) ([]int64, error)
	// NextChangeListNumbers takes a repository's next n CL numbers and returns the last.
	NextChangeListNumbers(ctx context.Context, repo int64, n int64) (int64, error)
	// InsertChangeList records a CL; ErrDuplicate for one of its number or change ID.
	InsertChangeList(ctx context.Context, repo int64, c ChangeList) error
	// UpdateChangeList records a CL's new current patchset.
	UpdateChangeList(ctx context.Context, repo, number int64, patchset int32, description string, parent *int64, status int16) error
	// InsertPatchset records a patchset.
	InsertPatchset(ctx context.Context, repo int64, p Patchset) error
	// RollingBack is a draft or open CL rolling back any of numbers, the lowest; ok is false when
	// none.
	RollingBack(ctx context.Context, repo int64, numbers []int64, statuses []int16) (int64, bool, error)
	// StackLandings are the landings of a submitted CL and of every submitted CL stacked above it
	// — one with a patchset uploaded on it, or on one above it — by their place in main's history.
	StackLandings(ctx context.Context, repo, number int64, submitted int16) ([]Landing, error)

	// ─── Candidates ───

	// Candidate reads a pending candidate; ok is false when there is none.
	Candidate(ctx context.Context, repo int64, commit encoding.ID) (Candidate, bool, error)
	// PutCandidate records a candidate, unless it is recorded, and returns it as recorded.
	PutCandidate(ctx context.Context, repo int64, c Candidate) (Candidate, error)
	// Candidates are a repository's pending candidates, oldest first.
	Candidates(ctx context.Context, repo int64) ([]Candidate, error)
	// DeleteCandidate drops a pending candidate.
	DeleteCandidate(ctx context.Context, repo int64, commit encoding.ID) error
	// DeleteCandidatesOf drops the pending candidates of CLs.
	DeleteCandidatesOf(ctx context.Context, repo int64, numbers []int64) error

	// ─── Dictionaries ───

	// DictionaryBody is a trained dictionary's body, of whichever repository; ok is false when
	// none is trained with the ID.
	DictionaryBody(ctx context.Context, id int64) ([]byte, bool, error)
	// RepositoryDictionary is a repository's trained dictionary's body.
	RepositoryDictionary(ctx context.Context, repo, id int64) ([]byte, bool, error)
	// NewestDictionary is a repository's newest trained dictionary; ok is false when none.
	NewestDictionary(ctx context.Context, repo int64) (id int64, body []byte, at time.Time, ok bool, err error)
	// InsertDictionary records a dictionary being trained on samples files, and returns its ID.
	InsertDictionary(ctx context.Context, repo int64, samples int) (int64, error)
	// SetDictionaryBody records a dictionary's trained body.
	SetDictionaryBody(ctx context.Context, id int64, body []byte) error
	// DeleteDictionary deletes a dictionary.
	DeleteDictionary(ctx context.Context, id int64) error

	// ─── Garbage collection ───

	// DueForGC are the repositories whose last successful pass began before scheduled and whose
	// last failure, if any, is not after retry, by ID.
	DueForGC(ctx context.Context, scheduled, retry time.Time) ([]int64, error)
	// InsertGCPass records a pass, once.
	InsertGCPass(ctx context.Context, p GCPass) error

	// ─── Workspaces and checkpoints ───

	// InsertWorkspace records a principal's workspace, reporting whether it was new.
	InsertWorkspace(ctx context.Context, repo int64, principal, name string) (bool, error)
	// LockWorkspace reads a workspace's lease, held; ok is false when there is no workspace.
	LockWorkspace(ctx context.Context, repo int64, principal, name string) (WorkspaceLease, bool, error)
	// SetTakeover records the machine asking to take a workspace over.
	SetTakeover(ctx context.Context, repo int64, principal, name, machine string) error
	// GrantLease gives a workspace's lease to machine at epoch, until expires, dropping a takeover
	// request that was machine's own.
	GrantLease(ctx context.Context, repo int64, principal, name, machine string, epoch int64, expires time.Time) error
	// ExtendLease moves a workspace's lease's expiry.
	ExtendLease(ctx context.Context, repo int64, principal, name string, expires time.Time) error
	// ReleaseLease gives a workspace's lease up, if machine holds it at epoch.
	ReleaseLease(ctx context.Context, repo int64, principal, name, machine string, epoch int64) error
	// CheckpointOf reports whether checkpoint id is a principal's workspace's.
	CheckpointOf(ctx context.Context, id, repo int64, principal, workspace string) (bool, error)
	// InsertCheckpoint records a checkpoint and returns its ID and when.
	InsertCheckpoint(ctx context.Context, principal string, c Checkpoint) (int64, time.Time, error)
	// LatestCheckpoint is a workspace's newest checkpoint that is not a fork; ok is false when
	// none.
	LatestCheckpoint(ctx context.Context, repo int64, principal, workspace string) (Checkpoint, bool, error)
	// PrincipalCheckpoint reads one of a principal's checkpoints; ok is false when it is none of
	// theirs.
	PrincipalCheckpoint(ctx context.Context, repo int64, principal string, id int64) (Checkpoint, bool, error)
	// Checkpoints are up to limit of a workspace's checkpoints below before, newest first.
	Checkpoints(ctx context.Context, repo int64, principal, workspace string, before int64, limit int) ([]Checkpoint, error)
	// Workspaces are a principal's workspaces of a repository, by name.
	Workspaces(ctx context.Context, repo int64, principal string) ([]Workspace, error)
	// ThinCheckpoints deletes — or, dry, counts — a repository's checkpoints created before
	// every, except each workspace's latest that is not a fork: before daily, every one; before
	// hourly, all but the last of each day; otherwise all but the last of each hour (UTC).
	ThinCheckpoints(ctx context.Context, repo int64, every, hourly, daily time.Time, dry bool) (int64, error)
	// CheckpointTrees are the trees of a repository's checkpoints, once each.
	CheckpointTrees(ctx context.Context, repo int64) ([]encoding.ID, error)
}
