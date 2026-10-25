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

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// ErrDuplicate is a write a uniqueness constraint refused: a repository's name, a CL's number.
var ErrDuplicate = errors.New("backend: duplicate")

// Backend is the SCM's database.
type Backend interface {
	Ops
	// Txn runs fn in a read-write transaction — on Spanner, again when Spanner aborts it, as the
	// aborted one reset, which keeps its priority — and the store runs it again when Retryable
	// says so. fn may run more than once. Notifications fn sends are delivered when it commits.
	Txn(ctx context.Context, fn func(tx Ops) error) error
	// Retryable reports a transaction the database aborted to break a deadlock or a conflict, or
	// one that refused what a transaction begun anew may not, which running again may commit.
	Retryable(err error) bool
	// SequencesAfterCommit reports whether a repository's CL numbers and places in its log are
	// given after the transaction that makes them commits, by its sequencing pass, rather than
	// taken from its counters in it (docs/scm.md, "Sequencing"): Spanner aborts the transactions
	// that would wait on those rows, and a long one, retried, meets the next.
	SequencesAfterCommit() bool
	// Snapshot runs fn in one read-only transaction that sees the database as of one moment.
	Snapshot(ctx context.Context, fn func(tx Ops) error) error
	// Ping reports whether the database answers.
	Ping(ctx context.Context) error
	// RelayEvents hands publish up to limit events no relay has published, by repository and
	// place in its log, then forgets them — unless publish fails, when they stay to be published
	// again. Relays beside it take other events. It returns how many it published.
	RelayEvents(ctx context.Context, limit int, publish func([]Event) error) (int, error)

	// ─── Moves (docs/hosted.md, "Cells, as built") ───

	// MoveTables are the tables that hold a network's rows, parents first.
	MoveTables(ctx context.Context) ([]string, error)
	// ExportRows is up to limit of a table's rows about a network and its repositories, after a
	// row by key, each named by PostgreSQL's columns.
	ExportRows(ctx context.Context, m Moving, table string, after *okpb.Row, limit int) ([]*okpb.Row, error)
	// ImportRows writes a table's rows of a network moving here, over any of the same key.
	ImportRows(ctx context.Context, m Moving, table string, rows []*okpb.Row) error
	// FinishImport moves what numbers a network's rows past those an import brought; the network
	// stays as its row came — frozen — until it opens (SetNetworkMove).
	FinishImport(ctx context.Context, m Moving) error
	// DropNetworkRows deletes up to limit of a network's rows, children first, and returns how
	// many.
	DropNetworkRows(ctx context.Context, m Moving, limit int) (int64, error)
	// SetNetworkMove sets a network's move: "" (none), frozen, or moved to cell; its commit waits
	// for every write transaction about the network under way (NetworkMove).
	SetNetworkMove(ctx context.Context, net Network, state, cell string) error
}

// Moving is a network that moves, and its repositories.
type Moving struct {
	Network      Network
	Repositories []int64
}

// A network's move states.
const (
	MoveFrozen = "frozen"
	MoveMoved  = "moved"
)

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
	// NetworkMove is a network's move state, and the cell it moved to: a write transaction about
	// the network reads it first, holding the row in share mode, so a move's freeze waits for it.
	NetworkMove(ctx context.Context, net Network) (state, cell string, err error)

	// ─── Repositories ───

	// InsertRepository records repository id in network net, whose main is head — and the network,
	// if it is new, with id training its dictionaries; ErrDuplicate for an ID recorded.
	InsertRepository(ctx context.Context, id int64, net Network, head encoding.ID, createdBy string) (Repository, error)
	// InsertFork records repository id, a fork of parent in its network whose main is parent's
	// main at its place; ErrDuplicate for an ID recorded.
	InsertFork(ctx context.Context, id int64, parent Repository, createdBy string) (Repository, error)
	// SetParent makes parent a fork's upstream (Store.Sync), in the same network.
	SetParent(ctx context.Context, id, parent int64) error
	// SetParentSeq records the place in its parent's main history a fork's follows up to.
	SetParentSeq(ctx context.Context, id, seq int64) error
	// DeleteRepository deletes a repository's row and its tail — every row keyed by it but its
	// content index and segment references, which a deletion record (repository_deletion) keeps
	// for FinishDeletion — clears its forks' parent, hands its network's dictionary training to the
	// network's oldest other repository, and returns its network; ok is false when there is none.
	DeleteRepository(ctx context.Context, id int64) (net Network, ok bool, err error)
	// DeleteRepositoryTail deletes up to limit of the rows DeleteRepository deletes of a repository
	// but its row and its keyed work — children before their parents — and returns how many: the
	// store calls it, each in a transaction of its own, until it deletes fewer than limit, so the
	// transaction that deletes the repository's row holds little.
	DeleteRepositoryTail(ctx context.Context, id int64, limit int) (int64, error)
	// DeleteContentIndexTail deletes up to limit of a content index's blocks and log rows, and
	// returns how many: what FinishDeletion deletes of them, a transaction at a time beforehand.
	DeleteContentIndexTail(ctx context.Context, scope Scope, limit int) (int64, error)
	// DeleteNetworkTail deletes up to limit of the rows DeleteNetwork deletes of a network — its
	// segment rows, packs, chunk releases, dictionaries, and its unfinished deletions' and its own
	// content indexes' blocks and log rows — but its row, its deletion records, content indexes'
	// rows, and counters, and returns how many: a transaction at a time beforehand.
	DeleteNetworkTail(ctx context.Context, net Network, limit int) (int64, error)
	// PendingDeletions are up to limit of the deletions not yet finished, oldest first.
	PendingDeletions(ctx context.Context, limit int) ([]Deletion, error)
	// FinishDeletion retires a deleted repository's segment references, deletes its content index,
	// and deletes its deletion record.
	FinishDeletion(ctx context.Context, id int64) error
	// DeleteNetwork deletes a network's rows, and every content index and segment row in it — its
	// repositories' whose deletions are not finished too.
	DeleteNetwork(ctx context.Context, net Network) error
	// NetworkRepositories are a network's repositories, by ID.
	NetworkRepositories(ctx context.Context, net Network) ([]int64, error)
	// LockRepository holds a repository's row, so it is not deleted meanwhile; ok is false when
	// there is none.
	LockRepository(ctx context.Context, id int64) (ok bool, err error)
	// Repository reads a repository; ok is false when there is none.
	Repository(ctx context.Context, id int64) (Repository, bool, error)
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
	InsertEvents(ctx context.Context, repo int64, evs []LogEvent) error
	// RepositoryEvents are up to limit of a repository's events after place after in its log, in
	// order.
	RepositoryEvents(ctx context.Context, repo, after int64, limit int) ([]Event, error)
	// LogHeads are, for up to limit repositories with IDs above after, by ID, the last place in
	// each one's log; a repository with no event is left out.
	LogHeads(ctx context.Context, after int64, limit int) ([][2]int64, error)

	// ─── Sequencing (Backend.SequencesAfterCommit) ───

	// Pend records what a transaction leaves its repository's sequencing pass, keyed by its commit
	// timestamp, which the database sets as it commits.
	Pend(ctx context.Context, rows []Pending) error
	// PendingRows are up to limit of a repository's pending rows, in commit order, read without a
	// lock as of now: every row committed before is among them, or after them.
	PendingRows(ctx context.Context, repo int64, limit int) ([]Pending, error)
	// PendingHeld are which of a repository's pending rows are pending still, each held until the
	// transaction ends.
	PendingHeld(ctx context.Context, repo int64, keys []PendingKey) (map[PendingKey]bool, error)
	// DeletePending deletes a repository's pending rows.
	DeletePending(ctx context.Context, repo int64, keys []PendingKey) error
	// NumberChangeLists gives a repository's CLs of ids the numbers, from their provisional ones,
	// and the CLs and patchsets stacked on them theirs.
	NumberChangeLists(ctx context.Context, repo int64, ids, numbers []int64) error
	// ChangeListNumbers are the numbers of a repository's CLs of ids, by ID: provisional where the
	// pass has not numbered them.
	ChangeListNumbers(ctx context.Context, repo int64, ids []int64) (map[int64]int64, error)
	// TransactionEvents are the places in a repository's log of the events the transaction of tie
	// txn wrote, in order.
	TransactionEvents(ctx context.Context, repo, txn int64) ([]int64, error)
	// PendingRepositories are up to limit of the repositories with a row pending that committed
	// before before, by ID.
	PendingRepositories(ctx context.Context, before time.Time, limit int) ([]int64, error)

	// ─── The content index ───

	// A content index is a repository's — the files and chunks it holds — or a network's — where
	// its chunks are — as its scope says.

	// InsertContentIndex records a content index at generation gen.
	InsertContentIndex(ctx context.Context, scope Scope, gen int64) error
	// ShareContentIndex holds a content index in share mode — writers that commute with each other
	// run beside it; a sweep, a compaction, or a fold does not — and returns today and its
	// generation, which no fold moves while it is held. ok is false when there is no such content
	// index.
	ShareContentIndex(ctx context.Context, scope Scope) (today time.Time, gen int64, ok bool, err error)
	// LockContentIndex holds a content index alone and returns its generation, and today.
	LockContentIndex(ctx context.Context, scope Scope) (gen int64, today time.Time, err error)
	// ContentIndexGeneration is a content index's row; ok is false when it has none.
	ContentIndexGeneration(ctx context.Context, scope Scope) (state IndexState, ok bool, err error)
	// SetContentIndexGeneration moves a content index to generation gen, which folded its log to
	// folded.
	SetContentIndexGeneration(ctx context.Context, scope Scope, gen int64, folded LogPlace) error
	// SetContentIndexSwept records the day a content index's garbage collection swept to.
	SetContentIndexSwept(ctx context.Context, scope Scope, before int64) error
	// ContentLogSince are a content index's log rows the snapshot sees — the operation's, which
	// must be one (Snapshot) — that a reader at since has not (nil: every one), in the order they
	// apply, and the snapshot's place in the log. A reader goes on from that place: nothing
	// committed since is before it, so it reads only what committed after.
	ContentLogSince(ctx context.Context, scope Scope, since LogPlace) ([]ContentLog, LogPlace, error)
	// InsertContentLog records a content log row, applied after the transaction's earlier ones.
	InsertContentLog(ctx context.Context, scope Scope, body []byte) error
	// DeleteFoldedContentLogs deletes up to limit of the log rows a fold read to folded — which its
	// generation holds — and of the rows of transactions that never committed, and returns how many:
	// after a fold, a transaction at a time.
	DeleteFoldedContentLogs(ctx context.Context, scope Scope, folded LogPlace, limit int) (int64, error)

	// ─── Packs ───

	// Packs are a network's.

	// PutPack records a pack a publish wrote — again, its size updated, if it is recorded — and
	// returns its number.
	PutPack(ctx context.Context, net Network, id encoding.ID, size int64) (uint64, error)
	// InsertPack records a pack compaction wrote, with the dictionary it considered (nil: none),
	// and returns its number.
	InsertPack(ctx context.Context, net Network, id encoding.ID, size int64, dictionary *int64) (uint64, error)
	// NumberedPacks are the IDs of a network's packs of numbers nums, retired ones too: a number is
	// one pack's for good. Read without locking them (a reader of a chunk index names many).
	NumberedPacks(ctx context.Context, net Network, nums []uint64) (map[uint64]encoding.ID, error)
	// LivePacks are a network's packs not retired, oldest first.
	LivePacks(ctx context.Context, net Network) ([]Pack, error)
	// RetiredPackIDs are a network's retired packs' IDs.
	RetiredPackIDs(ctx context.Context, net Network) ([]encoding.ID, error)
	// PackIDs are every one of a network's packs' IDs.
	PackIDs(ctx context.Context, net Network) ([]encoding.ID, error)
	// SetPacksDictionary records the dictionary that considered packs.
	SetPacksDictionary(ctx context.Context, net Network, ids []encoding.ID, dictionary int64) error
	// RetirePack retires a pack, now.
	RetirePack(ctx context.Context, net Network, id encoding.ID) error
	// LivePackNumbers are the numbers of the packs among ids not retired, by ID.
	LivePackNumbers(ctx context.Context, net Network, ids []encoding.ID) (map[encoding.ID]uint64, error)
	// RetirePacks retires the packs among ids not retired, now — or, dry, only counts them — and
	// returns how many and their total size.
	RetirePacks(ctx context.Context, net Network, ids []encoding.ID, dry bool) (int, int64, error)
	// RepositoriesRetiringBefore are the first repository of each network with a pack retired
	// before packs or a segment row retired before segments, by ID: their collections delete them.
	RepositoriesRetiringBefore(ctx context.Context, packs, segments time.Time) ([]int64, error)
	// RetiredPacksBefore are the packs retired before t, of one network or (net nil) every one, by
	// network and ID.
	RetiredPacksBefore(ctx context.Context, t time.Time, net *Network) ([]RetiredPack, error)
	// DeleteRetiredPack deletes a retired pack's row, reporting whether it did.
	DeleteRetiredPack(ctx context.Context, net Network, id encoding.ID) (bool, error)

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
	// UnpinnedObjects are, of the objects of kind among ids, the bodies of those unpinned, and
	// which are pinned: one in neither has no row — pinned, in a segment, or not held.
	UnpinnedObjects(ctx context.Context, repo int64, kind encoding.Kind, ids []encoding.ID) (unpinned map[encoding.ID][]byte, pinned map[encoding.ID]bool, err error)
	// UnpinnedCommits is UnpinnedObjects for commits.
	UnpinnedCommits(ctx context.Context, repo int64, ids []encoding.ID) (unpinned map[encoding.ID][]byte, pinned map[encoding.ID]bool, err error)
	// CommitGenerations are the generations of the commits among ids, by ID.
	CommitGenerations(ctx context.Context, repo int64, ids []encoding.ID) (map[encoding.ID]int64, error)
	// WalkObjects applies walk to the objects of kind among ids it applies to — unpinned and
	// older than today for a touch, unpinned for a refresh or a pin — and returns their bodies,
	// and the ids it found held that it does not apply to, committed before the transaction:
	// pinned, or touched today for a touch (none, when it does not tell them).
	WalkObjects(ctx context.Context, repo int64, kind encoding.Kind, ids []encoding.ID, walk Walk) (map[encoding.ID][]byte, map[encoding.ID]bool, error)
	// WalkCommits is WalkObjects for commits.
	WalkCommits(ctx context.Context, repo int64, ids []encoding.ID, walk Walk) (map[encoding.ID][]byte, map[encoding.ID]bool, error)
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
	// CopyChildren are the copy histories the tail records each of parents as a parent of, as
	// (copy, parent) edges.
	CopyChildren(ctx context.Context, repo int64, parents []encoding.ID) ([][2]encoding.ID, error)
	// TailAncestry walks the tail's commits: those of from it holds, and every ancestor it holds
	// down to generation minGen, each with its generation and parents (wherever they are).
	TailAncestry(ctx context.Context, repo int64, from []encoding.ID, minGen int64) ([]GraphCommit, error)
	// TailGraph is the generation and parents of each of ids the tail holds.
	TailGraph(ctx context.Context, repo int64, ids []encoding.ID) ([]GraphCommit, error)
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

	// ─── Segments ───

	// Segments are a scope's live segments of kind, newest first by age.
	Segments(ctx context.Context, scope Scope, kind segment.Kind) ([]Segment, error)
	// SegmentsUnlocked are the same, read without locking them: in a Spanner read-write
	// transaction a read locks what it scans, so every reader of a repository's segments would
	// wait on compaction's writes to its manifest, and compaction on every reader. For reading what
	// segments hold, which a merge or a fold retiring them does not change: a retired segment stays
	// readable for its grace. On PostgreSQL, Segments, which locks nothing.
	SegmentsUnlocked(ctx context.Context, scope Scope, kind segment.Kind) ([]Segment, error)
	// InsertSegments records written segments live.
	InsertSegments(ctx context.Context, segs []Segment) error
	// RetireSegments retires a scope's live segments of kind among ids, those it inherited aside,
	// reporting how many it retired.
	RetireSegments(ctx context.Context, scope Scope, kind segment.Kind, ids []encoding.ID) (int64, error)
	// RetiredSegmentsBefore are the segment rows retired before t, of every scope of one network or
	// (net nil) every network.
	RetiredSegmentsBefore(ctx context.Context, t time.Time, net *Network) ([]Segment, error)
	// AllSegments are a scope's segments, live and retired.
	AllSegments(ctx context.Context, scope Scope) ([]Segment, error)
	// NetworkSegments are every segment row of a network, of every scope, live and retired.
	NetworkSegments(ctx context.Context, net Network) ([]Segment, error)
	// SegmentNamed reports whether any of a network's segment rows but scope's names a segment
	// object — live, or retired since.
	SegmentNamed(ctx context.Context, net Network, scope Scope, kind segment.Kind, id encoding.ID, since time.Time) (bool, error)
	// SetInherited makes segs the live segments a repository inherits — each recorded inherited,
	// with its bound, live again if it was retired — and retires the rest it inherited, reporting
	// how many.
	SetInherited(ctx context.Context, repo int64, segs []Segment) (int64, error)

	// ─── Chunk releases ───

	// Releases are of what a pack holds, chunks and trees, by ID: an ID is one kind's.

	// ReleaseChunks records that a repository no longer holds chunks or trees, for its network's
	// collection.
	ReleaseChunks(ctx context.Context, net Network, ids []encoding.ID) error
	// ReleasedChunks are up to limit of a network's released chunks and trees, by ID.
	ReleasedChunks(ctx context.Context, net Network, limit int) ([]encoding.ID, error)
	// DeleteReleases deletes a network's releases among ids.
	DeleteReleases(ctx context.Context, net Network, ids []encoding.ID) error
	// DeleteSegment deletes a retired segment's row.
	DeleteSegment(ctx context.Context, scope Scope, kind segment.Kind, id encoding.ID) error
	// PinnedPaths are up to limit of the tail's rows of main's path history — every one is main's,
	// so pinned — by path, generation, and commit.
	PinnedPaths(ctx context.Context, repo int64, limit int) ([]PathRow, error)
	// SettledEvents are up to limit of the tail's events the relay has published, written before
	// before, by place.
	SettledEvents(ctx context.Context, repo int64, before time.Time, limit int) ([]Event, error)
	// SettledChangeLists are up to limit of the tail's landed CLs, by number, each with its
	// patchsets and its landing: nothing changes a CL once it has landed.
	SettledChangeLists(ctx context.Context, repo int64, limit int) ([]SettledChangeList, error)
	// PinnedCommits are up to limit of the tail's pinned commits, by ID.
	PinnedCommits(ctx context.Context, repo int64, limit int) ([]PinnedCommit, error)
	// PinnedObjects are up to limit of the tail's pinned objects, by ID.
	PinnedObjects(ctx context.Context, repo int64, limit int) ([]Object, error)
	// PinnedTreeSizes are up to limit of the tail's sizes records of trees that are pinned —
	// pinned in the tail, or gone from it into a segment — by tree ID, each as an Object.
	PinnedTreeSizes(ctx context.Context, repo int64, limit int) ([]Object, error)
	// PinnedCopyEdges are up to limit of the tail's (copy, parent) edges of copies that are
	// pinned, likewise, by parent then copy.
	PinnedCopyEdges(ctx context.Context, repo int64, limit int) ([][2]encoding.ID, error)
	// DeleteCompacted deletes from the tail what a compaction moved into segments: the objects
	// among it still pinned, which nothing undoes; the sizes records and edges; and the CLs among
	// it still landed, which nothing undoes either, with their patchsets and main's moves.
	DeleteCompacted(ctx context.Context, repo int64, c Compacted) error

	// ─── Main's history ───

	// InsertHeadLog records one move of main.
	InsertHeadLog(ctx context.Context, repo int64, h HeadMove) error
	// MainLog is up to limit of main's heads at seqs from to to, each with its seq as its
	// generation, newest first unless ascending.
	MainLog(ctx context.Context, repo, from, to int64, ascending bool, limit int) ([]GenCommit, error)
	// HeadSeqOf is the place in main's history where main moved to commit; ok is false when it
	// never did.
	HeadSeqOf(ctx context.Context, repo int64, commit encoding.ID) (seq int64, ok bool, err error)
	// InsertPathIndex records a landing whose path history is written after its transaction
	// (InsertCommitPaths), a bounded transaction at a time; PathIndexes are a repository's, by
	// generation; DeletePathIndex records one written.
	InsertPathIndex(ctx context.Context, repo int64, p PathIndex) error
	PathIndexes(ctx context.Context, repo int64) ([]PathIndex, error)
	DeletePathIndex(ctx context.Context, repo, gen int64) error
	// InsertCommitPaths records the paths a commit on main at generation gen changed, once each.
	InsertCommitPaths(ctx context.Context, repo, gen int64, commit encoding.ID, paths []string) error
	// PathCommits are up to limit of the commits of main that changed any of paths, at
	// generations from to to, once each, newest first unless ascending.
	PathCommits(ctx context.Context, repo int64, paths []string, from, to int64, ascending bool, limit int) ([]GenCommit, error)
	// LastPathCommits are, for each of paths, the newest commit of main at or below generation gen
	// that changed it (absent: none), with its generation, by its index in paths.
	LastPathCommits(ctx context.Context, repo, gen int64, paths []string) (map[int]GenCommit, error)
	// PathHistory is the commits of main at or below generation gen that changed path, with their
	// generations, newest first.
	PathHistory(ctx context.Context, repo int64, path string, gen int64) ([]GenCommit, error)

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
	// TouchOps touches the tail's operations or views (kind) among ids, now, returning those it
	// touched.
	TouchOps(ctx context.Context, log int64, kind encoding.Kind, ids []encoding.ID) ([]encoding.ID, error)
	// CollectOps deletes the tail's operations of a log among doomed last touched before before,
	// then its views last touched before before that no operation in the tail names and that are
	// not among keep — or, dry, counts what it would delete — and returns how many of each.
	CollectOps(ctx context.Context, log int64, doomed, keep []encoding.ID, before time.Time, dry bool) (ops, views int64, err error)
	// TombstoneOps records tombstones of a log's operations or views (kind) among ids the tail
	// holds no row or tombstone of — or, dry, counts them — and returns how many.
	TombstoneOps(ctx context.Context, log int64, kind encoding.Kind, ids []encoding.ID, dry bool) (int64, error)
	// TailOpTombstones are the tombstones the tail holds of a log's operations or views (kind)
	// among ids.
	TailOpTombstones(ctx context.Context, log int64, kind encoding.Kind, ids []encoding.ID) (map[encoding.ID]bool, error)
	// RepositoryOpTombstones are every tombstone the tail holds of a repository's logs.
	RepositoryOpTombstones(ctx context.Context, repo int64) ([]OpKey, error)
	// TailOpViewRefs are the views the tail's operations of a log name, but for operations among
	// doomed touched before before.
	TailOpViewRefs(ctx context.Context, log int64, doomed []encoding.ID, before time.Time) (map[encoding.ID]bool, error)
	// AllOpLogs are a repository's operation logs, by ID.
	AllOpLogs(ctx context.Context, repo int64) ([]int64, error)
	// SettledOps are up to limit of the tail's operations, and up to limit of its views, of a
	// repository's logs last touched before before, by log, kind, and ID.
	SettledOps(ctx context.Context, repo int64, before time.Time, limit int) ([]OpRecord, error)
	// OpTombstones are up to limit of the tail's tombstones of a repository's logs, by log, kind,
	// and ID.
	OpTombstones(ctx context.Context, repo int64, limit int) ([]OpKey, error)
	// WorkspaceOpLogs are a repository's workspaces' operation logs, by ID.
	WorkspaceOpLogs(ctx context.Context, repo int64) ([]int64, error)
	// DeleteOrphanOpLogs deletes a repository's operation logs of no workspace, and
	// DeleteOrphanWorkspaces its workspaces with no operation log — or, dry, counts them.
	DeleteOrphanOpLogs(ctx context.Context, repo int64, dry bool) (int64, error)
	DeleteOrphanWorkspaces(ctx context.Context, repo int64, dry bool) (int64, error)
	// OpContentPinned reports whether what a repository's settled operations and views reach is
	// pinned (docs/scm.md, "Garbage collection and compaction"): true of a repository made since
	// compaction pins it as it moves them, and of one a pass pinned the rest of; SetOpContentPinned
	// records it.
	OpContentPinned(ctx context.Context, repo int64) (bool, error)
	SetOpContentPinned(ctx context.Context, repo int64) error
	// TailOps are up to limit of the operations or views (kind) the tail holds of a repository's
	// operation logs, after the key after (zero: from the first), in key order: the roots
	// garbage collection's mark reads a page at a time.
	TailOps(ctx context.Context, repo int64, kind encoding.Kind, after OpKey, limit int) ([]OpRecord, error)
	// DivergeOpLog records that an operation log's heads moved other than forward, now
	// (diverged_at): only such a move leaves operations nothing reaches. OpLogsToCollect are a
	// repository's logs that diverged after what a collection covered (collected_through), with
	// when; SetOpLogCollected records a collection that took everything a divergence at through
	// left unreached.
	DivergeOpLog(ctx context.Context, opLog int64) error
	OpLogsToCollect(ctx context.Context, repo int64) ([]OpLogDivergence, error)
	SetOpLogCollected(ctx context.Context, opLog int64, through time.Time) error
	// RepositoryOperations and RepositoryViews are every operation and view the tail holds of a
	// repository's operation logs.
	RepositoryOperations(ctx context.Context, repo int64) ([]OpRecord, error)
	RepositoryViews(ctx context.Context, repo int64) ([]OpRecord, error)

	// ─── ChangeLists ───

	// ChangeLists are a repository's CLs numbered numbers, by number — held, with lock.
	ChangeLists(ctx context.Context, repo int64, numbers []int64, lock bool) ([]ChangeList, error)
	// ListChangeLists are up to limit of a repository's CLs numbered below before (0: any), in
	// statuses (none: any), by author ("": any), stacked on parent (0: any), newest first.
	ListChangeLists(ctx context.Context, repo, before int64, statuses []int16, author string, parent int64, limit int) ([]ChangeList, error)
	// ChangeListsOfChanges are a repository's CLs of change IDs, by number.
	ChangeListsOfChanges(ctx context.Context, repo int64, changes [][]byte) ([]ChangeList, error)
	// Patchsets are the patchsets of a repository's CLs of IDs cls, by CL and number.
	Patchsets(ctx context.Context, repo int64, cls []int64) ([]Patchset, error)
	// ChangeListStatus is a CL's status; ok is false when there is no CL.
	ChangeListStatus(ctx context.Context, repo, number int64) (int16, bool, error)
	// SetChangeListStatus moves the CL of ID id to status, with the status an abandoned one returns
	// to.
	SetChangeListStatus(ctx context.Context, repo, id int64, status int16, abandonedFrom *int16) error
	// SetLanded records the CL of ID id submitted, landed as commit by by.
	SetLanded(ctx context.Context, repo, id int64, status int16, commit encoding.ID, by string) error
	// UploadChangeLists are the CLs of changes, and those whose patchsets are among commits.
	UploadChangeLists(ctx context.Context, repo int64, changes, commits [][]byte) ([]int64, error)
	// NextChangeListNumbers takes a repository's next n CL numbers and returns the last.
	NextChangeListNumbers(ctx context.Context, repo int64, n int64) (int64, error)
	// InsertChangeList records a CL; ErrDuplicate for one of its ID, number, or change ID.
	InsertChangeList(ctx context.Context, repo int64, c ChangeList) error
	// UpdateChangeList records the new current patchset of the CL of ID id.
	UpdateChangeList(ctx context.Context, repo, id int64, patchset int32, description string, parent *int64, status int16) error
	// InsertPatchset records a patchset.
	InsertPatchset(ctx context.Context, repo int64, p Patchset) error
	// RollingBack is a draft or open CL rolling back any of numbers, the lowest; ok is false when
	// none. It may read as of now, outside the transaction: its caller holds the repository's head.
	RollingBack(ctx context.Context, repo int64, numbers []int64, statuses []int16) (int64, bool, error)
	// StackedOn are the tail's CLs with a patchset uploaded stacked on one of parents, as (parent,
	// CL) pairs.
	StackedOn(ctx context.Context, repo int64, parents []int64) ([][2]int64, error)
	// TailLandings are the tail's moves of main that landed cls.
	TailLandings(ctx context.Context, repo int64, cls []int64) ([]HeadMove, error)

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

	// NetworkDictionary is a network's trained dictionary's body, and who trained it; ok is false
	// when none is trained with the ID. A dictionary's ID is its network's: a frame names it, and a
	// network keeps its IDs wherever it moves.
	NetworkDictionary(ctx context.Context, net Network, id int64) (body []byte, trainer int64, ok bool, err error)
	// NetworkTrainer is the repository that trains a network's dictionaries.
	NetworkTrainer(ctx context.Context, net Network) (int64, error)
	// DictionaryGrants are, for each trainer, the newest of its dictionaries a repository may read
	// besides its own.
	DictionaryGrants(ctx context.Context, repo int64) (map[int64]int64, error)
	// SetDictionaryGrants replaces a repository's grants.
	SetDictionaryGrants(ctx context.Context, repo int64, grants map[int64]int64) error
	// NewestDictionaryOf is the newest trained dictionary a trainer trained, by ID; ok is false
	// when none.
	NewestDictionaryOf(ctx context.Context, trainer int64) (id int64, ok bool, err error)
	// NewestDictionary is a network's newest trained dictionary; ok is false when none.
	NewestDictionary(ctx context.Context, net Network) (id int64, body []byte, at time.Time, ok bool, err error)
	// InsertDictionary records a dictionary trainer is training on samples files, and returns its
	// ID.
	InsertDictionary(ctx context.Context, net Network, trainer int64, samples int) (int64, error)
	// SetDictionaryBody records a dictionary's trained body.
	SetDictionaryBody(ctx context.Context, net Network, id int64, body []byte) error
	// DeleteDictionary deletes a dictionary.
	DeleteDictionary(ctx context.Context, net Network, id int64) error

	// ─── Garbage collection ───

	// DueForGC are the repositories whose last successful pass began before scheduled and whose
	// last failure, if any, is not after retry, by ID; RepositoryDueForGC reports whether a
	// repository is one.
	DueForGC(ctx context.Context, scheduled, retry time.Time) ([]int64, error)
	RepositoryDueForGC(ctx context.Context, repo int64, scheduled, retry time.Time) (bool, error)

	// ─── Keyed work ───

	// MarkWork, TakeWork, RenewWork, ReleaseWork, LetWork, and DueWork are the SCM's keyed work's
	// (internal/work's Store).
	MarkWork(ctx context.Context, kind string, key int64) error
	MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error
	TakeWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	RenewWork(ctx context.Context, kind string, key int64, holder string, ttl time.Duration) (bool, error)
	ReleaseWork(ctx context.Context, kind string, key int64, holder string) (bool, error)
	LetWork(ctx context.Context, kind string, key int64, holder string) error
	DueWork(ctx context.Context, kind string, limit int) ([]int64, error)
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
	// Checkpoints are up to limit of a workspace's checkpoints made before the checkpoint before (0:
	// from the newest), newest first.
	Checkpoints(ctx context.Context, repo int64, principal, workspace string, before int64, limit int) ([]Checkpoint, error)
	// Workspaces are a principal's workspaces of a repository, by name.
	Workspaces(ctx context.Context, repo int64, principal string) ([]Workspace, error)
	// WorkspaceNames are up to limit of a repository's workspaces after one, by principal and name
	// (the zero WorkspaceKey: from the first).
	WorkspaceNames(ctx context.Context, repo int64, after WorkspaceKey, limit int) ([]WorkspaceKey, error)
	// ThinCheckpoints deletes — or, dry, counts — a repository's checkpoints created before
	// every, except each workspace's latest that is not a fork: before daily, every one; before
	// hourly, all but the last of each day; otherwise all but the last of each hour (UTC).
	ThinCheckpoints(ctx context.Context, repo int64, every, hourly, daily time.Time, dry bool) (int64, error)
	// CheckpointTrees are the trees of a repository's checkpoints whose trees are not pinned, once
	// each: garbage collection's roots.
	CheckpointTrees(ctx context.Context, repo int64) ([]encoding.ID, error)
	// CheckpointsToPin are up to limit of a repository's checkpoints made before before whose trees
	// are not pinned, with their trees; SetCheckpointsPinned records theirs pinned.
	CheckpointsToPin(ctx context.Context, repo int64, before time.Time, limit int) ([]CheckpointTreeIDs, error)
	SetCheckpointsPinned(ctx context.Context, repo int64, ids []int64) error
}
