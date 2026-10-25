package backend

import (
	"strconv"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/segment"
)

// Deletion is a deleted repository whose deletion is not finished, and its network.
type Deletion struct {
	Repo int64
	Net  Network
}

// NoBound is a segment read whole.
const NoBound = -1

// Network is a fork network's ID: random, as a repository's (docs/hosted.md, "Fork networks, as
// built"). A network holds what its repositories share — packs, dictionaries, and where each chunk
// is — and every repository is in one.
type Network int64

// Scope is what a content index or a segment is of: a repository, or a network.
type Scope struct {
	Network bool
	ID      int64
}

// RepositoryScope is a repository's scope.
func RepositoryScope(repo int64) Scope { return Scope{ID: repo} }

// NetworkScope is a network's scope.
func NetworkScope(net Network) Scope { return Scope{Network: true, ID: int64(net)} }

// String is the scope as a notification names it: r or n, then the ID.
func (s Scope) String() string {
	if s.Network {
		return "n" + strconv.FormatInt(s.ID, 10)
	}
	return "r" + strconv.FormatInt(s.ID, 10)
}

// Name is the scope in words: "repository 7", "network 7".
func (s Scope) Name() string {
	if s.Network {
		return "network " + strconv.FormatInt(s.ID, 10)
	}
	return "repository " + strconv.FormatInt(s.ID, 10)
}

// ParseScope reads a scope String wrote.
func ParseScope(v string) (Scope, bool) {
	if len(v) < 2 || (v[0] != 'r' && v[0] != 'n') {
		return Scope{}, false
	}
	id, err := strconv.ParseInt(v[1:], 10, 64)
	return Scope{Network: v[0] == 'n', ID: id}, err == nil
}

// Repository is a repository's row.
type Repository struct {
	ID      int64
	Network Network
	Parent  int64 // 0: a fork of none
	// ParentSeq is, of a fork, the place in its parent's main history its own follows up to — kept
	// when its parent is deleted, as its inherited segments are bounded there.
	ParentSeq int64
	Head      encoding.ID
	HeadSeq   int64
	CreatedAt time.Time
	CreatedBy string
}

// Event is one event, encoded, with its repository and place in that repository's log.
type Event struct {
	Repository    int64
	RepositorySeq int64
	Body          []byte
}

// ContentLog is a content log row: one transaction's changes to a content index, or a part of them.
type ContentLog struct {
	Body []byte
}

// LogPlace is a reader's place in a content index's log: what it has read of it, as the backend
// tells — a Spanner read timestamp, a PostgreSQL snapshot. The store keeps and hands it back,
// never looking inside.
type LogPlace []byte

// Pack is a pack's row: its number, size, and the dictionary that last considered it (nil:
// none).
type Pack struct {
	ID         encoding.ID
	Num        uint64
	Size       int64
	Dictionary *int64
}

// Segment is a manifest's reference to a segment: its scope's, a repository's or a network's. Net
// is the network whose object it names; Inherited, whether a fork took it from its parent; and
// Bound, for a kind read along main, the place in main's history it is read up to (NoBound: all of
// it).
type Segment struct {
	Scope     Scope
	Net       Network
	Inherited bool
	Bound     int64
	Kind      segment.Kind
	ID        encoding.ID
	Level     int16
	Records   int64
	Size      int64
	CreatedAt time.Time
	// Day is the day, since 1970-01-01, the fold that wrote it committed: a repository's unpinned
	// index's runs' (0: none).
	Day int64
	// Age orders a kind's segments, newest first: InsertSegments gives one written from the tail
	// (Age 0) the next; a merged one carries the newest of its inputs'.
	Age int64
}

// Compacted is what one compaction moved out of the tail into segments: objects, trees' sizes
// records, copies' parent edges, commits, main's path history, and events.
type Compacted struct {
	Objects   []encoding.ID
	TreeSizes []encoding.ID
	CopyEdges [][2]encoding.ID // copy, parent
	Commits   []encoding.ID
	Paths     []PathRow
	Events    []int64 // places in the repository's log
	// Ops are operations and views, deleted only if still touched before OpsBefore: a re-publish
	// since keeps its row. OpTombstones are tombstones.
	Ops          []OpRecord
	OpsBefore    time.Time
	OpTombstones []OpKey
	// ChangeLists are landed CLs, deleted with their patchsets; MainLog, main's moves by place.
	ChangeLists []int64
	MainLog     []int64
}

// OpKey names an operation or a view in an operation log.
type OpKey struct {
	Log  int64
	Kind encoding.Kind
	ID   encoding.ID
}

// OpRecord is an operation or a view of an operation log: an operation's view, its body, and when
// it was last published.
type OpRecord struct {
	OpKey
	View      encoding.ID
	Body      []byte
	TouchedAt time.Time
}

// PathRow is a row of main's path history: a commit of main that changed a path, or a path below
// a directory, at its generation.
type PathRow struct {
	Path       string
	Generation int64
	Commit     encoding.ID
}

// PathIndex is a landing whose path history is yet to be written: the commit at a generation on
// main, and its parent.
type PathIndex struct {
	Generation     int64
	Commit, Parent encoding.ID
}

// GraphCommit is a commit as the commit graph knows it: its generation and its parents, in order.
type GraphCommit struct {
	ID         encoding.ID
	Generation int64
	Parents    []encoding.ID
}

// PinnedCommit is a pinned commit in the tail, with its body, for compaction.
type PinnedCommit struct {
	GraphCommit
	Body []byte
}

// RetiredPack is a retired pack.
type RetiredPack struct {
	Net Network
	ID  encoding.ID
	Num uint64
}

// Object is a tree, symlink, copy history, operation, or view: its kind, ID, and canonical body
// (without the kind byte).
type Object struct {
	ID   encoding.ID
	Kind encoding.Kind
	Body []byte
}

// Commit is a commit as it is first recorded.
type Commit struct {
	ID          encoding.ID
	ChangeID    []byte
	Body        []byte
	Generation  int64
	Conflicted  bool
	CommittedAt time.Time
}

// StoredCommit is a commit as fsck reads it.
type StoredCommit struct {
	ID         encoding.ID
	Body       []byte
	Generation int64
}

// ParentEdge is one of a commit's parents, at its place among them.
type ParentEdge struct {
	Commit encoding.ID
	Ord    int16
	Parent encoding.ID
}

// Ancestor is a commit an ancestry walk reached, with its parents in order and their
// generations.
type Ancestor struct {
	ID          encoding.ID
	Parents     []encoding.ID
	Generations []int64
}

// HeadMove is one move of main: to New at Seq, from Old, landing a CL's patchset.
type HeadMove struct {
	Seq      int64
	Old, New encoding.ID
	CL       int64
	Patchset int32
	By       string
}

// GenCommit is a commit at its generation.
type GenCommit struct {
	Commit     encoding.ID
	Generation int64
}

// Operation is an operation of a log: its view, its body, and whether it was last touched
// before the time asked about.
type Operation struct {
	ID   encoding.ID
	View encoding.ID
	Body []byte
	Old  bool
}

// ChangeList is a CL's row, with its patchsets when a read asks for them. A CL its backend numbers
// after commit (Backend.SequencesAfterCommit) is numbered -ID until its repository's sequencing
// pass numbers it, and a CL stacked on one so has its parent's provisional number: each is unique,
// as numbers are, but never shown outside the SCM (Provisional).
type ChangeList struct {
	ID              int64
	Number          int64
	ChangeID        []byte
	Author          string
	Status          int16
	AbandonedFrom   int16 // 0: none
	Parent          int64 // 0: not stacked
	CurrentPatchset int32
	Landed          *encoding.ID
	SubmittedBy     string
	Description     string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	RollsBack       []int64
	RollbackPaths   []string
	RollbackLeft    []string
}

// Provisional reports whether a CL's row is not yet numbered: it, or the CL it is stacked on.
func (c ChangeList) Provisional() bool { return c.Number < 0 || c.Parent < 0 }

// Patchset is a patchset's row. CL is its CL's ID; Parent, the number of the CL it was uploaded
// stacked on (provisional, as a ChangeList's may be).
type Patchset struct {
	CL         int64
	Number     int32
	Commit     encoding.ID
	Base       encoding.ID
	Parent     int64 // 0: not stacked
	UploadedBy string
	UploadedAt time.Time
}

// SettledChangeList is a landed CL: its row, its patchsets, and the move of main that landed it,
// with when.
type SettledChangeList struct {
	ChangeList
	Patchsets []Patchset
	Landing   HeadMove
	LandedAt  time.Time
}

// Landing is a CL's landing on main: the commit it landed as, its parent on main, its place in
// main's history, and the CL's description and author.
type Landing struct {
	Number      int64
	Seq         int64
	Commit      encoding.ID
	Parent      encoding.ID
	Description string
	Author      string
}

// Candidate is a pending candidate.
type Candidate struct {
	Commit     encoding.ID
	Base       encoding.ID
	Number     int64
	Patchset   int32
	PreparedBy string
	PreparedAt time.Time
}

// IndexState is a content index's row: its generation; where in its log the fold that made it read
// to (nil: nowhere — the generation folded no log); and the day, since 1970-01-01, its last complete
// garbage collection swept to — the day its unpinned index's runs start from (0: none yet).
type IndexState struct {
	Generation  int64
	Folded      LogPlace
	SweptBefore int64
}

// Pending is what a transaction wrote for its repository's sequencing pass to order once it commits
// (Backend.SequencesAfterCommit): a CL to number (ChangeList, its ID), or an event to place in the
// log (Body, its new CLs by provisional number; At, when it was written). Key orders them: the
// transaction's commit timestamp, its tie, and the place in it.
type Pending struct {
	Repository int64
	Key        PendingKey
	ChangeList int64
	At         time.Time
	Body       []byte
}

// PendingKey is a pending row's place in commit order. CommittedAt is set by the database at commit.
type PendingKey struct {
	CommittedAt time.Time
	Txn, Place  int64
}

// LogEvent is an event at its place in its repository's log, as InsertEvents records it: when it
// was written, and the tie of the transaction that wrote it (0: none, where the transaction placed
// it itself).
type LogEvent struct {
	Seq  int64
	At   time.Time
	Txn  int64
	Body []byte
}

// GCPass is one garbage-collection pass's record.
type GCPass struct {
	Repository                                           int64
	Started, Finished                                    time.Time
	OperationsDeleted, ViewsDeleted                      int64
	Marked                                               int
	CommitsSwept, ObjectsSwept, FilesSwept, ChunksSwept  int64
	ChunkBytesSwept                                      int64
	PacksWritten, PacksRetired                           int
	BytesRead, BytesReclaimed                            int64
	PacksDeleted, OrphanPacks                            int
	Error                                                *string
	CheckpointsDeleted, OpLogsDeleted, WorkspacesDeleted int64
}

// WorkspaceLease is a workspace's lease: its holder (""), epoch, expiry (zero), and the machine
// asking to take it over ("").
type WorkspaceLease struct {
	Holder     string
	Epoch      int64
	ExpiresAt  time.Time
	TakeoverBy string
}

// Workspace is one of a principal's workspaces, and whether it has an operation log of its own.
type Workspace struct {
	Name string
	WorkspaceLease
	OwnLog bool
}

// WorkspaceKey names a workspace: its principal's, by name.
type WorkspaceKey struct {
	Principal, Name string
}

// Checkpoint is a checkpoint's row.
type Checkpoint struct {
	ID         int64
	Repo       int64
	Workspace  string
	Machine    string
	ParentID   int64 // 0: none
	Trees      []encoding.ID
	BaseCommit encoding.ID
	Fork       bool
	CreatedAt  time.Time
}

// OpLogDivergence is an operation log and when its heads last moved other than forward.
type OpLogDivergence struct {
	OpLog      int64
	DivergedAt time.Time
}

// CheckpointTreeIDs is a checkpoint and its trees.
type CheckpointTreeIDs struct {
	ID    int64
	Trees []encoding.ID
}
