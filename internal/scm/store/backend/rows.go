package backend

import (
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// Repository is a repository's row.
type Repository struct {
	ID        int64
	Name      string
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

// ContentLog is one content log row.
type ContentLog struct {
	ID   int64
	Body []byte
}

// Pack is a pack's row: its number, size, and the dictionary that last considered it (nil:
// none).
type Pack struct {
	ID         encoding.ID
	Num        uint64
	Size       int64
	Dictionary *int64
}

// RetiredPack is a retired pack.
type RetiredPack struct {
	Repo int64
	ID   encoding.ID
	Num  uint64
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

// ChangeList is a CL's row, with its patchsets when a read asks for them.
type ChangeList struct {
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

// Patchset is a patchset's row.
type Patchset struct {
	CL         int64
	Number     int32
	Commit     encoding.ID
	Base       encoding.ID
	Parent     int64 // 0: not stacked
	UploadedBy string
	UploadedAt time.Time
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
