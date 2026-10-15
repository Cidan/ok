package encoding

// The object types mirror jj's stored structs field for field (jj-lib 0.45.1:
// lib/src/backend.rs and lib/src/op_store.rs), because the stores must round-trip exactly
// what jj hands them.

// Timestamp is jj's Timestamp.
type Timestamp struct {
	// Millis is milliseconds since the Unix epoch.
	Millis int64
	// TZOffset is the time zone's offset from UTC in minutes.
	TZOffset int32
}

// Signature is jj's Signature: who authored or committed a commit, and when.
type Signature struct {
	Name  string
	Email string
	Time  Timestamp
}

// Merge is jj's Merge: an odd number of terms in jj's order, alternating adds and removes
// and starting and ending with an add (add₀, remove₀, add₁, …, addₙ). A resolved value is
// a single term.
type Merge[T any] []T

// File is a file's raw contents.
type File struct {
	Contents []byte
}

func (*File) Kind() Kind { return KindFile }

// Symlink is a symlink's target.
type Symlink struct {
	Target string
}

func (*Symlink) Kind() Kind { return KindSymlink }

// Chunk is a stored piece of a file's contents.
type Chunk struct {
	Contents []byte
}

func (*Chunk) Kind() Kind { return KindChunk }

// TreeValueKind tags a tree value with jj's TreeValue ordinal. jj's GitSubmodule
// (ordinal 3) has no encoding.
type TreeValueKind uint8

const (
	TreeValueFile    TreeValueKind = 0
	TreeValueSymlink TreeValueKind = 1
	TreeValueTree    TreeValueKind = 2
)

// TreeValue is jj's TreeValue: what a tree entry names.
type TreeValue struct {
	Kind TreeValueKind
	// ID is the file, symlink, or tree.
	ID ID
	// Executable is set only on files.
	Executable bool
	// CopyID is the file's copy history, or nil for jj's empty placeholder copy ID. It is
	// set only on files.
	CopyID *ID
}

// TreeEntry is one name in a tree.
type TreeEntry struct {
	Name  string
	Value TreeValue
}

// Tree is jj's Tree: a directory's entries, sorted by name bytes with no duplicates.
type Tree struct {
	Entries []TreeEntry
}

func (*Tree) Kind() Kind { return KindTree }

// CopyHistory is jj's CopyHistory.
type CopyHistory struct {
	// CurrentPath is the file's path, its components separated by '/'.
	CurrentPath string
	Parents     []ID
	Salt        []byte
}

func (*CopyHistory) Kind() Kind { return KindCopyHistory }

// Commit is jj's Commit.
type Commit struct {
	Parents []ID
	// Predecessors are recorded but weak: they distinguish otherwise identical rewrites
	// and are never closure-checked or followed by GC.
	Predecessors   []ID
	RootTree       Merge[ID]
	ConflictLabels Merge[string]
	ChangeID       ChangeID
	Description    string
	Author         Signature
	Committer      Signature
	// SecureSig is the commit's signature over SignedData, or nil when the commit is
	// unsigned. jj's SecureSig pairs it with the signed data, which is rebuilt from the
	// commit rather than stored.
	SecureSig []byte
}

func (*Commit) Kind() Kind { return KindCommit }

// RefTarget is jj's RefTarget: a merge of optional commit IDs, where nil is absent.
type RefTarget = Merge[*ID]

// RemoteRefState is jj's RemoteRefState.
type RemoteRefState uint8

const (
	RemoteRefNew     RemoteRefState = 0
	RemoteRefTracked RemoteRefState = 1
)

// RemoteRef is jj's RemoteRef.
type RemoteRef struct {
	Target RefTarget
	State  RemoteRefState
}

// RemoteView is jj's RemoteView.
type RemoteView struct {
	Bookmarks map[string]RemoteRef
	Tags      map[string]RemoteRef
}

// View is jj's View.
type View struct {
	// HeadIDs is a set. Encode accepts it in any order; Decode returns it sorted.
	HeadIDs        []ID
	LocalBookmarks map[string]RefTarget
	LocalTags      map[string]RefTarget
	RemoteViews    map[string]RemoteView
	GitRefs        map[string]RefTarget
	GitHeads       map[string]RefTarget
	// WCCommitIDs maps each workspace name to its working-copy commit.
	WCCommitIDs map[string]ID
}

func (*View) Kind() Kind { return KindView }

// OperationMetadata is jj's OperationMetadata.
type OperationMetadata struct {
	Start       Timestamp
	End         Timestamp
	Description string
	Hostname    string
	Username    string
	IsSnapshot  bool
	// WorkspaceName is the workspace the operation ran in, or nil.
	WorkspaceName *string
	Attributes    map[string]string
}

// Operation is jj's Operation.
type Operation struct {
	ViewID   ID
	Parents  []ID
	Metadata OperationMetadata
	// CommitPredecessors maps each commit the operation created or rewrote to its
	// predecessors. nil means the operation did not record them (jj's None); an empty,
	// non-nil map means it recorded none.
	CommitPredecessors map[ID][]ID
}

func (*Operation) Kind() Kind { return KindOperation }
