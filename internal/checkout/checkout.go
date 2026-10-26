// Package checkout gives an ok automation worker, or the deployer, a commit's files as a
// directory Bazel works in (docs/automation.md, "Execution"; docs/deploy.md).
//
// A Checkout presents one commit at a time. Virtual presents it through okfs
// (docs/okfs.md, "ok's roles"): the commit's tree is mounted read-only at once, a Sync
// moves it to another commit at once, and each file is read from the SCM the first time
// something reads it, so a fresh service plans or builds without first writing the whole tree.
// The service is started in a user and mount namespace of its own (RequireIsolated), where the
// mount is made.
package checkout

import (
	"context"
	"errors"
	"fmt"
)

// Checkout presents a commit's files as a directory.
type Checkout interface {
	// Sync makes Dir show exactly the commit's files — every file with its contents and
	// executable bit, every symlink, and nothing else — and, at its root beside them until the
	// next Sync, files (by name; nil for none), and says what it did. A commit that cannot be
	// presented (it has conflicts) is a *CommitError.
	Sync(ctx context.Context, repo int64, commit []byte, files map[string][]byte) (Stats, error)
	// Dir is the checkout's directory.
	Dir() string
}

// Stats say what a Sync did.
type Stats struct {
	// Mounted: this Sync mounted the presentation (the first, or the first after the daemon
	// restarted); the others move it.
	Mounted bool
	// ReadAhead and ReadAheadBytes: the files the Sync read ahead of their use, and their bytes —
	// for a real checkout, the files it wrote.
	ReadAhead      int
	ReadAheadBytes int64
	// Written: the Sync wrote the commit's files to disk (a real checkout).
	Written bool
}

func (s Stats) String() string {
	if s.Written {
		return fmt.Sprintf("written; %d files (%d bytes) changed", s.ReadAhead, s.ReadAheadBytes)
	}
	how := "moved"
	if s.Mounted {
		how = "mounted"
	}
	return fmt.Sprintf("presented (%s); %d files (%d bytes) read ahead", how, s.ReadAhead, s.ReadAheadBytes)
}

// CommitError is a commit that has no checkout, because of what it is: a job on it fails.
type CommitError struct{ Message string }

func (e *CommitError) Error() string { return e.Message }

// Kind is what a leaf of a tree is.
type Kind int

const (
	File Kind = iota + 1
	Symlink
)

// ID is a file's content ID: BLAKE3 of its kind byte and contents.
type ID [32]byte

// Entry is one leaf of a commit's tree.
type Entry struct {
	Path       string // slash-separated, relative to the tree's root
	Kind       Kind
	ID         ID // a file's
	Executable bool
	Size       int64  // a file's
	Target     string // a symlink's
}

// Source is what ok's services read from the SCM directly, beside a checkout: a commit's
// leaves, and files' contents (the workflow files a plan reads). A read the SCM broke off — it
// restarted, or the connection was lost — returns an error that is ErrInterrupted: reading again
// can succeed.
type Source interface {
	// Entries lists a commit's leaves.
	Entries(ctx context.Context, repo int64, commit []byte) ([]Entry, error)
	// ReadFiles reads files' contents: each file's bytes, in order and in pieces, through
	// write, the last piece with last set. A file ReadFiles does not have is an error.
	ReadFiles(ctx context.Context, repo int64, ids []ID, write func(id ID, data []byte, last bool) error) error
	// ReadFilesFrom reads files as ReadFiles does, each with a base the caller holds, which the
	// SCM may send it as a delta against (docs/scm.md, "Deltas").
	ReadFilesFrom(ctx context.Context, repo int64, reads []Read, write func(id ID, data []byte, last bool) error) error
	// Changes is what a commit's tree changes from another — every leaf added, changed, or gone,
	// by path — and the commit's tree. From a tree it cannot compare (none, or one it holds no
	// checkout of), it is every leaf of the commit, and whole.
	Changes(ctx context.Context, repo int64, from ID, commit []byte) (tree ID, changes []Change, whole bool, err error)
}

// Change is a leaf a commit's tree changes: New, or nil where the path holds no leaf now.
type Change struct {
	Path string
	New  *Entry
}

// ErrInterrupted is a read the SCM broke off.
var ErrInterrupted = errors.New("the SCM broke off the read")
