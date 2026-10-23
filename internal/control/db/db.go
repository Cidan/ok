// Package db is ok control's storage: its projections of the SCM's repositories and CLs, and what
// review adds to them — reviewers, label grants, comments, each CL's history, favorites. Store is
// the interface every backend implements (postgres, and the conformance suite in dbtest that each
// passes); nothing above it knows which database it runs on.
package db

import (
	"context"
	"strings"
	"time"
)

// Store is ok control's database.
type Store interface {
	Ops
	// Txn runs fn in one read-write transaction, again if the database aborts it to break a
	// deadlock or a conflict; fn has no effects outside the transaction.
	Txn(ctx context.Context, fn func(tx Ops) error) error
}

// Ops are the store's reads and writes. Outside a transaction each is its own; inside Txn they
// are the transaction's.
type Ops interface {
	// UpsertChangeList records the SCM's state of a CL, unless the projection already holds a
	// newer one (the feed and an action can race to record the same CL). The approved
	// projection is left as it is. It reports whether it wrote.
	UpsertChangeList(ctx context.Context, c ChangeList) (bool, error)
	// RollbacksOf lists the rollback CLs that roll back a CL, oldest first.
	RollbacksOf(ctx context.Context, repo, number int64) ([]ChangeList, error)
	// GetChangeList reads a CL's projection.
	GetChangeList(ctx context.Context, repo, number int64) (ChangeList, bool, error)
	// LockChangeList reads a CL's projection and, in a transaction, holds it against every other
	// writer until the transaction ends.
	LockChangeList(ctx context.Context, repo, number int64) (ChangeList, bool, error)
	// ListChangeLists lists a repository's CLs for a finder view, most recently updated first:
	// Open is the open CLs plus the viewer's own drafts; Needs My Review the open CLs the viewer
	// reviews and grants nothing on.
	ListChangeLists(ctx context.Context, repo int64, f Filter, viewer string, limit int) ([]ChangeList, error)
	// ActiveChangeLists lists a repository's drafts and open CLs, by number.
	ActiveChangeLists(ctx context.Context, repo int64) ([]ChangeList, error)
	// Stack is the CL's stack: its ancestors, itself, and its descendants, bottom first, each
	// descendant after its parent.
	Stack(ctx context.Context, repo, number int64) ([]ChangeList, error)
	// LandedBy maps landed commits to the CLs that landed as them.
	LandedBy(ctx context.Context, repo int64, commits [][]byte) (map[string]int64, error)
	// SetApproved stores the approved projection, reporting whether it changed.
	SetApproved(ctx context.Context, repo, number int64, approved bool) (bool, error)

	// LegacyCursor is the last event of the SCM's feed the projection applied before it followed
	// each repository's log (0: none); ForgetLegacyCursor forgets it.
	LegacyCursor(ctx context.Context) (int64, error)
	ForgetLegacyCursor(ctx context.Context) error
	// FollowCursor is the last place in key's log on topic applied (0: none). LockFollowCursor
	// reads it and, in a transaction, holds it against every other writer until the transaction
	// ends: the feed's appliers take it before applying an event. SetFollowCursor records it;
	// SeedFollowCursor sets it if there is none.
	FollowCursor(ctx context.Context, topic string, key int64) (int64, error)
	LockFollowCursor(ctx context.Context, topic string, key int64) (int64, error)
	SetFollowCursor(ctx context.Context, topic string, key, seq int64) error
	SeedFollowCursor(ctx context.Context, topic string, key, seq int64) error

	// CreateRepository records a repository the SCM created. The request that created it and
	// the change-feed follower both record it, often at once, so a repository already recorded —
	// by its ID, or by its name — is left as it is, without an error. The feed applies events in
	// the SCM's order, so a name another repository still holds here (renamed in the SCM, the
	// rename not yet applied) is recorded when the feed reaches the creation.
	CreateRepository(ctx context.Context, r Repository) error
	// RenameRepository records a rename.
	RenameRepository(ctx context.Context, id int64, name string) error
	// MoveHead records main moving forward: its commit count and when.
	MoveHead(ctx context.Context, id, headSeq int64, at time.Time) error
	// RepositoryByName reads a repository's projection by its name.
	RepositoryByName(ctx context.Context, name string) (Repository, bool, error)
	// ListRepositories lists the repositories whose name contains keyword literally (all of them
	// for ""), a page at a time: principal's favorites first, then the most recently updated.
	ListRepositories(ctx context.Context, principal, keyword string, offset, limit int) ([]Repository, error)

	// Reviewers lists a CL's reviewers in the order they were added.
	Reviewers(ctx context.Context, repo, number int64) ([]Reviewer, error)
	// AddReviewer assigns a reviewer, reporting whether they were new.
	AddReviewer(ctx context.Context, repo, number int64, principal, by string) (bool, error)
	// RemoveReviewer takes a reviewer off, reporting whether they were one.
	RemoveReviewer(ctx context.Context, repo, number int64, principal string) (bool, error)
	// Grants lists a CL's label grants and revocations.
	Grants(ctx context.Context, repo, number int64) ([]Grant, error)
	// GrantsOf lists the label grants and revocations of several CLs, by number, principal, and
	// label.
	GrantsOf(ctx context.Context, repo int64, numbers []int64) ([]Grant, error)
	// SetGrant grants or revokes a reviewer's label on a patchset, reporting whether its state
	// changed. Revoking a label never granted changes nothing.
	SetGrant(ctx context.Context, repo, number int64, principal string, label Label, granted bool, patchset int32) (bool, error)

	// Comments lists a CL's comments, oldest first.
	Comments(ctx context.Context, repo, number int64) ([]Comment, error)
	// GetComment reads one of a CL's comments.
	GetComment(ctx context.Context, repo, number, id int64) (Comment, bool, error)
	// CreateComment stores a comment and returns its ID. An inline comment (a Line) needs a
	// Side.
	CreateComment(ctx context.Context, repo int64, c Comment) (int64, error)
	// SetResolved sets a thread root's resolution, reporting whether it changed.
	SetResolved(ctx context.Context, repo, number, rootID int64, resolved bool) (bool, error)
	// DeleteComment deletes a comment and, for a thread root, its replies.
	DeleteComment(ctx context.Context, repo, number, id int64) error
	// UnresolvedThreads counts a CL's unresolved file and inline threads, from every patchset.
	// CL-level comments are messages, not threads.
	UnresolvedThreads(ctx context.Context, repo, number int64) (int64, error)

	// RecordEvent appends to a CL's history. A zero At is now.
	RecordEvent(ctx context.Context, repo, number int64, e Event) error
	// Events lists a CL's history, oldest first.
	Events(ctx context.Context, repo, number int64) ([]Event, error)
	// LastAbandon is a CL's most recent abandonment, if any.
	LastAbandon(ctx context.Context, repo, number int64) (Event, bool, error)

	// Favorites are the repositories a person has favorited.
	Favorites(ctx context.Context, principal string) (map[int64]bool, error)
	// SetFavorite favorites a repository for a person, or stops.
	SetFavorite(ctx context.Context, principal string, repo int64, on bool) error
}

// Status is a CL's lifecycle status; the values are the SCM's.
type Status int16

const (
	StatusDraft     Status = 1
	StatusOpen      Status = 2
	StatusSubmitted Status = 3
	StatusAbandoned Status = 4
)

// Active reports whether a CL in this status takes patchsets and grants.
func (s Status) Active() bool { return s == StatusDraft || s == StatusOpen }

// ChangeList is the projection of an SCM CL, with the approved projection.
type ChangeList struct {
	Repo, Number    int64
	ChangeID        []byte
	Author          string
	Status          Status
	ParentNumber    int64 // 0: not stacked
	CurrentPatchset int32
	Description     string
	LandedCommit    []byte
	SubmittedBy     string
	Approved        bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
	// RollsBack are the CLs a rollback CL rolls back; empty for any other CL.
	RollsBack []int64
}

// Title is the description's first line.
func (c ChangeList) Title() string {
	t, _, _ := strings.Cut(c.Description, "\n")
	return strings.TrimSpace(t)
}

// Filter is a CL finder view.
type Filter string

const (
	FilterOpen        Filter = "open"
	FilterDrafts      Filter = "drafts"
	FilterNeedsReview Filter = "review"
	FilterMerged      Filter = "merged"
	FilterAbandoned   Filter = "abandoned"
	FilterAll         Filter = "all"
)

// Filters are the finder's views, in order.
var Filters = []Filter{FilterOpen, FilterDrafts, FilterNeedsReview, FilterMerged, FilterAbandoned, FilterAll}

// ParseFilter reads a view name; anything unknown is Open.
func ParseFilter(s string) Filter {
	for _, f := range Filters {
		if string(f) == s {
			return f
		}
	}
	return FilterOpen
}

// Repository is the projection of an SCM repository.
type Repository struct {
	ID        int64
	Name      string
	CreatedBy string
	CreatedAt time.Time
	HeadSeq   int64
	UpdatedAt time.Time
	Favorite  bool // for the principal the listing is for
}
