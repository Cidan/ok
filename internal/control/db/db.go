// Package db is ok control's storage: its projections of the SCM's repositories and CLs, and what
// review adds to them — reviewers, label grants, comments, each CL's history, favorites. Store is
// the interface every backend implements (postgres, and the conformance suite in dbtest that each
// passes); nothing above it knows which database it runs on.
package db

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
)

// Store is ok control's database.
type Store interface {
	Ops
	// Txn runs fn in one read-write transaction, again if the database aborts it to break a
	// deadlock or a conflict; fn has no effects outside the transaction.
	Txn(ctx context.Context, fn func(tx Ops) error) error
	// Moves are its rows about repositories, for a move between cells (docs/hosted.md, "Cells, as
	// built"); every write transaction about a repository named in ctx (move.WithRepository)
	// reads its gate first.
	Moves() move.Rows
	// Work is its keyed work's store: the outboxes' draining (OutboxWork).
	Work() work.Store
	// Waits are the live requests waiting on its subjects (internal/wait).
	Waits() wait.Store
}

// Ops are the store's reads and writes. Outside a transaction each is its own; inside Txn they
// are the transaction's.
type Ops interface {
	// PendStep has a step taken once the transaction commits (work.Pend): the re-derivation of a
	// CL's approval its events changed. Only in a transaction.
	PendStep(ctx context.Context, kind string, key int64, at time.Time, body []byte) error
	// UpsertChangeList records the SCM's state of a CL, unless the projection already holds it or a
	// newer one (the feed and an action can race to record the same CL); one it holds already
	// (ChangeList.Holds) — what a fresh read of an unchanged CL finds, a watch's every status — is
	// read, not locked and written. The approved projection is left as it is. It reports whether
	// it wrote.
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

	// FollowCursor is the last place in key's log on topic applied (0: none). LockFollowCursor
	// reads it and, in a transaction, holds it against every other writer until the transaction
	// ends: the feed's appliers take it before applying an event. SetFollowCursor records it.
	FollowCursor(ctx context.Context, topic string, key int64) (int64, error)
	// FollowCursors are FollowCursor's of keys, at once; a key with none is absent.
	FollowCursors(ctx context.Context, topic string, keys []int64) (map[int64]int64, error)
	LockFollowCursor(ctx context.Context, topic string, key int64) (int64, error)
	SetFollowCursor(ctx context.Context, topic string, key, seq int64) error

	// CreateRepository records a repository the SCM created. The request that created it, the
	// change-feed follower, and the directory's follower each record it, often at once, so a
	// repository already recorded is left as it is, without an error — but for its name, which
	// the change feed does not know: one recorded with none takes the name given here, the one it
	// was created with, so the request that created it lists it at once. Its name after is the
	// directory's (NameRepository).
	CreateRepository(ctx context.Context, r Repository) error
	// NameRepository records a repository as the directory's events say it is: its full name, and
	// whether it is deleted — recording it, from its creator and creation, if it is not.
	NameRepository(ctx context.Context, r Repository) error
	// DropRepository forgets a repository the directory purged: its CLs — their reviewers, labels,
	// comments, and events — its favorites, and its row.
	DropRepository(ctx context.Context, id int64) error
	// MoveHead records main moving forward: its commit count and when.
	MoveHead(ctx context.Context, id, headSeq int64, at time.Time) error
	// RepositoryByName reads a repository's projection by its name.
	RepositoryByName(ctx context.Context, name string) (Repository, bool, error)
	// ListRepositories lists the repositories whose name contains keyword literally (all of them
	// for ""), a page at a time: principal's favorites first, then the most recently updated.
	ListRepositories(ctx context.Context, principal, keyword string, offset, limit int) ([]Repository, error)
	// RepositoriesByID are those of ids this cell holds and has not deleted — their names "" until
	// the directory's events name them — each with whether it is principal's favorite, in no order.
	RepositoriesByID(ctx context.Context, principal string, ids []int64) ([]Repository, error)

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

	// RecordEvent appends to a CL's history. A zero At is now. It and CreateComment put what they
	// record in the repository's outbox, and mark its keyed work (OutboxWork), in what they write.
	RecordEvent(ctx context.Context, repo, number int64, e Event) error
	// Outbox are up to limit of a repository's outbox's entries, oldest first; DoneOutbox removes
	// those done.
	Outbox(ctx context.Context, repo int64, limit int) ([]OutboxEntry, error)
	DoneOutbox(ctx context.Context, repo int64, ids []int64) error
	// GetEvent is one of a CL's events.
	GetEvent(ctx context.Context, repo, number, id int64) (Event, bool, error)
	// QueueHeadMoved puts main's move to a head, the repository's head_seq'th, in its outbox, and
	// marks its work.
	QueueHeadMoved(ctx context.Context, repo, seq int64, head []byte) error

	// ─── Webhooks (docs/hosted.md, "ok control at scale, as built") ───

	// CreateWebhook records a webhook; Webhooks are a repository's, oldest first; DeleteWebhook
	// removes one and its deliveries.
	CreateWebhook(ctx context.Context, w Webhook) error
	Webhooks(ctx context.Context, repo int64) ([]Webhook, error)
	DeleteWebhook(ctx context.Context, repo, id int64) error
	// QueueDeliveries queues deliveries and marks the repository's webhooks' work (WebhookWork).
	QueueDeliveries(ctx context.Context, repo int64, ds []Delivery) error
	// DueDeliveries are up to limit of a repository's waiting deliveries due by a time, oldest
	// first; NextDelivery is when its next waiting one is due (zero: none).
	DueDeliveries(ctx context.Context, repo int64, before time.Time, limit int) ([]Delivery, error)
	NextDelivery(ctx context.Context, repo int64) (time.Time, error)
	// UpdateDelivery writes a delivery's state, attempts, next try, and last answer.
	UpdateDelivery(ctx context.Context, d Delivery) error
	// Deliveries are a webhook's latest, up to limit; GetDelivery is one; PruneDeliveries removes
	// all but its latest keep that are done.
	Deliveries(ctx context.Context, repo, webhook int64, limit int) ([]Delivery, error)
	GetDelivery(ctx context.Context, repo, id int64) (Delivery, bool, error)
	PruneDeliveries(ctx context.Context, repo, webhook int64, keep int) error
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

// Holds reports whether a projection of a CL, c, already holds the SCM's state of it, s: what an
// upsert of s would write, c has (UpsertChangeList writes nothing then).
func (c ChangeList) Holds(s ChangeList) bool {
	return c.Status == s.Status && c.ParentNumber == s.ParentNumber && c.CurrentPatchset == s.CurrentPatchset &&
		c.Description == s.Description && bytes.Equal(c.LandedCommit, s.LandedCommit) && c.SubmittedBy == s.SubmittedBy &&
		c.UpdatedAt.Truncate(time.Microsecond).Equal(s.UpdatedAt.Truncate(time.Microsecond))
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
	Name      string // its full name, as the directory's events say; "" before they do
	Deleted   bool
	CreatedBy string
	CreatedAt time.Time
	HeadSeq   int64
	UpdatedAt time.Time
	Favorite  bool // for the principal the listing is for
}

// OutboxWork is the keyed work that drains a repository's outbox.
const OutboxWork = "outbox"

// OutboxEntry is what waits in a repository's outbox: an event of one of its CLs' histories, or a
// comment.
type OutboxEntry struct {
	ID        int64
	Number    int64 // 0: main's move, of no CL
	QueuedAt  time.Time
	EventID   int64
	CommentID int64
	// HeadSeq and Head are main's move's: the repository's head_seq'th head, and its commit.
	HeadSeq int64
	Head    []byte
}

// WebhookWork is the keyed work that sends a repository's webhooks' deliveries.
const WebhookWork = "webhooks"

// Webhook events, a bit each.
const (
	WebhookCL   = 1 << 0 // a CL's events and comments
	WebhookPush = 1 << 1 // main moved
)

// Webhook is a repository's endpoint for its events.
type Webhook struct {
	Repository int64
	ID         int64
	URL        string
	Events     int64
	Secret     []byte // sealed
	CreatedBy  string
	CreatedAt  time.Time
}

// Delivery states.
const (
	DeliveryWaiting   = 0
	DeliveryDelivered = 1
	DeliveryDead      = 2
)

// Delivery is an event sent, or to be sent, to a webhook.
type Delivery struct {
	Repository int64
	ID         int64
	Webhook    int64
	Event      string // cl, push, ping
	Payload    []byte
	State      int
	Attempts   int
	NextTry    time.Time
	LastStatus int
	LastError  string
	CreatedAt  time.Time
}

// PreAt is the CL whose pre covers a CL's — the top of its stack in review, the lowest numbered of
// them where the stack branches — when that is another CL; 0 otherwise. Pre runs at a stack's
// top, over every CL of it (docs/automation.md, "pre").
func PreAt(ctx context.Context, o Ops, repo, number int64) (int64, error) {
	stack, err := o.Stack(ctx, repo, number)
	if err != nil {
		return 0, err
	}
	// Its descendants follow it, each after its parent.
	var above []ChangeList
	for i, c := range stack {
		if c.Number == number {
			above = stack[i+1:]
			break
		}
	}
	stackedOn := map[int64]bool{} // a CL in review is stacked on it
	for _, c := range above {
		if c.Status == StatusOpen {
			stackedOn[c.ParentNumber] = true
		}
	}
	var top int64
	for _, c := range above {
		if c.Status == StatusOpen && !stackedOn[c.Number] && (top == 0 || c.Number < top) {
			top = c.Number
		}
	}
	return top, nil
}
