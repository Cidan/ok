// Package outbox drains ok control's outboxes (docs/hosted.md, "ok control at scale, as built"):
// each repository's events and comments, oldest first, into what they cause beyond its cell — its
// CLs' inbox entries in the directory, and whatever else a Consumer does — each entry removed once
// all of it is done.
package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/work"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// batch is how many entries a drain takes at once.
const batch = 100

// Consumer does what a repository's outbox entries cause beside inboxes. It may be asked again of
// entries it did — a drain that stopped short — so what it does is idempotent.
type Consumer interface {
	Consume(ctx context.Context, repo Repository, entries []db.OutboxEntry) error
}

// Repository is the repository a drain is of.
type Repository struct {
	ID   int64
	Name string // its full name
}

// Drainer drains outboxes.
type Drainer struct {
	DB       db.Store
	Delivery identitypb.DeliveryClient
	// Tenancy names a repository the directory's events have not named in the projection yet.
	Tenancy identitypb.TenancyClient
	// Consumers do what entries cause beside inboxes: notifications, webhooks.
	Consumers []Consumer
	Log       *slog.Logger
	// Now is the clock inbox versions are read from; unset, time.Now.
	Now func() time.Time
	// Look is how often marked outboxes are looked for: two seconds unless set.
	Look time.Duration
}

// sweepEvery is how often outboxes no mark names are marked.
const sweepEvery = time.Minute

// Run drains outboxes as they are marked until ctx ends; a repository whose outbox has entries no
// mark names — moved in from another cell — is marked every sweepEvery.
func (d *Drainer) Run(ctx context.Context) error {
	look := d.Look
	if look <= 0 {
		look = 2 * time.Second
	}
	var swept time.Time
	return (&work.Runner{
		Kind:  db.OutboxWork,
		Store: d.DB.Work(),
		Do:    d.Drain,
		Sweep: func(ctx context.Context) error {
			if time.Since(swept) < sweepEvery {
				return nil
			}
			swept = time.Now()
			repos, err := d.DB.OutboxRepositories(ctx)
			if err != nil {
				return err
			}
			for _, r := range repos {
				if err := d.DB.Work().Mark(ctx, db.OutboxWork, r); err != nil {
					return err
				}
			}
			return nil
		},
		Idle: look,
		Log:  d.Log,
	}).Run(ctx)
}

// Drain drains a repository's outbox.
func (d *Drainer) Drain(ctx context.Context, repo int64) error {
	ctx = move.WithRepository(ctx, repo)
	for {
		entries, err := d.DB.Outbox(ctx, repo, batch)
		if err != nil || len(entries) == 0 {
			return err
		}
		rs, err := d.DB.RepositoriesByID(ctx, "", []int64{repo})
		if err != nil {
			return err
		}
		if len(rs) == 0 {
			// Deleted: its CLs' inbox entries go with it in the directory's purge.
			return d.DB.DoneOutbox(ctx, repo, ids(entries))
		}
		r := Repository{ID: repo, Name: rs[0].Name}
		if r.Name == "" {
			// The directory's events, which name it here, have not come: the directory names it.
			e, err := d.Tenancy.GetRepositoryEntry(ctx, &identitypb.RepositoryRef{Id: repo})
			if status.Code(err) == codes.NotFound {
				return d.DB.DoneOutbox(ctx, repo, ids(entries))
			}
			if err != nil {
				return fmt.Errorf("the repository's name: %w", err)
			}
			r.Name = e.GetFullName()
		}
		var numbers []int64
		for _, e := range entries {
			if !slices.Contains(numbers, e.Number) {
				numbers = append(numbers, e.Number)
			}
		}
		for _, n := range numbers {
			if err := d.setInbox(ctx, r, n); err != nil {
				return fmt.Errorf("CL %d's inbox: %w", n, err)
			}
		}
		for _, c := range d.Consumers {
			if err := c.Consume(ctx, r, entries); err != nil {
				return err
			}
		}
		if err := d.DB.DoneOutbox(ctx, repo, ids(entries)); err != nil {
			return err
		}
		if len(entries) < batch {
			return nil
		}
	}
}

func ids(es []db.OutboxEntry) []int64 {
	out := make([]int64, 0, len(es))
	for _, e := range es {
		out = append(out, e.ID)
	}
	return out
}

func (d *Drainer) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

// setInbox sets a CL's inbox entries in the directory, as its projection says now.
func (d *Drainer) setInbox(ctx context.Context, r Repository, number int64) error {
	version := d.now().UnixNano()
	cl, ok, err := d.DB.GetChangeList(ctx, r.ID, number)
	if err != nil {
		return err
	}
	req := &identitypb.SetInboxRequest{RepositoryId: r.ID, Number: number, Version: version}
	if ok {
		entries, err := Entries(ctx, d.DB, r, cl)
		if err != nil {
			return err
		}
		req.Entries = entries
	}
	_, err = d.Delivery.SetInbox(ctx, req)
	return err
}

// Entries are a CL's inbox entries: its author's, and each reviewer's. A reviewer's waits on them
// while the CL is open and they have neither granted a label on nor commented on its current
// patchset; its author's, while it is open with unresolved threads.
func Entries(ctx context.Context, ops db.Ops, r Repository, cl db.ChangeList) ([]*identitypb.InboxEntry, error) {
	reviewers, err := ops.Reviewers(ctx, r.ID, cl.Number)
	if err != nil {
		return nil, err
	}
	grants, err := ops.Grants(ctx, r.ID, cl.Number)
	if err != nil {
		return nil, err
	}
	comments, err := ops.Comments(ctx, r.ID, cl.Number)
	if err != nil {
		return nil, err
	}
	unresolved, err := ops.UnresolvedThreads(ctx, r.ID, cl.Number)
	if err != nil {
		return nil, err
	}
	open := cl.Status == db.StatusOpen
	replied := map[string]bool{}
	for _, g := range grants {
		if g.Granted && g.Patchset == cl.CurrentPatchset {
			replied[g.Principal] = true
		}
	}
	for _, c := range comments {
		if c.Patchset == cl.CurrentPatchset {
			replied[c.Author] = true
		}
	}
	entry := func(who string, rel identitypb.InboxEntry_Relation, attention bool) *identitypb.InboxEntry {
		return &identitypb.InboxEntry{PrincipalId: who, RepositoryId: r.ID, Number: cl.Number, Relation: rel, Attention: attention,
			Repository: r.Name, Title: cl.Title(), Status: StatusName(cl.Status), UpdatedAt: timestamppb.New(cl.UpdatedAt)}
	}
	var out []*identitypb.InboxEntry
	if cl.Author != "" {
		out = append(out, entry(cl.Author, identitypb.InboxEntry_RELATION_AUTHORED, open && unresolved > 0))
	}
	for _, rv := range reviewers {
		if rv.Principal == cl.Author {
			continue
		}
		out = append(out, entry(rv.Principal, identitypb.InboxEntry_RELATION_REVIEWING, open && !replied[rv.Principal]))
	}
	return out, nil
}

// StatusName is a CL's status as inboxes name it.
func StatusName(s db.Status) string {
	switch s {
	case db.StatusDraft:
		return "draft"
	case db.StatusOpen:
		return "open"
	case db.StatusSubmitted:
		return "submitted"
	case db.StatusAbandoned:
		return "abandoned"
	}
	return ""
}
