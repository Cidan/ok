package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/follow"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/tenancy"
	"github.com/Cidan/ok/internal/work"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// The directory's log of repositories' entries (docs/hosted.md, "Tenancy, as built"): each change
// to an entry that stands — made, renamed, transferred, its settings or grants changed, deleted,
// restored, purged — is an event in that repository's log, written in the change's transaction,
// which the relay publishes on the bus's topic dirfeed.Topic for ok's roles to follow.

// DirectoryEventRow is one event of the log, encoded.
type DirectoryEventRow struct {
	Repository int64
	Seq        int64
	Body       []byte
}

// DirectoryHead is how far a repository's log reaches.
type DirectoryHead struct{ Repository, Seq int64 }

// relayPage bounds the events the relay publishes at once; dirPage those a read returns.
const (
	relayPage = 500
	dirPage   = 500
)

// EntryPB is a repository's entry as the directory shows it: its owner's handle and its full name,
// as they are now in q.
func (s *Store) EntryPB(ctx context.Context, q Ops, r Repository) (*identitypb.RepositoryEntry, error) {
	owner, full, err := s.fullName(ctx, q, r.Owner, r.Name)
	if err != nil {
		return nil, err
	}
	return &identitypb.RepositoryEntry{Id: r.ID, OwnerId: r.Owner, Owner: owner, Name: r.Name, FullName: full, Visibility: r.Visibility.String(),
		Propose: r.Propose.String(), ParentId: r.Parent, NetworkId: r.Network, Cell: s.CellOf(r), State: string(r.State),
		StateAt: timestamppb.New(r.StateAt), CreatedById: r.CreatedBy, CreatedAt: timestamppb.New(r.CreatedAt)}, nil
}

// fullName is an owner's handle, and the full name of its repository called name.
func (s *Store) fullName(ctx context.Context, q Ops, ownerID, name string) (handle, full string, err error) {
	p, err := q.PrincipalByID(ctx, ownerID)
	if err != nil {
		return "", "", fmt.Errorf("repository owner %s: %w", ownerID, err)
	}
	handle, _ = s.names.HandleOf(p.Name)
	return handle, tenancy.FullName(handle, name, ownerID == s.installation), nil
}

// logRepository appends an event to a repository's log in tx: what happened, the entry after it
// (nil when purged), and, renamed or transferred, the owner and name it had and its full name then
// — which, when ev does not say it, is the old owner's handle now and the old name.
func (s *Store) logRepository(ctx context.Context, tx Ops, repo int64, after *Repository, ev logged) error {
	seq, err := tx.NextDirectorySeq(ctx, repo)
	if err != nil {
		return err
	}
	at := s.now()
	out := &identitypb.DirectoryEvent{RepositoryId: repo, Seq: seq, At: timestamppb.New(at), Kind: ev.kind, OldOwnerId: ev.oldOwner,
		OldName: ev.oldName, OldFullName: ev.oldFullName}
	if ev.oldName != "" && ev.oldFullName == "" {
		if _, out.OldFullName, err = s.fullName(ctx, tx, ev.oldOwner, ev.oldName); err != nil {
			return err
		}
	}
	if after != nil {
		if out.Repository, err = s.EntryPB(ctx, tx, *after); err != nil {
			return err
		}
	}
	body, err := proto.Marshal(out)
	if err != nil {
		return err
	}
	if err := tx.InsertDirectoryEvent(ctx, DirectoryEventRow{Repository: repo, Seq: seq, Body: body}, at); err != nil {
		return err
	}
	// Its relay's job, enqueued as the transaction is about to commit, publishes it.
	return tx.PendStep(ctx, relayKind, repo, time.Time{}, binary.BigEndian.AppendUint64(nil, uint64(seq)))
}

// txn runs fn in a transaction that may log events: each is published by its relay's job.
func (s *Store) txn(ctx context.Context, fn func(tx Ops) error) error { return s.b.Txn(ctx, fn) }

// DirectoryEvents returns up to a page of a repository's events after a place in its log.
func (s *Store) DirectoryEvents(ctx context.Context, repo, after int64) ([]DirectoryEventRow, error) {
	return s.b.DirectoryEvents(ctx, repo, after, dirPage)
}

// DirectoryHeads returns how far each repository's log reaches, for up to a page of repositories
// after one, by repository.
func (s *Store) DirectoryHeads(ctx context.Context, after int64) ([]DirectoryHead, error) {
	return s.b.DirectoryHeads(ctx, after, dirPage)
}

// relayKind is the step that publishes an event the directory logged: a job its transaction
// enqueued as it was about to commit, naming its repository and its place.
const relayKind = "relay"

// commitWithin is how long a relay's job waits for the transaction that enqueued it to commit
// before it takes the transaction as one that never did.
const commitWithin = time.Minute

// relayUpgrade takes up, once, the events a release before relays' jobs left unpublished.
const relayUpgrade = "relay-upgrade"

// Relay relays the log's events to its followers' queues on q (follow.Relay), as their relays'
// jobs come, until ctx ends: every replica runs one, each taking a page no other holds. Once, it
// relays what a release before left pending.
func (s *Store) Relay(ctx context.Context, q bus.Queue) error {
	w := s.b.Work()
	var wg sync.WaitGroup
	wg.Go(func() { work.Consume(ctx, w, relayKind, 2, time.Second, s.relayStep(q), nil) })
	wg.Go(func() {
		if err := work.Once(ctx, w, relayUpgrade, func(ctx context.Context) error { return s.relayAll(ctx, q, 0) }, nil); err != nil && ctx.Err() == nil {
			slog.Warn("identity: publishing what a release before left", slog.Any("err", err))
		}
	})
	wg.Wait()
	return ctx.Err()
}

// relayStep publishes a repository's pending events, done once the job's event is published — by
// it, or by another relay, which holds it meanwhile. A job whose event is not placed may beat its
// transaction's commit: looked at again soon, until it is a minute old.
func (s *Store) relayStep(q bus.Queue) work.Step {
	return func(ctx context.Context, repo int64, body []byte, age time.Duration) error {
		if len(body) != 8 {
			return nil
		}
		seq := int64(binary.BigEndian.Uint64(body))
		written, relayed, err := s.b.DirectoryEventRelayed(ctx, repo, seq)
		switch {
		case err != nil:
			return err
		case !written && age < commitWithin:
			return work.Later{After: min(max(age, 20*time.Millisecond), 2*time.Second)}
		case !written || relayed:
			return nil
		}
		if err := s.relayAll(ctx, q, repo); err != nil {
			return err
		}
		if _, relayed, err = s.b.DirectoryEventRelayed(ctx, repo, seq); err != nil || relayed {
			return err
		}
		return work.Later{After: time.Second} // another relay holds it
	}
}

// relayAll relays repo's pending events — every repository's, for 0 — a page at a time, to the
// followers registered before it takes them: one registered after catches up, as it first starts,
// past every event placed before it registered (follow.Follower). The followers are read before a
// page is taken, which on PostgreSQL holds its connection.
func (s *Store) relayAll(ctx context.Context, q bus.Queue, repo int64) error {
	for {
		followers, err := s.b.Followers(ctx, dirfeed.Topic)
		if err != nil {
			return err
		}
		n, err := s.b.RelayDirectoryEvents(ctx, repo, relayPage, func(rows []DirectoryEventRow) error {
			out := make([]bus.Event, len(rows))
			for i, r := range rows {
				out[i] = bus.Event{Topic: dirfeed.Topic, Key: r.Repository, Seq: r.Seq}
			}
			return follow.Relay(ctx, q, dirfeed.Topic, followers, out)
		})
		if err != nil || n < relayPage {
			return err
		}
	}
}

// AddFollower records a role as one of the directory's events' followers (DirectoryEvents.Follow).
func (s *Store) AddFollower(ctx context.Context, name string) error {
	return s.b.AddFollower(ctx, dirfeed.Topic, name)
}
