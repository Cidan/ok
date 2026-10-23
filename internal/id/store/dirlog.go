package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/tenancy"
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
	return tx.InsertDirectoryEvent(ctx, DirectoryEventRow{Repository: repo, Seq: seq, Body: body}, at)
}

// txn runs fn in a transaction that may log events, and wakes this replica's relay once it
// commits: at worst for nothing, which costs it one look.
func (s *Store) txn(ctx context.Context, fn func(tx Ops) error) error {
	err := s.b.Txn(ctx, fn)
	if err == nil {
		select {
		case s.relayWake <- struct{}{}:
		default:
		}
	}
	return err
}

// DirectoryEvents returns up to a page of a repository's events after a place in its log.
func (s *Store) DirectoryEvents(ctx context.Context, repo, after int64) ([]DirectoryEventRow, error) {
	return s.b.DirectoryEvents(ctx, repo, after, dirPage)
}

// DirectoryHeads returns how far each repository's log reaches, for up to a page of repositories
// after one, by repository.
func (s *Store) DirectoryHeads(ctx context.Context, after int64) ([]DirectoryHead, error) {
	return s.b.DirectoryHeads(ctx, after, dirPage)
}

// relayPoll is how often a caught-up relay looks for events another replica logged.
const relayPoll = time.Second

// Relay publishes the log's pending events on stream until ctx ends: at once for what this replica
// logs, and within relayPoll for another's. Every replica runs one; each takes a page no other
// holds.
func (s *Store) Relay(ctx context.Context, stream bus.Stream) error {
	for {
		n, err := s.b.RelayDirectoryEvents(ctx, relayPage, func(rows []DirectoryEventRow) error {
			out := make([]bus.Event, len(rows))
			for i, r := range rows {
				out[i] = bus.Event{Topic: dirfeed.Topic, Key: r.Repository, Seq: r.Seq, Body: r.Body}
			}
			return stream.Publish(ctx, out...)
		})
		if err != nil && ctx.Err() == nil {
			slog.Warn("identity: relaying the directory's events", slog.Any("err", err))
		}
		if n == relayPage && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.relayWake:
		case <-time.After(relayPoll):
		}
	}
}
