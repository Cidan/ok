package store

import (
	"context"
	"time"
)

// Inboxes (docs/hosted.md, "ok control at scale, as built"): each CL's entries for its author and
// reviewers, as the cell that holds it last set them, so a person's inbox spans every cell.

// InboxRelation is how a person stands to a CL in their inbox.
type InboxRelation int

const (
	Authored  InboxRelation = 1
	Reviewing InboxRelation = 2
)

// InboxEntry is a CL in a person's inbox.
type InboxEntry struct {
	Principal  string
	Repository int64
	Number     int64
	Relation   InboxRelation
	// Attention: the CL waits on them.
	Attention bool
	// RepositoryName is its full name; Title its CL's; Status its CL's (draft, open, submitted,
	// abandoned).
	RepositoryName string
	Title          string
	Status         string
	UpdatedAt      time.Time
}

// ActiveStatus reports whether an entry's CL is still under way.
func ActiveStatus(s string) bool { return s == "draft" || s == "open" }

// SetInbox sets a CL's every inbox entry at a version, and reports whether it did: an older
// version than the directory holds changes nothing.
func (s *Store) SetInbox(ctx context.Context, repo, number, version int64, entries []InboxEntry) (bool, error) {
	for _, e := range entries {
		if e.Repository != repo || e.Number != number || e.Principal == "" || (e.Relation != Authored && e.Relation != Reviewing) {
			return false, invalid("an inbox entry is of its CL, names its person, and is authored or reviewing")
		}
	}
	applied := false
	err := s.txn(ctx, func(tx Ops) error {
		had, err := tx.InboxVersion(ctx, repo, number)
		if err != nil {
			return err
		}
		if applied = version > had; !applied {
			return nil
		}
		return tx.ReplaceInbox(ctx, repo, number, version, entries)
	})
	return applied, err
}

// Inbox are up to limit of a person's entries, those with attention first, then the latest changed.
func (s *Store) Inbox(ctx context.Context, principal string, relation InboxRelation, closed bool, limit int) ([]InboxEntry, error) {
	return s.b.InboxOf(ctx, principal, relation, closed, limit)
}
