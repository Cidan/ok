package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/tenancy"
)

// Deleting accounts and organizations (docs/hosted.md, "Deleting, renaming, transferring"): soft
// for RetentionPeriod — the account or organization suspended, its repositories deleted, and both
// restorable — then for good, as housekeeping finds it due: its handle retired, its memberships,
// teams, credentials, tokens, and ways to sign in gone; an organization's robots, apps, and
// identity provider with it, and the people its provider made.

// Deletion is a principal's deletion, and who asked for it.
type Deletion struct {
	Principal string
	Requested time.Time
	By        string
}

// ErrDeleting is an account or organization deleted already, within its retention period.
var ErrDeleting = errors.New("it is being deleted: restore it first")

// DeleteAccount deletes a person's account, by by — themselves, or an administrator: suspended,
// its sessions ended, its repositories deleted, restorable for RetentionPeriod. A person who is
// some organization's only owner is refused: that organization needs another owner first, or to be
// deleted. A person an identity provider made is its to delete.
func (s *Store) DeleteAccount(ctx context.Context, principal, by string) error {
	p, err := s.b.PrincipalByID(ctx, principal)
	if err != nil {
		return err
	}
	if p.Kind != Human {
		return invalid("%s is no person's account", p.Name)
	}
	if p.ExternalID != "" {
		return invalid("%s comes from the system of record, which deletes it", p.Name)
	}
	if org, _, err := s.b.PersonOrganization(ctx, principal); err == nil {
		o, _ := s.b.PrincipalByID(ctx, org)
		return invalid("%s is %s's identity provider's to delete", p.Name, o.Name)
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	err = s.txn(ctx, func(tx Ops) error {
		if _, err := tx.LockPrincipalByName(ctx, p.Name); err != nil {
			return err
		}
		ms, err := tx.MembershipsOf(ctx, principal)
		if err != nil {
			return err
		}
		for org, role := range ms {
			if role != tenancy.Owner {
				continue
			}
			members, err := tx.Members(ctx, org)
			if err != nil {
				return err
			}
			owners := 0
			for _, m := range members {
				if m.Role == tenancy.Owner {
					owners++
				}
			}
			if owners == 1 {
				o, _ := tx.PrincipalByID(ctx, org)
				return fmt.Errorf("%w: %s's only owner: give it another owner, or delete it, first", ErrLastOwner, o.Name)
			}
		}
		return s.beginDeletion(ctx, tx, p, by)
	})
	if err != nil {
		return err
	}
	return s.deleteRepositoriesOf(ctx, principal)
}

// DeleteOrganization deletes an organization, by by, one of its owners: suspended, its
// repositories deleted, restorable for RetentionPeriod. The installation's is refused.
func (s *Store) DeleteOrganization(ctx context.Context, org, by string) error {
	if org == s.installation {
		return invalid("the installation's organization is never deleted")
	}
	o, err := s.b.Organization(ctx, org)
	if err != nil {
		return err
	}
	if err := s.txn(ctx, func(tx Ops) error {
		if _, err := tx.LockOrganization(ctx, org); err != nil {
			return err
		}
		return s.beginDeletion(ctx, tx, o.Principal, by)
	}); err != nil {
		return err
	}
	return s.deleteRepositoriesOf(ctx, org)
}

func (s *Store) beginDeletion(ctx context.Context, tx Ops, p Principal, by string) error {
	if !p.Active() {
		if _, err := tx.DeletionOf(ctx, p.ID); err == nil {
			return ErrDeleting
		}
		return invalid("%s is not active", p.Name)
	}
	if err := tx.InsertDeletion(ctx, Deletion{Principal: p.ID, Requested: s.now(), By: by}); errors.Is(err, ErrTaken) {
		return ErrDeleting
	} else if err != nil {
		return err
	}
	if err := tx.SetStatus(ctx, p.ID, directory.Suspended); err != nil {
		return err
	}
	return tx.DeleteSessionsOf(ctx, p.ID)
}

// deleteRepositoriesOf deletes every active repository an owner has, each as DeleteRepository
// does.
func (s *Store) deleteRepositoriesOf(ctx context.Context, owner string) error {
	repos, err := s.b.RepositoriesOf(ctx, owner)
	if err != nil {
		return err
	}
	for _, r := range repos {
		if r.State != Active {
			continue
		}
		if _, err := s.DeleteRepository(ctx, r.ID); err != nil && !errors.Is(err, ErrRepositoryState) {
			return err
		}
	}
	return nil
}

// RestoreAccount restores an account deleted within RetentionPeriod, and the repositories its
// deletion deleted; RestoreOrganization an organization likewise.
func (s *Store) RestoreAccount(ctx context.Context, principal string) error {
	return s.restore(ctx, principal)
}

func (s *Store) RestoreOrganization(ctx context.Context, org string) error {
	return s.restore(ctx, org)
}

func (s *Store) restore(ctx context.Context, id string) error {
	d, err := s.b.DeletionOf(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return invalid("it is not being deleted")
	}
	if err != nil {
		return err
	}
	if s.now().Sub(d.Requested) > RetentionPeriod {
		return fmt.Errorf("%w: deleted on %s, past the retention period", ErrRepositoryState, d.Requested.Format(time.DateOnly))
	}
	if err := s.txn(ctx, func(tx Ops) error {
		ok, err := tx.RemoveDeletion(ctx, id)
		if err != nil || !ok {
			return err
		}
		return tx.SetStatus(ctx, id, directory.Active)
	}); err != nil {
		return err
	}
	repos, err := s.b.RepositoriesOf(ctx, id)
	if err != nil {
		return err
	}
	for _, r := range repos {
		if r.State == Deleted && !r.StateAt.Before(d.Requested) {
			if _, err := s.RestoreRepository(ctx, r.ID); err != nil && !errors.Is(err, ErrRepositoryState) {
				return err
			}
		}
	}
	return nil
}

// DeletionOf is a principal's deletion; ErrNotFound for none.
func (s *Store) DeletionOf(ctx context.Context, principal string) (Deletion, error) {
	return s.b.DeletionOf(ctx, principal)
}

// FinishDeletions finishes, for good, the deletions past RetentionPeriod, up to limit, and
// returns how many it finished. Each replica may run it: a deletion is finished by one.
func (s *Store) FinishDeletions(ctx context.Context, limit int) (int, error) {
	n := 0
	for n < limit {
		var d Deletion
		var found bool
		err := s.txn(ctx, func(tx Ops) error {
			due, err := tx.DueDeletions(ctx, s.now().Add(-RetentionPeriod), 1)
			if err != nil || len(due) == 0 {
				return err
			}
			d, found = due[0], true
			return s.finish(ctx, tx, d.Principal)
		})
		if err != nil {
			return n, fmt.Errorf("finishing %s's deletion: %w", d.Principal, err)
		}
		if !found {
			return n, nil
		}
		n++
	}
	return n, nil
}

// finish finishes a principal's deletion for good, in tx.
func (s *Store) finish(ctx context.Context, tx Ops, id string) error {
	p, err := tx.PrincipalByID(ctx, id)
	if err != nil {
		return err
	}
	if p.Kind == Org {
		if err := s.finishOrganization(ctx, tx, p); err != nil {
			return err
		}
	} else if err := s.finishPerson(ctx, tx, p, "the account was deleted"); err != nil {
		return err
	}
	_, err = tx.RemoveDeletion(ctx, id)
	return err
}

// finishPerson tombstones a person: retired, credentials and tokens revoked, sessions and ways to
// sign in gone, out of every organization and team. Their repositories were deleted with the
// account, and are purged on their own time.
func (s *Store) finishPerson(ctx context.Context, tx Ops, p Principal, why string) error {
	if err := tx.SetStatus(ctx, p.ID, directory.Deleted); err != nil {
		return err
	}
	for _, step := range []func() error{
		func() error { return tx.DeleteSessionsOf(ctx, p.ID) },
		func() error { return tx.RevokeCredentialsOf(ctx, p.ID) },
		func() error { return tx.RevokeTokensOf(ctx, p.ID, s.now()) },
		func() error { return tx.DeleteSignInsOf(ctx, p.ID) },
		func() error { return tx.DeleteMembershipsOf(ctx, p.ID) },
		func() error { return tx.RemoveFromTeams(ctx, p.ID) },
		func() error { return tx.RetireName(ctx, p.Name, why) },
	} {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

// finishOrganization tombstones an organization: its teams (innermost first), robots, apps and
// their installations, the installations of others' apps in it, its identity provider and issuers,
// the people its provider made, and its memberships gone, its handle retired.
func (s *Store) finishOrganization(ctx context.Context, tx Ops, o Principal) error {
	teams, err := tx.Teams(ctx, o.ID)
	if err != nil {
		return err
	}
	parent := map[string]string{}
	for _, t := range teams {
		parent[t.Name] = t.Parent
	}
	depth := func(name string) int {
		d := 0
		for p := parent[name]; p != "" && d <= len(teams); p = parent[p] {
			d++
		}
		return d
	}
	slices.SortFunc(teams, func(a, b Team) int { return depth(b.Name) - depth(a.Name) })
	for _, t := range teams {
		if err := tx.DeleteTeam(ctx, t.Name); err != nil {
			return err
		}
		repos, err := tx.DeleteGrantsTo(ctx, t.Name)
		if err != nil {
			return err
		}
		for _, id := range repos {
			if err := s.logGrants(ctx, tx, id); err != nil {
				return err
			}
		}
		if err := tx.RetireName(ctx, t.Name, "its organization was deleted"); err != nil {
			return err
		}
	}
	installations, err := tx.Installations(ctx, o.ID)
	if err != nil {
		return err
	}
	for _, i := range installations {
		if _, err := tx.DeleteInstallation(ctx, i.ID); err != nil {
			return err
		}
	}
	apps, err := tx.Apps(ctx, o.ID)
	if err != nil {
		return err
	}
	for _, a := range apps {
		if err := tx.DeleteApp(ctx, a.ID); err != nil {
			return err
		}
	}
	robots, err := tx.OrganizationRobots(ctx, o.ID)
	if err != nil {
		return err
	}
	for _, r := range robots {
		if r.Status == directory.Deleted {
			continue
		}
		for _, step := range []func() error{
			func() error { return tx.SetStatus(ctx, r.ID, directory.Deleted) },
			func() error { return tx.RevokeCredentialsOf(ctx, r.ID) },
			func() error { return tx.RevokeTokensOf(ctx, r.ID, s.now()) },
			func() error { return tx.DeleteBindingsOf(ctx, r.ID) },
			func() error { return tx.RetireName(ctx, r.Name, "its organization was deleted") },
		} {
			if err := step(); err != nil {
				return err
			}
		}
	}
	issuers, err := tx.Issuers(ctx, o.ID)
	if err != nil {
		return err
	}
	for _, iss := range issuers {
		if _, err := tx.DeleteIssuer(ctx, o.ID, iss); err != nil {
			return err
		}
	}
	if _, err := tx.DeleteIdentityProvider(ctx, o.ID); err != nil {
		return err
	}
	people, err := tx.OrganizationPeople(ctx, o.ID)
	if err != nil {
		return err
	}
	for _, p := range people {
		if p.Status != directory.Deleted {
			if err := s.finishPerson(ctx, tx, p, "its organization was deleted"); err != nil {
				return err
			}
		}
	}
	if err := tx.DeleteMembersOf(ctx, o.ID); err != nil {
		return err
	}
	if err := tx.SetStatus(ctx, o.ID, directory.Deleted); err != nil {
		return err
	}
	return tx.RetireName(ctx, o.Name, "the organization was deleted")
}
