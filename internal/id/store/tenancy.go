package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/dirfeed"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// The directory's tenancy (docs/hosted.md, "Tenancy, as built"): organizations, their members and
// teams, every repository's entry and the names it had, and the roles granted on it. Who may do
// each is the service's to decide; these are the rules of the data itself.

// Org is an organization: an account that owns repositories, named <handle>.org.<domain>, whose
// handle no person may hold, nor it a person's. It acts as no one.
const Org Kind = "org"

// RetentionPeriod is how long a deleted repository can be restored, and CreatingTimeout how long an
// entry may wait for its cell to make its storage before it is abandoned: its cell purges each then.
const (
	RetentionPeriod = dirfeed.RetentionPeriod
	CreatingTimeout = dirfeed.CreatingTimeout
)

// Errors of the tenancy's rules.
var (
	// ErrLastOwner is a change that would leave an organization without an owner.
	ErrLastOwner = errors.New("an organization keeps at least one owner")
	// ErrRepositoryState is a change to a repository its state refuses: a rename of one being
	// created, a restore of one not deleted, a restore past its retention period.
	ErrRepositoryState = errors.New("the repository's state refuses it")
	// ErrInvalid is a request the rules refuse outright.
	ErrInvalid = errors.New("invalid")
)

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Organization is an organization with its settings.
type Organization struct {
	Principal
	// BaseRole is the role every member has on every repository it owns.
	BaseRole tenancy.Role
	// MembersCreate: members may make its repositories; owners always may.
	MembersCreate bool
	// PrivateForks: its private repositories may be forked.
	PrivateForks bool
	// Enterprise is shared by organizations whose members read each other's internal
	// repositories; "" for none.
	Enterprise string
}

// Handle is the organization's handle.
func (o Organization) Handle() string { return o.Name[:strings.IndexByte(o.Name, '.')] }

// Member is a principal's membership of an organization.
type Member struct {
	Principal string // the member's ID
	Role      tenancy.Membership
}

// Team is an organization's team.
type Team struct {
	Name         string   // <team>.<org>.team.<domain>
	Organization string   // the organization's ID
	Parent       string   // the parent team's name; "" for none
	Members      []string // principals' IDs, sorted
	// ExternalID is a synced team's group in its organization's identity provider; its members
	// are the sync's alone. "" for a team the organization's owners keep.
	ExternalID string
}

// RepositoryState is where a repository's entry stands.
type RepositoryState string

const (
	// Creating: its cell has not made its storage yet.
	Creating RepositoryState = "creating"
	// Active: it stands.
	Active RepositoryState = "active"
	// Deleted: soft-deleted, restorable for RetentionPeriod.
	Deleted RepositoryState = "deleted"
)

// Repository is a repository's entry.
type Repository struct {
	ID         int64
	Owner      string // the owning person's or organization's ID
	Name       string
	Visibility tenancy.Visibility
	Propose    tenancy.Role // the role proposing a CL needs: Read or Write
	Parent     int64        // the repository it was forked from; 0 for none
	Network    int64        // its fork network
	Cell       string
	State      RepositoryState
	StateAt    time.Time // when it entered its state
	CreatedBy  string    // who made it, by ID
	CreatedAt  time.Time
}

// Grant is a role on a repository, granted to a principal (by ID) or a set (by name: a team, a
// group).
type Grant struct {
	Repository int64
	Subject    string
	Role       tenancy.Role
}

// handleHeld reports whether a handle is a person's or an organization's, or was: the two share
// one namespace.
func (s *Store) handleHeld(ctx context.Context, tx Ops, handle string) (bool, error) {
	for _, name := range []string{s.names.Human(handle), s.names.Org(handle)} {
		held, err := tx.NameHeld(ctx, name)
		if err != nil || held {
			return held, err
		}
	}
	return false, nil
}

// OrganizationSettings are what CreateOrganization sets.
type OrganizationSettings struct {
	BaseRole      tenancy.Role
	MembersCreate bool
	PrivateForks  bool
	Enterprise    string
}

// CreateOrganization makes an organization with a handle no person or organization has held, owned
// by owner — an active person, by ID — or, with none, by no one yet (an installation's own,
// administered by its administrators).
func (s *Store) CreateOrganization(ctx context.Context, handle, owner string, set OrganizationSettings) (Organization, error) {
	if err := tenancy.ValidHandle(handle); err != nil {
		return Organization{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if set.BaseRole < tenancy.None || set.BaseRole > tenancy.Admin {
		return Organization{}, invalid("base role %d", set.BaseRole)
	}
	var o Organization
	err := s.b.Txn(ctx, func(tx Ops) error {
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		held, err := s.handleHeld(ctx, tx, handle)
		if err != nil {
			return err
		}
		if held {
			return ErrTaken
		}
		if owner != "" {
			if err := activeHuman(ctx, tx, owner); err != nil {
				return err
			}
		}
		p, err := tx.InsertPrincipal(ctx, Principal{ID: newID("prn_"), Name: s.names.Org(handle), Kind: Org, Status: directory.Active})
		if err != nil {
			return err
		}
		o = Organization{Principal: p, BaseRole: set.BaseRole, MembersCreate: set.MembersCreate, PrivateForks: set.PrivateForks, Enterprise: set.Enterprise}
		if err := tx.InsertOrganization(ctx, o); err != nil {
			return err
		}
		if owner != "" {
			return tx.PutMember(ctx, o.ID, owner, tenancy.Owner)
		}
		return nil
	})
	return o, err
}

// EnsureOrganization makes an installation's organization, with no owner, unless it has been made:
// how the one organization of an installation comes to exist.
func (s *Store) EnsureOrganization(ctx context.Context, handle string, set OrganizationSettings) (Organization, error) {
	o, err := s.OrganizationByHandle(ctx, handle)
	if err == nil || !errors.Is(err, ErrNotFound) {
		return o, err
	}
	o, err = s.CreateOrganization(ctx, handle, "", set)
	if errors.Is(err, ErrTaken) {
		// Made by another replica meanwhile, or the handle is a person's.
		if o, err2 := s.OrganizationByHandle(ctx, handle); err2 == nil {
			return o, nil
		}
		return Organization{}, fmt.Errorf("the handle %q is a person's, or was: %w", handle, err)
	}
	return o, err
}

// activeHuman refuses an ID that is not an active person's.
func activeHuman(ctx context.Context, tx Ops, id string) error {
	p, err := tx.PrincipalByID(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return invalid("no principal has the ID %s", id)
	}
	if err != nil {
		return err
	}
	if p.Kind != Human || !p.Active() {
		return invalid("%s is not an active person", p.Name)
	}
	return nil
}

// Organization reads an organization by ID; ErrNotFound for none.
func (s *Store) Organization(ctx context.Context, id string) (Organization, error) {
	return s.b.Organization(ctx, id)
}

// OrganizationByHandle reads an organization by handle; ErrNotFound for none.
func (s *Store) OrganizationByHandle(ctx context.Context, handle string) (Organization, error) {
	p, err := s.b.PrincipalByName(ctx, s.names.Org(handle))
	if err != nil {
		return Organization{}, err
	}
	return s.b.Organization(ctx, p.ID)
}

// SetOrganization changes an organization's settings.
func (s *Store) SetOrganization(ctx context.Context, id string, set OrganizationSettings) (Organization, error) {
	if set.BaseRole < tenancy.None || set.BaseRole > tenancy.Admin {
		return Organization{}, invalid("base role %d", set.BaseRole)
	}
	var o Organization
	err := s.b.Txn(ctx, func(tx Ops) error {
		var err error
		if o, err = tx.Organization(ctx, id); err != nil {
			return err
		}
		o.BaseRole, o.MembersCreate, o.PrivateForks, o.Enterprise = set.BaseRole, set.MembersCreate, set.PrivateForks, set.Enterprise
		return tx.UpdateOrganization(ctx, o)
	})
	return o, err
}

// SetMember makes an active person a member or an owner of an organization, or changes which; an
// organization's last owner stays one.
func (s *Store) SetMember(ctx context.Context, org, principal string, role tenancy.Membership) error {
	if role != tenancy.Member && role != tenancy.Owner {
		return invalid("membership %s", role)
	}
	return s.b.Txn(ctx, func(tx Ops) error {
		if _, err := tx.LockOrganization(ctx, org); err != nil {
			return err
		}
		if err := activeHuman(ctx, tx, principal); err != nil {
			return err
		}
		if role == tenancy.Member {
			if err := keepsAnOwner(ctx, tx, org, principal); err != nil {
				return err
			}
		}
		return tx.PutMember(ctx, org, principal, role)
	})
}

// keepsAnOwner refuses a change that takes principal's ownership from an organization it is the
// last owner of. An organization made with no owner (an installation's) has none to keep.
func keepsAnOwner(ctx context.Context, tx Ops, org, principal string) error {
	ms, err := tx.Members(ctx, org)
	if err != nil {
		return err
	}
	owners, theirs := 0, false
	for _, m := range ms {
		if m.Role == tenancy.Owner {
			owners++
			theirs = theirs || m.Principal == principal
		}
	}
	if theirs && owners == 1 {
		return ErrLastOwner
	}
	return nil
}

// RemoveMember takes a principal out of an organization and every team of it, reporting whether
// they were a member; its last owner stays.
func (s *Store) RemoveMember(ctx context.Context, org, principal string) (bool, error) {
	removed := false
	err := s.b.Txn(ctx, func(tx Ops) error {
		if _, err := tx.LockOrganization(ctx, org); err != nil {
			return err
		}
		if err := keepsAnOwner(ctx, tx, org, principal); err != nil {
			return err
		}
		var err error
		if removed, err = tx.DeleteMember(ctx, org, principal); err != nil || !removed {
			return err
		}
		teams, err := tx.Teams(ctx, org)
		if err != nil {
			return err
		}
		for _, t := range teams {
			if i := slices.Index(t.Members, principal); i >= 0 {
				if err := tx.SetTeamMembers(ctx, t.Name, slices.Delete(slices.Clone(t.Members), i, i+1)); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return removed, err
}

// Members are an organization's members, by ID.
func (s *Store) Members(ctx context.Context, org string) ([]Member, error) {
	return s.b.Members(ctx, org)
}

// MembershipsOf are the organizations a principal is in, by the organization's ID.
func (s *Store) MembershipsOf(ctx context.Context, principal string) (map[string]tenancy.Membership, error) {
	return s.b.MembershipsOf(ctx, principal)
}

// CreateTeam makes an organization's team, under parent (a team of the same organization; "" for
// none), with a name no set has held.
func (s *Store) CreateTeam(ctx context.Context, org, team, parent string) (Team, error) {
	if !identity.ValidLabel(team) {
		return Team{}, invalid("team %q: use a lowercase DNS label", team)
	}
	var t Team
	err := s.b.Txn(ctx, func(tx Ops) error {
		o, err := tx.LockOrganization(ctx, org)
		if err != nil {
			return err
		}
		if parent != "" {
			p, err := tx.Team(ctx, parent)
			if errors.Is(err, ErrNotFound) || (err == nil && p.Organization != org) {
				return invalid("%s is no team of %s", parent, o.Name)
			}
			if err != nil {
				return err
			}
		}
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		name := s.names.Team(team, o.Handle())
		held, err := tx.NameHeld(ctx, name)
		if err != nil {
			return err
		}
		if held {
			return ErrTaken
		}
		t = Team{Name: name, Organization: org, Parent: parent, Members: []string{}}
		return tx.InsertTeam(ctx, t)
	})
	return t, err
}

// Team reads a team by name; ErrNotFound for none.
func (s *Store) Team(ctx context.Context, name string) (Team, error) { return s.b.Team(ctx, name) }

// Teams are an organization's teams, by name.
func (s *Store) Teams(ctx context.Context, org string) ([]Team, error) { return s.b.Teams(ctx, org) }

// AllTeams are every organization's teams, by name: sets, as the directory expands them.
func (s *Store) AllTeams(ctx context.Context) ([]Team, error) { return s.b.AllTeams(ctx) }

// TeamSets are every team as a set: its active members by their names now, and the teams below it
// by theirs, so membership through a team counts its teams' members.
func (s *Store) TeamSets(ctx context.Context) (map[string][]string, error) {
	teams, err := s.b.AllTeams(ctx)
	if err != nil {
		return nil, err
	}
	children := map[string][]string{}
	for _, t := range teams {
		if t.Parent != "" {
			children[t.Parent] = append(children[t.Parent], t.Name)
		}
	}
	recs := make([]SetRecord, len(teams))
	for i, t := range teams {
		recs[i] = SetRecord{Name: t.Name, Source: SetFromTeam, Members: t.Members, Children: children[t.Name], Organization: t.Organization}
	}
	return s.teamMemberNames(ctx, recs)
}

// ownersTeam refuses a change to a synced team, whose members and life are its organization's
// identity provider's.
func ownersTeam(t Team) error {
	if t.ExternalID != "" {
		return invalid("%s is synced from its organization's identity provider, which alone changes it", t.Name)
	}
	return nil
}

// AddTeamMember puts a member of a team's organization in the team.
func (s *Store) AddTeamMember(ctx context.Context, team, principal string) error {
	return s.b.Txn(ctx, func(tx Ops) error {
		t, err := tx.LockTeam(ctx, team)
		if err != nil {
			return err
		}
		if err := ownersTeam(t); err != nil {
			return err
		}
		ms, err := tx.MembershipsOf(ctx, principal)
		if err != nil {
			return err
		}
		if ms[t.Organization] == tenancy.NotMember {
			return invalid("%s is not a member of the team's organization", principal)
		}
		if slices.Contains(t.Members, principal) {
			return nil
		}
		members := append(slices.Clone(t.Members), principal)
		slices.Sort(members)
		return tx.SetTeamMembers(ctx, team, members)
	})
}

// RemoveTeamMember takes a principal out of a team, reporting whether it was in it.
func (s *Store) RemoveTeamMember(ctx context.Context, team, principal string) (bool, error) {
	removed := false
	err := s.b.Txn(ctx, func(tx Ops) error {
		t, err := tx.LockTeam(ctx, team)
		if err != nil {
			return err
		}
		if err := ownersTeam(t); err != nil {
			return err
		}
		i := slices.Index(t.Members, principal)
		if removed = i >= 0; !removed {
			return nil
		}
		return tx.SetTeamMembers(ctx, team, slices.Delete(slices.Clone(t.Members), i, i+1))
	})
	return removed, err
}

// DeleteTeam deletes a team with no team below it: its name retired, the roles granted it gone.
func (s *Store) DeleteTeam(ctx context.Context, team string) error {
	return s.txn(ctx, func(tx Ops) error {
		t, err := tx.LockTeam(ctx, team)
		if err != nil {
			return err
		}
		if err := ownersTeam(t); err != nil {
			return err
		}
		all, err := tx.Teams(ctx, t.Organization)
		if err != nil {
			return err
		}
		for _, c := range all {
			if c.Parent == team {
				return invalid("%s holds %s: delete or move it first", team, c.Name)
			}
		}
		if err := tx.DeleteTeam(ctx, team); err != nil {
			return err
		}
		repos, err := tx.DeleteGrantsTo(ctx, team)
		if err != nil {
			return err
		}
		for _, id := range repos {
			if err := s.logGrants(ctx, tx, id); err != nil {
				return err
			}
		}
		return tx.RetireName(ctx, team, "the team was deleted")
	})
}

// NewRepository is what ReserveRepository makes an entry of.
type NewRepository struct {
	Owner      string // the owning person or organization, by ID
	Name       string
	Visibility tenancy.Visibility
	Propose    tenancy.Role // Read or Write
	Parent     int64        // the repository forked; 0 for none
	Cell       string
	CreatedBy  string
}

// ReserveRepository makes a repository's entry, Creating: a new ID, and a network — its parent's,
// or a new one. A name its owner's repositories hold is refused (ErrTaken); a name only a redirect
// holds is taken from it. Asked again by the same person for the same owner, name, and parent while
// the entry is still Creating — a creation whose answer, or whose storage, was lost with a replica —
// it answers that entry, so the creation resumes rather than stranding the name.
func (s *Store) ReserveRepository(ctx context.Context, n NewRepository) (Repository, error) {
	if err := tenancy.ValidRepositoryName(n.Name); err != nil {
		return Repository{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if n.Propose != tenancy.Read && n.Propose != tenancy.Write {
		return Repository{}, invalid("proposing needs read or write, not %s", n.Propose)
	}
	if n.Cell != "" && len(s.cells) > 0 && !s.knownCell(n.Cell) {
		return Repository{}, invalid("no cell is named %s", n.Cell)
	}
	if n.Cell == "" && n.Parent == 0 {
		cell, err := s.place(ctx, n.Owner)
		if err != nil {
			return Repository{}, err
		}
		n.Cell = cell
	}
	var r Repository
	err := s.b.Txn(ctx, func(tx Ops) error {
		if err := activeOwner(ctx, tx, n.Owner); err != nil {
			return err
		}
		network := data.NewID()
		if n.Parent != 0 {
			p, err := tx.Repository(ctx, n.Parent)
			if errors.Is(err, ErrNotFound) {
				return invalid("no repository has the ID %d", n.Parent)
			}
			if err != nil {
				return err
			}
			if p.State != Active {
				return invalid("repository %d is %s, and is forked only while it stands", p.ID, p.State)
			}
			if p.Visibility == tenancy.Internal && p.Parent != 0 {
				return invalid("repository %d is a fork of an internal repository, which is forked once", p.ID)
			}
			// A fork's visibility is its upstream's (GitHub's rules), and its cell its network's.
			n.Visibility = p.Visibility
			network = p.Network
			n.Cell = s.CellOf(p)
		}
		if held, err := tx.RepositoryByName(ctx, n.Owner, n.Name); err == nil {
			if held.State == Creating && s.expiry(held) == ExpiredCreating {
				return ErrCreationExpired
			}
			if held.State == Creating && held.CreatedBy == n.CreatedBy && held.Parent == n.Parent {
				r = held
				return nil
			}
			return ErrTaken
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		if err := tx.DeleteRedirect(ctx, n.Owner, n.Name); err != nil {
			return err
		}
		var err error
		now := s.now()
		r, err = tx.InsertRepository(ctx, Repository{ID: data.NewID(), Owner: n.Owner, Name: n.Name, Visibility: n.Visibility,
			Propose: n.Propose, Parent: n.Parent, Network: network, Cell: n.Cell, State: Creating, StateAt: now, CreatedBy: n.CreatedBy, CreatedAt: now})
		return err
	})
	return r, err
}

// knownCell reports whether name is one of the cells.
func (s *Store) knownCell(name string) bool {
	for _, c := range s.cells {
		if c.Name == name {
			return true
		}
	}
	return false
}

// activeOwner refuses an owner that is no active person or organization.
func activeOwner(ctx context.Context, tx Ops, id string) error {
	p, err := tx.PrincipalByID(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return invalid("no principal has the ID %s", id)
	}
	if err != nil {
		return err
	}
	if (p.Kind != Human && p.Kind != Org) || !p.Active() {
		return invalid("%s owns no repository: only an active person or organization does", p.Name)
	}
	return nil
}

// ActivateRepository marks an entry Active once its cell has made its storage.
func (s *Store) ActivateRepository(ctx context.Context, id int64) (Repository, error) {
	return s.change(ctx, id, func(tx Ops, r *Repository) (logged, error) {
		switch r.State {
		case Active:
			return logged{}, nil
		case Creating:
			if s.expiry(*r) == ExpiredCreating {
				return logged{}, ErrCreationExpired
			}
			r.State, r.StateAt = Active, s.now()
			return logged{kind: identitypb.DirectoryEvent_KIND_CREATED}, nil
		}
		return logged{}, fmt.Errorf("%w: repository %d is %s", ErrRepositoryState, id, r.State)
	})
}

// AbandonRepository removes an entry still Creating, whose cell could not make its storage.
func (s *Store) AbandonRepository(ctx context.Context, id int64) error {
	return s.b.Txn(ctx, func(tx Ops) error {
		r, err := tx.LockRepository(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if r.State != Creating {
			return fmt.Errorf("%w: repository %d is %s", ErrRepositoryState, id, r.State)
		}
		return tx.DeleteRepository(ctx, id)
	})
}

// logged is the event a change logs: its kind (none for 0), and the owner and name the repository
// had.
type logged struct {
	kind              identitypb.DirectoryEvent_Kind
	oldOwner, oldName string
	oldFullName       string // when the old owner's handle now is not what it was
}

// change runs fn on a repository's entry, held, writes what it changed, and logs the event it
// names.
func (s *Store) change(ctx context.Context, id int64, fn func(tx Ops, r *Repository) (logged, error)) (Repository, error) {
	var r Repository
	err := s.txn(ctx, func(tx Ops) error {
		var err error
		if r, err = tx.LockRepository(ctx, id); err != nil {
			return err
		}
		before := r
		ev, err := fn(tx, &r)
		if err != nil {
			return err
		}
		if r != before {
			if err := tx.UpdateRepository(ctx, r); err != nil {
				return err
			}
		}
		if ev.kind == identitypb.DirectoryEvent_KIND_UNSPECIFIED {
			return nil
		}
		return s.logRepository(ctx, tx, r.ID, &r, ev)
	})
	return r, err
}

// logGrants logs that a repository's grants changed, if it stands or is deleted: an entry being
// made is in no event yet.
func (s *Store) logGrants(ctx context.Context, tx Ops, id int64) error {
	r, err := tx.LockRepository(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil || r.State == Creating {
		return err
	}
	return s.logRepository(ctx, tx, id, &r, logged{kind: identitypb.DirectoryEvent_KIND_GRANTS})
}

// logOwnerRenamed logs each repository an owner holds renamed, its full name changing with its
// owner's handle: oldName was the owner's name.
func (s *Store) logOwnerRenamed(ctx context.Context, tx Ops, owner, oldName string) error {
	rs, err := tx.RepositoriesOf(ctx, owner)
	if err != nil {
		return err
	}
	oldHandle, _ := s.names.HandleOf(oldName)
	for _, r := range rs {
		if r.State == Creating {
			continue
		}
		ev := logged{kind: identitypb.DirectoryEvent_KIND_RENAMED, oldOwner: owner, oldName: r.Name,
			oldFullName: tenancy.FullName(oldHandle, r.Name, owner == s.installation)}
		if err := s.logRepository(ctx, tx, r.ID, &r, ev); err != nil {
			return err
		}
	}
	return nil
}

// Entry is a repository's entry as the directory shows it: its owner's handle and its full name.
func (s *Store) Entry(ctx context.Context, r Repository) (*identitypb.RepositoryEntry, error) {
	return s.EntryPB(ctx, s.b, r)
}

// Repository reads a repository's entry by ID; ErrNotFound for none.
func (s *Store) Repository(ctx context.Context, id int64) (Repository, error) {
	return s.b.Repository(ctx, id)
}

// ResolveRepository is the repository an owner's name names: the repository holding it, or the one
// a redirect from it names. redirected says which.
func (s *Store) ResolveRepository(ctx context.Context, owner, name string) (r Repository, redirected bool, err error) {
	r, err = s.b.RepositoryByName(ctx, owner, name)
	if !errors.Is(err, ErrNotFound) {
		return r, false, err
	}
	id, err := s.b.Redirect(ctx, owner, name)
	if err != nil {
		return Repository{}, false, err
	}
	r, err = s.b.Repository(ctx, id)
	return r, true, err
}

// Owner is the active person or organization a handle names — the installation's organization for
// none; ErrNotFound for none.
func (s *Store) Owner(ctx context.Context, handle string) (Principal, error) {
	if handle == "" {
		switch {
		case s.installation != "":
			return s.b.PrincipalByID(ctx, s.installation)
		case s.movedIn != "":
			// An installation's bare names, once it moved in, are its organization's.
			return s.b.PrincipalByID(ctx, s.movedIn)
		}
		return Principal{}, ErrNotFound
	}
	if tenancy.ValidHandle(handle) != nil {
		return Principal{}, ErrNotFound
	}
	for _, name := range []string{s.names.Org(handle), s.names.Human(handle)} {
		p, err := s.Principal(ctx, name)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return Principal{}, err
		}
		if !p.Active() {
			break
		}
		return p, nil
	}
	return Principal{}, ErrNotFound
}

// ResolveFullName is the repository a full name names, or one it had (redirected); ErrNotFound
// for none.
func (s *Store) ResolveFullName(ctx context.Context, full string) (r Repository, redirected bool, err error) {
	handle, name, err := tenancy.ParseFullName(full)
	if err != nil {
		return Repository{}, false, ErrNotFound
	}
	owner, err := s.Owner(ctx, handle)
	if err != nil {
		return Repository{}, false, err
	}
	return s.ResolveRepository(ctx, owner.ID, name)
}

// RepositoriesOf are an owner's repositories, by name.
func (s *Store) RepositoriesOf(ctx context.Context, owner string) ([]Repository, error) {
	return s.b.RepositoriesOf(ctx, owner)
}

// standing refuses a change to a repository being created or deleted.
func standing(r *Repository) error {
	if r.State != Active {
		return fmt.Errorf("%w: repository %d is %s", ErrRepositoryState, r.ID, r.State)
	}
	return nil
}

// RenameRepository renames a repository within its owner, its old name redirecting to it.
func (s *Store) RenameRepository(ctx context.Context, id int64, name string) (Repository, error) {
	if err := tenancy.ValidRepositoryName(name); err != nil {
		return Repository{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return s.change(ctx, id, func(tx Ops, r *Repository) (logged, error) {
		if err := standing(r); err != nil || r.Name == name {
			return logged{}, err
		}
		if err := s.freeName(ctx, tx, r.Owner, name); err != nil {
			return logged{}, err
		}
		if err := tx.PutRedirect(ctx, r.Owner, r.Name, r.ID); err != nil {
			return logged{}, err
		}
		ev := logged{kind: identitypb.DirectoryEvent_KIND_RENAMED, oldOwner: r.Owner, oldName: r.Name}
		r.Name = name
		return ev, nil
	})
}

// freeName refuses a name an owner's repository holds, and takes it from a redirect.
func (s *Store) freeName(ctx context.Context, tx Ops, owner, name string) error {
	_, err := tx.RepositoryByName(ctx, owner, name)
	switch {
	case err == nil:
		return ErrTaken
	case !errors.Is(err, ErrNotFound):
		return err
	}
	return tx.DeleteRedirect(ctx, owner, name)
}

// TransferRepository gives a repository to another owner, under its name, its old place
// redirecting to it. The roles granted to the old owner's teams and groups go with it; those
// granted to principals stay.
func (s *Store) TransferRepository(ctx context.Context, id int64, owner string) (Repository, error) {
	return s.change(ctx, id, func(tx Ops, r *Repository) (logged, error) {
		if err := standing(r); err != nil || r.Owner == owner {
			return logged{}, err
		}
		if err := activeOwner(ctx, tx, owner); err != nil {
			return logged{}, err
		}
		if err := s.freeName(ctx, tx, owner, r.Name); err != nil {
			return logged{}, err
		}
		if err := tx.PutRedirect(ctx, r.Owner, r.Name, r.ID); err != nil {
			return logged{}, err
		}
		grants, err := tx.Grants(ctx, r.ID)
		if err != nil {
			return logged{}, err
		}
		for _, g := range grants {
			if s.isSet(g.Subject) {
				if _, err := tx.DeleteGrant(ctx, r.ID, g.Subject); err != nil {
					return logged{}, err
				}
			}
		}
		ev := logged{kind: identitypb.DirectoryEvent_KIND_TRANSFERRED, oldOwner: r.Owner, oldName: r.Name}
		r.Owner = owner
		return ev, nil
	})
}

// SetVisibility changes who reads a repository without a grant.
func (s *Store) SetVisibility(ctx context.Context, id int64, v tenancy.Visibility) (Repository, error) {
	if v < tenancy.Private || v > tenancy.Public {
		return Repository{}, invalid("visibility %d", v)
	}
	return s.change(ctx, id, func(tx Ops, r *Repository) (logged, error) {
		if err := standing(r); err != nil || r.Visibility == v {
			return logged{}, err
		}
		if r.Parent != 0 {
			return logged{}, fmt.Errorf("%w: a fork's visibility is its upstream's: it changes only once the fork stands alone", ErrRepositoryState)
		}
		// Its forks keep theirs, each the root of a fork tree of its own (GitHub's rules).
		if err := s.detachForks(ctx, tx, r.ID); err != nil {
			return logged{}, err
		}
		r.Visibility = v
		return logged{kind: identitypb.DirectoryEvent_KIND_SETTINGS}, nil
	})
}

// GitHub's rules (docs/hosted.md, "GitHub's rules"): what happens to a repository's forks as it is
// deleted or its visibility changes, each change to a fork logged in its own log.

// setUpstream sets a fork's upstream (0: none), logged.
func (s *Store) setUpstream(ctx context.Context, tx Ops, f Repository, up int64) error {
	if f.Parent == up {
		return nil
	}
	f.Parent = up
	if err := tx.UpdateRepository(ctx, f); err != nil {
		return err
	}
	if f.State == Creating {
		return nil // an entry being made is in no event yet
	}
	return s.logRepository(ctx, tx, f.ID, &f, logged{kind: identitypb.DirectoryEvent_KIND_UPSTREAM})
}

// detachForks makes each of a repository's forks stand alone, the root of a fork tree of its own.
func (s *Store) detachForks(ctx context.Context, tx Ops, id int64) error {
	forks, err := tx.ForksOf(ctx, id)
	if err != nil {
		return err
	}
	for _, f := range forks {
		if err := s.setUpstream(ctx, tx, f, 0); err != nil {
			return err
		}
	}
	return nil
}

// promoteFork, as a public repository is deleted, makes its oldest active fork the upstream of the
// others, in its place.
func (s *Store) promoteFork(ctx context.Context, tx Ops, r Repository) error {
	forks, err := tx.ForksOf(ctx, r.ID)
	if err != nil {
		return err
	}
	var heir int64
	for _, f := range forks {
		switch {
		case f.State != Active:
		case heir == 0:
			heir = f.ID
			if err := s.setUpstream(ctx, tx, f, r.Parent); err != nil {
				return err
			}
		default:
			if err := s.setUpstream(ctx, tx, f, heir); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteForks, as a private or internal repository is deleted, deletes its forks, and theirs.
func (s *Store) deleteForks(ctx context.Context, tx Ops, id int64) error {
	forks, err := tx.ForksOf(ctx, id)
	if err != nil {
		return err
	}
	for _, f := range forks {
		if f.State != Active {
			continue
		}
		f.State, f.StateAt = Deleted, s.now()
		if err := tx.UpdateRepository(ctx, f); err != nil {
			return err
		}
		if err := s.logRepository(ctx, tx, f.ID, &f, logged{kind: identitypb.DirectoryEvent_KIND_DELETED}); err != nil {
			return err
		}
		if err := s.deleteForks(ctx, tx, f.ID); err != nil {
			return err
		}
	}
	return nil
}

// SetPropose changes the role proposing a CL needs: Read or Write.
func (s *Store) SetPropose(ctx context.Context, id int64, role tenancy.Role) (Repository, error) {
	if role != tenancy.Read && role != tenancy.Write {
		return Repository{}, invalid("proposing needs read or write, not %s", role)
	}
	return s.change(ctx, id, func(tx Ops, r *Repository) (logged, error) {
		if err := standing(r); err != nil || r.Propose == role {
			return logged{}, err
		}
		r.Propose = role
		return logged{kind: identitypb.DirectoryEvent_KIND_SETTINGS}, nil
	})
}

// DeleteRepository soft-deletes a repository: its name stays its own, and it can be restored for
// RetentionPeriod. A private or internal one's forks are deleted with it; a public one's oldest
// active fork takes its place, the upstream of the others (GitHub's rules).
func (s *Store) DeleteRepository(ctx context.Context, id int64) (Repository, error) {
	return s.change(ctx, id, func(tx Ops, r *Repository) (logged, error) {
		if r.State == Deleted {
			return logged{}, nil
		}
		if err := standing(r); err != nil {
			return logged{}, err
		}
		r.State, r.StateAt = Deleted, s.now()
		if r.Visibility == tenancy.Public {
			if err := s.promoteFork(ctx, tx, *r); err != nil {
				return logged{}, err
			}
		} else if err := s.deleteForks(ctx, tx, r.ID); err != nil {
			return logged{}, err
		}
		return logged{kind: identitypb.DirectoryEvent_KIND_DELETED}, nil
	})
}

// RestoreRepository restores a repository deleted within RetentionPeriod.
func (s *Store) RestoreRepository(ctx context.Context, id int64) (Repository, error) {
	return s.change(ctx, id, func(tx Ops, r *Repository) (logged, error) {
		if r.State != Deleted || s.now().Sub(r.StateAt) > RetentionPeriod {
			return logged{}, fmt.Errorf("%w: repository %d is %s since %s", ErrRepositoryState, id, r.State, r.StateAt.Format(time.RFC3339))
		}
		r.State, r.StateAt = Active, s.now()
		return logged{kind: identitypb.DirectoryEvent_KIND_RESTORED}, nil
	})
}

// PurgeRepository removes the entry of a repository deleted more than RetentionPeriod ago — its
// grants and redirects with it, its name free — once every role has deleted what it held of it.
func (s *Store) PurgeRepository(ctx context.Context, id int64) error {
	return s.txn(ctx, func(tx Ops) error {
		r, err := tx.LockRepository(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if r.State != Deleted || s.now().Sub(r.StateAt) <= RetentionPeriod {
			return fmt.Errorf("%w: repository %d is %s since %s", ErrRepositoryState, id, r.State, r.StateAt.Format(time.RFC3339))
		}
		if err := s.logRepository(ctx, tx, id, nil, logged{kind: identitypb.DirectoryEvent_KIND_PURGED}); err != nil {
			return err
		}
		return tx.DeleteRepository(ctx, id)
	})
}

// Expiry is whether a repository's time is up, and how.
type Expiry int

const (
	NotExpired      Expiry = iota // standing, restorable, or gone
	ExpiredDeleted                // deleted more than RetentionPeriod ago
	ExpiredCreating               // Creating for more than CreatingTimeout
)

// ErrCreationExpired is a creation resumed, or its storage's making finished, after CreatingTimeout:
// the entry is the SCM's to remove, and the name is free again once it has.
var ErrCreationExpired = errors.New("the repository's creation took too long, and it is being removed: try again in a few minutes")

func (s *Store) expiry(r Repository) Expiry {
	age := s.now().Sub(r.StateAt)
	switch {
	case r.State == Deleted && age > RetentionPeriod:
		return ExpiredDeleted
	case r.State == Creating && age > CreatingTimeout:
		return ExpiredCreating
	}
	return NotExpired
}

// ExpiryOf is a repository's expiry by the directory's clock; NotExpired for one there is none of.
func (s *Store) ExpiryOf(ctx context.Context, id int64) (Expiry, error) {
	e, _, err := s.Expiration(ctx, id)
	return e, err
}

// Expiration is a repository's expiry by the directory's clock, and, for one that will expire and
// has not yet — deleted, or being made — when it will.
func (s *Store) Expiration(ctx context.Context, id int64) (Expiry, time.Time, error) {
	r, err := s.b.Repository(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return NotExpired, time.Time{}, nil
	}
	if err != nil {
		return NotExpired, time.Time{}, err
	}
	e := s.expiry(r)
	if e != NotExpired {
		return e, time.Time{}, nil
	}
	switch r.State {
	case Deleted:
		return e, r.StateAt.Add(RetentionPeriod), nil
	case Creating:
		return e, r.StateAt.Add(CreatingTimeout), nil
	}
	return e, time.Time{}, nil
}

// RemoveExpired removes an expired repository's entry — a deleted one's purge logged, a Creating
// one abandoned — and answers how it had expired; NotExpired, removing nothing, for one that had
// not, or is gone.
func (s *Store) RemoveExpired(ctx context.Context, id int64) (Expiry, error) {
	e, err := s.ExpiryOf(ctx, id)
	if err != nil {
		return NotExpired, err
	}
	switch e {
	case ExpiredDeleted:
		err = s.PurgeRepository(ctx, id)
	case ExpiredCreating:
		err = s.AbandonRepository(ctx, id)
	}
	return e, err
}

// Expired are the repositories deleted more than RetentionPeriod ago, and the entries Creating
// for more than CreatingTimeout, up to limit of each, oldest first.
func (s *Store) Expired(ctx context.Context, limit int) (deleted, creating []Repository, err error) {
	now := s.now()
	if deleted, err = s.b.RepositoriesIn(ctx, Deleted, now.Add(-RetentionPeriod), limit); err != nil {
		return nil, nil, err
	}
	creating, err = s.b.RepositoriesIn(ctx, Creating, now.Add(-CreatingTimeout), limit)
	return deleted, creating, err
}

// PersonalForksIn are the active forks people own in networks.
func (s *Store) PersonalForksIn(ctx context.Context, networks []int64) ([]Repository, error) {
	return s.b.PersonalForksIn(ctx, networks)
}

// IsSet reports whether a grant's subject names a set — a team, a group, an ACL — rather than a
// principal by ID.
func (s *Store) IsSet(subject string) bool { return s.isSet(subject) }

// isSet reports whether a grant's subject names a set — a team, a group, an ACL — rather than a
// principal by ID.
func (s *Store) isSet(subject string) bool {
	k, ok := s.names.KindOf(subject)
	return ok && (k == identity.TeamMember || k == identity.GroupMember || k == identity.ACLMember)
}

// SetGrant grants a role on a repository to a principal, by ID, or a set — a team, a group, an
// ACL — by name; a repository has one grant per subject.
func (s *Store) SetGrant(ctx context.Context, g Grant) error {
	if g.Role < tenancy.Read || g.Role > tenancy.Admin {
		return invalid("a grant is of read, triage, write, maintain, or admin, not %s", g.Role)
	}
	return s.txn(ctx, func(tx Ops) error {
		if _, err := tx.LockRepository(ctx, g.Repository); err != nil {
			return err
		}
		if !s.isSet(g.Subject) {
			p, err := tx.PrincipalByID(ctx, g.Subject)
			if errors.Is(err, ErrNotFound) {
				return invalid("%s is neither a principal's ID nor a set's name", g.Subject)
			}
			if err != nil {
				return err
			}
			if p.Kind == Org {
				return invalid("%s is an organization: grant its teams, or its people", p.Name)
			}
			if p.Kind == Robot {
				// A robot of an organization reaches that organization's repositories alone.
				of, err := tx.RobotOrganization(ctx, p.ID)
				if err != nil && !errors.Is(err, ErrNotFound) {
					return err
				}
				r, err := tx.Repository(ctx, g.Repository)
				if err != nil {
					return err
				}
				if of != "" && of != r.Owner {
					return invalid("%s is another organization's robot: a robot is granted its own organization's repositories", p.Name)
				}
			}
		}
		if err := tx.PutGrant(ctx, g); err != nil {
			return err
		}
		return s.logGrants(ctx, tx, g.Repository)
	})
}

// RemoveGrant revokes the role a subject was granted on a repository, reporting whether it had one.
func (s *Store) RemoveGrant(ctx context.Context, repo int64, subject string) (bool, error) {
	removed := false
	err := s.txn(ctx, func(tx Ops) error {
		var err error
		if removed, err = tx.DeleteGrant(ctx, repo, subject); err != nil || !removed {
			return err
		}
		return s.logGrants(ctx, tx, repo)
	})
	return removed, err
}

// Grants are a repository's grants, by subject.
func (s *Store) Grants(ctx context.Context, repo int64) ([]Grant, error) {
	return s.b.Grants(ctx, repo)
}
