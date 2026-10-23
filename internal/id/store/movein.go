package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Moving an installation in (docs/hosted.md, "Moving an installation in"): its organization keeps
// its ID and takes the handle given; each person keeps their ID and handle, a member of it, its
// administrators its owners; its synced groups become its teams, their grants with them; its own
// robots become the organization's; its system of record, when it has one ok can run for an
// organization, becomes the organization's identity provider, the names its administrators set
// over the system of record's still holding; its repositories become
// <organization>/<name>. Every name the move gives up is an alias of the one that took its place,
// so OWNERS files, the groups-and-ACLs tree, and old URLs keep naming what they named, and an
// installation's bare repository names resolve to the organization moved in. No other role's rows
// change: they hold IDs. It is one transaction.

// Modes the directory serves.
const (
	ModeInstallation = "installation"
	ModeHosted       = "hosted"
)

// MoveInPlan is what moving an installation in does.
type MoveInPlan struct {
	Organization string // the installation's organization, by ID
	Handle       string // the handle it takes: its own when ""
	Owners       []string
	Provider     *IdentityProvider // the system of record, as the organization's; nil for none
	// SCIMToken, for a SCIM system of record, is the token its provider pushes with: what it
	// provisioned becomes the organization's, at the organization's endpoint, with that token.
	SCIMToken string
}

// MoveInReport is what moving in did.
type MoveInReport struct {
	Handle                string
	Renamed               bool
	Repositories, Members int
	Owners, Teams, Robots int
	Linked                int      // people linked to the identity provider
	KeptRobots            []string // robots that stay ok's: its roles', and those named for a ServiceAccount
}

// ErrMode is a directory that serves another mode than asked.
var ErrMode = errors.New("the directory serves another mode")

// CheckMode records the mode the directory serves on its first start, and refuses a start in
// another: an installation's directory moved in serves a hosted service, and a hosted service's
// directory was never an installation's but moved in. It answers the organization moved in, if one
// was.
func (s *Store) CheckMode(ctx context.Context, hosted bool) (string, error) {
	mode, movedIn, err := s.b.TenancyState(ctx)
	if err != nil {
		return "", err
	}
	want := ModeInstallation
	if hosted {
		want = ModeHosted
	}
	switch mode {
	case "":
		return "", s.b.PutTenancyState(ctx, want, "", s.now())
	case want:
		return movedIn, nil
	case ModeInstallation:
		return "", fmt.Errorf("%w: it is an installation's — move it in first (ok identity move-in), or run it as an installation", ErrMode)
	}
	return "", fmt.Errorf("%w: it was moved in to a hosted service (tenancy.mode: hosted)", ErrMode)
}

// SetMovedIn names the organization moved in, which an installation's bare repository names
// resolve to. Call it before the store serves.
func (s *Store) SetMovedIn(org string) { s.movedIn = org }

// MoveIn moves the installation in, as one transaction, and reports what it did.
func (s *Store) MoveIn(ctx context.Context, p MoveInPlan) (MoveInReport, error) {
	var rep MoveInReport
	before := s.installation
	// What it logs names repositories as the hosted service will.
	s.installation = ""
	err := s.txn(ctx, func(tx Ops) error {
		rep = MoveInReport{}
		if mode, _, err := tx.TenancyState(ctx); err != nil {
			return err
		} else if mode == ModeHosted {
			return fmt.Errorf("%w: it was moved in already", ErrMode)
		}
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		o, err := tx.LockOrganization(ctx, p.Organization)
		if err != nil {
			return err
		}
		old := o.Handle()
		rep.Handle = old
		if p.Handle != "" && p.Handle != old {
			if err := tenancy.ValidHandle(p.Handle); err != nil {
				return fmt.Errorf("%w: %v", ErrInvalid, err)
			}
			if held, err := s.handleHeld(ctx, tx, p.Handle); err != nil {
				return err
			} else if held {
				return fmt.Errorf("%w: the handle %s", ErrTaken, p.Handle)
			}
			name := s.names.Org(p.Handle)
			if err := tx.UpdatePrincipal(ctx, o.ID, name, directory.Active, o.Email, o.DisplayName); err != nil {
				return err
			}
			if err := s.renamed(ctx, tx, o.Name, name); err != nil {
				return err
			}
			rep.Handle, rep.Renamed = p.Handle, true
		}

		// Its repositories, named as a hosted service names them, each a rename in its log.
		repos, err := tx.RepositoriesOf(ctx, o.ID)
		if err != nil {
			return err
		}
		for _, r := range repos {
			if r.State == Creating {
				continue
			}
			ev := logged{kind: identitypb.DirectoryEvent_KIND_RENAMED, oldOwner: o.ID, oldName: r.Name, oldFullName: r.Name}
			if err := s.logRepository(ctx, tx, r.ID, &r, ev); err != nil {
				return err
			}
			rep.Repositories++
		}

		// Its people, members; its administrators, owners.
		people, err := tx.Principals(ctx, Filter{Kind: Human, IncludeInactive: true})
		if err != nil {
			return err
		}
		ids := map[string]string{} // name -> ID, of people
		for _, h := range people {
			ids[h.Name] = h.ID
			if !h.Active() {
				continue
			}
			role := tenancy.Member
			if slices.Contains(p.Owners, h.ID) {
				role, rep.Owners = tenancy.Owner, rep.Owners+1
			}
			if err := tx.PutMember(ctx, o.ID, h.ID, role); err != nil {
				return err
			}
			rep.Members++
		}

		// Its synced groups, its teams: their members flattened — a team holds people — and their
		// grants with them.
		groups, err := tx.SyncedGroups(ctx)
		if err != nil {
			return err
		}
		members := map[string][]string{}
		for _, g := range groups {
			members[g.Name] = g.Members
		}
		for _, g := range groups {
			if g.Deleted {
				continue
			}
			label, _, _ := strings.Cut(g.Name, ".")
			team := s.names.Team(label, rep.Handle)
			if held, err := tx.NameHeld(ctx, team); err != nil {
				return err
			} else if held {
				return fmt.Errorf("%w: the team %s", ErrTaken, team)
			}
			var in []string
			for _, name := range flatten(g.Name, members) {
				if id, ok := ids[name]; ok {
					in = append(in, id)
				}
			}
			slices.Sort(in)
			in = slices.Compact(in)
			if in == nil {
				in = []string{}
			}
			// A team keeps its group's external ID when the provider moves with it, which syncs it; with
			// none, its owners keep it.
			t := Team{Name: team, Organization: o.ID, Members: in}
			if p.Provider != nil {
				t.ExternalID = g.ExternalID
			}
			if err := tx.InsertTeam(ctx, t); err != nil {
				return err
			}
			changed, err := tx.RenameGrantSubject(ctx, g.Name, team)
			if err != nil {
				return err
			}
			for _, id := range changed {
				if err := s.logGrants(ctx, tx, id); err != nil {
					return err
				}
			}
			if err := tx.DeleteSyncedGroup(ctx, g.Name); err != nil {
				return err
			}
			if err := s.renamed(ctx, tx, g.Name, team); err != nil {
				return err
			}
			rep.Teams++
		}

		// Its own robots, the organization's: ok's roles' stay ok's, as do robots named for a
		// ServiceAccount (two labels), which an organization's names have no room for.
		robots, err := tx.Principals(ctx, Filter{Kind: Robot})
		if err != nil {
			return err
		}
		for _, r := range robots {
			if _, err := tx.RobotOrganization(ctx, r.ID); err == nil {
				continue
			} else if !errors.Is(err, ErrNotFound) {
				return err
			}
			labels := strings.Split(strings.TrimSuffix(r.Name, ".robot."+s.names.Domain()), ".")
			if len(labels) != 1 {
				rep.KeptRobots = append(rep.KeptRobots, r.Name)
				continue
			}
			name := s.names.Robot(labels[0] + "." + rep.Handle)
			if held, err := tx.NameHeld(ctx, name); err != nil {
				return err
			} else if held {
				return fmt.Errorf("%w: the robot %s", ErrTaken, name)
			}
			if err := tx.UpdatePrincipal(ctx, r.ID, name, r.Status, r.Email, r.DisplayName); err != nil {
				return err
			}
			if err := tx.InsertOrganizationRobot(ctx, r.ID, o.ID); err != nil {
				return err
			}
			if err := s.renamed(ctx, tx, r.Name, name); err != nil {
				return err
			}
			rep.Robots++
		}

		// Its system of record, the organization's identity provider, its people linked to it.
		if p.Provider != nil {
			pr := *p.Provider
			pr.Organization, pr.Updated = o.ID, s.now()
			if pr.Interval < MinSyncInterval {
				pr.Interval = MinSyncInterval
			}
			if err := tx.PutIdentityProvider(ctx, pr); err != nil {
				return err
			}
			for _, h := range people {
				if h.ExternalID == "" || h.Status == directory.Deleted {
					continue
				}
				if err := tx.InsertOrganizationPerson(ctx, o.ID, h.ExternalID, h.ID); err != nil {
					return err
				}
				rep.Linked++
			}
		}
		if p.SCIMToken != "" {
			if err := tx.MoveSCIM(ctx, o.ID); err != nil {
				return err
			}
			if err := tx.PutSCIMToken(ctx, o.ID, hashToken(p.SCIMToken), s.now()); err != nil {
				return err
			}
		}
		if err := tx.ClearSyncState(ctx); err != nil {
			return err
		}
		return tx.PutTenancyState(ctx, ModeHosted, o.ID, s.now())
	})
	if err != nil {
		s.installation = before
		return MoveInReport{}, err
	}
	s.movedIn = p.Organization
	return rep, nil
}

// renamed retires a name a move gave up, an alias of the one that took its place.
func (s *Store) renamed(ctx context.Context, tx Ops, old, name string) error {
	if err := tx.RetireName(ctx, old, "moved in as "+name); err != nil {
		return err
	}
	return tx.InsertAlias(ctx, old, name)
}

// flatten is everyone a group holds, through the groups it holds, by name.
func flatten(group string, members map[string][]string) []string {
	var out []string
	seen := map[string]bool{group: true}
	queue := []string{group}
	for len(queue) > 0 {
		g := queue[0]
		queue = queue[1:]
		for _, m := range members[g] {
			if _, isGroup := members[m]; isGroup {
				if !seen[m] {
					seen[m] = true
					queue = append(queue, m)
				}
				continue
			}
			out = append(out, m)
		}
	}
	return out
}

// Alias is the name an old one became, as a move gave it up; ErrNotFound for none.
func (s *Store) Alias(ctx context.Context, old string) (string, error) { return s.b.Alias(ctx, old) }

// Aliases are every name a move gave up, and what each became.
func (s *Store) Aliases(ctx context.Context) (map[string]string, error) { return s.b.Aliases(ctx) }
