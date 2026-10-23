package store

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/tenancy"
)

// Enterprise organizations (docs/hosted.md, "Enterprise organizations"): an organization's own
// identity provider, which provisions and deprovisions its people — linked by their external ID,
// named <handle>-<org>, so they take no one else's handle — and whose groups are synced as its
// teams. The store keeps the provider's settings as given and its secrets as sealed; what they
// mean is internal/id/enterprise's.

// IdentityProvider is an organization's identity provider, and how its sync last went.
type IdentityProvider struct {
	Organization string
	Source       string          // google, entra, ldap, or scim
	Settings     json.RawMessage // no secret
	Sealed       []byte          // the secrets, sealed
	Interval     time.Duration
	// PlainHandles: the people it makes are named by their handles alone — an organization moved
	// in, whose people the installation named so — not <handle>-<org>.
	PlainHandles bool
	LastRun      time.Time
	LastError    string
	Waiting      []directory.Waiting
	Notes        []string
	Updated      time.Time
	// Mirror is the system of record as the last sync read it; nil before the first.
	Mirror json.RawMessage
}

// MinSyncInterval bounds how often an organization's provider is read.
const MinSyncInterval = 5 * time.Minute

// SetIdentityProvider gives an organization an identity provider, or changes it. The installation's
// is its configuration's.
func (s *Store) SetIdentityProvider(ctx context.Context, p IdentityProvider) error {
	if p.Organization == s.installation {
		return invalid("the installation's identity provider is its configuration's (identity.system_of_record)")
	}
	switch p.Source {
	case "google", "entra", "ldap", "scim":
	default:
		return invalid("an identity provider is google, entra, ldap, or scim, not %q", p.Source)
	}
	if p.Interval < MinSyncInterval {
		p.Interval = MinSyncInterval
	}
	if _, err := s.b.Organization(ctx, p.Organization); err != nil {
		return err
	}
	p.Updated = s.now()
	return s.b.PutIdentityProvider(ctx, p)
}

// IdentityProviderOf is an organization's identity provider; ErrNotFound for none.
func (s *Store) IdentityProviderOf(ctx context.Context, org string) (IdentityProvider, error) {
	return s.b.IdentityProviderOf(ctx, org)
}

// RemoveIdentityProvider removes an organization's identity provider. Its people stay as they are,
// and sign in no more through it.
func (s *Store) RemoveIdentityProvider(ctx context.Context, org string) (bool, error) {
	return s.b.DeleteIdentityProvider(ctx, org)
}

// DueIdentityProviders are the organizations whose provider's sync is due.
func (s *Store) DueIdentityProviders(ctx context.Context) ([]string, error) {
	return s.b.DueIdentityProviders(ctx, s.now())
}

// OrganizationSyncFailed records that an organization's sync failed, and how.
func (s *Store) OrganizationSyncFailed(ctx context.Context, org string, cause error) error {
	return s.b.PutOrganizationSyncFailure(ctx, org, cause.Error(), s.now())
}

// OrganizationPerson is the person an organization's provider provisioned under an external ID;
// ErrNotFound for none.
func (s *Store) OrganizationPerson(ctx context.Context, org, externalID string) (Principal, error) {
	people, err := s.b.OrganizationPeople(ctx, org)
	if err != nil {
		return Principal{}, err
	}
	p, ok := people[externalID]
	if !ok {
		return Principal{}, ErrNotFound
	}
	return p, nil
}

// OrganizationPeople are the people an organization's provider provisioned, by external ID.
func (s *Store) OrganizationPeople(ctx context.Context, org string) (map[string]Principal, error) {
	return s.b.OrganizationPeople(ctx, org)
}

// PersonOrganization is the organization that provisioned a person, and their external ID there;
// ErrNotFound for a person no organization provisioned.
func (s *Store) PersonOrganization(ctx context.Context, principal string) (string, string, error) {
	return s.b.PersonOrganization(ctx, principal)
}

// SetSCIMToken makes an organization's SCIM bearer token, replacing any it had, and returns it.
func (s *Store) SetSCIMToken(ctx context.Context, org string) (string, error) {
	t := "oks_" + token()
	return t, s.b.PutSCIMToken(ctx, org, hashToken(t), s.now())
}

// SCIMTokenOrganization is the organization a SCIM bearer token is of; ErrNotFound for none.
func (s *Store) SCIMTokenOrganization(ctx context.Context, secret string) (string, error) {
	return s.b.SCIMTokenOrganization(ctx, hashToken(secret))
}

// OrganizationSync is an organization's sync, as a directory.Store: the organization's people and
// synced teams are what it decides; every other name is held.
func (s *Store) OrganizationSync(org string) *OrganizationSync {
	return &OrganizationSync{s: s, org: org}
}

// OrganizationSync is one organization's directory.Store.
type OrganizationSync struct {
	s   *Store
	org string
}

// Options are the sync's options for the organization: its people named <handle>-<org>, its
// groups as its teams.
func (o *OrganizationSync) Options(ctx context.Context) (directory.Options, error) {
	org, err := o.s.b.Organization(ctx, o.org)
	if err != nil {
		return directory.Options{}, err
	}
	p, err := o.s.b.IdentityProviderOf(ctx, o.org)
	if err != nil {
		return directory.Options{}, err
	}
	h := org.Handle()
	opts := directory.Options{
		HumanName: func(handle string) string { return o.s.names.Human(handle + "-" + h) },
		GroupName: func(label string) string { return o.s.names.Team(label, h) },
	}
	if p.PlainHandles {
		// An organization moved in: its people named as the installation named them, by the names
		// its administrators set over the system of record's too.
		opts.HumanName = o.s.names.Human
		if opts.Handles, opts.GroupNames, err = o.s.b.SyncNames(ctx); err != nil {
			return directory.Options{}, err
		}
	}
	return opts, nil
}

// Load reads the organization's people and synced teams, every other name — principals, retired
// names, groups, teams — held, and the mirror the last sync followed.
func (o *OrganizationSync) Load(ctx context.Context) (*directory.State, *directory.Snapshot, error) {
	st := directory.NewState()
	people, err := o.s.b.OrganizationPeople(ctx, o.org)
	if err != nil {
		return nil, nil, err
	}
	mine := map[string]bool{}
	for id, p := range people {
		st.Principals[p.Name] = directory.Principal{Name: p.Name, ExternalID: id, Email: p.Email, DisplayName: p.DisplayName, Status: p.Status}
		mine[p.Name] = true
	}
	all, err := o.s.Principals(ctx, Filter{IncludeInactive: true})
	if err != nil {
		return nil, nil, err
	}
	byID := map[string]string{}
	for _, p := range all {
		byID[p.ID] = p.Name
		if !mine[p.Name] {
			st.Retired[p.Name] = true
		}
	}
	retired, err := o.s.RetiredNames(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, r := range retired {
		st.Retired[r.Name] = true
	}
	groups, err := o.s.b.SyncedGroups(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, g := range groups {
		st.Retired[g.Name] = true
	}
	p, err := o.s.b.IdentityProviderOf(ctx, o.org)
	if err != nil {
		return nil, nil, err
	}
	var mirror *directory.Snapshot
	if len(p.Mirror) > 0 {
		mirror = &directory.Snapshot{}
		if err := json.Unmarshal(p.Mirror, mirror); err != nil {
			return nil, nil, err
		}
	}
	teams, err := o.s.b.AllTeams(ctx)
	if err != nil {
		return nil, nil, err
	}
	teamOf := map[string]string{} // a synced team's name, by its group's external ID
	for _, t := range teams {
		if t.Organization == o.org && t.ExternalID != "" {
			teamOf[t.ExternalID] = t.Name
		}
	}
	// A synced team's members are what its group declared when the provider was last read — its
	// people, and the groups it holds by their teams' names — as Reconcile wants them; a team holds
	// the people they come to (flattenTeams). Before the provider is first read, its people.
	declared := map[string][]string{}
	if mirror != nil {
		for _, g := range mirror.Groups {
			ms := []string{}
			for _, m := range g.Members {
				if name, ok := teamOf[m.ExternalID]; m.Group && ok {
					ms = append(ms, name)
				} else if person, ok := people[m.ExternalID]; !m.Group && ok {
					ms = append(ms, person.Name)
				}
			}
			declared[g.ExternalID] = ms
		}
	}
	for _, t := range teams {
		if t.Organization != o.org || t.ExternalID == "" {
			st.Retired[t.Name] = true
			continue
		}
		g := directory.SyncedGroup{Name: t.Name, ExternalID: t.ExternalID, Members: []string{}}
		if ms, ok := declared[t.ExternalID]; ok {
			g.Members = ms
		} else {
			for _, id := range t.Members {
				g.Members = append(g.Members, byID[id])
			}
		}
		slices.Sort(g.Members)
		g.Members = slices.Compact(g.Members)
		st.Groups[t.Name] = g
	}
	for name := range st.Retired {
		o.s.retireAsPerson(st, name)
	}
	return st, mirror, nil
}

// Apply carries out the organization's plan — its people made, members of it, changed, renamed (the
// old name retired), suspended (their sessions ended), and deleted (their name retired, their
// credentials revoked, their membership dropped); its synced teams likewise — a chunk at a time,
// each in a transaction of its own, then records the snapshot it followed, as the installation's
// sync does (Store.Apply). The organization's sync's lock is held throughout, so what it reads of
// the organization's people between its transactions is its own.
func (o *OrganizationSync) Apply(ctx context.Context, plan *directory.Plan, mirror *directory.Snapshot) error {
	raw, err := json.Marshal(mirror)
	if err != nil {
		return err
	}
	waiting, _ := json.Marshal(nonNil(plan.Waiting))
	notes, _ := json.Marshal(nonNil(plan.Notes))
	people, err := o.s.b.OrganizationPeople(ctx, o.org)
	if err != nil {
		return err
	}
	if err := inChunks(ctx, o.s, plan.Humans, func(tx Ops, chunk []directory.HumanChange) error {
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		for _, c := range chunk {
			a := c.After
			if c.Before == nil || c.Before.Name != a.Name {
				held, err := tx.NameHeld(ctx, a.Name)
				if err != nil {
					return err
				}
				if h, ok := o.s.names.HandleOf(a.Name); ok && !held {
					if held, err = tx.NameHeld(ctx, o.s.names.Org(h)); err != nil {
						return err
					}
				}
				if held {
					return ErrTaken
				}
			}
			if c.Before == nil {
				p, err := tx.InsertPrincipal(ctx, Principal{ID: newID("prn_"), Name: a.Name, Kind: Human, Status: a.Status, Email: a.Email,
					DisplayName: a.DisplayName})
				if err != nil {
					return err
				}
				if err := tx.InsertOrganizationPerson(ctx, o.org, a.ExternalID, p.ID); err != nil {
					return err
				}
				if err := tx.PutMember(ctx, o.org, p.ID, tenancy.Member); err != nil {
					return err
				}
				continue
			}
			cur, ok := people[a.ExternalID]
			if !ok {
				return fmt.Errorf("the plan changes %s, whom the organization did not provision", a.ExternalID)
			}
			if err := tx.UpdatePrincipal(ctx, cur.ID, a.Name, a.Status, a.Email, a.DisplayName); err != nil {
				return err
			}
			if c.Before.Name != a.Name {
				if err := tx.RetireName(ctx, c.Before.Name, "renamed to "+a.Name); err != nil {
					return err
				}
				if err := o.s.logOwnerRenamed(ctx, tx, cur.ID, c.Before.Name); err != nil {
					return err
				}
			}
			if a.Status != directory.Active {
				if err := tx.DeleteSessionsOf(ctx, cur.ID); err != nil {
					return err
				}
			}
			if a.Status == directory.Deleted {
				if err := tx.RevokeCredentialsOf(ctx, cur.ID); err != nil {
					return err
				}
				if err := tx.RetireName(ctx, a.Name, "deleted from the organization's identity provider"); err != nil {
					return err
				}
				if _, err := tx.DeleteMember(ctx, o.org, cur.ID); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// The organization's people after the plan, by name: its teams' members.
	after, err := o.s.b.OrganizationPeople(ctx, o.org)
	if err != nil {
		return err
	}
	ids := map[string]string{}
	for _, p := range after {
		ids[p.Name] = p.ID
	}
	if err := inChunks(ctx, o.s, plan.Groups, func(tx Ops, chunk []directory.GroupChange) error {
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		for _, c := range chunk {
			a := c.After
			members := []string{}
			for _, name := range a.Members {
				if id, ok := ids[name]; ok {
					members = append(members, id)
				}
			}
			slices.Sort(members)
			switch {
			case c.Before == nil:
				if err := tx.InsertTeam(ctx, Team{Name: a.Name, Organization: o.org, Members: members, ExternalID: a.ExternalID}); err != nil {
					return err
				}
			case a.Deleted:
				name, err := tx.DeleteSyncedTeam(ctx, o.org, a.ExternalID)
				if err != nil {
					return err
				}
				if err := tx.RetireName(ctx, name, "deleted from the organization's identity provider"); err != nil {
					return err
				}
			default:
				before, err := tx.UpdateSyncedTeam(ctx, o.org, a.ExternalID, a.Name, members)
				if err != nil {
					return err
				}
				if before != a.Name {
					if err := tx.RetireName(ctx, before, "renamed to "+a.Name); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := o.flattenTeams(ctx, mirror); err != nil {
		return err
	}
	return o.s.txn(ctx, func(tx Ops) error { return tx.PutOrganizationSync(ctx, o.org, raw, waiting, notes, o.s.now()) })
}

// teamMembers are a team's members to set.
type teamMembers struct {
	name string
	ids  []string
}

// flattenTeams sets each synced team's members to everyone its group holds in the snapshot, through
// the groups it holds: a team holds people, and a group of groups is its groups' people. Those whose
// members change are set a chunk at a time.
func (o *OrganizationSync) flattenTeams(ctx context.Context, mirror *directory.Snapshot) error {
	if mirror == nil {
		return nil
	}
	people, err := o.s.b.OrganizationPeople(ctx, o.org)
	if err != nil {
		return err
	}
	byGroup := map[string][]directory.Member{}
	for _, g := range mirror.Groups {
		byGroup[g.ExternalID] = g.Members
	}
	teams, err := o.s.b.Teams(ctx, o.org)
	if err != nil {
		return err
	}
	var changed []teamMembers
	for _, t := range teams {
		if t.ExternalID == "" {
			continue
		}
		var ids []string
		seen := map[string]bool{t.ExternalID: true}
		queue := []string{t.ExternalID}
		for len(queue) > 0 {
			g := queue[0]
			queue = queue[1:]
			for _, m := range byGroup[g] {
				switch {
				case m.Group && !seen[m.ExternalID]:
					seen[m.ExternalID] = true
					queue = append(queue, m.ExternalID)
				case !m.Group:
					if p, ok := people[m.ExternalID]; ok && p.Status != directory.Deleted {
						ids = append(ids, p.ID)
					}
				}
			}
		}
		slices.Sort(ids)
		ids = slices.Compact(ids)
		if ids == nil {
			ids = []string{}
		}
		if !slices.Equal(ids, t.Members) {
			changed = append(changed, teamMembers{t.Name, ids})
		}
	}
	return inChunks(ctx, o.s, changed, func(tx Ops, chunk []teamMembers) error {
		for _, t := range chunk {
			if err := tx.SetTeamMembers(ctx, t.name, t.ids); err != nil {
				return err
			}
		}
		return nil
	})
}
