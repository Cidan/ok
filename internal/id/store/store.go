// Package store is ok identity's directory: principals and retired names, the system of record's
// mirror (a directory.Store), the groups-and-ACLs tree, robots' bindings, credentials, sessions,
// device logins, passkeys, and sign-in ceremonies. Store holds the rules; a Backend (postgres) is
// the database it keeps them in, and every change that must hold together is one transaction.
package store

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/lock"
	"github.com/Cidan/ok/internal/work"
)

// Errors the store answers with.
var (
	ErrNotFound = errors.New("not found")
	// ErrTaken is a name held, or once held, by another.
	ErrTaken = errors.New("the name is taken, or was, and a name is never given to another")
	// ErrBindingTaken is a binding that names another robot already.
	ErrBindingTaken = errors.New("the binding names a robot already")
	// ErrUserCodeTaken is a device login's user code another holds.
	ErrUserCodeTaken = errors.New("the user code is in use")
)

// Kind is what a principal is.
type Kind string

const (
	Human Kind = "human"
	Robot Kind = "robot"
)

// Principal is a principal as the store holds it.
type Principal struct {
	ID          string
	Name        string
	Kind        Kind
	Status      directory.Status
	ExternalID  string
	Email       string
	DisplayName string
	Created     time.Time
}

// Active reports whether the principal acts.
func (p Principal) Active() bool { return p.Status == directory.Active }

// Store is the directory's store.
type Store struct {
	b     Backend
	names identity.Names
	now   func() time.Time
	// relayWake tells this replica's relay there may be events to publish.
	relayWake chan struct{}
	// installation is the installation's organization, by ID; "" for a hosted service.
	installation string
	// movedIn is the organization an installation moved in as; "" for none.
	movedIn string
	// cells are the cells repositories are placed in, and defaultRegion where an owner's go when it
	// names no region (SetCells).
	cells         []Cell
	defaultRegion string
	// defaultQuota is an account's quota of CI when it has none of its own (SetDefaultQuota).
	defaultQuota Quota
}

// Cell is a cell repositories are placed in (docs/hosted.md, "Cells, as built"): its region, and
// whether it is open to new ones.
type Cell struct {
	Name   string
	Region string
	Open   bool
}

// SetCells names the cells repositories are placed in, the first holding those made before there
// were cells, and the region an owner's are placed in when it names none ("" for any). Call it
// before the store serves; with none, entries name no cell.
func (s *Store) SetCells(cells []Cell, defaultRegion string) {
	s.cells, s.defaultRegion = cells, defaultRegion
}

// CellOf is the cell holding a repository: its entry's, or the first cell for one made before there
// were cells.
func (s *Store) CellOf(r Repository) string {
	if r.Cell == "" && len(s.cells) > 0 {
		return s.cells[0].Name
	}
	return r.Cell
}

// place is the cell an owner's new repository goes to: of the open cells in the region it is
// placed in (PlacedIn) — any open cell when none is open there — the one holding the fewest,
// counted outside any transaction — a count read in one would lock every entry it counts, and
// placement needs no exact count — the first of them on a tie. "" with no cells.
func (s *Store) place(ctx context.Context, owner string) (string, error) {
	if len(s.cells) == 0 {
		return "", nil
	}
	region, err := s.PlacedIn(ctx, owner)
	if err != nil {
		return "", err
	}
	counts, err := s.b.RepositoryCellCounts(ctx)
	if err != nil {
		return "", err
	}
	counts[s.cells[0].Name] += counts[""]
	pick := func(inRegion bool) string {
		best, fewest := "", int64(-1)
		for _, c := range s.cells {
			if c.Open && (!inRegion || c.Region == region) && (fewest < 0 || counts[c.Name] < fewest) {
				best, fewest = c.Name, counts[c.Name]
			}
		}
		return best
	}
	if region != "" {
		if c := pick(true); c != "" {
			return c, nil
		}
	}
	return pick(false), nil
}

// PlacedIn is the region an owner's (by ID) new repositories go to: the one it named, or the
// directory's default; "" for any.
func (s *Store) PlacedIn(ctx context.Context, owner string) (string, error) {
	region, err := s.b.OwnerRegion(ctx, owner)
	if err != nil || region != "" {
		return region, err
	}
	return s.defaultRegion, nil
}

// OwnerRegion is the region an owner (by ID) named for its new repositories; "" for none.
func (s *Store) OwnerRegion(ctx context.Context, owner string) (string, error) {
	return s.b.OwnerRegion(ctx, owner)
}

// SetOwnerRegion names the region an owner's (by ID) new repositories go to — one a cell is in —
// or, with "", forgets it.
func (s *Store) SetOwnerRegion(ctx context.Context, owner, region string) error {
	if region != "" {
		known := false
		for _, c := range s.cells {
			known = known || c.Region == region
		}
		if !known {
			return invalid("no cell is in region %s", region)
		}
	}
	return s.b.SetOwnerRegion(ctx, owner, region)
}

// New is a store over a backend, its principals and sets named under names.
func New(b Backend, names identity.Names) *Store {
	return &Store{b: b, names: names, now: time.Now, relayWake: make(chan struct{}, 1)}
}

// SetInstallation names the installation's organization (EnsureOrganization), whose repositories'
// full names are their names alone. Call it before the store serves.
func (s *Store) SetInstallation(org string) { s.installation = org }

// Installation is the installation's organization, by ID; "" for a hosted service.
func (s *Store) Installation() string { return s.installation }

// SetClock replaces the store's clock, which times device logins, sessions, and ceremonies: for
// tests.
func (s *Store) SetClock(now func() time.Time) { s.now = now }

// Names is the naming the store's principals and sets are under.
func (s *Store) Names() identity.Names { return s.names }

// Healthy reports whether the database answers.
func (s *Store) Healthy(ctx context.Context) bool { return s.b.Ping(ctx) == nil }

// LockSync runs fn holding the sync's lock, which one replica holds at a time, waiting up to wait
// for it; it returns lock.ErrNotAcquired when it could not take it, and otherwise fn's error.
func (s *Store) LockSync(ctx context.Context, wait time.Duration, fn lock.LockFn) error {
	return s.b.Lock(ctx, "identity", "sync", wait, fn)
}

// LockOrganizationSync runs fn holding an organization's sync's lock, as LockSync does the
// installation's.
func (s *Store) LockOrganizationSync(ctx context.Context, org string, wait time.Duration, fn lock.LockFn) error {
	return s.b.Lock(ctx, "identity", "sync:"+org, wait, fn)
}

// newID makes an ID: prefix and 26 random base32 characters.
func newID(prefix string) string {
	var b [16]byte
	rand.Read(b[:])
	return prefix + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:]))
}

// Principal returns the principal with a name.
func (s *Store) Principal(ctx context.Context, name string) (Principal, error) {
	p, err := s.b.PrincipalByName(ctx, name)
	if errors.Is(err, ErrNotFound) {
		// A name a move gave up names what took its place.
		if to, aerr := s.b.Alias(ctx, name); aerr == nil {
			return s.b.PrincipalByName(ctx, to)
		}
	}
	return p, err
}

// PrincipalByID returns the principal with an ID.
func (s *Store) PrincipalByID(ctx context.Context, id string) (Principal, error) {
	return s.b.PrincipalByID(ctx, id)
}

// Filter selects principals to list.
type Filter struct {
	Kind            Kind   // "" for both
	Query           string // a substring of the name, email, or display name, case-insensitively
	IncludeInactive bool
	After           string // list names after this one
	Limit           int
}

// Principals lists principals by name, from after f.After, at most f.Limit (all for 0).
func (s *Store) Principals(ctx context.Context, f Filter) ([]Principal, error) {
	return s.b.Principals(ctx, f)
}

// RetiredName is a name no one will have again.
type RetiredName struct {
	Name    string
	Retired time.Time
	Reason  string
}

// RetiredNames lists every retired name.
func (s *Store) RetiredNames(ctx context.Context) ([]RetiredName, error) {
	return s.b.RetiredNames(ctx)
}

// Audit records what an actor did.
func (s *Store) Audit(ctx context.Context, actor, action string, detail map[string]any) error {
	if detail == nil {
		detail = map[string]any{}
	}
	return s.b.AppendAudit(ctx, actor, action, detail)
}

// CreateRobot makes a robot: name is its full name, <name>.robot.<domain>.
func (s *Store) CreateRobot(ctx context.Context, name string) (Principal, error) {
	if k, ok := s.names.KindOf(name); !ok || k != identity.Robot {
		return Principal{}, fmt.Errorf("%q is not a robot's name under %s", name, s.names.Domain())
	}
	var p Principal
	err := s.b.Txn(ctx, func(tx Ops) error {
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		held, err := tx.NameHeld(ctx, name)
		if err != nil {
			return err
		}
		if held {
			return ErrTaken
		}
		p, err = tx.InsertPrincipal(ctx, Principal{ID: newID("prn_"), Name: name, Kind: Robot, Status: directory.Active})
		return err
	})
	return p, err
}

// EnsureRobot makes a robot if there is none of the name, and returns it: how the installation's
// configured robots — its roles' — come to exist.
func (s *Store) EnsureRobot(ctx context.Context, name string) (Principal, error) {
	p, err := s.Principal(ctx, name)
	if err == nil {
		if p.Kind != Robot {
			return Principal{}, fmt.Errorf("%s is a person, not a robot", name)
		}
		if !p.Active() {
			return Principal{}, fmt.Errorf("the robot %s was deleted; its name is retired", name)
		}
		return p, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Principal{}, err
	}
	return s.CreateRobot(ctx, name)
}

// DeleteRobot deletes a robot: a tombstone, its name retired, its credentials revoked, its
// bindings gone.
func (s *Store) DeleteRobot(ctx context.Context, name string) error {
	return s.b.Txn(ctx, func(tx Ops) error {
		p, err := tx.LockPrincipalByName(ctx, name)
		if err != nil {
			return err
		}
		if p.Kind != Robot {
			return fmt.Errorf("%s is a person: the system of record deletes people", name)
		}
		if p.Status == directory.Deleted {
			return nil
		}
		if err := tx.SetStatus(ctx, p.ID, directory.Deleted); err != nil {
			return err
		}
		if err := tx.RevokeCredentialsOf(ctx, p.ID); err != nil {
			return err
		}
		if err := tx.DeleteBindingsOf(ctx, p.ID); err != nil {
			return err
		}
		return tx.RetireName(ctx, name, "the robot was deleted")
	})
}

// Binding names a robot by the tokens another issuer signs for it.
type Binding struct {
	// Organization is the organization whose robot it names, by ID; "" for the installation's — an
	// issuer the configuration names.
	Organization    string
	Issuer, Subject string
}

// Bind lets the robot sign in with tokens for b. A binding names one robot.
func (s *Store) Bind(ctx context.Context, robot string, b Binding) error {
	if b.Issuer == "" || b.Subject == "" {
		return errors.New("a binding names an issuer and a subject")
	}
	p, err := s.Principal(ctx, robot)
	if err != nil {
		return err
	}
	if p.Kind != Robot || !p.Active() {
		return fmt.Errorf("%s is not an active robot", robot)
	}
	if _, err := s.b.RobotOrganization(ctx, p.ID); err == nil {
		return fmt.Errorf("%s is an organization's robot: its organization binds it, to an issuer it trusts", robot)
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	b.Organization = ""
	if err := s.b.InsertBinding(ctx, p.ID, b); errors.Is(err, ErrBindingTaken) {
		return fmt.Errorf("issuer %s's subject %s already names a robot", b.Issuer, b.Subject)
	} else if err != nil {
		return err
	}
	return nil
}

// Unbind removes a binding from a robot.
func (s *Store) Unbind(ctx context.Context, robot string, b Binding) error {
	ok, err := s.b.DeleteBinding(ctx, robot, b)
	if err == nil && !ok {
		return ErrNotFound
	}
	return err
}

// BoundRobot returns the robot a binding names.
func (s *Store) BoundRobot(ctx context.Context, b Binding) (Principal, error) {
	return s.b.BoundPrincipal(ctx, b)
}

// RobotWithBindings is a robot and its bindings.
type RobotWithBindings struct {
	Principal
	Bindings []Binding
}

// Robots lists the robots that are not deleted, with their bindings.
func (s *Store) Robots(ctx context.Context) ([]RobotWithBindings, error) {
	robots, err := s.Principals(ctx, Filter{Kind: Robot})
	if err != nil {
		return nil, err
	}
	bindings, err := s.b.Bindings(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]RobotWithBindings, 0, len(robots))
	for _, r := range robots {
		out = append(out, RobotWithBindings{Principal: r, Bindings: bindings[r.ID]})
	}
	return out, nil
}

// ByEmail returns the person with an email, compared case-insensitively. Two people with the
// email are no answer: an authenticator's email must name one.
func (s *Store) ByEmail(ctx context.Context, email string) (Principal, error) {
	if email == "" {
		return Principal{}, ErrNotFound
	}
	found, err := s.b.PeopleByEmail(ctx, email)
	if err != nil {
		return Principal{}, err
	}
	if len(found) != 1 {
		return Principal{}, ErrNotFound
	}
	return found[0], nil
}

// ByExternalID returns the person the system of record knows by an ID.
func (s *Store) ByExternalID(ctx context.Context, id string) (Principal, error) {
	return s.b.PrincipalByExternalID(ctx, id)
}

// EnsureBinding binds a robot to b unless it is bound already; b bound to another robot is an
// error.
func (s *Store) EnsureBinding(ctx context.Context, robot string, b Binding) error {
	p, err := s.BoundRobot(ctx, b)
	switch {
	case err == nil && p.Name == robot:
		return nil
	case err == nil:
		return fmt.Errorf("issuer %s's subject %s is bound to %s, not %s", b.Issuer, b.Subject, p.Name, robot)
	case !errors.Is(err, ErrNotFound):
		return err
	}
	return s.Bind(ctx, robot, b)
}

// Work is identity's keyed work's store.
func (s *Store) Work() work.Store { return s.b.Work() }
