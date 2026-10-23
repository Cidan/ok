package store

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/x509"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/tenancy"
)

// Apps (docs/hosted.md, "Robots and tokens per organization"): an organization's robot with a
// public key. An organization installs one — its own, or another's made public — with a role on its
// repositories (named, or every one); the app trades a token it signs for an installation token,
// good for an hour, which reaches the installing organization's repositories alone.

// InstallationTokenPrefix starts every installation token.
const InstallationTokenPrefix = "oki_"

// InstallationTokenLifetime is how long an installation token lives.
const InstallationTokenLifetime = time.Hour

// App is an app.
type App struct {
	ID           string // app_…
	Robot        string // its robot, by ID
	Organization string // its maker, by ID
	Public       bool   // installable by any organization
	PublicKey    []byte // PKIX DER
	Created      time.Time
}

// Installation is an app installed in an organization.
type Installation struct {
	ID           string // ins_…
	App          string
	Organization string
	Role         tenancy.Role
	Repositories []int64 // none: every one of the organization's
	CreatedBy    string
	Created      time.Time
}

// Covers reports whether an installation reaches a repository of its organization.
func (i Installation) Covers(repo int64) bool {
	return len(i.Repositories) == 0 || slices.Contains(i.Repositories, repo)
}

// ParseAppKey parses an app's public key: PKIX DER, ECDSA P-256 or Ed25519.
func ParseAppKey(der []byte) (any, error) {
	k, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, invalid("an app's key is PKIX DER: %v", err)
	}
	switch k := k.(type) {
	case *ecdsa.PublicKey:
		if k.Curve != elliptic.P256() {
			return nil, invalid("an app's ECDSA key is on P-256")
		}
	case ed25519.PublicKey:
	default:
		return nil, invalid("an app's key is ECDSA P-256 or Ed25519, not %T", k)
	}
	return k, nil
}

// CreateApp makes an app of an organization: a robot named under it, and its key.
func (s *Store) CreateApp(ctx context.Context, org, name string, public bool, key []byte) (App, Principal, error) {
	if !identity.ValidLabel(name) {
		return App{}, Principal{}, invalid("app %q: use a lowercase DNS label", name)
	}
	if _, err := ParseAppKey(key); err != nil {
		return App{}, Principal{}, err
	}
	var a App
	var p Principal
	err := s.b.Txn(ctx, func(tx Ops) error {
		o, err := tx.LockOrganization(ctx, org)
		if err != nil {
			return err
		}
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		full := s.names.Robot(name + "." + o.Handle())
		held, err := tx.NameHeld(ctx, full)
		if err != nil {
			return err
		}
		if held {
			return ErrTaken
		}
		if p, err = tx.InsertPrincipal(ctx, Principal{ID: newID("prn_"), Name: full, Kind: Robot, Status: directory.Active}); err != nil {
			return err
		}
		if err := tx.InsertOrganizationRobot(ctx, p.ID, org); err != nil {
			return err
		}
		a = App{ID: newID("app_"), Robot: p.ID, Organization: org, Public: public, PublicKey: key, Created: s.now()}
		return tx.InsertApp(ctx, a)
	})
	return a, p, err
}

// App reads an app; AppOfRobot the app a robot is.
func (s *Store) App(ctx context.Context, id string) (App, error) { return s.b.App(ctx, id) }
func (s *Store) AppOfRobot(ctx context.Context, robot string) (App, error) {
	return s.b.AppOfRobot(ctx, robot)
}

// Apps are an organization's apps.
func (s *Store) Apps(ctx context.Context, org string) ([]App, error) { return s.b.Apps(ctx, org) }

// SetAppKey replaces an app's key: the old one signs nothing more.
func (s *Store) SetAppKey(ctx context.Context, id string, key []byte) error {
	if _, err := ParseAppKey(key); err != nil {
		return err
	}
	return s.b.SetAppKey(ctx, id, key)
}

// DeleteApp deletes an app: uninstalled everywhere, its tokens gone, its robot deleted.
func (s *Store) DeleteApp(ctx context.Context, id string) error {
	a, err := s.b.App(ctx, id)
	if err != nil {
		return err
	}
	if err := s.b.Txn(ctx, func(tx Ops) error { return tx.DeleteApp(ctx, id) }); err != nil {
		return err
	}
	p, err := s.b.PrincipalByID(ctx, a.Robot)
	if err != nil {
		return err
	}
	return s.DeleteRobot(ctx, p.Name)
}

// Install installs an app in an organization — its own app, or a public one — with a role on its
// repositories: those named, every one of them when none is. Installing again changes the role and
// the repositories.
func (s *Store) Install(ctx context.Context, appID, org string, role tenancy.Role, repos []int64, by string) (Installation, error) {
	if role < tenancy.Read || role > tenancy.Admin {
		return Installation{}, invalid("an app is installed with read, triage, write, maintain, or admin")
	}
	a, err := s.b.App(ctx, appID)
	if err != nil {
		return Installation{}, err
	}
	if !a.Public && a.Organization != org {
		return Installation{}, invalid("the app is its organization's alone: it is not public")
	}
	repos = slices.Clone(repos)
	slices.Sort(repos)
	repos = slices.Compact(repos)
	for _, id := range repos {
		r, err := s.b.Repository(ctx, id)
		if err != nil {
			return Installation{}, fmt.Errorf("repository %d: %w", id, err)
		}
		if r.Owner != org {
			return Installation{}, invalid("repository %d is not the organization's", id)
		}
	}
	var out Installation
	err = s.b.Txn(ctx, func(tx Ops) error {
		if _, err := tx.LockOrganization(ctx, org); err != nil {
			return err
		}
		i, err := tx.InstallationOf(ctx, appID, org)
		switch {
		case err == nil:
			i.Role, i.Repositories = role, repos
			out = i
			return tx.UpdateInstallation(ctx, i)
		case errors.Is(err, ErrNotFound):
			out = Installation{ID: newID("ins_"), App: appID, Organization: org, Role: role, Repositories: repos, CreatedBy: by, Created: s.now()}
			return tx.InsertInstallation(ctx, out)
		}
		return err
	})
	return out, err
}

// AppInstallation reads an installation; InstallationOf an app's in an organization.
func (s *Store) AppInstallation(ctx context.Context, id string) (Installation, error) {
	return s.b.Installation(ctx, id)
}
func (s *Store) InstallationOf(ctx context.Context, app, org string) (Installation, error) {
	return s.b.InstallationOf(ctx, app, org)
}

// Installations are an organization's installations; InstallationsOfApp an app's.
func (s *Store) Installations(ctx context.Context, org string) ([]Installation, error) {
	return s.b.Installations(ctx, org)
}
func (s *Store) InstallationsOfApp(ctx context.Context, app string) ([]Installation, error) {
	return s.b.InstallationsOfApp(ctx, app)
}

// Uninstall removes an installation, and its tokens with it.
func (s *Store) Uninstall(ctx context.Context, id string) (bool, error) {
	var ok bool
	err := s.b.Txn(ctx, func(tx Ops) error {
		var err error
		ok, err = tx.DeleteInstallation(ctx, id)
		return err
	})
	return ok, err
}

// CreateInstallationToken makes an installation's token: its app's robot's, its role and
// repositories, the installing organization's alone, for an hour. It returns its secret.
func (s *Store) CreateInstallationToken(ctx context.Context, installation string) (string, Token, error) {
	i, err := s.b.Installation(ctx, installation)
	if err != nil {
		return "", Token{}, err
	}
	a, err := s.b.App(ctx, i.App)
	if err != nil {
		return "", Token{}, err
	}
	p, err := s.b.PrincipalByID(ctx, a.Robot)
	if err != nil {
		return "", Token{}, err
	}
	if !p.Active() {
		return "", Token{}, invalid("the app's robot is not active")
	}
	secret := InstallationTokenPrefix + token()
	now := s.now()
	t := Token{ID: newID("tok_"), Principal: p.ID, Kind: "installation", Label: "installation " + i.ID, Role: i.Role,
		Repositories: i.Repositories, Installation: i.ID, Owner: i.Organization, Created: now, Expires: now.Add(InstallationTokenLifetime)}
	if err := s.b.InsertToken(ctx, t, hashToken(secret)); err != nil {
		return "", Token{}, err
	}
	return secret, t, nil
}
