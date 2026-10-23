package store

import (
	"context"
	"errors"
	"net/url"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/identity"
)

// An organization's robots (docs/hosted.md, "Robots and tokens per organization"): CI identities,
// deploy agents, apps — named under it, <name>.<org>.robot.<domain>, and reaching its repositories
// through their grants alone: a robot is no member of its organization, and is granted nothing of
// another's.

// CreateOrganizationRobot makes a robot of an organization, named under its handle.
func (s *Store) CreateOrganizationRobot(ctx context.Context, org, name string) (Principal, error) {
	if !identity.ValidLabel(name) {
		return Principal{}, invalid("robot %q: use a lowercase DNS label", name)
	}
	var p Principal
	err := s.b.Txn(ctx, func(tx Ops) error {
		o, err := tx.LockOrganization(ctx, org)
		if err != nil {
			return err
		}
		if org == s.installation {
			return invalid("the installation's robots are its administrators' to make (ok admin robot create)")
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
		return tx.InsertOrganizationRobot(ctx, p.ID, org)
	})
	return p, err
}

// OrganizationRobots are an organization's robots, by name, deleted ones too.
func (s *Store) OrganizationRobots(ctx context.Context, org string) ([]Principal, error) {
	return s.b.OrganizationRobots(ctx, org)
}

// RobotOrganization is the organization a robot is of; ErrNotFound for a robot of none — one of the
// installation's, or one of ok's roles.
func (s *Store) RobotOrganization(ctx context.Context, robot string) (string, error) {
	return s.b.RobotOrganization(ctx, robot)
}

// DeleteOrganizationRobot deletes one of an organization's robots, as DeleteRobot does: a
// tombstone, its name retired, its credentials revoked, its bindings gone.
func (s *Store) DeleteOrganizationRobot(ctx context.Context, org, robot string) error {
	of, err := s.b.RobotOrganization(ctx, robot)
	if errors.Is(err, ErrNotFound) || (err == nil && of != org) {
		return invalid("%s is no robot of this organization", robot)
	}
	if err != nil {
		return err
	}
	p, err := s.b.PrincipalByID(ctx, robot)
	if err != nil {
		return err
	}
	return s.DeleteRobot(ctx, p.Name)
}

// Federation per organization: the OpenID Connect issuers an organization trusts to sign its robots
// in, and its robots' bindings to them. A token for an organization carries the organization's ID
// as its audience (OrganizationAudience).

// OrganizationAudience is the audience a token an organization's issuer signs for ok must carry:
// the organization's ID, which no other service accepts and no rename changes.
func OrganizationAudience(org string) string { return org }

// validIssuer refuses what is no issuer's URL: an absolute http or https URL with a host, and no
// user, query, or fragment.
func validIssuer(issuer string) error {
	u, err := url.Parse(issuer)
	switch {
	case err != nil, len(issuer) > 512, u.Scheme != "https" && u.Scheme != "http", u.Host == "", u.User != nil,
		u.RawQuery != "", u.Fragment != "", u.Opaque != "":
		return invalid("%q is no issuer's URL: name it as its tokens' iss claim does, https://…", issuer)
	}
	return nil
}

// TrustIssuer has an organization trust an issuer to sign its robots in. The installation's issuers
// are its configuration's.
func (s *Store) TrustIssuer(ctx context.Context, org, issuer string) error {
	if err := validIssuer(issuer); err != nil {
		return err
	}
	if org == s.installation {
		return invalid("the installation's issuers are its configuration's (identity.federation)")
	}
	if _, err := s.b.Organization(ctx, org); err != nil {
		return err
	}
	return s.b.InsertIssuer(ctx, org, issuer, s.now())
}

// DistrustIssuer stops an organization trusting an issuer, unbinding its robots from it.
func (s *Store) DistrustIssuer(ctx context.Context, org, issuer string) (bool, error) {
	var ok bool
	err := s.b.Txn(ctx, func(tx Ops) error {
		var err error
		ok, err = tx.DeleteIssuer(ctx, org, issuer)
		return err
	})
	return ok, err
}

// Issuers are the issuers an organization trusts, sorted.
func (s *Store) Issuers(ctx context.Context, org string) ([]string, error) {
	return s.b.Issuers(ctx, org)
}

// TrustsIssuer reports whether an organization trusts an issuer.
func (s *Store) TrustsIssuer(ctx context.Context, org, issuer string) (bool, error) {
	return s.b.TrustsIssuer(ctx, org, issuer)
}

// BindOrganizationRobot binds one of an organization's robots to the tokens an issuer the
// organization trusts signs for a subject, with the organization's audience.
func (s *Store) BindOrganizationRobot(ctx context.Context, org, robot, issuer, subject string) error {
	if subject == "" || len(subject) > 512 {
		return invalid("a binding names its issuer's subject: 1 to 512 characters")
	}
	of, err := s.b.RobotOrganization(ctx, robot)
	if errors.Is(err, ErrNotFound) || (err == nil && of != org) {
		return invalid("%s is no robot of this organization", robot)
	}
	if err != nil {
		return err
	}
	p, err := s.b.PrincipalByID(ctx, robot)
	if err != nil {
		return err
	}
	if !p.Active() {
		return invalid("%s is not an active robot", p.Name)
	}
	trusted, err := s.b.TrustsIssuer(ctx, org, issuer)
	if err != nil {
		return err
	}
	if !trusted {
		return invalid("the organization does not trust %s: trust it first", issuer)
	}
	err = s.b.InsertBinding(ctx, robot, Binding{Organization: org, Issuer: issuer, Subject: subject})
	if errors.Is(err, ErrBindingTaken) {
		return invalid("%s's subject %s already names one of the organization's robots", issuer, subject)
	}
	return err
}

// UnbindOrganizationRobot removes one of an organization's robots' bindings, reporting whether it
// had it.
func (s *Store) UnbindOrganizationRobot(ctx context.Context, org, robot, issuer, subject string) (bool, error) {
	return s.b.DeleteOrganizationBinding(ctx, robot, Binding{Organization: org, Issuer: issuer, Subject: subject})
}

// OrganizationBindings are an organization's robots' bindings, by robot ID.
func (s *Store) OrganizationBindings(ctx context.Context, org string) (map[string][]Binding, error) {
	return s.b.OrganizationBindings(ctx, org)
}
