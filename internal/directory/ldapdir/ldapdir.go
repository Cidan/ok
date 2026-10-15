// Package ldapdir reads an LDAP directory — OpenLDAP, Active Directory, FreeIPA, any — as ok's
// system of record: the entries a filter finds under a base are people, and those another finds
// are groups, whose members are DNs (member, uniqueMember) or usernames (memberUid).
package ldapdir

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-ldap/ldap/v3"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/ldapdial"
)

// People is where and how to find people.
type People struct {
	BaseDN string
	// Filter finds people: (objectClass=inetOrgPerson), or Active Directory's
	// (&(objectCategory=person)(objectClass=user)).
	Filter string
	// DisabledFilter, when set, finds the disabled among them, who are suspended: Active
	// Directory's (userAccountControl:1.2.840.113556.1.4.803:=2).
	DisabledFilter string
	// The attributes a person's ID (immutable: entryUUID, objectGUID, ipaUniqueID), handle (uid,
	// sAMAccountName), email, and name are in.
	IDAttribute, HandleAttribute, EmailAttribute, NameAttribute string
}

// Groups is where and how to find groups.
type Groups struct {
	BaseDN string
	// Filter finds groups: (objectClass=groupOfNames), (objectClass=group).
	Filter string
	// The attributes a group's ID and name are in.
	IDAttribute, NameAttribute string
	// MemberAttribute holds a group's members: member or uniqueMember (DNs), or memberUid
	// (people's handles).
	MemberAttribute string
}

// Config is a directory to read.
type Config struct {
	Dial ldapdial.Config
	// BindDN and BindPassword are the account ok reads as.
	BindDN, BindPassword string
	People               People
	// Groups, when its BaseDN is empty, syncs no groups.
	Groups Groups
}

// Source is an LDAP directory as a directory.Source. It says no deltas: each sync reads it whole.
type Source struct {
	cfg  Config
	dial func() (ldapdial.Conn, error)
}

// New returns the directory source over cfg, defaulting the attributes to OpenLDAP's.
func New(cfg Config) (*Source, error) {
	p := &cfg.People
	if p.BaseDN == "" || p.Filter == "" {
		return nil, errors.New("an LDAP system of record needs the base DN and filter that find people")
	}
	p.IDAttribute = or(p.IDAttribute, "entryUUID")
	p.HandleAttribute = or(p.HandleAttribute, "uid")
	p.EmailAttribute = or(p.EmailAttribute, "mail")
	p.NameAttribute = or(p.NameAttribute, "cn")
	g := &cfg.Groups
	if g.BaseDN != "" {
		g.Filter = or(g.Filter, "(objectClass=groupOfNames)")
		g.IDAttribute = or(g.IDAttribute, "entryUUID")
		g.NameAttribute = or(g.NameAttribute, "cn")
		g.MemberAttribute = or(g.MemberAttribute, "member")
	}
	dial, err := ldapdial.Dialer(cfg.Dial)
	if err != nil {
		return nil, err
	}
	return &Source{cfg: cfg, dial: dial}, nil
}

func or(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// Changes says no deltas.
func (s *Source) Changes(context.Context, string) (*directory.Delta, error) {
	return nil, directory.ErrNoDelta
}

// pageSize is how many entries each page of a search brings.
const pageSize = 500

func (s *Source) search(conn ldapdial.Conn, base, filter string, attrs []string) ([]*ldap.Entry, error) {
	res, err := conn.SearchWithPaging(ldap.NewSearchRequest(base, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false, filter, attrs, nil), pageSize)
	if err != nil {
		return nil, fmt.Errorf("searching %s for %s: %w", base, filter, err)
	}
	return res.Entries, nil
}

// Snapshot reads the people and groups the filters find.
func (s *Source) Snapshot(ctx context.Context) (*directory.Snapshot, error) {
	conn, err := s.dial()
	if err != nil {
		return nil, fmt.Errorf("the LDAP directory: %w", err)
	}
	defer conn.Close()
	if s.cfg.BindDN != "" {
		if err := conn.Bind(s.cfg.BindDN, s.cfg.BindPassword); err != nil {
			return nil, fmt.Errorf("the LDAP directory refused ok's account: %w", err)
		}
	}
	p := s.cfg.People
	entries, err := s.search(conn, p.BaseDN, p.Filter, []string{p.IDAttribute, p.HandleAttribute, p.EmailAttribute, p.NameAttribute})
	if err != nil {
		return nil, err
	}
	disabled := map[string]bool{}
	if p.DisabledFilter != "" {
		off, err := s.search(conn, p.BaseDN, "(&"+p.Filter+p.DisabledFilter+")", []string{"dn"})
		if err != nil {
			return nil, err
		}
		for _, e := range off {
			disabled[ldapdial.NormalizeDN(e.DN)] = true
		}
	}
	snap := &directory.Snapshot{}
	byDN := map[string]directory.Member{}
	byHandle := map[string]string{}
	for _, e := range entries {
		id := ldapdial.ID(e, p.IDAttribute)
		if id == "" {
			continue // an entry without the ID attribute cannot be followed across renames
		}
		handle := e.GetAttributeValue(p.HandleAttribute)
		dn := ldapdial.NormalizeDN(e.DN)
		snap.People = append(snap.People, directory.Person{
			ExternalID: id, Handle: handle, Email: strings.ToLower(e.GetAttributeValue(p.EmailAttribute)),
			Name: e.GetAttributeValue(p.NameAttribute), Active: !disabled[dn],
		})
		byDN[dn] = directory.Member{ExternalID: id}
		byHandle[strings.ToLower(handle)] = id
	}
	g := s.cfg.Groups
	if g.BaseDN == "" {
		return snap, nil
	}
	groups, err := s.search(conn, g.BaseDN, g.Filter, []string{g.IDAttribute, g.NameAttribute, g.MemberAttribute})
	if err != nil {
		return nil, err
	}
	for _, e := range groups {
		if id := ldapdial.ID(e, g.IDAttribute); id != "" {
			byDN[ldapdial.NormalizeDN(e.DN)] = directory.Member{ExternalID: id, Group: true}
		}
	}
	byUID := strings.EqualFold(g.MemberAttribute, "memberUid")
	for _, e := range groups {
		id := ldapdial.ID(e, g.IDAttribute)
		if id == "" {
			continue
		}
		dg := directory.Group{ExternalID: id, Name: e.GetAttributeValue(g.NameAttribute)}
		seen := map[string]bool{}
		for _, v := range e.GetAttributeValues(g.MemberAttribute) {
			var m directory.Member
			var ok bool
			if byUID {
				var pid string
				pid, ok = byHandle[strings.ToLower(v)]
				m = directory.Member{ExternalID: pid}
			} else {
				m, ok = byDN[ldapdial.NormalizeDN(v)]
			}
			// A member the filters did not find — outside the bases, a service account — is no one
			// ok knows.
			if ok && !seen[m.ExternalID] && m.ExternalID != id {
				seen[m.ExternalID] = true
				dg.Members = append(dg.Members, m)
			}
		}
		snap.Groups = append(snap.Groups, dg)
	}
	return snap, nil
}
