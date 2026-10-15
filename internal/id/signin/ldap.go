package signin

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-ldap/ldap/v3"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/ldapdial"
)

// LDAPConfig is an LDAP directory people sign in to with their password.
type LDAPConfig struct {
	// URL is the server: ldaps://ldap.example.com, or ldap:// with StartTLS.
	URL      string
	StartTLS bool
	// CAFile verifies the server's certificate, when it is not one the system trusts.
	CAFile string
	// BindDN and BindPassword are the account ok searches as.
	BindDN       string
	BindPassword string
	// BaseDN is where people's entries are.
	BaseDN string
	// UserFilter finds the entry of a username, which replaces {username}, escaped:
	// (&(objectClass=person)(uid={username})).
	UserFilter string
	// Match is how the entry names the person; default ByEmail.
	Match Match
	// Attribute is the entry's attribute Match reads: mail, entryUUID, objectGUID.
	Attribute string
}

// ErrBadPassword is a username and password the directory refused.
var ErrBadPassword = errors.New("the username or the password is wrong")

// LDAP signs people in against an LDAP directory.
type LDAP struct {
	cfg  LDAPConfig
	dial func() (ldapdial.Conn, error)
}

// NewLDAP returns the LDAP authenticator over cfg.
func NewLDAP(cfg LDAPConfig) (*LDAP, error) {
	if cfg.BaseDN == "" || !strings.Contains(cfg.UserFilter, "{username}") {
		return nil, errors.New("an LDAP directory needs a base DN, and a user filter with {username} in it")
	}
	if cfg.Match == "" {
		cfg.Match = ByEmail
	}
	if cfg.Attribute == "" {
		cfg.Attribute = "mail"
	}
	dial, err := ldapdial.Dialer(ldapdial.Config{URL: cfg.URL, StartTLS: cfg.StartTLS, CAFile: cfg.CAFile})
	if err != nil {
		return nil, err
	}
	return &LDAP{cfg: cfg, dial: dial}, nil
}

// SignIn checks a username and password against the directory and returns the person.
func (l *LDAP) SignIn(ctx context.Context, people People, username, password string) (store.Principal, error) {
	username = strings.TrimSpace(username)
	// An empty password is an unauthenticated bind, which LDAP servers accept without checking
	// anything.
	if username == "" || password == "" {
		return store.Principal{}, ErrBadPassword
	}
	conn, err := l.dial()
	if err != nil {
		return store.Principal{}, fmt.Errorf("the LDAP directory: %w", err)
	}
	defer conn.Close()
	if l.cfg.BindDN != "" {
		if err := conn.Bind(l.cfg.BindDN, l.cfg.BindPassword); err != nil {
			return store.Principal{}, fmt.Errorf("the LDAP directory refused ok's own account: %w", err)
		}
	}
	filter := strings.ReplaceAll(l.cfg.UserFilter, "{username}", ldap.EscapeFilter(username))
	res, err := conn.Search(ldap.NewSearchRequest(l.cfg.BaseDN, ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 2, 10, false,
		filter, []string{l.cfg.Attribute}, nil))
	if err != nil {
		return store.Principal{}, fmt.Errorf("searching the LDAP directory: %w", err)
	}
	if len(res.Entries) != 1 {
		return store.Principal{}, ErrBadPassword
	}
	entry := res.Entries[0]
	if err := conn.Bind(entry.DN, password); err != nil {
		if ldap.IsErrorWithCode(err, ldap.LDAPResultInvalidCredentials) {
			return store.Principal{}, ErrBadPassword
		}
		return store.Principal{}, fmt.Errorf("the LDAP directory: %w", err)
	}
	value := ldapdial.ID(entry, l.cfg.Attribute)
	if l.cfg.Match == ByEmail {
		value = strings.ToLower(value)
	}
	return find(ctx, people, l.cfg.Match, value)
}
