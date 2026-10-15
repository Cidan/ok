package signin

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-ldap/ldap/v3"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/ldapdial"
)

// fakeLDAP is a directory of entries by DN, each with a password and attributes.
type fakeLDAP struct {
	entries  map[string]map[string][]string
	password map[string]string
	searched []string
	bound    []string
}

func (f *fakeLDAP) Bind(dn, pw string) error {
	f.bound = append(f.bound, dn)
	if want, ok := f.password[dn]; ok && pw == want && pw != "" {
		return nil
	}
	return ldap.NewError(ldap.LDAPResultInvalidCredentials, errors.New("invalid credentials"))
}

func (f *fakeLDAP) Search(r *ldap.SearchRequest) (*ldap.SearchResult, error) {
	f.searched = append(f.searched, r.Filter)
	res := &ldap.SearchResult{}
	for dn, attrs := range f.entries {
		if strings.Contains(r.Filter, "(uid="+attrs["uid"][0]+")") {
			e := ldap.NewEntry(dn, attrs)
			res.Entries = append(res.Entries, e)
		}
	}
	return res, nil
}

func (f *fakeLDAP) SearchWithPaging(r *ldap.SearchRequest, _ uint32) (*ldap.SearchResult, error) {
	return f.Search(r)
}

func (f *fakeLDAP) Close() error { return nil }

// people is a directory of people in memory.
type people map[string]store.Principal // by email and by external ID

func (p people) ByEmail(_ context.Context, email string) (store.Principal, error) {
	if pr, ok := p["email:"+email]; ok {
		return pr, nil
	}
	return store.Principal{}, store.ErrNotFound
}

func (p people) ByExternalID(_ context.Context, id string) (store.Principal, error) {
	if pr, ok := p["id:"+id]; ok {
		return pr, nil
	}
	return store.Principal{}, store.ErrNotFound
}

func TestLDAPSignIn(t *testing.T) {
	ada := store.Principal{ID: "p1", Name: "ada.human.example.com", Kind: store.Human, Status: directory.Active}
	gone := store.Principal{ID: "p2", Name: "gone.human.example.com", Kind: store.Human, Status: directory.Suspended}
	dir := people{"email:ada@example.com": ada, "id:e-1": ada, "email:gone@example.com": gone}
	fake := &fakeLDAP{
		entries: map[string]map[string][]string{
			"uid=ada,ou=people,dc=example,dc=com":  {"uid": {"ada"}, "mail": {"Ada@Example.com"}, "entryUUID": {"e-1"}},
			"uid=gone,ou=people,dc=example,dc=com": {"uid": {"gone"}, "mail": {"gone@example.com"}},
		},
		password: map[string]string{
			"cn=ok,dc=example,dc=com":              "service",
			"uid=ada,ou=people,dc=example,dc=com":  "right",
			"uid=gone,ou=people,dc=example,dc=com": "right",
		},
	}
	l, err := NewLDAP(LDAPConfig{URL: "ldaps://ldap.example.com", BindDN: "cn=ok,dc=example,dc=com", BindPassword: "service",
		BaseDN: "ou=people,dc=example,dc=com", UserFilter: "(&(objectClass=person)(uid={username}))"})
	if err != nil {
		t.Fatal(err)
	}
	l.dial = func() (ldapdial.Conn, error) { return fake, nil }
	ctx := context.Background()

	got, err := l.SignIn(ctx, dir, " ada ", "right")
	if err != nil || got.ID != "p1" {
		t.Fatalf("SignIn = %+v, %v", got, err)
	}
	for _, tc := range []struct{ user, pw string }{{"ada", "wrong"}, {"ada", ""}, {"nobody", "right"}, {"", "right"}} {
		if _, err := l.SignIn(ctx, dir, tc.user, tc.pw); !errors.Is(err, ErrBadPassword) {
			t.Errorf("SignIn(%q, %q) = %v, want ErrBadPassword", tc.user, tc.pw, err)
		}
	}
	if _, err := l.SignIn(ctx, dir, "gone", "right"); !errors.Is(err, ErrNoPerson) {
		t.Errorf("a suspended person: %v", err)
	}
	// A username cannot change the filter.
	l.SignIn(ctx, dir, "*)(uid=ada", "right")
	if last := fake.searched[len(fake.searched)-1]; !strings.Contains(last, `\2a\29\28uid=ada`) {
		t.Errorf("the filter was not escaped: %s", last)
	}

	l.cfg.Match, l.cfg.Attribute = ByExternalID, "entryUUID"
	if got, err := l.SignIn(ctx, dir, "ada", "right"); err != nil || got.ID != "p1" {
		t.Errorf("by external ID: %+v, %v", got, err)
	}

	if _, err := NewLDAP(LDAPConfig{URL: "ldap://ldap.example.com", BaseDN: "dc=x", UserFilter: "(uid={username})"}); err == nil {
		t.Error("a plaintext LDAP directory was accepted")
	}
}
