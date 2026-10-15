package ldapdir

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/go-ldap/ldap/v3"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/ldapdial"
)

// fake answers searches from entries under the search base: groups are those under ou=groups, and
// a filter naming "group" finds them; the disabled filter finds the disabled DNs.
type fake struct {
	entries  []*ldap.Entry
	disabled []string // DNs the disabled filter finds
	bound    string
}

func (f *fake) Bind(dn, _ string) error { f.bound = dn; return nil }
func (f *fake) Close() error            { return nil }
func (f *fake) Search(r *ldap.SearchRequest) (*ldap.SearchResult, error) {
	return f.SearchWithPaging(r, 0)
}

func (f *fake) SearchWithPaging(r *ldap.SearchRequest, _ uint32) (*ldap.SearchResult, error) {
	res := &ldap.SearchResult{}
	if strings.Contains(r.Filter, "userAccountControl") {
		for _, dn := range f.disabled {
			res.Entries = append(res.Entries, ldap.NewEntry(dn, nil))
		}
		return res, nil
	}
	group := strings.Contains(strings.ToLower(r.Filter), "group")
	for _, e := range f.entries {
		isGroup := strings.Contains(strings.ToLower(e.DN), "ou=groups")
		if strings.HasSuffix(strings.ToLower(e.DN), strings.ToLower(r.BaseDN)) && isGroup == group {
			res.Entries = append(res.Entries, e)
		}
	}
	return res, nil
}

func entry(dn string, attrs map[string][]string) *ldap.Entry { return ldap.NewEntry(dn, attrs) }

func TestOpenLDAP(t *testing.T) {
	f := &fake{entries: []*ldap.Entry{
		entry("uid=ada,ou=people,dc=example,dc=com", map[string][]string{"entryUUID": {"e1"}, "uid": {"ada"}, "mail": {"Ada@Example.com"}, "cn": {"Ada L"}}),
		entry("uid=bea,ou=people,dc=example,dc=com", map[string][]string{"entryUUID": {"e2"}, "uid": {"bea"}, "mail": {"bea@example.com"}, "cn": {"Bea"}}),
		entry("uid=noid,ou=people,dc=example,dc=com", map[string][]string{"uid": {"noid"}}),
		entry("cn=eng,ou=groups,dc=example,dc=com", map[string][]string{"entryUUID": {"g1"}, "cn": {"eng"},
			"member": {"UID=Ada, OU=People,DC=example,DC=com", "cn=infra,ou=groups,dc=example,dc=com", "uid=svc,ou=services,dc=example,dc=com", "uid=ada,ou=people,dc=example,dc=com"}}),
		entry("cn=infra,ou=groups,dc=example,dc=com", map[string][]string{"entryUUID": {"g2"}, "cn": {"infra"}, "member": {"uid=bea,ou=people,dc=example,dc=com"}}),
	}}
	src, err := New(Config{
		Dial:   ldapdial.Config{URL: "ldaps://ldap.example.com"},
		BindDN: "cn=ok,dc=example,dc=com",
		People: People{BaseDN: "ou=people,dc=example,dc=com", Filter: "(objectClass=inetOrgPerson)"},
		Groups: Groups{BaseDN: "ou=groups,dc=example,dc=com"},
	})
	if err != nil {
		t.Fatal(err)
	}
	src.dial = func() (ldapdial.Conn, error) { return f, nil }
	snap, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantPeople := []directory.Person{
		{ExternalID: "e1", Handle: "ada", Email: "ada@example.com", Name: "Ada L", Active: true},
		{ExternalID: "e2", Handle: "bea", Email: "bea@example.com", Name: "Bea", Active: true},
	}
	if !reflect.DeepEqual(snap.People, wantPeople) {
		t.Errorf("people = %+v", snap.People)
	}
	wantGroups := []directory.Group{
		{ExternalID: "g1", Name: "eng", Members: []directory.Member{{ExternalID: "e1"}, {ExternalID: "g2", Group: true}}},
		{ExternalID: "g2", Name: "infra", Members: []directory.Member{{ExternalID: "e2"}}},
	}
	if !reflect.DeepEqual(snap.Groups, wantGroups) {
		t.Errorf("groups = %+v", snap.Groups)
	}
	if f.bound != "cn=ok,dc=example,dc=com" {
		t.Errorf("bound as %q", f.bound)
	}
	if _, err := directory.Reconcile(identity.MustNames("example.com"), directory.NewState(), snap, directory.Options{}); err != nil {
		t.Errorf("Reconcile: %v", err)
	}
}

func TestActiveDirectoryAndMemberUID(t *testing.T) {
	guid := []byte{0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
	ada := ldap.NewEntry("CN=Ada,OU=Staff,DC=corp,DC=example", nil)
	ada.Attributes = []*ldap.EntryAttribute{
		{Name: "objectGUID", Values: []string{string(guid)}, ByteValues: [][]byte{guid}},
		{Name: "sAMAccountName", Values: []string{"ada"}},
		{Name: "mail", Values: []string{"ada@corp.example"}},
	}
	f := &fake{
		entries: []*ldap.Entry{
			ada,
			entry("cn=ops,ou=groups,dc=corp,dc=example", map[string][]string{"entryUUID": {"g1"}, "cn": {"ops"}, "memberUid": {"ADA", "nobody"}}),
		},
		disabled: []string{"cn=ada,ou=staff,dc=corp,dc=example"},
	}
	src, err := New(Config{
		Dial: ldapdial.Config{URL: "ldaps://dc.corp.example"},
		People: People{BaseDN: "OU=Staff,DC=corp,DC=example", Filter: "(&(objectCategory=person)(objectClass=user))",
			DisabledFilter: "(userAccountControl:1.2.840.113556.1.4.803:=2)", IDAttribute: "objectGUID", HandleAttribute: "sAMAccountName"},
		Groups: Groups{BaseDN: "ou=groups,dc=corp,dc=example", Filter: "(objectClass=posixGroup)", MemberAttribute: "memberUid"},
	})
	if err != nil {
		t.Fatal(err)
	}
	src.dial = func() (ldapdial.Conn, error) { return f, nil }
	snap, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.People) != 1 || snap.People[0].ExternalID != "00112233-4455-6677-8899-aabbccddeeff" || snap.People[0].Active {
		t.Errorf("people = %+v", snap.People)
	}
	if len(snap.Groups) != 1 || !reflect.DeepEqual(snap.Groups[0].Members, []directory.Member{{ExternalID: "00112233-4455-6677-8899-aabbccddeeff"}}) {
		t.Errorf("groups = %+v", snap.Groups)
	}
	if _, err := New(Config{Dial: ldapdial.Config{URL: "ldaps://x"}}); err == nil {
		t.Error("no people base was accepted")
	}
}
