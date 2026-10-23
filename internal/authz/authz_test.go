package authz_test

import (
	"testing"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/authz/authztest"
	"github.com/Cidan/ok/internal/tenancy"
)

const (
	none  = tenancy.None
	read  = tenancy.Read
	write = tenancy.Write
	admin = tenancy.Admin
)

// The permission matrix (docs/hosted.md, "Tenancy, as built", "Done when"): every kind of caller,
// at every visibility, for every action.
func TestMatrix(t *testing.T) {
	for _, c := range authztest.Callers {
		for v := tenancy.Private; v <= tenancy.Public; v++ {
			f := authz.Facts{Repository: authztest.Repository(v), Caller: c.Caller}
			want := c.Roles[v]
			if got := authz.RoleOf(f); got != want {
				t.Errorf("%s, %s: role %s, want %s", c.Name, v, got, want)
			}
			for _, a := range authz.Actions {
				d := authz.Decide(f, a)
				if d.Allowed != authztest.Allowed(c, v, a) || d.Visible != authztest.Visible(c, v) || d.Role != want {
					t.Errorf("%s, %s, %s: %+v; want allowed %v", c.Name, v, a, d, authztest.Allowed(c, v, a))
				}
				if !d.Allowed && d.Why == "" {
					t.Errorf("%s, %s, %s: refused with no reason", c.Name, v, a)
				}
			}
		}
	}
}

// A repository may require write to propose.
func TestProposeNeedsWrite(t *testing.T) {
	r := authztest.Repository(tenancy.Public)
	r.Propose = write
	for _, c := range authztest.Callers {
		d := authz.Decide(authz.Facts{Repository: r, Caller: c.Caller}, authz.Propose)
		if want := c.Roles[tenancy.Public] >= write && c.Caller.ID != ""; d.Allowed != want {
			t.Errorf("%s proposing where proposing needs write: %+v, want %v", c.Name, d, want)
		}
	}
}

// An organization may let its private repositories be forked, by those who read them.
func TestPrivateForks(t *testing.T) {
	r := authztest.Repository(tenancy.Private)
	r.Org.PrivateForks = true
	for _, c := range authztest.Callers {
		d := authz.Decide(authz.Facts{Repository: r, Caller: c.Caller}, authz.Fork)
		if want := c.Roles[tenancy.Private] >= read && c.Caller.ID != ""; d.Allowed != want {
			t.Errorf("%s forking a private repository its organization lets fork: %+v, want %v", c.Name, d, want)
		}
	}
}

// A person's repository is theirs to administer; others' roles come from grants and visibility.
func TestPersonsRepository(t *testing.T) {
	r := authz.Repository{ID: 7, Visibility: tenancy.Private, Propose: read, Owner: "prn_ada"}
	for name, tc := range map[string]struct {
		c    authz.Caller
		want tenancy.Role
	}{
		"its owner":            {authz.Caller{ID: "prn_ada", Active: true}, admin},
		"a collaborator":       {authz.Caller{ID: "prn_bob", Active: true, Granted: []tenancy.Role{write}}, write},
		"a stranger":           {authz.Caller{ID: "prn_eve", Active: true}, none},
		"a member of no one's": {authz.Caller{ID: "prn_eve", Active: true, Membership: tenancy.Owner}, none},
		"its owner, suspended": {authz.Caller{ID: "prn_ada"}, none},
	} {
		if got := authz.RoleOf(authz.Facts{Repository: r, Caller: tc.c}); got != tc.want {
			t.Errorf("%s: %s, want %s", name, got, tc.want)
		}
	}
	// A person's private repository forks for those who read it: there is no organization to say no.
	if d := authz.Decide(authz.Facts{Repository: r, Caller: authz.Caller{ID: "prn_bob", Active: true, Granted: []tenancy.Role{read}}}, authz.Fork); !d.Allowed {
		t.Errorf("a collaborator forking a person's private repository: %+v", d)
	}
}

// A deleted repository is its admins' alone: they see it and may restore it; nothing else is done
// to it, by anyone. A repository standing is not restored.
func TestDeleted(t *testing.T) {
	for _, c := range authztest.Callers {
		r := authztest.Repository(tenancy.Public)
		r.State = authz.Deleted
		isAdmin := c.Roles[tenancy.Public] == admin
		for _, a := range authz.Actions {
			d := authz.Decide(authz.Facts{Repository: r, Caller: c.Caller}, a)
			if want := a == authz.Restore && isAdmin; d.Allowed != want || d.Visible != isAdmin {
				t.Errorf("%s, deleted, %s: %+v; want allowed %v, visible %v", c.Name, a, d, want, isAdmin)
			}
		}
	}
	owner := authz.Caller{ID: "prn_owner", Active: true, Membership: tenancy.Owner}
	if d := authz.Decide(authz.Facts{Repository: authztest.Repository(tenancy.Public), Caller: owner}, authz.Restore); d.Allowed {
		t.Errorf("restoring a repository that stands: %+v", d)
	}
}

// No one signed in reads a public repository and is told to sign in for anything more; anyone else
// is told what they lack.
func TestWhy(t *testing.T) {
	public := authztest.Repository(tenancy.Public)
	if d := authz.Decide(authz.Facts{Repository: public}, authz.ReadCode); !d.Allowed {
		t.Errorf("anonymous reading a public repository: %+v", d)
	}
	if d := authz.Decide(authz.Facts{Repository: public}, authz.Propose); d.Allowed || d.Why != "to propose, sign in" {
		t.Errorf("anonymous proposing: %+v", d)
	}
	d := authz.Decide(authz.Facts{Repository: public, Caller: authz.Caller{ID: "prn_x", Active: true}}, authz.Land)
	if d.Allowed || d.Why != "to land needs write, and the caller is read" {
		t.Errorf("a stranger landing: %+v", d)
	}
}
