package role

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/id/enterprise"
	"github.com/Cidan/ok/internal/id/seal"
	"github.com/Cidan/ok/internal/id/store"
	idpostgres "github.com/Cidan/ok/internal/id/store/postgres"
	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/testdb"
)

type snapshot struct{ s *directory.Snapshot }

func (s snapshot) Snapshot(context.Context) (*directory.Snapshot, error) { return s.s, nil }
func (snapshot) Changes(context.Context, string) (*directory.Delta, error) {
	return nil, directory.ErrNoDelta
}

// An organization's SCIM endpoint takes its own token and no other — not another organization's,
// not none — and what is provisioned there is the organization's, whose sync makes its people.
func TestOrganizationSCIM(t *testing.T) {
	ctx := context.Background()
	b, err := idpostgres.Open(ctx, testdb.Pool(t))
	if err != nil {
		t.Fatal(err)
	}
	names := identity.MustNames("example.com")
	st := store.New(b, names)
	if _, err := directory.Sync(ctx, names, snapshot{&directory.Snapshot{People: []directory.Person{
		{ExternalID: "1", Handle: "ada", Email: "ada@example.com", Name: "Ada", Active: true}}}}, st, directory.Options{}); err != nil {
		t.Fatal(err)
	}
	ada, _ := st.Principal(ctx, "ada.human.example.com")
	var tokens [2]string
	for i, h := range []string{"acme", "globex"} {
		o, err := st.CreateOrganization(ctx, h, ada.ID, store.OrganizationSettings{})
		if err != nil {
			t.Fatal(err)
		}
		if err := st.SetIdentityProvider(ctx, store.IdentityProvider{Organization: o.ID, Source: "scim", Settings: []byte(`{"scim":{}}`)}); err != nil {
			t.Fatal(err)
		}
		if tokens[i], err = st.SetSCIMToken(ctx, o.ID); err != nil {
			t.Fatal(err)
		}
	}
	sealer, _ := seal.New(bytes.Repeat([]byte{5}, 32))
	o := &orgSCIM{st: st, sync: &enterprise.Syncer{Store: st, Sealer: sealer, AllowPrivate: true}, base: "https://ok.example.com", pending: map[string]bool{}}
	sub := chi.NewMux()
	prefix, err := o.Mount(sub)
	if err != nil {
		t.Fatal(err)
	}
	r := chi.NewRouter()
	r.Mount(prefix, sub)
	srv := httptest.NewServer(r)
	defer srv.Close()

	post := func(handle, token string) int {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/id/scim/v2/orgs/"+handle+"/Users",
			strings.NewReader(`{"schemas":["urn:ietf:params:scim:schemas:core:2.0:User"],"userName":"erin@acme.example","active":true}`))
		req.Header.Set("Content-Type", "application/scim+json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	for name, code := range map[string]int{
		"no token":                     post("acme", ""),
		"another organization's":       post("acme", tokens[1]),
		"an organization there is not": post("nobody", tokens[0]),
	} {
		if code != http.StatusUnauthorized {
			t.Errorf("%s: %d", name, code)
		}
	}
	if code := post("acme", tokens[0]); code != http.StatusCreated {
		t.Fatalf("provisioning with acme's token: %d", code)
	}
	acme, _ := st.OrganizationByHandle(ctx, "acme")
	globex, _ := st.OrganizationByHandle(ctx, "globex")
	if us, _ := st.SCIMOf(acme.ID).Users(ctx); len(us) != 1 {
		t.Errorf("acme's SCIM users: %d", len(us))
	}
	if us, _ := st.SCIMOf(globex.ID).Users(ctx); len(us) != 0 {
		t.Errorf("globex's SCIM users: %d", len(us))
	}
	if _, err := o.sync.Sync(ctx, acme.ID); err != nil {
		t.Fatal(err)
	}
	people, err := st.OrganizationPeople(ctx, acme.ID)
	if err != nil || len(people) != 1 {
		t.Fatalf("acme's people after its sync = %v, %v", people, err)
	}
	for _, p := range people {
		if p.Name != "erin-acme.human.example.com" {
			t.Errorf("the provisioned person: %s", p.Name)
		}
	}
}
