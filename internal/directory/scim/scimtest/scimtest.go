// Package scimtest is a conformance suite for scim.Store: SCIM conversations, as Microsoft Entra ID
// and Okta provision, run against a store over the real handler.
package scimtest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/identity"
)

const bearer = "0123456789abcdef0123456789abcdef-scim"

type client struct {
	t       *testing.T
	srv     *httptest.Server
	changes *int
}

func newClient(t *testing.T, newStore func(*testing.T) scim.Store) (*client, scim.Store) {
	t.Helper()
	store := newStore(t)
	n := 0
	var srv *httptest.Server
	mux := http.NewServeMux()
	var h *scim.Handler
	mux.Handle("/id/scim/v2/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r) }))
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	var err error
	h, err = scim.NewHandler(store, bearer, srv.URL+"/id/scim/v2", func() { n++ })
	if err != nil {
		t.Fatal(err)
	}
	return &client{t: t, srv: srv, changes: &n}, store
}

// do sends a request and returns the status and the decoded body.
func (c *client) do(method, path, body string) (int, map[string]any) {
	c.t.Helper()
	req, _ := http.NewRequest(method, c.srv.URL+"/id/scim/v2"+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/scim+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			c.t.Fatalf("%s %s answered %s: %s", method, path, resp.Status, raw)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/scim+json" {
			c.t.Errorf("%s %s: Content-Type %q", method, path, ct)
		}
	}
	return resp.StatusCode, out
}

func q(filter string) string { return "?filter=" + url.QueryEscape(filter) }

func total(body map[string]any) int { return int(body["totalResults"].(float64)) }

func authentication(t *testing.T, newStore func(*testing.T) scim.Store) {
	c, _ := newClient(t, newStore)
	for _, auth := range []string{"", "Bearer wrong", "Basic " + bearer, "Bearer " + bearer + "x"} {
		req, _ := http.NewRequest("GET", c.srv.URL+"/id/scim/v2/Users", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, _ := http.DefaultClient.Do(req)
		resp.Body.Close()
		if resp.StatusCode != 401 {
			t.Errorf("Authorization %q: %d", auth, resp.StatusCode)
		}
	}
	if _, err := scim.NewHandler(newStore(t), "short", "", nil); err == nil {
		t.Error("a short token was accepted")
	}
}

// entraUsers follows Microsoft Entra ID's provisioning of a user, as its documentation shows
// the requests.
func entraUsers(t *testing.T, newStore func(*testing.T) scim.Store) {
	c, _ := newClient(t, newStore)
	if code, body := c.do("GET", "/Users"+q(`userName eq "jyoung@testuser.com"`), ""); code != 200 || total(body) != 0 {
		t.Fatalf("an empty query: %d %v", code, body)
	}
	code, created := c.do("POST", "/Users", `{
		"schemas": ["urn:ietf:params:scim:schemas:core:2.0:User","urn:ietf:params:scim:schemas:extension:enterprise:2.0:User"],
		"externalId":"jyoung","userName":"jyoung@testuser.com","active":true,"addresses":null,"displayName":"Joy Young",
		"emails":[{"type":"work","value":"jyoung@Contoso.com","primary":true}],"meta":{"resourceType":"User"},
		"name":{"familyName":"Young","givenName":"Joy"},"phoneNumbers":null,"preferredLanguage":null,"title":null,"department":null,"manager":null}`)
	if code != 201 || created["id"] == "" || created["meta"].(map[string]any)["resourceType"] != "User" {
		t.Fatalf("POST = %d %v", code, created)
	}
	id := created["id"].(string)
	// The bare value Entra sends.
	if code, body := c.do("GET", "/Users"+q(`externalId eq jyoung`), ""); code != 200 || total(body) != 1 {
		t.Errorf("by externalId: %d %v", code, body)
	}
	// userName is not case-exact.
	if _, body := c.do("GET", "/Users"+q(`userName eq "JYOUNG@testuser.com"`), ""); total(body) != 1 {
		t.Errorf("by userName, another case: %v", body)
	}
	// ok keeps no manager: the manager check finds no one.
	if _, body := c.do("GET", "/Users"+q(`id eq "`+id+`" and manager eq "someone"`), ""); total(body) != 0 {
		t.Errorf("a manager check: %v", body)
	}
	if code, _ := c.do("POST", "/Users", `{"userName":"JYoung@testuser.com"}`); code != 409 {
		t.Errorf("a duplicate userName: %d", code)
	}
	code, patched := c.do("PATCH", "/Users/"+id, `{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[
		{"op":"Replace","path":"active","value":"False"},
		{"op":"Replace","value":{"displayName":"Joy Y","name.givenName":"Joyce"}},
		{"op":"Add","path":"emails[type eq \"work\"].value","value":"joy@contoso.com"},
		{"op":"Add","path":"manager","value":[{"value":"m1"}]}]}`)
	if code != 200 || patched["active"] != false || patched["displayName"] != "Joy Y" || patched["name"].(map[string]any)["givenName"] != "Joyce" {
		t.Fatalf("PATCH = %d %v", code, patched)
	}
	if emails := patched["emails"].([]any); len(emails) != 1 || emails[0].(map[string]any)["value"] != "joy@contoso.com" {
		t.Errorf("emails = %v", emails)
	}
	// A soft-deleted user is still served.
	if code, body := c.do("GET", "/Users/"+id, ""); code != 200 || body["active"] != false {
		t.Errorf("an inactive user: %d %v", code, body)
	}
	if code, _ := c.do("PATCH", "/Users/"+id, `{"Operations":[{"op":"Move","path":"active","value":true}]}`); code != 400 {
		t.Errorf("an unknown op: %d", code)
	}
	if code, _ := c.do("DELETE", "/Users/"+id, ""); code != 204 {
		t.Errorf("DELETE: %d", code)
	}
	if code, body := c.do("GET", "/Users/"+id, ""); code != 404 || body["status"] != "404" {
		t.Errorf("a deleted user: %d %v", code, body)
	}
	if *c.changes != 3 {
		t.Errorf("%d changes told, want 3", *c.changes)
	}
}

// groups follows Entra's and Okta's group provisioning.
func groups(t *testing.T, newStore func(*testing.T) scim.Store) {
	c, store := newClient(t, newStore)
	var ids []string
	for _, u := range []string{"ada@example.com", "bea@example.com", "cal@example.com"} {
		_, body := c.do("POST", "/Users", `{"userName":"`+u+`"}`)
		ids = append(ids, body["id"].(string))
	}
	code, g := c.do("POST", "/Groups", `{"schemas":["urn:ietf:params:scim:schemas:core:2.0:Group"],"displayName":"Eng","externalId":"x1","members":[{"value":"`+ids[0]+`"}]}`)
	if code != 201 {
		t.Fatalf("POST group = %d %v", code, g)
	}
	gid := g["id"].(string)
	_, sub := c.do("POST", "/Groups", `{"displayName":"Infra"}`)
	subID := sub["id"].(string)

	_, body := c.do("GET", "/Groups"+q(`displayName eq "eng"`)+"&excludedAttributes=members", "")
	res := body["Resources"].([]any)
	if total(body) != 1 || res[0].(map[string]any)["members"] != nil {
		t.Errorf("a group query without members: %v", body)
	}
	// Entra adds and removes with values; Okta removes with a filter.
	for _, patch := range []string{
		`{"Operations":[{"op":"Add","path":"members","value":[{"value":"` + ids[1] + `"},{"value":"` + ids[2] + `"},{"value":"` + subID + `"}]}]}`,
		`{"Operations":[{"op":"Remove","path":"members","value":[{"value":"` + ids[1] + `"}]}]}`,
		`{"Operations":[{"op":"remove","path":"members[value eq \"` + ids[2] + `\"]"}]}`,
		`{"Operations":[{"op":"Replace","path":"displayName","value":"Engineering"}]}`,
	} {
		if code, body := c.do("PATCH", "/Groups/"+gid, patch); code != 204 {
			t.Fatalf("PATCH %s = %d %v", patch, code, body)
		}
	}
	_, got := c.do("GET", "/Groups/"+gid, "")
	if got["displayName"] != "Engineering" {
		t.Errorf("displayName = %v", got["displayName"])
	}
	members := got["members"].([]any)
	if len(members) != 2 || members[1].(map[string]any)["type"] != "Group" {
		t.Errorf("members = %v", members)
	}
	if code, _ := c.do("PUT", "/Groups/"+subID, `{"displayName":"Engineering"}`); code != 409 {
		t.Errorf("a duplicate displayName: %d", code)
	}

	snap, err := scim.Source{Store: store}.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.People) != 3 || snap.People[0].Handle == "" {
		t.Errorf("people = %+v", snap.People)
	}
	var eng directory.Group
	for _, grp := range snap.Groups {
		if grp.ExternalID == gid {
			eng = grp
		}
	}
	want := []directory.Member{{ExternalID: ids[0]}, {ExternalID: subID, Group: true}}
	if eng.Name != "Engineering" || !reflect.DeepEqual(eng.Members, want) {
		t.Errorf("the synced group = %+v", eng)
	}
	if _, err := directory.Reconcile(identity.MustNames("example.com"), directory.NewState(), snap, directory.Options{}); err != nil {
		t.Errorf("Reconcile: %v", err)
	}

	// Deleting a user takes them out of groups.
	c.do("DELETE", "/Users/"+ids[0], "")
	_, got = c.do("GET", "/Groups/"+gid, "")
	if ms := got["members"].([]any); len(ms) != 1 {
		t.Errorf("after deleting a member: %v", ms)
	}
}

func pagingAndDiscovery(t *testing.T, newStore func(*testing.T) scim.Store) {
	c, _ := newClient(t, newStore)
	for _, u := range []string{"a", "b", "c", "d", "e"} {
		c.do("POST", "/Users", `{"userName":"`+u+`@example.com"}`)
	}
	_, body := c.do("GET", "/Users?startIndex=2&count=2", "")
	if total(body) != 5 || body["itemsPerPage"].(float64) != 2 || body["startIndex"].(float64) != 2 {
		t.Errorf("a page: %v", body)
	}
	_, body = c.do("GET", "/Users?startIndex=9", "")
	if res := body["Resources"].([]any); len(res) != 0 {
		t.Errorf("past the end: %v", res)
	}
	if code, body := c.do("GET", "/Users"+q(`userName eq "a" or userName eq "b"`), ""); code != 400 || body["scimType"] != "invalidFilter" {
		t.Errorf("an or filter: %d %v", code, body)
	}
	for _, p := range []string{"/ServiceProviderConfig", "/ResourceTypes", "/Schemas"} {
		if code, body := c.do("GET", p, ""); code != 200 || len(body["schemas"].([]any)) == 0 {
			t.Errorf("%s: %d %v", p, code, body)
		}
	}
}

// Run runs the suite against stores newStore makes, one for each test.
func Run(t *testing.T, newStore func(*testing.T) scim.Store) {
	t.Run("authentication", func(t *testing.T) { authentication(t, newStore) })
	t.Run("entra users", func(t *testing.T) { entraUsers(t, newStore) })
	t.Run("groups", func(t *testing.T) { groups(t, newStore) })
	t.Run("paging and discovery", func(t *testing.T) { pagingAndDiscovery(t, newStore) })
}
