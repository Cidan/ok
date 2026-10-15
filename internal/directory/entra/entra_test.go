package entra

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/identity"
)

func fake(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		id, secret, _ := r.BasicAuth()
		if id == "" {
			id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
		}
		if r.PostForm.Get("grant_type") != "client_credentials" || id != "app" || secret != "secret" || r.PostForm.Get("scope") != "https://graph.microsoft.com/.default" {
			http.Error(w, `{"error":"invalid_client"}`, 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token":"good","token_type":"Bearer","expires_in":3600}`))
	})
	authed := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer good" {
				http.Error(w, "unauthenticated", 401)
				return
			}
			h(w, r)
		}
	}
	mux.HandleFunc("/v1.0/users", authed(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			w.Write([]byte(`{"value":[{"id":"u3","userPrincipalName":"guest_x.com#EXT#@acme.onmicrosoft.com","userType":"Guest","accountEnabled":true}]}`))
			return
		}
		if !strings.Contains(r.URL.Query().Get("$select"), "accountEnabled") {
			http.Error(w, "no select", 400)
			return
		}
		w.Write([]byte(`{"value":[
			{"id":"u1","userPrincipalName":"ada@acme.com","mail":"Ada.L@acme.com","displayName":"Ada","accountEnabled":true,"userType":"Member"},
			{"id":"u2","userPrincipalName":"bea@acme.com","displayName":"Bea","accountEnabled":false,"userType":"Member"}],
			"@odata.nextLink":"` + srv.URL + `/v1.0/users?page=2"}`))
	}))
	mux.HandleFunc("/v1.0/groups", authed(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("$filter") != "startswith(displayName,'eng')" {
			http.Error(w, "no filter", 400)
			return
		}
		w.Write([]byte(`{"value":[{"id":"g1","displayName":"Engineering","mailNickname":"eng"},{"id":"g2","displayName":"eng-infra"}]}`))
	}))
	mux.HandleFunc("/v1.0/groups/g1/members", authed(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"value":[
			{"@odata.type":"#microsoft.graph.user","id":"u1"},
			{"@odata.type":"#microsoft.graph.user","id":"u3"},
			{"@odata.type":"#microsoft.graph.group","id":"g2"},
			{"@odata.type":"#microsoft.graph.servicePrincipal","id":"sp"},
			{"@odata.type":"#microsoft.graph.device","id":"d"}]}`))
	}))
	mux.HandleFunc("/v1.0/groups/g2/members", authed(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"value":[{"@odata.type":"#microsoft.graph.user","id":"u2"}]}`))
	}))
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestSnapshot(t *testing.T) {
	srv := fake(t)
	src, err := New(context.Background(), Config{TenantID: "t", ClientID: "app", ClientSecret: "secret",
		GroupFilter: "startswith(displayName,'eng')", GraphURL: srv.URL + "/v1.0", TokenURL: srv.URL + "/token"})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantPeople := []directory.Person{
		{ExternalID: "u1", Handle: "ada", Email: "ada.l@acme.com", Name: "Ada", Active: true},
		{ExternalID: "u2", Handle: "bea", Email: "bea@acme.com", Name: "Bea"},
	}
	if !reflect.DeepEqual(snap.People, wantPeople) {
		t.Errorf("people = %+v", snap.People)
	}
	wantGroups := []directory.Group{
		{ExternalID: "g1", Name: "eng", Members: []directory.Member{{ExternalID: "u1"}, {ExternalID: "g2", Group: true}}},
		{ExternalID: "g2", Name: "eng-infra", Members: []directory.Member{{ExternalID: "u2"}}},
	}
	if !reflect.DeepEqual(snap.Groups, wantGroups) {
		t.Errorf("groups = %+v", snap.Groups)
	}
	if _, err := directory.Reconcile(identity.MustNames("acme.com"), directory.NewState(), snap, directory.Options{}); err != nil {
		t.Errorf("Reconcile: %v", err)
	}

	src.cfg.IncludeGuests = true
	snap, _ = src.Snapshot(context.Background())
	if len(snap.People) != 3 || len(snap.Groups[0].Members) != 3 {
		t.Errorf("with guests: %d people, eng holds %v", len(snap.People), snap.Groups[0].Members)
	}

	bad, _ := New(context.Background(), Config{TenantID: "t", ClientID: "app", ClientSecret: "wrong", GraphURL: srv.URL + "/v1.0", TokenURL: srv.URL + "/token"})
	if _, err := bad.Snapshot(context.Background()); err == nil {
		t.Error("a wrong secret read the tenant")
	}
	if _, err := New(context.Background(), Config{TenantID: "t"}); err == nil {
		t.Error("a tenant with no client was accepted")
	}
}
