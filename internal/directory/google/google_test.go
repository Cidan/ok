package google

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/identity"
)

// fake is the Directory API and Google's token endpoint.
func fake(t *testing.T) (*httptest.Server, *[]string) {
	t.Helper()
	var assertions []string
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.PostForm.Get("grant_type") != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
			http.Error(w, "bad grant", 400)
			return
		}
		parts := strings.Split(r.PostForm.Get("assertion"), ".")
		claims, _ := base64.RawURLEncoding.DecodeString(parts[1])
		assertions = append(assertions, string(claims))
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"access_token":"good","token_type":"Bearer","expires_in":3600}`))
	})
	page := func(w http.ResponseWriter, r *http.Request, pages ...string) {
		if r.Header.Get("Authorization") != "Bearer good" {
			http.Error(w, "unauthenticated", 401)
			return
		}
		i := 0
		if tok := r.URL.Query().Get("pageToken"); tok != "" {
			i = int(tok[0] - '0')
		}
		w.Write([]byte(pages[i]))
	}
	mux.HandleFunc("/admin/directory/v1/users", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("customer") != "my_customer" {
			http.Error(w, "no customer", 400)
			return
		}
		page(w, r,
			`{"users":[{"id":"u1","primaryEmail":"Ada@example.com","name":{"fullName":"Ada L"}},{"id":"u2","primaryEmail":"bea@example.com","suspended":true}],"nextPageToken":"1"}`,
			`{"users":[{"id":"u3","primaryEmail":"cal@example.com","archived":true}]}`)
	})
	mux.HandleFunc("/admin/directory/v1/groups", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") != "email:eng*" {
			http.Error(w, "no query", 400)
			return
		}
		page(w, r, `{"groups":[{"id":"g1","email":"eng@example.com","name":"Engineering"},{"id":"g2","email":"eng-infra@example.com"}]}`)
	})
	mux.HandleFunc("/admin/directory/v1/groups/g1/members", func(w http.ResponseWriter, r *http.Request) {
		page(w, r,
			`{"members":[{"id":"u1","type":"USER"},{"id":"x9","type":"USER"}],"nextPageToken":"1"}`,
			`{"members":[{"id":"g2","type":"GROUP"},{"id":"gx","type":"GROUP"},{"id":"c","type":"CUSTOMER"}]}`)
	})
	mux.HandleFunc("/admin/directory/v1/groups/g2/members", func(w http.ResponseWriter, r *http.Request) {
		page(w, r, `{"members":[{"id":"u2","type":"USER"}]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &assertions
}

func serviceAccountKey(t *testing.T, tokenURL string) []byte {
	t.Helper()
	k, _ := rsa.GenerateKey(rand.Reader, 2048)
	der, _ := x509.MarshalPKCS8PrivateKey(k)
	key, _ := json.Marshal(map[string]string{
		"type": "service_account", "client_email": "ok-sync@acme.iam.gserviceaccount.com",
		"private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})), "token_uri": tokenURL,
	})
	return key
}

func TestSnapshot(t *testing.T) {
	srv, assertions := fake(t)
	src, err := New(context.Background(), Config{KeyFile: serviceAccountKey(t, srv.URL+"/token"), Subject: "admin@example.com", GroupQuery: "email:eng*", BaseURL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantPeople := []directory.Person{
		{ExternalID: "u1", Handle: "Ada", Email: "ada@example.com", Name: "Ada L", Active: true},
		{ExternalID: "u2", Handle: "bea", Email: "bea@example.com"},
		{ExternalID: "u3", Handle: "cal", Email: "cal@example.com"},
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
	if len(*assertions) != 1 || !strings.Contains((*assertions)[0], `"sub":"admin@example.com"`) || !strings.Contains((*assertions)[0], "admin.directory.group.readonly") {
		t.Errorf("the token assertions: %v", *assertions)
	}
	// The snapshot is one the sync takes.
	if _, err := directory.Reconcile(namesForTest, directory.NewState(), snap, directory.Options{}); err != nil {
		t.Errorf("Reconcile: %v", err)
	}
}

func TestRefusals(t *testing.T) {
	if _, err := New(context.Background(), Config{KeyFile: []byte("{}"), Subject: "a@b"}); err == nil {
		t.Error("an empty key was accepted")
	}
	srv, _ := fake(t)
	if _, err := New(context.Background(), Config{KeyFile: serviceAccountKey(t, srv.URL+"/token")}); err == nil {
		t.Error("no subject was accepted")
	}
	src := NewWithClient(Config{BaseURL: srv.URL}, http.DefaultClient)
	if _, err := src.Snapshot(context.Background()); err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("an unauthenticated read: %v", err)
	}
}

var namesForTest = identity.MustNames("example.com")
