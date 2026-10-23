package vault

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Cidan/ok/internal/secrets/secretstore/storetest"
)

// fake is Vault as the store reaches it: a KV version 2 mount, ok, in namespace team; and the
// Kubernetes auth method, giving role ok-secrets a token for jwt sa-token.
type fake struct {
	t      *testing.T
	mu     sync.Mutex
	kv     map[string]map[string]any // by path below ok/data/
	tokens map[string]bool
	n      int
	logins int
}

func newFake(t *testing.T) (*fake, *httptest.Server) {
	f := &fake{t: t, kv: map[string]map[string]any{}, tokens: map[string]bool{}}
	s := httptest.NewServer(f)
	t.Cleanup(s.Close)
	return f, s
}

func (f *fake) fail(w http.ResponseWriter, code int, msg ...string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if msg == nil {
		msg = []string{}
	}
	_ = json.NewEncoder(w).Encode(map[string][]string{"errors": msg})
}

// expire revokes every token, as their lease ending does.
func (f *fake) expire() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.tokens)
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("X-Vault-Namespace") != "team" || r.Header.Get("X-Vault-Request") != "true" {
		f.fail(w, http.StatusBadRequest, "no namespace")
		return
	}
	var body map[string]any
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	if r.URL.Path == "/v1/auth/kubernetes/login" {
		if r.Method != http.MethodPost || body["role"] != "ok-secrets" || body["jwt"] != "sa-token" {
			f.fail(w, http.StatusBadRequest, "invalid role or JWT")
			return
		}
		f.n++
		f.logins++
		tok := "hvs." + strings.Repeat("x", f.n)
		f.tokens[tok] = true
		_ = json.NewEncoder(w).Encode(map[string]any{"auth": map[string]any{"client_token": tok, "lease_duration": 3600, "renewable": true}})
		return
	}
	if !f.tokens[r.Header.Get("X-Vault-Token")] {
		f.fail(w, http.StatusForbidden, "permission denied")
		return
	}
	switch {
	case strings.HasPrefix(r.URL.Path, "/v1/ok/data/"):
		p := strings.TrimPrefix(r.URL.Path, "/v1/ok/data/")
		switch r.Method {
		case http.MethodPost:
			opts, _ := body["options"].(map[string]any)
			if _, held := f.kv[p]; held && opts["cas"] == float64(0) {
				f.fail(w, http.StatusBadRequest, "check-and-set parameter did not match the current version")
				return
			}
			data, _ := body["data"].(map[string]any)
			f.kv[p] = data
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"version": 1}})
		case http.MethodGet:
			data, ok := f.kv[p]
			if !ok {
				f.fail(w, http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": data, "metadata": map[string]any{"version": 1}}})
		default:
			f.fail(w, http.StatusMethodNotAllowed)
		}
	case strings.HasPrefix(r.URL.Path, "/v1/ok/metadata/") && r.Method == http.MethodDelete:
		p := strings.TrimPrefix(r.URL.Path, "/v1/ok/metadata/")
		if _, ok := f.kv[p]; !ok {
			f.fail(w, http.StatusNotFound)
			return
		}
		delete(f.kv, p)
		w.WriteHeader(http.StatusNoContent)
	default:
		f.fail(w, http.StatusNotFound, "unsupported path")
	}
}

func newStore(t *testing.T, addr string) *Store {
	tok := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tok, []byte("sa-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return New(Config{Address: addr, Mount: "ok", Role: "ok-secrets", TokenFile: tok, Namespace: "team"})
}

func TestConformance(t *testing.T) {
	_, s := newFake(t)
	storetest.Run(t, newStore(t, s.URL))
}

// A token Vault no longer takes is replaced by logging in again, once; a value is kept as a path
// of the mount, base64 in its one field.
func TestVault(t *testing.T) {
	f, s := newFake(t)
	st := newStore(t, s.URL)
	ctx := t.Context()
	if err := st.Put(ctx, "ok/7/secret/prod/db@ab", []byte("sealed")); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	got := f.kv["ok/7/secret/prod/db@ab"]
	f.mu.Unlock()
	if got["sealed"] != "c2VhbGVk" || len(got) != 1 {
		t.Errorf("vault holds %v", got)
	}
	f.expire()
	if v, err := st.Get(ctx, "ok/7/secret/prod/db@ab"); err != nil || string(v) != "sealed" {
		t.Errorf("after the token expired: %q, %v", v, err)
	}
	if f.logins != 2 {
		t.Errorf("%d logins, want 2", f.logins)
	}
	bad := New(Config{Address: s.URL, Mount: "ok", Role: "other", TokenFile: st.cfg.TokenFile, Namespace: "team"})
	if _, err := bad.Get(ctx, "ok/7/secret/prod/db@ab"); err == nil || !strings.Contains(err.Error(), "logging in as role other") {
		t.Errorf("a role Vault refuses: %v", err)
	}
}
