// Package fedtest is a token issuer for tests: OpenID discovery, its signing keys, and tokens it
// signs with the claims a test names.
package fedtest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// Issuer is a running issuer; it stops when the test ends.
type Issuer struct {
	*httptest.Server
	key *rsa.PrivateKey
	// KeyAuth, when set, serves the keys only to this bearer token, as an API server that serves
	// them only to the authenticated does.
	KeyAuth string
	// KeyFetches counts the requests for the keys.
	KeyFetches atomic.Int32
}

// New starts an issuer.
func New(t testing.TB) *Issuer {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	iss := &Issuer{key: k}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"issuer": iss.URL, "jwks_uri": iss.URL + "/keys", "id_token_signing_alg_values_supported": []string{"RS256"},
			"authorization_endpoint": iss.URL + "/authorize", "response_types_supported": []string{"id_token"}, "subject_types_supported": []string{"public"}})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		iss.KeyFetches.Add(1)
		if iss.KeyAuth != "" && r.Header.Get("Authorization") != "Bearer "+iss.KeyAuth {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &k.PublicKey, KeyID: "k", Algorithm: "RS256", Use: "sig"}}})
	})
	iss.Server = httptest.NewServer(mux)
	t.Cleanup(iss.Close)
	return iss
}

// Token signs a token with the issuer, an hour long, with claims (aud, sub, and any to override).
func (i *Issuer) Token(t testing.TB, claims map[string]any) string {
	t.Helper()
	s, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: i.key}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "k"))
	if err != nil {
		t.Fatal(err)
	}
	c := map[string]any{"iss": i.URL, "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix()}
	for k, v := range claims {
		c[k] = v
	}
	tok, err := jwt.Signed(s).Claims(c).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return tok
}
