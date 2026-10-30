// Package vault keeps sealed values in HashiCorp Vault's KV secrets engine, version 2 (docs/
// secrets.md, "Stores"): each under its key as a path of the mount, written once (check-and-set
// at version 0), its bytes base64 in the one field Vault's JSON holds, and deleted with every
// version and its metadata. ok secrets logs in with its Kubernetes service account's token, as
// a role of Vault's Kubernetes auth method: no credential is in its configuration.
package vault

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/secrets/secretstore"
)

// DefaultTokenFile is a pod's own service account token.
const DefaultTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// Config is how the store reaches Vault.
type Config struct {
	// Address is Vault's: https://vault.example.com:8200.
	Address string
	// Mount is the KV version 2 mount; Role the Kubernetes auth role, at auth/<AuthMount>
	// (kubernetes unless set), logged in as with TokenFile's token (DefaultTokenFile unless set);
	// Namespace Vault Enterprise's, or "".
	Mount, Role, AuthMount, TokenFile, Namespace string
	// HTTP is the client that reaches Vault: http.DefaultClient's transport unless set.
	HTTP *http.Client
}

// Store is a KV version 2 mount, as a secretstore.Store.
type Store struct {
	cfg Config

	mu      sync.Mutex
	token   string
	renewAt time.Time
}

var _ secretstore.Store = (*Store)(nil)

// New is the store cfg says.
func New(cfg Config) *Store {
	if cfg.AuthMount == "" {
		cfg.AuthMount = "kubernetes"
	}
	if cfg.TokenFile == "" {
		cfg.TokenFile = DefaultTokenFile
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	cfg.Address = strings.TrimSuffix(cfg.Address, "/")
	return &Store{cfg: cfg}
}

// FromConfig is the store an installation's configuration names.
func FromConfig(c config.SecretStore) (*Store, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("vault's CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("vault's CA: %s holds no PEM certificate", c.CAFile)
		}
		client.Transport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, Proxy: http.ProxyFromEnvironment}
	}
	return New(Config{Address: c.Address, Mount: c.Mount, Role: c.Role, AuthMount: c.AuthMount, TokenFile: c.TokenFile, Namespace: c.Namespace, HTTP: client}), nil
}

// path is the API path of a key under one of the mount's endpoints (data, metadata).
func (s *Store) path(endpoint, key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return s.cfg.Mount + "/" + endpoint + "/" + strings.Join(parts, "/")
}

// entry is what Vault keeps of a value: its sealed bytes, base64 in Vault's JSON.
type entry struct {
	Sealed string `json:"sealed"`
}

func (s *Store) Put(ctx context.Context, key string, sealed []byte) error {
	body := map[string]any{"options": map[string]int{"cas": 0}, "data": entry{Sealed: base64.StdEncoding.EncodeToString(sealed)}}
	_, err := s.call(ctx, http.MethodPost, s.path("data", key), body, nil)
	if err == nil {
		return nil
	}
	// The write is refused once the key holds a version: then it is held, the same or other.
	have, gerr := s.Get(ctx, key)
	switch {
	case errors.Is(gerr, secretstore.ErrNotFound):
		return err
	case gerr != nil:
		return gerr
	case bytes.Equal(have, sealed):
		return nil
	}
	return secretstore.ErrConflict
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	var out struct {
		Data *struct {
			Data *entry `json:"data"`
		} `json:"data"`
	}
	code, err := s.call(ctx, http.MethodGet, s.path("data", key), nil, &out)
	switch {
	case code == http.StatusNotFound:
		return nil, secretstore.ErrNotFound
	case err != nil:
		return nil, err
	case out.Data == nil || out.Data.Data == nil:
		return nil, secretstore.ErrNotFound // the latest version deleted
	}
	b, err := base64.StdEncoding.DecodeString(out.Data.Data.Sealed)
	if err != nil {
		return nil, fmt.Errorf("vault holds %s as no base64: %w", key, err)
	}
	return b, nil
}

func (s *Store) Delete(ctx context.Context, keys []string) error {
	for _, k := range keys {
		if code, err := s.call(ctx, http.MethodDelete, s.path("metadata", k), nil, nil); err != nil && code != http.StatusNotFound {
			return err
		}
	}
	return nil
}

// call makes a request with a token: logged in again once when Vault refuses the token, and asked
// again a few times when Vault Enterprise says its data is not yet consistent (412). It returns
// the status, and an error for any but 2xx.
func (s *Store) call(ctx context.Context, method, path string, body, out any) (int, error) {
	relogged := false
	for attempt := 0; ; attempt++ {
		token, err := s.login(ctx)
		if err != nil {
			return 0, err
		}
		code, err := s.do(ctx, method, path, token, body, out)
		switch {
		case code == http.StatusForbidden && !relogged:
			relogged = true
			s.forget(token)
			continue
		case code == http.StatusPreconditionFailed && attempt < 5:
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(time.Duration(50<<attempt) * time.Millisecond):
			}
			continue
		}
		return code, err
	}
}

// forget drops a token Vault refused, unless another login replaced it meanwhile.
func (s *Store) forget(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == token {
		s.token = ""
	}
}

// login is a token: the one held until four-fifths of its lease has passed, or a new one, logged
// in for with the service account's token.
func (s *Store) login(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && time.Now().Before(s.renewAt) {
		return s.token, nil
	}
	jwt, err := os.ReadFile(s.cfg.TokenFile)
	if err != nil {
		return "", fmt.Errorf("vault: the service account token to log in with: %w", err)
	}
	var out struct {
		Auth *struct {
			ClientToken   string `json:"client_token"`
			LeaseDuration int64  `json:"lease_duration"`
		} `json:"auth"`
	}
	if _, err := s.do(ctx, http.MethodPost, "auth/"+s.cfg.AuthMount+"/login", "",
		map[string]string{"role": s.cfg.Role, "jwt": strings.TrimSpace(string(jwt))}, &out); err != nil {
		return "", fmt.Errorf("vault: logging in as role %s: %w", s.cfg.Role, err)
	}
	if out.Auth == nil || out.Auth.ClientToken == "" {
		return "", fmt.Errorf("vault: logging in as role %s gave no token", s.cfg.Role)
	}
	lease := time.Duration(out.Auth.LeaseDuration) * time.Second
	if lease <= 0 {
		lease = time.Hour
	}
	s.token, s.renewAt = out.Auth.ClientToken, time.Now().Add(lease*4/5)
	return s.token, nil
}

// vaultAttempts bounds the attempts at a request that fails for a moment.
const vaultAttempts = 6

// do makes a request of Vault's API, decoding a 2xx's body into out — again, a while after, should
// it fail for a moment (a 429, a 5xx, a dropped connection): every request of the store's is one it
// may make again, a put refused once the key holds a version, whosever it is (Put). Its error names
// what Vault said.
func (s *Store) do(ctx context.Context, method, path, token string, body, out any) (int, error) {
	var b []byte
	if body != nil {
		var err error
		if b, err = json.Marshal(body); err != nil {
			return 0, err
		}
	}
	for attempt := 1; ; attempt++ {
		code, err := s.once(ctx, method, path, token, b, body != nil, out)
		transient := code == http.StatusTooManyRequests || code >= 500 || (code == 0 && err != nil && ctx.Err() == nil)
		if !transient || attempt == vaultAttempts {
			return code, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Duration(100<<(attempt-1)) * time.Millisecond):
		}
	}
}

// once makes one request of Vault's API.
func (s *Store) once(ctx context.Context, method, path, token string, b []byte, hasBody bool, out any) (int, error) {
	var r io.Reader
	if hasBody {
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.cfg.Address+"/v1/"+path, r)
	if err != nil {
		return 0, err
	}
	req.Header.Set("X-Vault-Request", "true")
	if hasBody {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("X-Vault-Token", token)
	}
	if s.cfg.Namespace != "" {
		req.Header.Set("X-Vault-Namespace", s.cfg.Namespace)
	}
	resp, err := s.cfg.HTTP.Do(req)
	if err != nil {
		return 0, fmt.Errorf("vault: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return resp.StatusCode, fmt.Errorf("vault: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		var e struct {
			Errors []string `json:"errors"`
		}
		_ = json.Unmarshal(data, &e)
		return resp.StatusCode, fmt.Errorf("vault: %s %s: %d %s", method, path, resp.StatusCode, strings.Join(e.Errors, "; "))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, fmt.Errorf("vault: %s %s: %w", method, path, err)
		}
	}
	return resp.StatusCode, nil
}
