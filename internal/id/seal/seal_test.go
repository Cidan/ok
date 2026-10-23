package seal

import (
	"bytes"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A sealed secret opens with its key for its purpose alone: another purpose, another key, a value
// tampered with, or one cut short does not open; sealing twice differs.
func TestSeal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key")
	os.WriteFile(path, []byte(NewKey()+"\n"), 0o600)
	s, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("the client secret")
	sealed := s.Seal(secret, "acme:oidc:client_secret")
	if bytes.Contains(sealed, secret) {
		t.Fatal("the secret is in the clear")
	}
	if again := s.Seal(secret, "acme:oidc:client_secret"); bytes.Equal(again, sealed) {
		t.Error("sealing twice gives the same value: no nonce")
	}
	if got, err := s.Open(sealed, "acme:oidc:client_secret"); err != nil || !bytes.Equal(got, secret) {
		t.Errorf("Open = %q, %v", got, err)
	}
	other, _ := New(bytes.Repeat([]byte{7}, 32))
	tampered := append([]byte(nil), sealed...)
	tampered[len(tampered)-1] ^= 1
	for name, open := range map[string]func() ([]byte, error){
		"another purpose": func() ([]byte, error) { return s.Open(sealed, "globex:oidc:client_secret") },
		"another key":     func() ([]byte, error) { return other.Open(sealed, "acme:oidc:client_secret") },
		"tampered":        func() ([]byte, error) { return s.Open(tampered, "acme:oidc:client_secret") },
		"cut short":       func() ([]byte, error) { return s.Open(sealed[:10], "acme:oidc:client_secret") },
	} {
		if got, err := open(); !errors.Is(err, ErrOpen) || got != nil {
			t.Errorf("%s: %q, %v", name, got, err)
		}
	}
	if _, err := New([]byte("short")); err == nil {
		t.Error("a short key accepted")
	}
	os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString([]byte("sixteen byte key"))), 0o600)
	if _, err := Load(path); err == nil {
		t.Error("a 16-byte key file accepted")
	}
}
