package ca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/credential"
)

func TestInitIssueVerify(t *testing.T) {
	dir := t.TempDir()
	if err := Init(dir, "Example"); err != nil {
		t.Fatal(err)
	}
	if err := Init(dir, "Example"); err == nil {
		t.Error("Init overwrote a CA")
	}
	if info, _ := os.Stat(filepath.Join(dir, "tls.key")); info.Mode().Perm() != 0o600 {
		t.Errorf("the key is %v", info.Mode().Perm())
	}
	c, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := credential.NewKey()
	chain, notAfter, err := c.Issue(&key.PublicKey, "ada.human.example.com", "crd_1")
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Until(notAfter); d < Lifetime-time.Minute || d > Lifetime {
		t.Errorf("the certificate lives %v", d)
	}
	v := credential.NewVerifier(c.Pool)
	proof, _ := credential.Prove(key, "/ok.identity.v1.Credentials/Renew", time.Now())
	got, err := v.Verify("/ok.identity.v1.Credentials/Renew", chain[0], proof)
	if err != nil || got.Principal != "ada.human.example.com" || got.Credential != "crd_1" {
		t.Errorf("Verify = %+v, %v", got, err)
	}

	p384, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if _, _, err := c.Issue(&p384.PublicKey, "ada.human.example.com", "crd_2"); err == nil {
		t.Error("a P-384 key was certified")
	}
}

// A rotation: the new CA in place, the old one in trusted.crt, and both issuers' certificates
// verify; certificates are capped at the CA's own expiry.
func TestRotation(t *testing.T) {
	oldDir, newDir := t.TempDir(), t.TempDir()
	Init(oldDir, "Example")
	Init(newDir, "Example")
	old, _ := Load(oldDir)
	key, _ := credential.NewKey()
	oldChain, _, _ := old.Issue(&key.PublicKey, "ada.human.example.com", "crd_1")

	oldPEM, _ := os.ReadFile(filepath.Join(oldDir, "tls.crt"))
	os.WriteFile(filepath.Join(newDir, "trusted.crt"), oldPEM, 0o644)
	cur, err := Load(newDir)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := cur.Roots()
	if err != nil || len(roots) != 2 {
		t.Fatalf("Roots = %d, %v", len(roots), err)
	}
	v := credential.NewVerifier(cur.Pool)
	proof, _ := credential.Prove(key, "/m", time.Now())
	if _, err := v.Verify("/m", oldChain[0], proof); err != nil {
		t.Errorf("the old CA's certificate during the rotation: %v", err)
	}

	cur.now = func() time.Time { return roots[0].NotAfter.Add(-time.Hour) }
	_, notAfter, err := cur.Issue(&key.PublicKey, "ada.human.example.com", "crd_1")
	if err != nil || !notAfter.Equal(roots[0].NotAfter) {
		t.Errorf("near the CA's expiry: %v, %v", notAfter, err)
	}
	cur.now = func() time.Time { return roots[0].NotAfter.Add(time.Hour) }
	if _, _, err := cur.Issue(&key.PublicKey, "ada.human.example.com", "crd_1"); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Errorf("an expired CA: %v", err)
	}
}

func TestLoadRefuses(t *testing.T) {
	if _, err := Load(t.TempDir()); err == nil {
		t.Error("an empty directory loaded")
	}
	// A certificate that is not a CA's.
	dir := t.TempDir()
	Init(dir, "Example")
	c, _ := Load(dir)
	key, _ := credential.NewKey()
	chain, _, _ := c.Issue(&key.PublicKey, "ada.human.example.com", "crd_1")
	leafDir := t.TempDir()
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	os.WriteFile(filepath.Join(leafDir, "tls.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: chain[0]}), 0o644)
	os.WriteFile(filepath.Join(leafDir, "tls.key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600)
	if _, err := Load(leafDir); err == nil || !strings.Contains(err.Error(), "may not sign") {
		t.Errorf("a leaf as the CA: %v", err)
	}
}
