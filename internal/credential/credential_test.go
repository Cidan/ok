package credential

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net/url"
	"testing"
	"time"
)

// testCA is a CA and a certificate it issued for a principal's key.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ok identity CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &testCA{cert: cert, key: key}
}

func (ca *testCA) pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca.cert)
	return p
}

func (ca *testCA) issue(t *testing.T, principal string, pub *ecdsa.PublicKey, from, to time.Time, usage x509.ExtKeyUsage) []byte {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(0xbeef),
		Subject:      pkix.Name{CommonName: principal},
		NotBefore:    from,
		NotAfter:     to,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	if principal != "" {
		tmpl.URIs = []*url.URL{{Scheme: "ok", Opaque: "principal:" + principal}, {Scheme: "ok", Opaque: "credential:cred1"}}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func TestProveAndVerify(t *testing.T) {
	ca := newCA(t)
	key, _ := NewKey()
	now := time.Now()
	der := ca.issue(t, "ada.human.example.com", &key.PublicKey, now.Add(-time.Minute), now.Add(time.Hour), x509.ExtKeyUsageClientAuth)
	v := NewVerifier(ca.pool)

	proof, err := Prove(key, "/ok.scm.v1.Store/Read", now)
	if err != nil {
		t.Fatal(err)
	}
	got, err := v.Verify("/ok.scm.v1.Store/Read", der, proof)
	if err != nil {
		t.Fatal(err)
	}
	if got.Principal != "ada.human.example.com" || got.Credential != "cred1" {
		t.Errorf("verified %+v", got)
	}
	// A proof is good once.
	if _, err := v.Verify("/ok.scm.v1.Store/Read", der, proof); !errors.Is(err, ErrReplay) {
		t.Errorf("a replayed proof: %v", err)
	}
	// A proof is for its method alone.
	proof, _ = Prove(key, "/ok.scm.v1.Store/Read", now)
	if _, err := v.Verify("/ok.scm.v1.Landing/Land", der, proof); !errors.Is(err, ErrProof) {
		t.Errorf("a proof for another method: %v", err)
	}
	// Signed by another key, it proves nothing.
	other, _ := NewKey()
	proof, _ = Prove(other, "/ok.scm.v1.Store/Read", now)
	if _, err := v.Verify("/ok.scm.v1.Store/Read", der, proof); !errors.Is(err, ErrProof) {
		t.Errorf("a proof by another key: %v", err)
	}
	// Too old, or from the future.
	for _, at := range []time.Time{now.Add(-Skew - time.Minute), now.Add(Skew + time.Minute)} {
		proof, _ = Prove(key, "/m", at)
		if _, err := v.Verify("/m", der, proof); !errors.Is(err, ErrProof) {
			t.Errorf("a proof at %v: %v", at.Sub(now), err)
		}
	}
	for _, bad := range []string{"", "v2.1.2.3", "v1.x.y.z", "v1.1.short.sig"} {
		if _, err := v.Verify("/m", der, bad); !errors.Is(err, ErrProof) {
			t.Errorf("proof %q: %v", bad, err)
		}
	}
}

func TestVerifyRefusesCertificates(t *testing.T) {
	ca := newCA(t)
	key, _ := NewKey()
	now := time.Now()
	v := NewVerifier(ca.pool)
	stranger := newCA(t)
	cases := map[string][]byte{
		"expired":        ca.issue(t, "ada.human.example.com", &key.PublicKey, now.Add(-2*time.Hour), now.Add(-time.Hour), x509.ExtKeyUsageClientAuth),
		"another CA's":   stranger.issue(t, "ada.human.example.com", &key.PublicKey, now.Add(-time.Minute), now.Add(time.Hour), x509.ExtKeyUsageClientAuth),
		"a server's":     ca.issue(t, "ada.human.example.com", &key.PublicKey, now.Add(-time.Minute), now.Add(time.Hour), x509.ExtKeyUsageServerAuth),
		"no principal":   ca.issue(t, "", &key.PublicKey, now.Add(-time.Minute), now.Add(time.Hour), x509.ExtKeyUsageClientAuth),
		"not a DER cert": []byte("nope"),
	}
	for name, der := range cases {
		proof, _ := Prove(key, "/m", now)
		if _, err := v.Verify("/m", der, proof); !errors.Is(err, ErrCertificate) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// Without the CA's certificates nothing verifies: an absent pool is never the system's roots.
func TestVerifyWithoutRoots(t *testing.T) {
	ca := newCA(t)
	key, _ := NewKey()
	now := time.Now()
	der := ca.issue(t, "ada.human.example.com", &key.PublicKey, now.Add(-time.Minute), now.Add(time.Hour), x509.ExtKeyUsageClientAuth)
	for name, roots := range map[string]func() *x509.CertPool{
		"no pool":    func() *x509.CertPool { return nil },
		"empty pool": x509.NewCertPool,
	} {
		proof, _ := Prove(key, "/m", now)
		if _, err := NewVerifier(roots).Verify("/m", der, proof); !errors.Is(err, ErrCertificate) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
