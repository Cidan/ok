// Package ca is ok identity's certificate authority: it certifies the keys principals hold —
// people's machines, robots — with short-lived client certificates naming the principal and the
// credential (package credential). Its certificate and key are files, tls.crt and tls.key in a
// directory: a Kubernetes Secret mounted there (cert-manager's, or one Init made), re-read on every
// issuance, so a rotation needs no restart. A bundle of earlier CA certificates, trusted.crt in the
// same directory, keeps the certificates they issued verifying while a rotation finishes.
package ca

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/credential"
)

// Lifetime is how long a certificate lives; clients renew before then.
const Lifetime = 24 * time.Hour

// clockSkew backdates NotBefore so a client whose clock runs slightly behind accepts a certificate
// the moment it is issued.
const clockSkew = 5 * time.Minute

// CA issues certificates from the files in a directory.
type CA struct {
	dir string
	now func() time.Time

	mu      sync.Mutex
	failing string // why the files last could not be read; "" while they can
}

// Load returns the CA in dir, checking its certificate and key are there, match, and may sign.
func Load(dir string) (*CA, error) {
	c := &CA{dir: dir, now: time.Now}
	if _, _, err := c.current(); err != nil {
		return nil, err
	}
	return c, nil
}

// current reads the CA's certificate and key as they are on disk now.
func (c *CA) current() (*x509.Certificate, crypto.Signer, error) {
	pair, err := tls.LoadX509KeyPair(filepath.Join(c.dir, "tls.crt"), filepath.Join(c.dir, "tls.key"))
	if err != nil {
		return nil, nil, fmt.Errorf("the identity CA: %w", err)
	}
	cert, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, nil, fmt.Errorf("the identity CA's certificate: %w", err)
	}
	if !cert.IsCA || cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		return nil, nil, errors.New("the identity CA's certificate may not sign certificates")
	}
	signer, ok := pair.PrivateKey.(crypto.Signer)
	if !ok {
		return nil, nil, errors.New("the identity CA's key cannot sign")
	}
	return cert, signer, nil
}

// Roots are the CA certificates credentials verify against: the current one, then those
// trusted.crt keeps.
func (c *CA) Roots() ([]*x509.Certificate, error) {
	cur, _, err := c.current()
	if err != nil {
		return nil, err
	}
	out := []*x509.Certificate{cur}
	data, err := os.ReadFile(filepath.Join(c.dir, "trusted.crt"))
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	for len(data) > 0 {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("trusted.crt: %w", err)
		}
		if !cert.Equal(cur) {
			out = append(out, cert)
		}
	}
	return out, nil
}

// Pool is Roots as a pool, for a credential.Verifier; it is empty when the CA cannot be read, so
// every credential is refused — which it logs, once for each reason it cannot, and again when it
// can.
func (c *CA) Pool() *x509.CertPool {
	p := x509.NewCertPool()
	roots, err := c.Roots()
	c.note(err)
	for _, r := range roots {
		p.AddCert(r)
	}
	return p
}

// Readable reports whether the CA's files can be read: identity is not ready while they cannot.
func (c *CA) Readable() error {
	_, err := c.Roots()
	c.note(err)
	return err
}

// note logs a change in whether the CA's files can be read.
func (c *CA) note(err error) {
	why := ""
	if err != nil {
		why = err.Error()
	}
	c.mu.Lock()
	changed := why != c.failing
	c.failing = why
	c.mu.Unlock()
	switch {
	case !changed:
	case err != nil:
		slog.Error("identity CA: its files cannot be read, so every credential is refused until they can", "dir", c.dir, "err", err)
	default:
		slog.Info("identity CA: its files can be read again", "dir", c.dir)
	}
}

// Issue certifies a key — ECDSA P-256, as package credential requires — for a principal's
// credential, and returns the chain, leaf first, as DER, with when the leaf expires.
func (c *CA) Issue(key crypto.PublicKey, principal, credentialID string) ([][]byte, time.Time, error) {
	pub, ok := key.(*ecdsa.PublicKey)
	if !ok || pub.Curve != elliptic.P256() {
		return nil, time.Time{}, errors.New("a credential's key is ECDSA P-256")
	}
	caCert, signer, err := c.current()
	if err != nil {
		return nil, time.Time{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, time.Time{}, err
	}
	now := c.now()
	notAfter := now.Add(Lifetime)
	if notAfter.After(caCert.NotAfter) {
		notAfter = caCert.NotAfter
	}
	if !notAfter.After(now) {
		return nil, time.Time{}, errors.New("the identity CA has expired")
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: principal},
		URIs: []*url.URL{
			{Scheme: "ok", Opaque: "principal:" + principal},
			{Scheme: "ok", Opaque: "credential:" + credentialID},
		},
		NotBefore:             now.Add(-clockSkew),
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	leaf, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, pub, signer)
	if err != nil {
		return nil, time.Time{}, err
	}
	if cert, err := x509.ParseCertificate(leaf); err != nil {
		return nil, time.Time{}, err
	} else if p, ok := credential.Principal(cert); !ok || p != principal {
		return nil, time.Time{}, fmt.Errorf("%q cannot be named in a certificate", principal)
	}
	return [][]byte{leaf, caCert.Raw}, notAfter, nil
}

// Init makes a CA in dir: a new ECDSA P-256 key and a self-signed certificate for it, valid ten
// years, named for the installation. It refuses to overwrite a CA already there.
func Init(dir, name string) error {
	for _, f := range []string{"tls.crt", "tls.key"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err == nil {
			return fmt.Errorf("%s already holds a CA (%s)", dir, f)
		}
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name + " identity CA", Organization: []string{name}},
		NotBefore:             now.Add(-clockSkew),
		NotAfter:              now.AddDate(10, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "tls.key"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "tls.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644)
}
