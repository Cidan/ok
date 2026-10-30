// Package credential is how a principal proves itself to ok's services on every request: a
// certificate ok's identity CA issued for a key the principal holds, and a proof — a signature
// by that key over the request's method, the time, and a nonce — so the certificate alone, seen
// by a proxy or copied from a disk, is useless.
//
// On the wire a request carries two metadata entries (headers, on the REST surface):
//
//	ok-credential-bin  the certificate, DER (gRPC base64-encodes -bin values itself)
//	ok-proof           v1.<unix seconds>.<nonce>.<signature>
//
// The signature is ECDSA P-256, ASN.1, over SHA-256 of
//
//	"ok-proof v1\n" + method + "\n" + seconds + "\n" + nonce
//
// where method is the gRPC full method (/ok.scm.v1.Store/Read), the nonce 16 random bytes and
// the signature both base64url without padding. The certificate names its principal and its
// credential in URI SANs, ok:principal:<name> and ok:credential:<id>: the credential is the key's
// enrollment, which renewals keep and the directory can revoke, while each certificate has a
// serial of its own.
//
// A verifier accepts each proof once; replicas do not share what they have seen, so a captured
// proof could be replayed to another replica within Skew — which capturing it at all, under TLS,
// would already have required breaking.
package credential

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The metadata keys a request carries.
const (
	CertificateKey = "ok-credential-bin"
	ProofKey       = "ok-proof"
)

// The URI SANs a certificate names its principal and its credential with.
const (
	PrincipalURIPrefix  = "ok:principal:"
	CredentialURIPrefix = "ok:credential:"
)

// Skew is how far a proof's time may be from the server's.
const Skew = 2 * time.Minute

// signed is what a proof's signature covers.
func signed(method string, seconds int64, nonce string) []byte {
	sum := sha256.Sum256([]byte("ok-proof v1\n" + method + "\n" + strconv.FormatInt(seconds, 10) + "\n" + nonce))
	return sum[:]
}

// Prove is the proof for one request to method, signed by key at now.
func Prove(key crypto.Signer, method string, now time.Time) (string, error) {
	var n [16]byte
	if _, err := rand.Read(n[:]); err != nil {
		return "", err
	}
	nonce := base64.RawURLEncoding.EncodeToString(n[:])
	sec := now.Unix()
	sig, err := key.Sign(rand.Reader, signed(method, sec, nonce), crypto.SHA256)
	if err != nil {
		return "", err
	}
	return "v1." + strconv.FormatInt(sec, 10) + "." + nonce + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// NewKey makes a key a credential is issued for.
func NewKey() (*ecdsa.PrivateKey, error) { return ecdsa.GenerateKey(elliptic.P256(), rand.Reader) }

// Principal is the principal a certificate names.
func Principal(cert *x509.Certificate) (string, bool) { return uriValue(cert, PrincipalURIPrefix) }

// ID is the credential a certificate is of.
func ID(cert *x509.Certificate) (string, bool) { return uriValue(cert, CredentialURIPrefix) }

// uriValue is what follows prefix in the certificate's one URI SAN that has it.
func uriValue(cert *x509.Certificate, prefix string) (string, bool) {
	var v string
	for _, u := range cert.URIs {
		if rest, ok := strings.CutPrefix(u.String(), prefix); ok {
			if v != "" || rest == "" {
				return "", false
			}
			v = rest
		}
	}
	return v, v != ""
}

// Verified is a request's credential, checked.
type Verified struct {
	Principal  string
	Credential string
	Cert       *x509.Certificate
}

// Verifier checks requests' credentials against the identity CA's certificates.
//
// A proof is accepted once by a verifier: it keeps the nonces it accepted while their proofs could
// be accepted (Skew either way). That is what the verifier has seen, no copy of anything a database
// holds; a proof replayed to another replica within the window is accepted there. Recording every
// nonce in the directory's database would put a write on every request; a proof bound to its TLS
// connection (its exporter) would make a replay fail anywhere, holding nothing at all.
type Verifier struct {
	roots func(context.Context) (*x509.CertPool, error)
	now   func() time.Time

	mu   sync.Mutex
	seen map[string]time.Time // nonces used within Skew, with when they expire
	next time.Time            // when expired nonces are next swept
}

// NewVerifier checks certificates against the CA certificates roots returns as each request is
// verified: as the CA rotates, a certificate it adds is trusted, and one it removes is not, at once.
func NewVerifier(roots func(context.Context) (*x509.CertPool, error)) *Verifier {
	return &Verifier{roots: roots, now: time.Now, seen: map[string]time.Time{}}
}

// Errors a verification fails with.
var (
	ErrCertificate = errors.New("the credential's certificate is not one ok's CA issued, or has expired")
	ErrProof       = errors.New("the credential's proof is not valid for this request")
	ErrReplay      = errors.New("the credential's proof was used before")
)

// Verify checks a request to method: certDER chains to the CA, is a client certificate, and is
// current; proof is signed by its key, for method, within Skew of now, with a nonce this verifier
// has not seen before.
func (v *Verifier) Verify(ctx context.Context, method string, certDER []byte, proof string) (*Verified, error) {
	now := v.now()
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, ErrCertificate
	}
	// Without the CA's certificates nothing is verified: a nil pool would mean the system's roots.
	roots, err := v.roots(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading the identity CA's certificates: %w", err)
	}
	if roots == nil {
		return nil, ErrCertificate
	}
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots:       roots,
		CurrentTime: now,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return nil, ErrCertificate
	}
	name, ok := Principal(cert)
	if !ok {
		return nil, ErrCertificate
	}
	id, ok := ID(cert)
	if !ok {
		return nil, ErrCertificate
	}
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok || pub.Curve != elliptic.P256() {
		return nil, ErrCertificate
	}
	parts := strings.Split(proof, ".")
	if len(parts) != 4 || parts[0] != "v1" {
		return nil, ErrProof
	}
	sec, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, ErrProof
	}
	at := time.Unix(sec, 0)
	if at.Before(now.Add(-Skew)) || at.After(now.Add(Skew)) {
		return nil, ErrProof
	}
	nonce := parts[2]
	if n, err := base64.RawURLEncoding.DecodeString(nonce); err != nil || len(n) != 16 {
		return nil, ErrProof
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[3])
	if err != nil || !ecdsa.VerifyASN1(pub, signed(method, sec, nonce), sig) {
		return nil, ErrProof
	}
	if !v.use(nonce, now) {
		return nil, ErrReplay
	}
	return &Verified{Principal: name, Credential: id, Cert: cert}, nil
}

// use records a nonce, reporting false for one already used.
func (v *Verifier) use(nonce string, now time.Time) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if now.After(v.next) {
		for n, exp := range v.seen {
			if now.After(exp) {
				delete(v.seen, n)
			}
		}
		v.next = now.Add(Skew)
	}
	if _, used := v.seen[nonce]; used {
		return false
	}
	v.seen[nonce] = now.Add(2 * Skew)
	return true
}
