// Package seal seals secrets an organization gives ok — its identity provider's client secret, its
// directory's password — before they are stored, so the directory's database holds none in the
// clear: AES-256-GCM under the installation's key (identity.sealing_key_file), each sealing with a
// nonce of its own, bound to what the secret is for, so a sealed value moved to another row does not
// open.
package seal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
)

// version starts every sealed value, so a later key or cipher is told apart.
const version = 1

// Sealer seals and opens with one key.
type Sealer struct {
	aead cipher.AEAD
	id   [4]byte // the key's fingerprint, carried in each sealed value
}

// New makes a sealer of a 32-byte key.
func New(key []byte) (*Sealer, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("seal: a key is 32 bytes, not %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	s := &Sealer{aead: aead}
	sum := sha256.Sum256(key)
	copy(s.id[:], sum[:4])
	return s, nil
}

// Load reads a key file — 32 bytes, base64 in a line — as a sealer.
func Load(path string) (*Sealer, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return nil, fmt.Errorf("seal: %s holds no base64 key: %w", path, err)
	}
	return New(key)
}

// NewKey makes a key file's contents: 32 random bytes, base64.
func NewKey() string {
	var k [32]byte
	rand.Read(k[:])
	return base64.StdEncoding.EncodeToString(k[:])
}

// Seal seals a secret for a purpose — what it is and whose: "acme:oidc:client_secret" — which
// opening must name again.
func (s *Sealer) Seal(secret []byte, purpose string) []byte {
	nonce := make([]byte, s.aead.NonceSize())
	rand.Read(nonce)
	out := append([]byte{version}, s.id[:]...)
	out = append(out, nonce...)
	return s.aead.Seal(out, nonce, secret, []byte(purpose))
}

// ErrOpen is a value that does not open: another key's, another purpose's, or tampered with.
var ErrOpen = errors.New("seal: the value does not open with this key for this purpose")

// Open opens a value Seal sealed for the same purpose.
func (s *Sealer) Open(sealed []byte, purpose string) ([]byte, error) {
	n := s.aead.NonceSize()
	if len(sealed) < 1+4+n+s.aead.Overhead() || sealed[0] != version || string(sealed[1:5]) != string(s.id[:]) {
		return nil, ErrOpen
	}
	nonce, body := sealed[5:5+n], sealed[5+n:]
	out, err := s.aead.Open(nil, nonce, body, []byte(purpose))
	if err != nil {
		return nil, ErrOpen
	}
	return out, nil
}
