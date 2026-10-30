// Package keys is ok secrets' key hierarchy (docs/secrets.md, "Keys"): root keys, which never leave
// a key management service or a keyring outside the database; repository keys, wrapped by a root;
// data keys, wrapped by their repository's key; and values, sealed by their data key. Each layer
// below the root is AES-256-GCM, and each binds as associated data what it is for — the
// repository, the key's version, the path and the digest — so no ciphertext can be moved to
// another row and opened.
package keys

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Cidan/ok/internal/secrets"
)

// Root is a root key: it wraps and unwraps repository keys, binding associated data, and never
// gives up its own key.
type Root interface {
	// ID names the root as the configuration and a repository key's row do: keyring:<file>,
	// gcp-kms:<crypto key>, aws-kms:<key ARN>.
	ID() string
	Wrap(ctx context.Context, plaintext, aad []byte) ([]byte, error)
	Unwrap(ctx context.Context, wrapped, aad []byte) ([]byte, error)
}

// ErrOpen is a ciphertext that does not open: tampered with, moved, or under another key.
var ErrOpen = errors.New("keys: the ciphertext does not open under this key and what it is for")

// Opener opens the roots a repository key may name: each scheme's, by its ID.
type Opener struct {
	// GCP and AWS reach those KMSes. Nil, DialGCP and DialAWS make them when a root first needs
	// one; with neither, their roots are refused.
	GCP     GCPKMS
	AWS     AWSKMS
	DialGCP func(ctx context.Context) (GCPKMS, error)
	DialAWS func(ctx context.Context) (AWSKMS, error)

	mu    sync.Mutex
	roots map[string]Root
}

// Root is the root an ID names: a KMS's key, which it names for good, or a keyring file, read at
// each use.
func (o *Opener) Root(ctx context.Context, id string) (Root, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if r, ok := o.roots[id]; ok {
		return r, nil
	}
	scheme, rest, ok := strings.Cut(id, ":")
	if !ok || rest == "" {
		return nil, fmt.Errorf("keys: %q is not a root (keyring:<file>, gcp-kms:<key>, aws-kms:<ARN>)", id)
	}
	var r Root
	var err error
	switch scheme {
	case "keyring":
		// Its file must read as a keyring now; it is read again at each use (keyringFile).
		if _, err = OpenKeyring(rest); err == nil {
			r = keyringFile{file: rest}
		}
	case "gcp-kms":
		if o.GCP == nil && o.DialGCP != nil {
			if o.GCP, err = o.DialGCP(ctx); err != nil {
				return nil, fmt.Errorf("keys: %s: reaching Cloud KMS: %w", id, err)
			}
		}
		if o.GCP == nil {
			return nil, fmt.Errorf("keys: %s: this ok secrets reaches no GCP KMS", id)
		}
		r = &gcpRoot{c: o.GCP, name: rest}
	case "aws-kms":
		if o.AWS == nil && o.DialAWS != nil {
			if o.AWS, err = o.DialAWS(ctx); err != nil {
				return nil, fmt.Errorf("keys: %s: reaching AWS KMS: %w", id, err)
			}
		}
		if o.AWS == nil {
			return nil, fmt.Errorf("keys: %s: this ok secrets reaches no AWS KMS", id)
		}
		r = &awsRoot{c: o.AWS, arn: rest}
	default:
		return nil, fmt.Errorf("keys: %q is not a root's kind (keyring, gcp-kms, aws-kms)", scheme)
	}
	if err != nil {
		return nil, err
	}
	if o.roots == nil {
		o.roots = map[string]Root{}
	}
	o.roots[id] = r
	return r, nil
}

// Key is an AES-256 key: a repository's or a value's data key.
type Key [32]byte

// NewKey draws a key from the system's CSPRNG.
func NewKey() (Key, error) {
	var k Key
	if _, err := rand.Read(k[:]); err != nil {
		return k, fmt.Errorf("keys: drawing a key: %w", err)
	}
	return k, nil
}

// Clear overwrites the key, for when it is no longer needed.
func (k *Key) Clear() { clear(k[:]) }

// KeyOf reads a key from its bytes.
func KeyOf(b []byte) (Key, error) {
	var k Key
	if len(b) != len(k) {
		return k, fmt.Errorf("keys: a key of %d bytes, not %d", len(b), len(k))
	}
	copy(k[:], b)
	return k, nil
}

// Seal encrypts plaintext under key with AES-256-GCM, binding aad: a fresh nonce, then the
// ciphertext and its tag.
func Seal(key Key, plaintext, aad []byte) ([]byte, error) {
	g, err := gcm(key)
	if err != nil {
		return nil, err
	}
	out := make([]byte, g.NonceSize(), g.NonceSize()+len(plaintext)+g.Overhead())
	if _, err := rand.Read(out); err != nil {
		return nil, fmt.Errorf("keys: drawing a nonce: %w", err)
	}
	return g.Seal(out, out, plaintext, aad), nil
}

// Open decrypts what Seal sealed under key for aad: ErrOpen for anything else.
func Open(key Key, sealed, aad []byte) ([]byte, error) {
	g, err := gcm(key)
	if err != nil {
		return nil, err
	}
	if len(sealed) < g.NonceSize()+g.Overhead() {
		return nil, ErrOpen
	}
	out, err := g.Open(nil, sealed[:g.NonceSize()], sealed[g.NonceSize():], aad)
	if err != nil {
		return nil, ErrOpen
	}
	return out, nil
}

func gcm(key Key) (cipher.AEAD, error) {
	b, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

// aad is what a ciphertext is for: a purpose, then each part length-prefixed, so no two different
// lists of parts encode alike.
func aad(purpose string, parts ...[]byte) []byte {
	out := []byte("ok secrets " + purpose + " v1")
	var n [8]byte
	for _, p := range parts {
		binary.LittleEndian.PutUint64(n[:], uint64(len(p)))
		out = append(append(out, n[:]...), p...)
	}
	return out
}

func i64(v int64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(v))
	return b[:]
}

// RepositoryKeyAAD is what a repository key's wrap by its root binds: its repository and version.
func RepositoryKeyAAD(repository int64, version int64) []byte {
	return aad("repository key", i64(repository), i64(version))
}

// DataKeyAAD is what a data key's wrap by its repository's key binds: the version it seals — its
// repository, path, and digest — and the repository key's version.
func DataKeyAAD(repository int64, path string, digest secrets.Digest, keyVersion int64) []byte {
	return aad("data key", i64(repository), []byte(path), digest[:], i64(keyVersion))
}

// ValueAAD is what a sealed value binds: its repository, path, and digest.
func ValueAAD(repository int64, path string, digest secrets.Digest) []byte {
	return aad("value", i64(repository), []byte(path), digest[:])
}
