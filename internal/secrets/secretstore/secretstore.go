// Package secretstore is the contract between ok secrets and the external stores that may keep a
// repository's sealed values (docs/secrets.md, "Stores"): HashiCorp Vault's KV, AWS Secrets
// Manager, Google Secret Manager. ok secrets seals every value before it stores it, so a store
// only ever holds ciphertext, by key, immutably; which versions exist, and who may read them, is
// ok secrets' own.
package secretstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/workflow"
)

var (
	// ErrNotFound is a key the store does not hold.
	ErrNotFound = errors.New("the secret store holds no such value")
	// ErrConflict is a put of other bytes under a key the store holds — which a digest's key
	// makes impossible unless something is wrong.
	ErrConflict = errors.New("the secret store holds other bytes under this key")
	// ErrTooLarge is a put of more than the store holds in one value.
	ErrTooLarge = errors.New("the secret store holds no value this large")
	// ErrNoStore is a store the installation does not configure.
	ErrNoStore = errors.New("this installation configures no such secret store (secrets.stores)")
)

// Store keeps sealed values by key.
type Store interface {
	// Put keeps sealed under key, once: the same bytes again are no error, other bytes ErrConflict.
	Put(ctx context.Context, key string, sealed []byte) error
	// Get returns what Put kept: ErrNotFound for a key never kept, or deleted.
	Get(ctx context.Context, key string) ([]byte, error)
	// Delete deletes keys for good; a key not held is passed over.
	Delete(ctx context.Context, keys []string) error
}

// Key is the key a version's sealed value is kept under: ok/<repository>/<path>@<digest hex>.
func Key(repository int64, path string, digest secrets.Digest) string {
	return "ok/" + strconv.FormatInt(repository, 10) + "/" + path + "@" + digest.String()
}

// Stores are the external stores an installation configures, by the names its repositories'
// settings give them (secrets.store).
type Stores map[string]Store

// For is the store a name names: nil for the default, ok secrets' own database.
func (s Stores) For(name string) (Store, error) {
	if name == "" || name == workflow.DefaultSecretStore {
		return nil, nil
	}
	st, ok := s[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoStore, name)
	}
	return st, nil
}
