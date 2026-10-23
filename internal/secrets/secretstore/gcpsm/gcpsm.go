// Package gcpsm keeps sealed values in Google Secret Manager (docs/secrets.md, "Stores"): each a
// secret of its own — its ID the key's hash, as an ID holds none of a key's slashes, the key in an
// annotation — holding one version, the sealed bytes, sent and read with their CRC32C; deleted
// with every version. ok secrets reaches it with the workload's credentials (Application Default
// Credentials: GKE's workload identity, say): none are in its configuration.
package gcpsm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/secrets/secretstore"
)

const (
	// format is the first byte of what a version holds: its format, the sealed bytes after it.
	format = 1
	// maxValue is the most Secret Manager holds in one version.
	maxValue = 64 << 10
	// keyAnnotation names the annotation holding a secret's key, for whoever lists them.
	keyAnnotation = "ok-key"
	// maxAnnotation bounds the key an annotation holds: annotations hold under 16 KiB in all.
	maxAnnotation = 8 << 10
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Store is Secret Manager, as a secretstore.Store.
type Store struct {
	c         *secretmanager.Client
	project   string
	locations []string
}

var _ secretstore.Store = (*Store)(nil)

// New is Secret Manager reached by c, keeping secrets in project, replicated to locations (where
// Google chooses, when none).
func New(c *secretmanager.Client, project string, locations []string) *Store {
	return &Store{c: c, project: project, locations: locations}
}

// FromConfig is the store an installation's configuration names, reached with the workload's
// credentials.
func FromConfig(ctx context.Context, c config.SecretStore) (*Store, error) {
	cl, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("google secret manager: %w", err)
	}
	return New(cl, c.Project, c.Locations), nil
}

// secret is a key's secret's resource name.
func (s *Store) secret(key string) string {
	h := sha256.Sum256([]byte(key))
	return "projects/" + s.project + "/secrets/ok-" + hex.EncodeToString(h[:])
}

// Close closes the client.
func (s *Store) Close() error { return s.c.Close() }

func (s *Store) Put(ctx context.Context, key string, sealed []byte) error {
	if 1+len(sealed) > maxValue {
		return fmt.Errorf("%w: Google Secret Manager holds %d bytes, and this value is %d sealed", secretstore.ErrTooLarge, maxValue-1, len(sealed))
	}
	name := s.secret(key)
	repl := &secretmanagerpb.Replication{Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{}}}
	if len(s.locations) > 0 {
		um := &secretmanagerpb.Replication_UserManaged{}
		for _, l := range s.locations {
			um.Replicas = append(um.Replicas, &secretmanagerpb.Replication_UserManaged_Replica{Location: l})
		}
		repl.Replication = &secretmanagerpb.Replication_UserManaged_{UserManaged: um}
	}
	annotated := key
	if len(annotated) > maxAnnotation {
		annotated = annotated[:maxAnnotation]
	}
	_, err := s.c.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{Parent: "projects/" + s.project, SecretId: name[len("projects/"+s.project+"/secrets/"):],
		Secret: &secretmanagerpb.Secret{Replication: repl, Annotations: map[string]string{keyAnnotation: annotated}}})
	switch {
	case status.Code(err) == codes.AlreadyExists:
		have, err := s.Get(ctx, key)
		switch {
		case err == nil && bytes.Equal(have, sealed):
			return nil
		case err == nil:
			return secretstore.ErrConflict
		case !errors.Is(err, secretstore.ErrNotFound):
			return err
		}
		// Made, and its version never added: added now.
	case err != nil:
		return fmt.Errorf("google secret manager: %w", err)
	}
	data := append([]byte{format}, sealed...)
	crc := int64(crc32.Checksum(data, castagnoli))
	if _, err := s.c.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{Parent: name,
		Payload: &secretmanagerpb.SecretPayload{Data: data, DataCrc32C: &crc}}); err != nil {
		return fmt.Errorf("google secret manager: %w", err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	r, err := s.c.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: s.secret(key) + "/versions/latest"})
	switch {
	case status.Code(err) == codes.NotFound:
		return nil, secretstore.ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("google secret manager: %w", err)
	}
	data := r.GetPayload().GetData()
	if c := r.GetPayload().DataCrc32C; c == nil || int64(crc32.Checksum(data, castagnoli)) != *c {
		return nil, fmt.Errorf("google secret manager: %s arrived damaged (its CRC32C)", s.secret(key))
	}
	if len(data) == 0 || data[0] != format {
		return nil, fmt.Errorf("google secret manager: %s holds no value of ok secrets'", s.secret(key))
	}
	return bytes.Clone(data[1:]), nil
}

func (s *Store) Delete(ctx context.Context, keys []string) error {
	for _, k := range keys {
		err := s.c.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: s.secret(k)})
		if err != nil && status.Code(err) != codes.NotFound {
			return fmt.Errorf("google secret manager: %w", err)
		}
	}
	return nil
}
