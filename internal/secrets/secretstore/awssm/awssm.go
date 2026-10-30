// Package awssm keeps sealed values in AWS Secrets Manager (docs/secrets.md, "Stores"): each a
// secret of its own, named by its key, created once with the sealed bytes as its binary value,
// and deleted without a recovery window. ok secrets reaches it with the workload's credentials
// (the SDK's default chain: an EKS service account's role, say): none are in its configuration.
package awssm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"

	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/secrets/secretstore"
)

// format is the first byte of what a secret holds: its format, the sealed bytes after it — so
// even an empty value is held as one byte, as Secrets Manager holds none empty.
const format = 1

// maxName is the longest name Secrets Manager takes; maxValue the most it holds in one secret.
const (
	maxName  = 512
	maxValue = 65536
)

// Store is Secrets Manager, as a secretstore.Store.
type Store struct {
	c *secretsmanager.Client
	// kmsKey is the KMS key Secrets Manager encrypts at rest under: "" for its own.
	kmsKey string
}

var _ secretstore.Store = (*Store)(nil)

// New is Secrets Manager reached by c, encrypting under kmsKey ("": its own).
func New(c *secretsmanager.Client, kmsKey string) *Store { return &Store{c: c, kmsKey: kmsKey} }

// FromConfig is the store an installation's configuration names, reached with the workload's
// credentials.
func FromConfig(ctx context.Context, c config.SecretStore) (*Store, error) {
	// The SDK makes again a call that fails for a moment — throttled, a 5xx — every call of the
	// store's being one it may make again (Put), up to six times, as ok's other stores do.
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.Region), awsconfig.WithRetryMaxAttempts(6))
	if err != nil {
		return nil, fmt.Errorf("aws secrets manager: %w", err)
	}
	return New(secretsmanager.NewFromConfig(cfg), c.KMSKeyID), nil
}

// name is a key's secret's name: the key itself — whose characters Secrets Manager's names
// allow — or, past their length, its hash, which no key's form can be.
func name(key string) string {
	if len(key) <= maxName {
		return key
	}
	h := sha256.Sum256([]byte(key))
	return "ok/sha256/" + hex.EncodeToString(h[:])
}

func (s *Store) Put(ctx context.Context, key string, sealed []byte) error {
	if 1+len(sealed) > maxValue {
		return fmt.Errorf("%w: AWS Secrets Manager holds %d bytes, and this value is %d sealed", secretstore.ErrTooLarge, maxValue-1, len(sealed))
	}
	in := &secretsmanager.CreateSecretInput{
		Name:         aws.String(name(key)),
		SecretBinary: append([]byte{format}, sealed...),
		Description:  aws.String(description(key)),
	}
	if s.kmsKey != "" {
		in.KmsKeyId = aws.String(s.kmsKey)
	}
	_, err := s.c.CreateSecret(ctx, in)
	var exists *types.ResourceExistsException
	if !errors.As(err, &exists) {
		if err != nil {
			return fmt.Errorf("aws secrets manager: %w", err)
		}
		return nil
	}
	have, err := s.Get(ctx, key)
	switch {
	case err != nil:
		return err
	case bytes.Equal(have, sealed):
		return nil
	}
	return secretstore.ErrConflict
}

// description says what a secret is, to whoever lists them.
func description(key string) string {
	d := "a value ok secrets keeps, sealed (docs/secrets.md): " + key
	if len(d) > 2048 {
		d = d[:2048]
	}
	return d
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.c.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(name(key))})
	var missing *types.ResourceNotFoundException
	var invalid *types.InvalidRequestException
	switch {
	case errors.As(err, &missing):
		return nil, secretstore.ErrNotFound
	case errors.As(err, &invalid):
		// A secret being deleted, which a delete without recovery does in the background.
		return nil, secretstore.ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("aws secrets manager: %w", err)
	}
	b := out.SecretBinary
	if len(b) == 0 || b[0] != format {
		return nil, fmt.Errorf("aws secrets manager: %s holds no value of ok secrets'", name(key))
	}
	return bytes.Clone(b[1:]), nil
}

func (s *Store) Delete(ctx context.Context, keys []string) error {
	for _, k := range keys {
		_, err := s.c.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{SecretId: aws.String(name(k)), ForceDeleteWithoutRecovery: aws.Bool(true)})
		var missing *types.ResourceNotFoundException
		if err != nil && !errors.As(err, &missing) {
			return fmt.Errorf("aws secrets manager: %w", err)
		}
	}
	return nil
}
