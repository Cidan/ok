package keys

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	kmsapi "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/aws/aws-sdk-go-v2/aws"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GCPKMS is what a root in Google Cloud KMS calls: *kmsapi.KeyManagementClient, or a test's.
type GCPKMS interface {
	Encrypt(ctx context.Context, req *kmspb.EncryptRequest, opts ...gax.CallOption) (*kmspb.EncryptResponse, error)
	Decrypt(ctx context.Context, req *kmspb.DecryptRequest, opts ...gax.CallOption) (*kmspb.DecryptResponse, error)
}

var _ GCPKMS = (*kmsapi.KeyManagementClient)(nil)

// gcpRoot is a symmetric crypto key in Cloud KMS: it encrypts under its primary version and
// decrypts under whichever version encrypted, binding the associated data itself.
type gcpRoot struct {
	c    GCPKMS
	name string // projects/…/locations/…/keyRings/…/cryptoKeys/…
}

func (r *gcpRoot) ID() string { return "gcp-kms:" + r.name }

func (r *gcpRoot) Wrap(ctx context.Context, plaintext, aad []byte) ([]byte, error) {
	resp, err := r.c.Encrypt(ctx, &kmspb.EncryptRequest{Name: r.name, Plaintext: plaintext, AdditionalAuthenticatedData: aad})
	if err != nil {
		return nil, fmt.Errorf("keys: %s: encrypting: %w", r.ID(), err)
	}
	return resp.GetCiphertext(), nil
}

func (r *gcpRoot) Unwrap(ctx context.Context, wrapped, aad []byte) ([]byte, error) {
	resp, err := r.c.Decrypt(ctx, &kmspb.DecryptRequest{Name: r.name, Ciphertext: wrapped, AdditionalAuthenticatedData: aad})
	if status.Code(err) == codes.InvalidArgument {
		return nil, fmt.Errorf("%w: %s: %v", ErrOpen, r.ID(), err)
	}
	if err != nil {
		return nil, fmt.Errorf("keys: %s: decrypting: %w", r.ID(), err)
	}
	return resp.GetPlaintext(), nil
}

// AWSKMS is what a root in AWS KMS calls: *awskms.Client, or a test's.
type AWSKMS interface {
	Encrypt(ctx context.Context, in *awskms.EncryptInput, opts ...func(*awskms.Options)) (*awskms.EncryptOutput, error)
	Decrypt(ctx context.Context, in *awskms.DecryptInput, opts ...func(*awskms.Options)) (*awskms.DecryptOutput, error)
}

var _ AWSKMS = (*awskms.Client)(nil)

// awsRoot is a symmetric KMS key in AWS: associated data travels as its encryption context, which
// AWS KMS binds as GCM does.
type awsRoot struct {
	c   AWSKMS
	arn string
}

// contextKey is the encryption context's one key: the associated data, base64.
const contextKey = "ok-secrets-aad"

func (r *awsRoot) ID() string { return "aws-kms:" + r.arn }

func (r *awsRoot) Wrap(ctx context.Context, plaintext, aad []byte) ([]byte, error) {
	out, err := r.c.Encrypt(ctx, &awskms.EncryptInput{KeyId: aws.String(r.arn), Plaintext: plaintext,
		EncryptionContext: map[string]string{contextKey: base64.StdEncoding.EncodeToString(aad)}})
	if err != nil {
		return nil, fmt.Errorf("keys: %s: encrypting: %w", r.ID(), err)
	}
	return out.CiphertextBlob, nil
}

func (r *awsRoot) Unwrap(ctx context.Context, wrapped, aad []byte) ([]byte, error) {
	out, err := r.c.Decrypt(ctx, &awskms.DecryptInput{KeyId: aws.String(r.arn), CiphertextBlob: wrapped,
		EncryptionContext: map[string]string{contextKey: base64.StdEncoding.EncodeToString(aad)}})
	var invalid interface{ ErrorCode() string }
	if errors.As(err, &invalid) && (invalid.ErrorCode() == "InvalidCiphertextException" || invalid.ErrorCode() == "IncorrectKeyException") {
		return nil, fmt.Errorf("%w: %s: %v", ErrOpen, r.ID(), err)
	}
	if err != nil {
		return nil, fmt.Errorf("keys: %s: decrypting: %w", r.ID(), err)
	}
	return out.Plaintext, nil
}
