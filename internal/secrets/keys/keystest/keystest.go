// Package keystest is what tests of ok secrets' roots share: fakes of Google Cloud KMS and AWS KMS
// in the test's process, each encrypting for real under keys of its own, and the conformance suite
// every root passes.
package keystest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	kmsapi "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/secrets/keys"
)

// Keyring writes a keyring of n keys in the test's directory and returns its root's ID.
func Keyring(t testing.TB, n int) string {
	t.Helper()
	var b strings.Builder
	for i := range n {
		line, err := keys.NewKeyringLine("k" + string(rune('0'+i)))
		if err != nil {
			t.Fatal(err)
		}
		b.WriteString(line)
	}
	file := filepath.Join(t.TempDir(), "keyring")
	if err := os.WriteFile(file, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return "keyring:" + file
}

// fakeKeys are a fake KMS's keys by name, each an AES-256 key.
type fakeKeys struct {
	mu   sync.Mutex
	keys map[string]keys.Key
}

func (f *fakeKeys) key(name string) (keys.Key, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[name]
	return k, ok
}

func newFakeKeys(t testing.TB, names ...string) *fakeKeys {
	f := &fakeKeys{keys: map[string]keys.Key{}}
	for _, n := range names {
		k, err := keys.NewKey()
		if err != nil {
			t.Fatal(err)
		}
		f.keys[n] = k
	}
	return f
}

// GCPKeys are the crypto keys GCPKMS makes, by resource name.
var GCPKeys = []string{
	"projects/ok-root/locations/us/keyRings/ok/cryptoKeys/root",
	"projects/acme-prod/locations/us/keyRings/byok/cryptoKeys/root",
}

// GCPKMS runs a fake of Cloud KMS's Encrypt and Decrypt with GCPKeys, and returns a client of it.
func GCPKMS(t testing.TB) *kmsapi.KeyManagementClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	kmspb.RegisterKeyManagementServiceServer(srv, &gcpFake{k: newFakeKeys(t, GCPKeys...)})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := kmsapi.NewKeyManagementClient(context.Background(), option.WithEndpoint(lis.Addr().String()), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

type gcpFake struct {
	kmspb.UnimplementedKeyManagementServiceServer
	k *fakeKeys
}

func (f *gcpFake) Encrypt(_ context.Context, req *kmspb.EncryptRequest) (*kmspb.EncryptResponse, error) {
	k, ok := f.k.key(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no crypto key %s", req.GetName())
	}
	ct, err := keys.Seal(k, req.GetPlaintext(), req.GetAdditionalAuthenticatedData())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &kmspb.EncryptResponse{Name: req.GetName() + "/cryptoKeyVersions/1", Ciphertext: ct}, nil
}

func (f *gcpFake) Decrypt(_ context.Context, req *kmspb.DecryptRequest) (*kmspb.DecryptResponse, error) {
	k, ok := f.k.key(req.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no crypto key %s", req.GetName())
	}
	pt, err := keys.Open(k, req.GetCiphertext(), req.GetAdditionalAuthenticatedData())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Decryption failed: the ciphertext is invalid.")
	}
	return &kmspb.DecryptResponse{Plaintext: pt}, nil
}

// AWSKeys are the KMS keys AWSKMS makes, by ARN.
var AWSKeys = []string{
	"arn:aws:kms:us-east-1:111111111111:key/00000000-0000-0000-0000-000000000001",
	"arn:aws:kms:us-east-1:222222222222:key/00000000-0000-0000-0000-000000000002",
}

// AWSKMS runs a fake of AWS KMS's Encrypt and Decrypt, speaking its JSON protocol, with AWSKeys,
// and returns a client of it.
func AWSKMS(t testing.TB) *awskms.Client {
	t.Helper()
	f := &awsFake{k: newFakeKeys(t, AWSKeys...)}
	s := httptest.NewServer(f)
	t.Cleanup(s.Close)
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
	return awskms.NewFromConfig(cfg, func(o *awskms.Options) { o.BaseEndpoint = aws.String(s.URL) })
}

type awsFake struct{ k *fakeKeys }

func (f *awsFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		KeyId             string
		Plaintext         []byte
		CiphertextBlob    []byte
		EncryptionContext map[string]string
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, "SerializationException", err.Error())
		return
	}
	ctxAAD := encryptionContext(req.EncryptionContext)
	switch r.Header.Get("X-Amz-Target") {
	case "TrentService.Encrypt":
		k, ok := f.k.key(req.KeyId)
		if !ok {
			fail(w, "NotFoundException", "no key "+req.KeyId)
			return
		}
		sealed, err := keys.Seal(k, req.Plaintext, ctxAAD)
		if err != nil {
			fail(w, "KMSInternalException", err.Error())
			return
		}
		var n [2]byte
		binary.BigEndian.PutUint16(n[:], uint16(len(req.KeyId)))
		reply(w, map[string]any{"KeyId": req.KeyId, "CiphertextBlob": slices.Concat(n[:], []byte(req.KeyId), sealed)})
	case "TrentService.Decrypt":
		b := req.CiphertextBlob
		if len(b) < 2 || len(b) < 2+int(binary.BigEndian.Uint16(b)) {
			fail(w, "InvalidCiphertextException", "")
			return
		}
		id := string(b[2 : 2+binary.BigEndian.Uint16(b)])
		if req.KeyId != "" && req.KeyId != id {
			fail(w, "IncorrectKeyException", "the ciphertext was encrypted under another key")
			return
		}
		k, ok := f.k.key(id)
		if !ok {
			fail(w, "InvalidCiphertextException", "")
			return
		}
		pt, err := keys.Open(k, b[2+len(id):], ctxAAD)
		if err != nil {
			fail(w, "InvalidCiphertextException", "")
			return
		}
		reply(w, map[string]any{"KeyId": id, "Plaintext": pt})
	default:
		fail(w, "UnknownOperationException", r.Header.Get("X-Amz-Target"))
	}
}

// encryptionContext is a context's bytes, its pairs sorted: what AWS KMS binds.
func encryptionContext(c map[string]string) []byte {
	ks := make([]string, 0, len(c))
	for k := range c {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	var b bytes.Buffer
	for _, k := range ks {
		b.WriteString(base64.StdEncoding.EncodeToString([]byte(k)) + "=" + base64.StdEncoding.EncodeToString([]byte(c[k])) + ";")
	}
	return b.Bytes()
}

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code, msg string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.Header().Set("X-Amzn-Errortype", code)
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]string{"__type": code, "message": msg})
}

// Run is the conformance suite of a root: what it wraps unwraps for the same associated data,
// and for no other; tampered with, nothing opens; and another root's key — other — opens none
// of it.
func Run(t *testing.T, root, other keys.Root) {
	ctx := context.Background()
	secret := []byte("a repository key, thirty-two bytes")
	aad := keys.RepositoryKeyAAD(7, 1)
	w, err := root.Wrap(ctx, secret, aad)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(w, secret) {
		t.Error("the wrapped key holds the key")
	}
	got, err := root.Unwrap(ctx, w, aad)
	if err != nil || !bytes.Equal(got, secret) {
		t.Fatalf("unwrapped %q, %v", got, err)
	}
	if again, err := root.Wrap(ctx, secret, aad); err != nil || bytes.Equal(again, w) {
		t.Errorf("wrapping twice gave the same ciphertext: %v", err)
	}
	if _, err := root.Unwrap(ctx, w, keys.RepositoryKeyAAD(7, 2)); !errors.Is(err, keys.ErrOpen) {
		t.Errorf("unwrapped for another version: %v", err)
	}
	if _, err := root.Unwrap(ctx, w, keys.RepositoryKeyAAD(8, 1)); !errors.Is(err, keys.ErrOpen) {
		t.Errorf("unwrapped for another repository: %v", err)
	}
	bad := slices.Clone(w)
	bad[len(bad)-1] ^= 1
	if _, err := root.Unwrap(ctx, bad, aad); !errors.Is(err, keys.ErrOpen) {
		t.Errorf("unwrapped a tampered ciphertext: %v", err)
	}
	if other != nil {
		if _, err := other.Unwrap(ctx, w, aad); err == nil {
			t.Error("another root's key unwrapped it")
		}
	}
}
