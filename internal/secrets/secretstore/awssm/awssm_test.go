package awssm

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/Cidan/ok/internal/secrets/secretstore"
	"github.com/Cidan/ok/internal/secrets/secretstore/storetest"
)

// fake is Secrets Manager's CreateSecret, GetSecretValue, and DeleteSecret, speaking its JSON
// protocol: each secret's binary value, and the KMS key it names.
type fake struct {
	mu      sync.Mutex
	secrets map[string][]byte
	kms     map[string]string
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var req struct {
		Name, SecretId, KmsKeyId   string
		SecretBinary               []byte
		ForceDeleteWithoutRecovery bool
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, "SerializationException", err.Error())
		return
	}
	switch r.Header.Get("X-Amz-Target") {
	case "secretsmanager.CreateSecret":
		if _, ok := f.secrets[req.Name]; ok {
			fail(w, "ResourceExistsException", "the secret "+req.Name+" already exists")
			return
		}
		if len(req.SecretBinary) == 0 || len(req.Name) > 512 || strings.ContainsFunc(req.Name, func(r rune) bool {
			return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("/_+=.@-", r))
		}) {
			fail(w, "InvalidParameterException", "invalid name or value")
			return
		}
		f.secrets[req.Name], f.kms[req.Name] = req.SecretBinary, req.KmsKeyId
		reply(w, map[string]any{"Name": req.Name, "ARN": "arn:aws:secretsmanager:us-east-1:111111111111:secret:" + req.Name})
	case "secretsmanager.GetSecretValue":
		v, ok := f.secrets[req.SecretId]
		if !ok {
			fail(w, "ResourceNotFoundException", "Secrets Manager can't find the specified secret.")
			return
		}
		reply(w, map[string]any{"Name": req.SecretId, "SecretBinary": v})
	case "secretsmanager.DeleteSecret":
		if !req.ForceDeleteWithoutRecovery {
			fail(w, "InvalidParameterException", "a recovery window")
			return
		}
		delete(f.secrets, req.SecretId)
		reply(w, map[string]any{"Name": req.SecretId})
	default:
		fail(w, "UnknownOperationException", r.Header.Get("X-Amz-Target"))
	}
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

func newStore(t *testing.T, kmsKey string) (*Store, *fake) {
	f := &fake{secrets: map[string][]byte{}, kms: map[string]string{}}
	s := httptest.NewServer(f)
	t.Cleanup(s.Close)
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
	return New(secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) { o.BaseEndpoint = aws.String(s.URL) }), kmsKey), f
}

func TestConformance(t *testing.T) {
	st, _ := newStore(t, "")
	storetest.Run(t, st)
}

// A secret is named by its key, or past Secrets Manager's longest name by its hash; it holds the
// sealed bytes after their format, under the KMS key configured.
func TestSecretsManager(t *testing.T) {
	st, f := newStore(t, "arn:aws:kms:us-east-1:111111111111:key/k")
	ctx := t.Context()
	long := "ok/7/secret/" + strings.Repeat("a/", 300) + "x@ab"
	for _, k := range []string{"ok/7/secret/prod/db@ab", long} {
		if err := st.Put(ctx, k, []byte("sealed")); err != nil {
			t.Fatal(err)
		}
		if v, err := st.Get(ctx, k); err != nil || string(v) != "sealed" {
			t.Errorf("%.40s: %q, %v", k, v, err)
		}
	}
	f.mu.Lock()
	held, kms := f.secrets["ok/7/secret/prod/db@ab"], f.kms["ok/7/secret/prod/db@ab"]
	_, hashed := f.secrets[name(long)]
	f.mu.Unlock()
	if string(held) != "\x01sealed" || kms != "arn:aws:kms:us-east-1:111111111111:key/k" {
		t.Errorf("secrets manager holds %q under %q", held, kms)
	}
	if !hashed || !strings.HasPrefix(name(long), "ok/sha256/") {
		t.Errorf("the long key's name: %s", name(long))
	}
	if err := st.Put(ctx, "ok/7/secret/big@ab", make([]byte, 65536)); !errors.Is(err, secretstore.ErrTooLarge) {
		t.Errorf("a value past a secret's most: %v", err)
	}
}
