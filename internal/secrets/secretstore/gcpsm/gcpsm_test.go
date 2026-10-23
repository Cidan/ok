package gcpsm

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/Cidan/ok/internal/secrets/secretstore"
	"github.com/Cidan/ok/internal/secrets/secretstore/storetest"
	"github.com/Cidan/ok/internal/testgcp"
)

// fake is Secret Manager's CreateSecret, AddSecretVersion, AccessSecretVersion, and DeleteSecret:
// each secret, its replication and annotations, and its versions.
type fake struct {
	secretmanagerpb.UnimplementedSecretManagerServiceServer
	mu       sync.Mutex
	secrets  map[string]*secretmanagerpb.Secret
	versions map[string][][]byte
	// failAdd fails the next AddSecretVersion, as a lost connection would.
	failAdd bool
}

var secretID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,255}$`)

func (f *fake) CreateSecret(_ context.Context, r *secretmanagerpb.CreateSecretRequest) (*secretmanagerpb.Secret, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !secretID.MatchString(r.GetSecretId()) || r.GetSecret().GetReplication() == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid secret")
	}
	name := r.GetParent() + "/secrets/" + r.GetSecretId()
	if _, ok := f.secrets[name]; ok {
		return nil, status.Error(codes.AlreadyExists, "Secret ["+name+"] already exists.")
	}
	sec := r.GetSecret()
	sec.Name = name
	f.secrets[name] = sec
	return sec, nil
}

func (f *fake) AddSecretVersion(_ context.Context, r *secretmanagerpb.AddSecretVersionRequest) (*secretmanagerpb.SecretVersion, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAdd {
		f.failAdd = false
		return nil, status.Error(codes.Unavailable, "the connection was lost")
	}
	if _, ok := f.secrets[r.GetParent()]; !ok {
		return nil, status.Error(codes.NotFound, "no secret")
	}
	d := r.GetPayload().GetData()
	if c := r.GetPayload().DataCrc32C; c == nil || int64(crc32.Checksum(d, castagnoli)) != *c || len(d) > 64<<10 {
		return nil, status.Error(codes.InvalidArgument, "a damaged or large payload")
	}
	f.versions[r.GetParent()] = append(f.versions[r.GetParent()], d)
	return &secretmanagerpb.SecretVersion{Name: r.GetParent() + "/versions/1"}, nil
}

func (f *fake) AccessSecretVersion(_ context.Context, r *secretmanagerpb.AccessSecretVersionRequest) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name, ok := strings.CutSuffix(r.GetName(), "/versions/latest")
	vs := f.versions[name]
	if !ok || len(vs) == 0 {
		return nil, status.Error(codes.NotFound, "no version")
	}
	d := vs[len(vs)-1]
	crc := int64(crc32.Checksum(d, castagnoli))
	return &secretmanagerpb.AccessSecretVersionResponse{Name: r.GetName(), Payload: &secretmanagerpb.SecretPayload{Data: d, DataCrc32C: &crc}}, nil
}

func (f *fake) DeleteSecret(_ context.Context, r *secretmanagerpb.DeleteSecretRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.secrets[r.GetName()]; !ok {
		return nil, status.Error(codes.NotFound, "no secret")
	}
	delete(f.secrets, r.GetName())
	delete(f.versions, r.GetName())
	return &emptypb.Empty{}, nil
}

func newStore(t *testing.T, locations []string) (*Store, *fake) {
	f := &fake{secrets: map[string]*secretmanagerpb.Secret{}, versions: map[string][][]byte{}}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	secretmanagerpb.RegisterSecretManagerServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := secretmanager.NewClient(context.Background(), option.WithEndpoint(lis.Addr().String()), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return New(c, "acme-secrets", locations), f
}

func TestConformance(t *testing.T) {
	st, _ := newStore(t, nil)
	storetest.Run(t, st)
}

// A secret's ID is its key's hash, the key annotated; its replication is where Google chooses, or
// the locations configured; a secret made whose version never came gets it on the next put; and
// a value past what a version holds is refused before it is sent.
func TestSecretManager(t *testing.T) {
	st, f := newStore(t, []string{"us-east1", "us-west1"})
	ctx := t.Context()
	const key = "ok/7/secret/prod/db@ab"
	f.failAdd = true
	if err := st.Put(ctx, key, []byte("sealed")); err == nil {
		t.Fatal("a lost connection was no error")
	}
	if _, err := st.Get(ctx, key); err != secretstore.ErrNotFound {
		t.Errorf("a secret with no version: %v", err)
	}
	if err := st.Put(ctx, key, []byte("sealed")); err != nil {
		t.Fatalf("putting again: %v", err)
	}
	if v, err := st.Get(ctx, key); err != nil || string(v) != "sealed" {
		t.Errorf("got %q, %v", v, err)
	}
	f.mu.Lock()
	sec := f.secrets[st.secret(key)]
	f.mu.Unlock()
	if um := sec.GetReplication().GetUserManaged(); len(um.GetReplicas()) != 2 || um.GetReplicas()[0].GetLocation() != "us-east1" ||
		sec.GetAnnotations()["ok-key"] != key || !strings.HasPrefix(sec.GetName(), "projects/acme-secrets/secrets/ok-") {
		t.Errorf("the secret: %v", sec)
	}
	if err := st.Put(ctx, "ok/7/secret/big@ab", make([]byte, 64<<10)); !errors.Is(err, secretstore.ErrTooLarge) {
		t.Errorf("a value past a version's most: %v", err)
	}
}

// prefixed is a store whose keys are prefixed, each it is asked of recorded: a run of the
// conformance suite of its own, in a real service other runs share, and everything it made.
type prefixed struct {
	secretstore.Store
	prefix string
	mu     sync.Mutex
	keys   []string
}

func (p *prefixed) key(k string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	full := p.prefix + k
	if !slices.Contains(p.keys, full) {
		p.keys = append(p.keys, full)
	}
	return full
}

func (p *prefixed) Put(ctx context.Context, key string, sealed []byte) error {
	return p.Store.Put(ctx, p.key(key), sealed)
}

func (p *prefixed) Get(ctx context.Context, key string) ([]byte, error) {
	return p.Store.Get(ctx, p.key(key))
}

func (p *prefixed) Delete(ctx context.Context, keys []string) error {
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = p.key(k)
	}
	return p.Store.Delete(ctx, full)
}

// A real project's Secret Manager passes the conformance suite (internal/testgcp), leaving nothing.
func TestSecretManagerLive(t *testing.T) {
	project := testgcp.SecretProject(t)
	ctx := context.Background()
	c, err := secretmanager.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	s := &prefixed{Store: New(c, project, nil), prefix: fmt.Sprintf("test-%d/", time.Now().UnixNano())}
	t.Cleanup(func() {
		if err := s.Store.Delete(context.Background(), s.keys); err != nil {
			t.Errorf("cleaning up: %v", err)
		}
		for _, k := range s.keys {
			if _, err := s.Store.Get(context.Background(), k); !errors.Is(err, secretstore.ErrNotFound) {
				t.Errorf("%s left behind: %v", k, err)
			}
		}
	})
	storetest.Run(t, s)
}
