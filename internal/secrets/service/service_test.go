package service_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/keys"
	"github.com/Cidan/ok/internal/secrets/keys/keystest"
	"github.com/Cidan/ok/internal/secrets/service"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/secrets/store/postgres"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/testdb"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

var names = identity.MustNames("example.com")

// fakeMain is main in memory: each repository's head, and each commit's files.
type fakeMain struct {
	mu    sync.Mutex
	heads map[int64][]byte
	files map[string]map[string]string // by commit
	reads int
	// parents are each commit's ancestors on main.
	parents map[string][]string
}

func (m *fakeMain) Head(_ context.Context, repo int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.heads[repo]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no repository %d", repo)
	}
	return h, nil
}

func (m *fakeMain) Files(_ context.Context, _ int64, commit []byte, paths []string, limit int) (map[string][]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	out := map[string][]byte{}
	for _, p := range paths {
		if v, ok := m.files[string(commit)][p]; ok {
			if len(v) > limit {
				return nil, fmt.Errorf("%s is larger than %d bytes", p, limit)
			}
			out[p] = []byte(v)
		}
	}
	return out, nil
}

func (m *fakeMain) IsAncestor(_ context.Context, _ int64, ancestor, descendant []byte) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Contains(m.parents[string(descendant)], string(ancestor)), nil
}

// land moves a repository's main to a commit holding files.
func (m *fakeMain) land(repo int64, commit string, files map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heads[repo] = []byte(commit)
	m.files[commit] = files
}

// fakeMembers are sets' members, by principal ID.
type fakeMembers struct {
	mu sync.Mutex
	in map[string][]string
}

func (f *fakeMembers) Member(_ context.Context, principal, set string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.in[principal], set), nil
}

// fakeApprovals are passkey approvals a test gives or declines.
type fakeApprovals struct {
	mu      sync.Mutex
	started map[string]string // approval → scope
	answer  map[string]bool   // approval → approved, once answered
}

func (f *fakeApprovals) Start(_ context.Context, principal, scope, _ string) (string, string, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := "approval-" + principal + "-" + string(rune('a'+len(f.started)))
	f.started[id] = scope
	return id, "https://ok.example.com/id/approve/" + id, time.Now().Add(5 * time.Minute), nil
}

func (f *fakeApprovals) Status(_ context.Context, id, _, scope string) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.started[id] != scope {
		return false, true, nil // an approval of something else approves nothing here
	}
	a, done := f.answer[id]
	return a, done, nil
}

const (
	ada  = "ada-id" // in ops: reads and writes secret/
	bea  = "bea-id" // in nothing
	cy   = "cy-id"  // in deployers: writes secret/, reads nothing
	repo = 7
)

type env struct {
	t         *testing.T
	ctx       context.Context
	main      *fakeMain
	members   *fakeMembers
	approvals *fakeApprovals
	store     *store.Store
	policy    *service.Policy
	secrets   *service.Secrets
	versions  *service.Versions
}

const settings = "build: none\nsecrets: {}\n"

func newEnv(t *testing.T) *env {
	t.Helper()
	b, err := postgres.Open(context.Background(), testdb.Pool(t))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.New(b, store.Config{Root: keystest.Keyring(t, 1), Opener: &keys.Opener{}})
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, ctx: context.Background(), store: st,
		main:      &fakeMain{heads: map[int64][]byte{}, files: map[string]map[string]string{}},
		members:   &fakeMembers{in: map[string][]string{ada: {"ops.acl.example.com"}, cy: {"deployers.acl.example.com"}}},
		approvals: &fakeApprovals{started: map[string]string{}, answer: map[string]bool{}}}
	e.main.land(repo, "c1", map[string]string{
		".ok/repository.yaml":   settings,
		"secret/acl.yaml":       "readers: [ops]\nwriters: [ops, deployers]\n",
		"secret/prod/acl.yaml":  "readers: [ops]\nwriters: [ops]\nunseal: passkey\n",
		"secret/empty/acl.yaml": "readers: []\nwriters: []\n",
	})
	e.main.land(8, "plain", map[string]string{".ok/repository.yaml": "build: none\n"})
	e.policy = service.NewPolicy(e.main, e.members, names)
	cfg := service.Config{Store: st, Policy: e.policy, Approvals: e.approvals}
	e.secrets, e.versions = service.NewSecrets(cfg), service.NewVersions(cfg)
	return e
}

// as is a context of a request a person made with their device credential.
func as(id string) context.Context {
	return authn.NewContext(context.Background(), authn.Caller{Principal: strings.TrimSuffix(id, "-id") + ".human.example.com", ID: id, Human: true,
		Via: authn.ViaCredential})
}

func code(err error) codes.Code { return status.Code(err) }

func (e *env) write(who, path, value string) []byte {
	e.t.Helper()
	r, err := e.secrets.WriteSecret(as(who), &secretspb.WriteSecretRequest{RepositoryId: repo, Path: path, Value: []byte(value)})
	if err != nil {
		e.t.Fatalf("%s writing %s: %v", who, path, err)
	}
	d, err := secrets.ParsePointer(r.GetPointer())
	if err != nil || string(d[:]) != string(r.GetDigest()) {
		e.t.Fatalf("the pointer %q, digest %x: %v", r.GetPointer(), r.GetDigest(), err)
	}
	return r.GetDigest()
}

func (e *env) unseal(who string, paths ...string) *secretspb.UnsealResponse {
	e.t.Helper()
	r, err := e.secrets.Unseal(as(who), &secretspb.UnsealRequest{RepositoryId: repo, Paths: paths})
	if err != nil {
		e.t.Fatalf("%s unsealing %v: %v", who, paths, err)
	}
	return r
}

func (e *env) read(who, path string, digest []byte) (string, error) {
	r, err := e.secrets.ReadSecret(as(who), &secretspb.ReadSecretRequest{RepositoryId: repo, Path: path, Digest: digest})
	if err != nil {
		return "", err
	}
	var d secrets.Digest
	var o secrets.Opening
	copy(d[:], digest)
	copy(o[:], r.GetOpening())
	if !secrets.Verify(d, repo, path, o, r.GetValue()) {
		e.t.Errorf("%s's value does not match its pointer", path)
	}
	return string(r.GetValue()), nil
}

// What each person may do, as the folders' acl.yaml on main say, and why not.
func TestCheckAccess(t *testing.T) {
	e := newEnv(t)
	for _, tc := range []struct {
		who, path   string
		read, write bool
		why         string
	}{
		{ada, "secret/token", true, true, ""},
		{bea, "secret/token", false, false, "you are in none of secret/acl.yaml's readers: ops.acl.example.com"},
		{cy, "secret/token", false, true, "you are in none of secret/acl.yaml's readers"},
		{ada, "secret/empty/x", false, false, "secret/empty/acl.yaml names no readers"},
		{bea, "secret/new/x", false, true, "main has no secret/new/acl.yaml yet"},
	} {
		r, err := e.secrets.CheckAccess(as(tc.who), &secretspb.CheckAccessRequest{RepositoryId: repo, Path: tc.path})
		if err != nil {
			t.Fatal(err)
		}
		if !r.GetKeepsSecrets() || r.GetRead() != tc.read || r.GetWrite() != tc.write || !strings.Contains(r.GetWhyNotRead(), tc.why) {
			t.Errorf("%s at %s: %+v", tc.who, tc.path, r)
		}
	}
	if r, err := e.secrets.CheckAccess(as(ada), &secretspb.CheckAccessRequest{RepositoryId: repo, Path: "secret/prod/db"}); err != nil || !r.GetPasskey() {
		t.Errorf("secret/prod asks for a passkey: %+v, %v", r, err)
	}
	if r, err := e.secrets.CheckAccess(as(ada), &secretspb.CheckAccessRequest{RepositoryId: 8, Path: "secret/x"}); err != nil || r.GetKeepsSecrets() || r.GetRead() {
		t.Errorf("a repository that keeps no secrets: %+v, %v", r, err)
	}
	if _, err := e.secrets.CheckAccess(as(ada), &secretspb.CheckAccessRequest{RepositoryId: repo, Path: "secret/acl.yaml"}); code(err) != codes.InvalidArgument {
		t.Errorf("an acl.yaml's path: %v", err)
	}
	if r, err := e.secrets.CheckAccess(as(bea), &secretspb.CheckAccessRequest{RepositoryId: repo, Path: "secret"}); err != nil || !r.GetKeepsSecrets() || r.GetRead() {
		t.Errorf("the tree's root: %+v, %v", r, err)
	}
	if r, err := e.secrets.CheckAccess(as(ada), &secretspb.CheckAccessRequest{RepositoryId: 8, Path: "secret"}); err != nil || r.GetKeepsSecrets() {
		t.Errorf("the tree's root where none are kept: %+v, %v", r, err)
	}
}

// A reader reads a secret once for each unseal; without one, not at all.
func TestUnsealAndRead(t *testing.T) {
	e := newEnv(t)
	d := e.write(ada, "secret/token", "t0ken")
	if _, err := e.read(ada, "secret/token", d); code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "jj secret unseal //secret/token") {
		t.Fatalf("a read before unsealing: %v", err)
	}
	if r := e.unseal(ada, "secret/token"); r.GetState() != secretspb.UnsealResponse_STATE_GRANTED {
		t.Fatalf("unsealing: %+v", r)
	}
	if l, err := e.secrets.ListUnseals(as(ada), &secretspb.ListUnsealsRequest{RepositoryId: repo}); err != nil || len(l.GetUnseals()) != 1 {
		t.Errorf("unseals %+v, %v", l, err)
	}
	if v, err := e.read(ada, "secret/token", d); err != nil || v != "t0ken" {
		t.Fatalf("read %q, %v", v, err)
	}
	if _, err := e.read(ada, "secret/token", d); code(err) != codes.FailedPrecondition {
		t.Errorf("a second read on one unseal: %v", err)
	}
	// Not a reader: no unseal, no read.
	if _, err := e.secrets.Unseal(as(bea), &secretspb.UnsealRequest{RepositoryId: repo, Paths: []string{"secret/token"}}); code(err) != codes.PermissionDenied {
		t.Errorf("bea unsealing: %v", err)
	}
	if _, err := e.read(bea, "secret/token", d); code(err) != codes.PermissionDenied {
		t.Errorf("bea reading: %v", err)
	}
	if _, err := e.read(ada, "secret/token", make([]byte, 32)); code(err) != codes.NotFound {
		t.Errorf("a digest never written: %v", err)
	}
}

// A writer who is no reader writes, and reads back only what they wrote, while still a writer.
func TestWriterReadsBackWhatTheyWrote(t *testing.T) {
	e := newEnv(t)
	mine := e.write(cy, "secret/deploy-key", "cy's")
	theirs := e.write(ada, "secret/deploy-key", "ada's")
	e.unseal(cy, "secret/deploy-key", "secret/deploy-key")
	if v, err := e.read(cy, "secret/deploy-key", mine); err != nil || v != "cy's" {
		t.Errorf("cy reading back: %q, %v", v, err)
	}
	if _, err := e.read(cy, "secret/deploy-key", theirs); code(err) != codes.PermissionDenied {
		t.Errorf("cy reading ada's: %v", err)
	}
	// No longer a writer, no longer a reader of it.
	e.members.mu.Lock()
	e.members.in[cy] = nil
	e.members.mu.Unlock()
	e.unsealDenied(cy, "secret/deploy-key")
	if _, err := e.read(cy, "secret/deploy-key", mine); code(err) != codes.PermissionDenied {
		t.Errorf("cy, no longer a writer, reading back: %v", err)
	}
	// A reader who is no writer may not write.
	e.members.mu.Lock()
	e.members.in[bea] = []string{"ops.acl.example.com"}
	e.members.mu.Unlock()
	e.main.land(repo, "c2", map[string]string{".ok/repository.yaml": settings, "secret/acl.yaml": "readers: [ops]\nwriters: [deployers]\n"})
	if _, err := e.secrets.WriteSecret(as(bea), &secretspb.WriteSecretRequest{RepositoryId: repo, Path: "secret/x", Value: []byte("v")}); code(err) != codes.PermissionDenied {
		t.Errorf("a reader writing: %v", err)
	}
}

func (e *env) unsealDenied(who string, paths ...string) {
	e.t.Helper()
	if _, err := e.secrets.Unseal(as(who), &secretspb.UnsealRequest{RepositoryId: repo, Paths: paths}); code(err) != codes.PermissionDenied {
		e.t.Errorf("%s unsealing %v: %v", who, paths, err)
	}
}

// A folder that asks for a passkey is unsealed once its approval is given — of exactly what was
// asked — and once however often it is polled.
func TestPasskeyUnseal(t *testing.T) {
	e := newEnv(t)
	d := e.write(ada, "secret/prod/db", "hunter2")
	r := e.unseal(ada, "secret/prod/db", "secret/token")
	if r.GetState() != secretspb.UnsealResponse_STATE_APPROVE || r.GetApprovalId() == "" || !strings.Contains(r.GetUrl(), r.GetApprovalId()) {
		t.Fatalf("unsealing a passkey folder: %+v", r)
	}
	id := r.GetApprovalId()
	poll := func(paths ...string) (*secretspb.UnsealResponse, error) {
		return e.secrets.Unseal(as(ada), &secretspb.UnsealRequest{RepositoryId: repo, Paths: paths, ApprovalId: id})
	}
	if r, err := poll("secret/token", "secret/prod/db"); err != nil || r.GetState() != secretspb.UnsealResponse_STATE_WAITING {
		t.Fatalf("before the approval: %+v, %v", r, err)
	}
	if _, err := e.read(ada, "secret/prod/db", d); code(err) != codes.FailedPrecondition {
		t.Errorf("a read before the approval: %v", err)
	}
	e.approvals.mu.Lock()
	e.approvals.answer[id] = true
	e.approvals.mu.Unlock()
	if _, err := poll("secret/prod/db"); code(err) != codes.PermissionDenied {
		t.Errorf("the approval of other paths: %v", err)
	}
	for range 2 {
		if r, err := poll("secret/prod/db", "secret/token"); err != nil || r.GetState() != secretspb.UnsealResponse_STATE_GRANTED {
			t.Fatalf("after the approval: %+v, %v", r, err)
		}
	}
	if v, err := e.read(ada, "secret/prod/db", d); err != nil || v != "hunter2" {
		t.Fatalf("read %q, %v", v, err)
	}
	if _, err := e.read(ada, "secret/prod/db", d); code(err) != codes.FailedPrecondition {
		t.Errorf("a second read after polling twice: %v", err)
	}
	// Declined.
	r = e.unseal(ada, "secret/prod/db")
	e.approvals.mu.Lock()
	e.approvals.answer[r.GetApprovalId()] = false
	e.approvals.mu.Unlock()
	if _, err := e.secrets.Unseal(as(ada), &secretspb.UnsealRequest{RepositoryId: repo, Paths: []string{"secret/prod/db"}, ApprovalId: r.GetApprovalId()}); code(err) != codes.PermissionDenied {
		t.Errorf("a declined approval: %v", err)
	}
}

// A grant removed on main takes effect as main moves; one given there, likewise.
func TestMainDecides(t *testing.T) {
	e := newEnv(t)
	d := e.write(ada, "secret/token", "t0ken")
	e.unseal(ada, "secret/token")
	e.main.land(repo, "c2", map[string]string{".ok/repository.yaml": settings, "secret/acl.yaml": "readers: [deployers]\nwriters: [deployers]\n"})
	if _, err := e.read(ada, "secret/token", d); code(err) != codes.PermissionDenied {
		t.Errorf("a reader and writer removed on main reading: %v", err)
	}
	if _, err := e.read(cy, "secret/token", d); code(err) != codes.FailedPrecondition {
		t.Errorf("a reader given on main, before unsealing: %v", err)
	}
	e.unseal(cy, "secret/token")
	if v, err := e.read(cy, "secret/token", d); err != nil || v != "t0ken" {
		t.Errorf("a reader given on main: %q, %v", v, err)
	}
	// A repository that keeps no secrets on main keeps none here.
	e.main.land(repo, "c3", map[string]string{".ok/repository.yaml": "build: none\n", "secret/acl.yaml": "readers: [ops]\nwriters: [ops]\n"})
	if _, err := e.secrets.WriteSecret(as(ada), &secretspb.WriteSecretRequest{RepositoryId: repo, Path: "secret/x", Value: []byte("v")}); code(err) != codes.FailedPrecondition {
		t.Errorf("writing once main keeps no secrets: %v", err)
	}
}

// Secrets are a person's with their device credential: not a robot's, nor a personal token's.
func TestCallers(t *testing.T) {
	e := newEnv(t)
	robot := authn.NewContext(context.Background(), authn.Caller{Principal: "ci.robot.example.com", ID: "ci-id"})
	token := authn.NewContext(context.Background(), authn.Caller{Principal: "ada.human.example.com", ID: ada, Human: true, Via: authn.ViaPersonalToken})
	for name, ctx := range map[string]context.Context{"a robot": robot, "a personal token": token, "no one": context.Background()} {
		if _, err := e.secrets.WriteSecret(ctx, &secretspb.WriteSecretRequest{RepositoryId: repo, Path: "secret/x", Value: []byte("v")}); code(err) != codes.PermissionDenied && code(err) != codes.Unauthenticated {
			t.Errorf("%s writing: %v", name, err)
		}
	}
	for svc, want := range map[string][]okpb.Caller{
		"ok.secrets.v1.Secrets":       {okpb.Caller_CALLER_PEOPLE},
		"ok.secrets.v1.Versions":      {okpb.Caller_CALLER_SCM},
		"ok.secrets.v1.DeploySecrets": {okpb.Caller_CALLER_DEPLOY},
		"ok.secrets.v1.RunSecrets":    {okpb.Caller_CALLER_WORKER},
	} {
		if got := authn.Callers(svc); !slices.Equal(got, want) {
			t.Errorf("%s's callers %v, want %v", svc, got, want)
		}
	}
}

// Every decision is audited: writes, unseals, reads, and refusals.
func TestAudit(t *testing.T) {
	e := newEnv(t)
	d := e.write(ada, "secret/token", "t0ken")
	e.unseal(ada, "secret/token")
	if _, err := e.read(ada, "secret/token", d); err != nil {
		t.Fatal(err)
	}
	if _, err := e.read(bea, "secret/token", d); err == nil {
		t.Fatal("bea read")
	}
	got, err := e.store.AuditOf(e.ctx, repo, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	var ops []string
	for _, a := range got {
		ops = append(ops, a.Op+" "+a.Actor+" "+a.Outcome)
	}
	want := []string{"write ada-id ok", "unseal ada-id ok", "read ada-id ok"}
	if len(ops) != 4 || !slices.Equal(ops[:3], want) || !strings.HasPrefix(ops[3], "read bea-id you may not read") {
		t.Errorf("audit:\n%s", strings.Join(ops, "\n"))
	}
	if got[0].Digest == "" || !strings.HasPrefix(secrets.Digest(d32(d)).String(), got[0].Digest) {
		t.Errorf("the audit's digest %q", got[0].Digest)
	}
}

func d32(b []byte) (d [32]byte) { copy(d[:], b); return }

// The SCM's checks: which pointers ok secrets keeps, and who writes which folders.
func TestVersions(t *testing.T) {
	e := newEnv(t)
	d := e.write(ada, "secret/token", "t0ken")
	r, err := e.versions.CheckPointers(e.ctx, &secretspb.CheckPointersRequest{RepositoryId: repo, Pointers: []*secretspb.Pointer{
		{Path: "secret/token", Digest: d}, {Path: "secret/other", Digest: d}, {Path: "secret/token", Digest: make([]byte, 32)}}})
	if err != nil || !slices.Equal(r.GetKept(), []bool{true, false, false}) {
		t.Errorf("kept %v, %v", r.GetKept(), err)
	}
	if _, err := e.versions.CheckPointers(e.ctx, &secretspb.CheckPointersRequest{RepositoryId: repo, Pointers: []*secretspb.Pointer{{Path: "secret/token", Digest: []byte{1}}}}); code(err) != codes.InvalidArgument {
		t.Errorf("a short digest: %v", err)
	}
	w, err := e.versions.CheckWriters(e.ctx, &secretspb.CheckWritersRequest{RepositoryId: repo, PrincipalId: cy,
		Folders: []string{"secret", "secret/prod", "secret/new"}})
	if err != nil || !slices.Equal(w.GetWriter(), []bool{true, false, true}) {
		t.Errorf("cy writes %v, %v", w.GetWriter(), err)
	}
	if _, err := e.versions.CheckWriters(e.ctx, &secretspb.CheckWritersRequest{RepositoryId: repo, PrincipalId: cy, Folders: []string{"other"}}); code(err) != codes.InvalidArgument {
		t.Errorf("a folder outside //secret: %v", err)
	}
}

// What a commit holds is read once; main's head as often as it is kept.
func TestPolicyKeepsWhatACommitHolds(t *testing.T) {
	e := newEnv(t)
	for range 3 {
		if _, err := e.policy.Folder(e.ctx, repo, "secret/prod"); err != nil {
			t.Fatal(err)
		}
	}
	e.main.mu.Lock()
	reads := e.main.reads
	e.main.mu.Unlock()
	if reads != 1 {
		t.Errorf("a commit's files read %d times", reads)
	}
	if _, err := e.policy.Folder(e.ctx, 99, "secret"); code(err) != codes.NotFound {
		t.Errorf("a repository the SCM does not hold: %v", err)
	}
}

// fakeRefs are what the SCM says a repository holds.
type fakeRefs struct {
	refs   map[store.VersionKey]bool
	purged bool
}

func (f *fakeRefs) References(context.Context, int64) (map[store.VersionKey]bool, bool, error) {
	return f.refs, f.purged, nil
}

// A pass keeps what the SCM says is referenced, marks the rest, and destroys what stayed
// unreferenced past the grace; a repository the SCM purged is purged.
func TestCollector(t *testing.T) {
	e := newEnv(t)
	keep := d32(e.write(ada, "secret/token", "kept"))
	drop := d32(e.write(ada, "secret/token", "rotated away"))
	refs := &fakeRefs{refs: map[store.VersionKey]bool{{Path: "secret/token", Digest: keep}: true}}
	c := &service.Collector{Store: e.store, Refs: refs, Grace: time.Nanosecond}
	for range 2 { // marked, then destroyed
		if err := c.Pass(e.ctx, repo); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	if _, _, err := e.store.Open(e.ctx, repo, "secret/token", drop); !errors.Is(err, store.ErrDestroyed) {
		t.Errorf("an unreferenced version after its grace: %v", err)
	}
	if _, v, err := e.store.Open(e.ctx, repo, "secret/token", keep); err != nil || string(v) != "kept" {
		t.Errorf("a referenced version: %q, %v", v, err)
	}
	refs.purged = true
	if err := c.Pass(e.ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.store.Open(e.ctx, repo, "secret/token", keep); err == nil {
		t.Error("a version of a purged repository opened")
	}
	if repos, err := e.store.Repositories(e.ctx, 0, 10); err != nil || len(repos) != 0 {
		t.Errorf("repositories after the purge: %v, %v", repos, err)
	}
}

func TestNextDue(t *testing.T) {
	at := func(h, m int) time.Time { return time.Date(2026, 10, 22, h, m, 0, 0, time.UTC) }
	for now, want := range map[time.Time]time.Time{
		at(9, 59):  at(10, 0),
		at(10, 0):  at(10, 0).AddDate(0, 0, 1),
		at(23, 30): at(10, 0).AddDate(0, 0, 1),
	} {
		if got := service.NextDue(now, 10); !got.Equal(want) {
			t.Errorf("after %v: %v, want %v", now, got, want)
		}
	}
}

// fakeApprovalsClient is ok identity's Approvals, answering one approval.
type fakeApprovalsClient struct {
	identitypb.ApprovalsClient
	a *identitypb.Approval
}

func (f fakeApprovalsClient) GetApproval(context.Context, *identitypb.GetApprovalRequest, ...grpc.CallOption) (*identitypb.Approval, error) {
	return f.a, nil
}

// An approval approves only its own person's request of exactly its scope.
func TestIdentityApprovals(t *testing.T) {
	ctx := context.Background()
	approved := &identitypb.Approval{Id: "a", PrincipalId: ada, Scope: "s", State: identitypb.Approval_STATE_APPROVED}
	for name, tc := range map[string]struct {
		a                   *identitypb.Approval
		principal, scope    string
		wantApproved, wantD bool
	}{
		"approved":         {approved, ada, "s", true, true},
		"another person's": {approved, bea, "s", false, true},
		"another scope's":  {approved, ada, "t", false, true},
		"waiting":          {&identitypb.Approval{PrincipalId: ada, Scope: "s", State: identitypb.Approval_STATE_WAITING}, ada, "s", false, false},
		"declined":         {&identitypb.Approval{PrincipalId: ada, Scope: "s", State: identitypb.Approval_STATE_DECLINED}, ada, "s", false, true},
		"expired":          {&identitypb.Approval{PrincipalId: ada, Scope: "s", State: identitypb.Approval_STATE_EXPIRED}, ada, "s", false, true},
	} {
		got, done, err := service.IdentityApprovals{Client: fakeApprovalsClient{a: tc.a}}.Status(ctx, "a", tc.principal, tc.scope)
		if err != nil || got != tc.wantApproved || done != tc.wantD {
			t.Errorf("%s: approved %v, done %v, %v", name, got, done, err)
		}
	}
}

// auditOps is the audit's decisions, a line each: op, actor, for, outcome.
func auditOps(a []store.AuditEntry) string {
	var out []string
	for _, e := range a {
		out = append(out, e.Op+" "+e.Actor+" "+e.For+" "+e.Outcome)
	}
	return strings.Join(out, "\n")
}
