package store

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/secrets"
)

// fakeSecrets is ok secrets as upload asks it: the versions it keeps, and who writes which folder.
type fakeSecrets struct {
	mu      sync.Mutex
	kept    map[SecretPointer]bool
	writers map[string][]string // principal → folders
}

func (f *fakeSecrets) Kept(_ context.Context, _ int64, ps []SecretPointer) ([]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]bool, len(ps))
	for i, p := range ps {
		out[i] = f.kept[p]
	}
	return out, nil
}

func (f *fakeSecrets) Writers(_ context.Context, _ int64, principal string, folders []string) ([]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]bool, len(folders))
	for i, folder := range folders {
		out[i] = slices.Contains(f.writers[principal], folder)
	}
	return out, nil
}

func digestOf(b byte) secrets.Digest {
	var d secrets.Digest
	for i := range d {
		d[i] = b
	}
	return d
}

func pointer(b byte) string { return string(secrets.Pointer(digestOf(b))) }

// secretsEnv is a repository whose main keeps secrets, under //secret's root folder admins
// read and write, and ok secrets keeping versions 1 to 3 of secret/token, alice its writer.
func secretsEnv(t *testing.T) (*clEnv, *fakeSecrets) {
	t.Helper()
	e := newCLEnv(t)
	sec := &fakeSecrets{kept: map[SecretPointer]bool{}, writers: map[string][]string{alice: {"secret"}}}
	for _, b := range []byte{1, 2, 3} {
		sec.kept[SecretPointer{Path: "secret/token", Digest: digestOf(b)}] = true
	}
	e.s.SetSecrets(sec)
	e.landChange("keep secrets", map[string]string{
		".ok/repository.yaml": "build: none\nsecrets: {}\n",
		"secret/acl.yaml":     "readers: [admins]\nwriters: [admins]\n",
	})
	return e, sec
}

// uploadProblems uploads a change of files on main as principal, and returns the problems an
// INVALID_SECRETS refusal lists — none for an upload taken.
func (e *clEnv) uploadProblems(principal string, files map[string]string) []string {
	e.t.Helper()
	files = withMainFiles(e, files)
	_, err := e.upload(principal, Ready, nil, e.commit(principal, newChange(), e.head(), "a change", files))
	if err == nil {
		return nil
	}
	var rej *UploadRejection
	if !errors.As(err, &rej) || rej.Reason != ReasonInvalidSecrets {
		e.t.Fatalf("upload: %v, want INVALID_SECRETS or nothing", err)
	}
	return rej.Problems
}

// withMainFiles is files over main's settings and root acl.yaml, which every change keeps.
func withMainFiles(e *clEnv, files map[string]string) map[string]string {
	out := map[string]string{".ok/repository.yaml": "build: none\nsecrets: {}\n", "secret/acl.yaml": "readers: [admins]\nwriters: [admins]\n"}
	for p, c := range files {
		if c == "" {
			delete(out, p)
		} else {
			out[p] = c
		}
	}
	return out
}

func mentions(problems []string, want string) bool {
	return slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, want) })
}

// Upload holds a commit's //secret to its rules, asking ok secrets what only it knows.
func TestUploadSecrets(t *testing.T) {
	e, sec := secretsEnv(t)
	if ps := e.uploadProblems(alice, map[string]string{"secret/token": pointer(1)}); ps != nil {
		t.Fatalf("a writer's pointer to a kept version: %v", ps)
	}
	for name, tc := range map[string]struct {
		who   string
		files map[string]string
		want  string
	}{
		"a version not kept":           {alice, map[string]string{"secret/token": pointer(9)}, "names a version ok secrets does not keep"},
		"not a writer":                 {bob, map[string]string{"secret/token": pointer(2)}, "only a writer of secret changes its secrets, and bob is none"},
		"a folder without an acl.yaml": {alice, map[string]string{"secret/new/x": pointer(3)}, "secret/new/: has no acl.yaml"},
		"an unknown set":               {alice, map[string]string{"secret/acl.yaml": "readers: [nobody]\nwriters: [admins]\n"}, "nobody.acl.example.com is no ACL or team"},
		"no stanza":                    {alice, map[string]string{".ok/repository.yaml": "build: none\n", "secret/token": pointer(1)}, "keeps none"},
	} {
		if ps := e.uploadProblems(tc.who, tc.files); !mentions(ps, tc.want) {
			t.Errorf("%s: problems %v, want one mentioning %q", name, ps, tc.want)
		}
	}
	// Deleting a secret is a writer's too.
	sec.mu.Lock()
	sec.kept[SecretPointer{Path: "secret/old", Digest: digestOf(1)}] = true
	sec.mu.Unlock()
	e.landChange("a secret", withMainFiles(e, map[string]string{"secret/old": pointer(1)}))
	if ps := e.uploadProblems(bob, map[string]string{"secret/old": ""}); !mentions(ps, "secret/old: only a writer") {
		t.Errorf("bob deleting a secret: %v", ps)
	}
	// OWNERS files there are OWNERS', and no secret.
	if ps := e.uploadProblems(bob, map[string]string{"secret/OWNERS": "user: alice\n", "secret/old": pointer(1)}); ps != nil {
		t.Errorf("an OWNERS file in //secret: %v", ps)
	}
	// Without ok secrets, no secret changes.
	e.s.SetSecrets(nil)
	if ps := e.uploadProblems(alice, map[string]string{"secret/token": pointer(1)}); !mentions(ps, "runs no ok secrets") {
		t.Errorf("without ok secrets: %v", ps)
	}
}

// Where neither main nor the commit keeps secrets, //secret is an ordinary folder; a commit that
// starts keeping them is held to the rules — a value under it refused as it is published.
func TestUploadSecretsOrdinaryFolder(t *testing.T) {
	e := newCLEnv(t)
	if n := e.open(e.commit(alice, newChange(), e.head(), "notes", map[string]string{"secret/README": "not a secret\n"})); n == 0 {
		t.Fatal("no CL")
	}
	b := newBuilder(t)
	b.change(newChange(), e.head(), "start keeping", map[string]string{".ok/repository.yaml": "build: none\nsecrets: {}\n",
		"secret/acl.yaml": "readers: [admins]\nwriters: [admins]\n", "secret/password": "hunter2\n"})
	if _, err := publish(context.Background(), e.s, e.repo, alice, b, nil); !errors.Is(err, ErrPrecondition) {
		t.Errorf("a commit starting to keep secrets with a value: %v", err)
	}
	e.s.SetSecrets(&fakeSecrets{})
	_, err := e.upload(alice, Ready, nil, e.commit(alice, newChange(), e.head(), "start keeping, wrongly",
		map[string]string{".ok/repository.yaml": "build: none\nsecrets: {}\n", "secret/x/token": pointer(1)}))
	var rej *UploadRejection
	if !errors.As(err, &rej) || rej.Reason != ReasonInvalidSecrets || !mentions(rej.Problems, "secret/: has no acl.yaml") {
		t.Errorf("a commit starting to keep secrets that breaks the rules: %v", err)
	}
}

// A publish, and a checkpoint, carrying a value under //secret are refused in a repository that
// keeps secrets, or in a tree whose settings start keeping them; a client that does not guard
// secrets is refused there; elsewhere both pass.
func TestPublishRefusesValues(t *testing.T) {
	ctx := context.Background()
	e, _ := secretsEnv(t)
	b := newBuilder(t)
	b.change(newChange(), e.head(), "a value", withMainFiles(e, map[string]string{"secret/password": "hunter2\n"}))
	if _, err := publish(ctx, e.s, e.repo, alice, b, nil); !errors.Is(err, ErrPrecondition) || !strings.Contains(err.Error(), "secret/password: holds a value") {
		t.Fatalf("publishing a value: %v", err)
	}
	b = newBuilder(t)
	b.change(newChange(), e.head(), "a pointer", withMainFiles(e, map[string]string{"secret/token": pointer(1)}))
	if _, err := publish(ctx, e.s, e.repo, alice, b, nil); err != nil {
		t.Errorf("publishing a pointer: %v", err)
	}
	if err := e.s.RequireGuard(ctx, e.repo); !errors.Is(err, ErrPrecondition) {
		t.Errorf("a client that does not guard secrets: %v", err)
	}

	plain := newCLEnv(t)
	if err := plain.s.RequireGuard(ctx, plain.repo); err != nil {
		t.Errorf("a repository that keeps none: %v", err)
	}
	b = newBuilder(t)
	b.change(newChange(), plain.head(), "an ordinary folder", map[string]string{"secret/password": "hunter2\n"})
	if _, err := publish(ctx, plain.s, plain.repo, alice, b, nil); err != nil {
		t.Errorf("an ordinary folder: %v", err)
	}
	b = newBuilder(t)
	b.change(newChange(), plain.head(), "starts keeping", map[string]string{".ok/repository.yaml": "build: none\nsecrets: {}\n", "secret/password": "hunter2\n"})
	if _, err := publish(ctx, plain.s, plain.repo, alice, b, nil); !errors.Is(err, ErrPrecondition) {
		t.Errorf("a tree starting to keep secrets with a value: %v", err)
	}

	// A checkpoint's tree is held to the same.
	b = newBuilder(t)
	bad := b.flat(withMainFiles(e, map[string]string{"secret/password": "hunter2\n"}))
	b.commit(bad, "base", encoding.RootCommitID)
	if _, err := publish(ctx, e.s, e.repo, alice, b, nil); err == nil {
		t.Fatal("published a value")
	}
	b = newBuilder(t)
	good := b.flat(withMainFiles(e, map[string]string{"secret/token": pointer(1)}))
	base := b.commit(good, "base", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	if _, err := e.s.CreateWorkspace(ctx, alice, e.repo, "w"); err != nil {
		t.Fatal(err)
	}
	a, err := e.s.Attach(ctx, alice, e.repo, "w", "m1", false, time.Now())
	if err != nil || !a.Granted {
		t.Fatal(a, err)
	}
	if _, err := e.s.RecordCheckpoint(ctx, alice, a.Lease, 0, []encoding.ID{good}, base); err != nil {
		t.Errorf("a checkpoint of pointers: %v", err)
	}
}

// Rolling back a change to a secret is a writer's.
func TestRollbackSecrets(t *testing.T) {
	e, sec := secretsEnv(t)
	n := e.landChange("rotate", withMainFiles(e, map[string]string{"secret/token": pointer(1)}))
	_, err := e.rollback(n) // bob, who writes nothing
	if rej := rollbackRejection(t, err, RollbackNotWriter); !slices.Equal(rej.Paths, []string{"secret/token"}) {
		t.Errorf("refused for %v", rej.Paths)
	}
	sec.mu.Lock()
	sec.writers[bob] = []string{"secret"}
	sec.mu.Unlock()
	e.mustRollback(n)
}

// The pointers a repository holds: main's, an open or draft CL's current patchset's, and a
// workspace's latest checkpoint's — not an abandoned CL's; and none, for a repository the SCM no
// longer holds.
func TestSecretReferences(t *testing.T) {
	ctx := context.Background()
	e, sec := secretsEnv(t)
	sec.mu.Lock()
	for _, b := range []byte{4, 5, 6, 7} {
		sec.kept[SecretPointer{Path: "secret/token", Digest: digestOf(b)}] = true
	}
	sec.mu.Unlock()
	e.landChange("main's", withMainFiles(e, map[string]string{"secret/token": pointer(1)}))
	e.open(e.commit(alice, newChange(), e.head(), "open", withMainFiles(e, map[string]string{"secret/token": pointer(2)})))
	if _, err := e.upload(alice, WIP, nil, e.commit(alice, newChange(), e.head(), "draft", withMainFiles(e, map[string]string{"secret/token": pointer(4)}))); err != nil {
		t.Fatal(err)
	}
	abandoned := e.open(e.commit(alice, newChange(), e.head(), "abandoned", withMainFiles(e, map[string]string{"secret/token": pointer(5)})))
	e.transition(abandoned, Abandon)
	b := newBuilder(t)
	ws := b.flat(withMainFiles(e, map[string]string{"secret/token": pointer(3)}))
	base := b.commit(ws, "base", encoding.RootCommitID)
	mustPublish(t, e.s, e.repo, alice, b, nil)
	if _, err := e.s.CreateWorkspace(ctx, alice, e.repo, "w"); err != nil {
		t.Fatal(err)
	}
	a, err := e.s.Attach(ctx, alice, e.repo, "w", "m1", false, time.Now())
	if err != nil || !a.Granted {
		t.Fatal(a, err)
	}
	if _, err := e.s.RecordCheckpoint(ctx, alice, a.Lease, 0, []encoding.ID{ws}, base); err != nil {
		t.Fatal(err)
	}
	refs, err := e.s.SecretReferences(ctx, e.repo)
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	for _, r := range refs {
		if r.Path != "secret/token" {
			t.Errorf("a reference at %s", r.Path)
		}
		got = append(got, r.Digest[0])
	}
	if want := []byte{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("references %v, want %v", got, want)
	}
	if err := e.s.DeleteRepository(ctx, e.repo); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.SecretReferences(ctx, e.repo); !errors.Is(err, ErrNotFound) {
		t.Errorf("a repository the SCM no longer holds: %v", err)
	}
}

// A stanza in a workflow file a commit edits names only secrets the commit holds, in folders whose
// acl.yaml there gives the deploy or job them; in a repository keeping none, it names none.
func TestUploadSecretStanzas(t *testing.T) {
	e, _ := secretsEnv(t)
	const job = "post:\n  push:\n    target: //a:push\n    secrets: [//secret/token]\n"
	const deploy = "deploy:\n  db:\n    migrate: [//db:migrate]\n    secrets: [//secret/token]\n"
	granting := "readers: [admins]\nwriters: [admins]\nruns: [\"//.ok/workflow/ci.yaml:post:push\"]\ndeploys: [\"//.ok/workflow/cd.yaml:deploy:db\"]\n"
	if ps := e.uploadProblems(alice, map[string]string{"secret/token": pointer(1), "secret/acl.yaml": granting,
		".ok/workflow/ci.yaml": job, ".ok/workflow/cd.yaml": deploy}); ps != nil {
		t.Fatalf("stanzas the folder gives their secrets: %v", ps)
	}
	for name, tc := range map[string]struct {
		files map[string]string
		want  string
	}{
		"a job not given": {map[string]string{"secret/token": pointer(1), ".ok/workflow/ci.yaml": job},
			`.ok/workflow/ci.yaml:4: job push names //secret/token, and secret/acl.yaml does not give it the folder's secrets (runs: ["//.ok/workflow/ci.yaml:post:push"])`},
		"a deploy not given": {map[string]string{"secret/token": pointer(1), ".ok/workflow/cd.yaml": deploy},
			`deploy db names //secret/token, and secret/acl.yaml does not give it the folder's secrets (deploys:`},
		"a secret not held": {map[string]string{"secret/acl.yaml": granting, ".ok/workflow/ci.yaml": job},
			"job push names //secret/token, which the commit holds no secret at"},
	} {
		if ps := e.uploadProblems(alice, tc.files); !mentions(ps, tc.want) {
			t.Errorf("%s: problems %v, want one mentioning %q", name, ps, tc.want)
		}
	}

	plain := newCLEnv(t)
	_, err := plain.upload(alice, Ready, nil, plain.commit(alice, newChange(), plain.head(), "a job", map[string]string{".ok/workflow/ci.yaml": job}))
	var rej *UploadRejection
	if !errors.As(err, &rej) || rej.Reason != ReasonInvalidSecrets || !mentions(rej.Problems, "and the repository keeps no secrets") {
		t.Errorf("a stanza where no secrets are kept: %v", err)
	}
}
