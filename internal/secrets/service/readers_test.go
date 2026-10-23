package service_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/service"
	"github.com/Cidan/ok/internal/serve/authn"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

const (
	deployID = "//db/.ok/workflow/db.yaml:deploy:migrate"
	jobID    = "//.ok/workflow/ci.yaml:post:push"
	preID    = "//.ok/workflow/ci.yaml:pre:lint"
)

// fakeRuns are ok automation's runs, each under the lease of one key.
type fakeRuns struct{ runs map[int64]*automationpb.Run }

func (f fakeRuns) CheckRun(_ context.Context, id int64, key []byte) (*automationpb.Run, error) {
	r, ok := f.runs[id]
	if !ok || string(key) != strings.Repeat("k", 32) {
		return nil, status.Error(codes.NotFound, "not running under that lease")
	}
	return r, nil
}

// readersEnv is a repository whose main (m1) and its parent (m0) hold a deploy and a post job
// naming secret/prod/db, which secret/prod/acl.yaml gives both; and c9, a commit off main.
func readersEnv(t *testing.T) (*env, *service.DeploySecrets, *service.RunSecrets, []byte) {
	t.Helper()
	e := newEnv(t)
	d := e.write(ada, "secret/prod/db", "hunter2")
	files := map[string]string{
		".ok/repository.yaml":     settings,
		"secret/acl.yaml":         "readers: [ops]\nwriters: [ops]\n",
		"secret/prod/acl.yaml":    "readers: [ops]\nwriters: [ops]\ndeploys: [\"" + deployID + "\"]\nruns: [\"" + jobID + "\"]\n",
		"secret/prod/db":          string(secrets.Pointer(d32(d))),
		"secret/prod/other":       string(secrets.Pointer(d32(d))),
		"db/.ok/workflow/db.yaml": "deploy:\n  migrate:\n    migrate: [//db:migrate]\n    secrets: [//secret/prod/db]\n",
		".ok/workflow/ci.yaml":    "pre:\n  lint:\n    target: //a:lint\npost:\n  push:\n    target: //a:push\n    secrets: [//secret/prod/db]\n",
	}
	e.main.land(repo, "m0", files)
	e.main.land(repo, "c9", files)
	e.main.land(repo, "m1", files)
	e.main.parents = map[string][]string{"m1": {"m0"}}
	runs := fakeRuns{runs: map[int64]*automationpb.Run{
		1: {Id: 1, RepositoryId: repo, CommitId: []byte("c9"), Origin: &automationpb.Origin{Phase: automationpb.Origin_PHASE_POST, Job: jobID}},
		2: {Id: 2, RepositoryId: repo, CommitId: []byte("c9"), Origin: &automationpb.Origin{Phase: automationpb.Origin_PHASE_PRE, Job: preID}},
	}}
	cfg := service.Config{Store: e.store, Policy: e.policy}
	return e, service.NewDeploySecrets(cfg), service.NewRunSecrets(cfg, runs), d
}

func robot(id string) context.Context {
	return authn.NewContext(context.Background(), authn.Caller{Principal: id + ".robot.example.com", ID: id})
}

// The deployer reads what a deploy's stanza at a commit of main names and main's acl.yaml gives it.
func TestReadDeploySecret(t *testing.T) {
	e, deploys, _, _ := readersEnv(t)
	read := func(commit, deploy, p string) (*secretspb.ReadSecretResponse, error) {
		return deploys.ReadDeploySecret(robot("deploy-id"), &secretspb.ReadDeploySecretRequest{RepositoryId: repo,
			CommitId: []byte(strings.Repeat(commit, 1) + strings.Repeat("\x00", 32-len(commit))), Deploy: deploy, Path: p})
	}
	// Commits are named by 32-byte IDs: the fakes', padded.
	pad := func(c string) string { return c + strings.Repeat("\x00", 32-len(c)) }
	e.main.mu.Lock()
	for _, c := range []string{"m0", "m1", "c9"} {
		e.main.files[pad(c)] = e.main.files[c]
	}
	e.main.heads[repo] = []byte(pad("m1"))
	e.main.parents = map[string][]string{pad("m1"): {pad("m0")}}
	e.main.mu.Unlock()
	for _, c := range []string{"m1", "m0"} {
		if r, err := read(c, deployID, "secret/prod/db"); err != nil || string(r.GetValue()) != "hunter2" {
			t.Errorf("at %s: %q, %v", c, r.GetValue(), err)
		}
	}
	for name, tc := range map[string]struct {
		commit, deploy, path string
		code                 codes.Code
	}{
		"a commit off main":             {"c9", deployID, "secret/prod/db", codes.FailedPrecondition},
		"a secret the stanza names not": {"m1", deployID, "secret/prod/other", codes.PermissionDenied},
		"a deploy the file lacks":       {"m1", "//db/.ok/workflow/db.yaml:deploy:other", "secret/prod/db", codes.FailedPrecondition},
		"not a deploy's identity":       {"m1", jobID, "secret/prod/db", codes.InvalidArgument},
	} {
		if _, err := read(tc.commit, tc.deploy, tc.path); status.Code(err) != tc.code {
			t.Errorf("%s: %v, want %s", name, err, tc.code)
		}
	}
	// A grant removed on main takes effect.
	files := map[string]string{}
	for k, v := range e.main.files[pad("m1")] {
		files[k] = v
	}
	files["secret/prod/acl.yaml"] = "readers: [ops]\nwriters: [ops]\n"
	e.main.land(repo, pad("m2"), files)
	e.main.mu.Lock()
	e.main.parents[pad("m2")] = []string{pad("m1"), pad("m0")}
	e.main.mu.Unlock()
	if _, err := read("m1", deployID, "secret/prod/db"); status.Code(err) != codes.PermissionDenied {
		t.Errorf("a grant removed on main: %v", err)
	}
	if a, err := e.store.AuditOf(e.ctx, repo, 0, 100); err != nil || !strings.Contains(auditOps(a), "deploy-read deploy-id "+deployID+" ok") {
		t.Errorf("the audit: %s, %v", auditOps(a), err)
	}
}

// A worker reads what its post run's stanza names, with the run's lease key; a pre run reads
// nothing, nor does a key of no lease.
func TestReadRunSecret(t *testing.T) {
	_, _, runs, _ := readersEnv(t)
	key := []byte(strings.Repeat("k", 32))
	read := func(run int64, key []byte, p string) (*secretspb.ReadSecretResponse, error) {
		return runs.ReadRunSecret(robot("worker-id"), &secretspb.ReadRunSecretRequest{RepositoryId: repo, RunId: run, RunKey: key, Path: p})
	}
	if r, err := read(1, key, "secret/prod/db"); err != nil || string(r.GetValue()) != "hunter2" {
		t.Fatalf("a post run's secret: %q, %v", r.GetValue(), err)
	}
	for name, tc := range map[string]struct {
		run  int64
		key  []byte
		path string
		code codes.Code
	}{
		"another lease's key":           {1, []byte(strings.Repeat("x", 32)), "secret/prod/db", codes.PermissionDenied},
		"a pre run":                     {2, key, "secret/prod/db", codes.PermissionDenied},
		"a secret the stanza names not": {1, key, "secret/prod/other", codes.PermissionDenied},
	} {
		if _, err := read(tc.run, tc.key, tc.path); status.Code(err) != tc.code {
			t.Errorf("%s: %v, want %s", name, err, tc.code)
		}
	}
	none := service.NewRunSecrets(service.Config{}, nil)
	if _, err := none.ReadRunSecret(robot("w"), &secretspb.ReadRunSecretRequest{RepositoryId: repo, RunId: 1, RunKey: key, Path: "secret/prod/db"}); status.Code(err) != codes.Unavailable {
		t.Errorf("with no ok automation: %v", err)
	}
}
