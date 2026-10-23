package worker

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

// The log never holds a value, however its writes split it, and holds back only what may begin
// one: less than the longest value.
func TestLogRedacts(t *testing.T) {
	values := map[string][]byte{"secret/prod/db": []byte("hunter2-correct"), "secret/short": []byte("abc"), "secret/inner": []byte("correct")}
	for _, chunk := range []int{1, 2, 3, 5, 7, 64} {
		l := newLogStream(nil, 1, "w")
		l.hide(map[string][]byte{"secret/prod/db": bytes.Clone(values["secret/prod/db"]), "secret/short": bytes.Clone(values["secret/short"]),
			"secret/inner": bytes.Clone(values["secret/inner"])})
		text := []byte("connecting with hunter2-correct; then correct; abc stays; hunter2-correcthunter2-correct.\n")
		for i := 0; i < len(text); i += chunk {
			if n, err := l.Write(text[i:min(i+chunk, len(text))]); err != nil || n != min(chunk, len(text)-i) {
				t.Fatalf("write: %d, %v", n, err)
			}
			if len(l.held) > len("hunter2-correct")-1 {
				t.Errorf("chunk %d: %d bytes held back", chunk, len(l.held))
			}
		}
		l.reveal()
		got := string(l.pending)
		want := "connecting with [secret //secret/prod/db]; then [secret //secret/inner]; abc stays; [secret //secret/prod/db][secret //secret/prod/db].\n"
		if got != want {
			t.Errorf("chunk %d: %q, want %q", chunk, got, want)
		}
		l.Write([]byte("hunter2-correct after the run\n"))
		if !strings.HasSuffix(string(l.pending), "hunter2-correct after the run\n") {
			t.Error("the log still redacts after the run")
		}
	}
}

// A post run's secrets are visible in its sandbox alone, read-only, after its fresh /dev.
func TestSandboxSecrets(t *testing.T) {
	got := strings.Join(sandbox("/usr/bin/bwrap", nil, "/s/run-1", "", "/dev/shm/ok-secrets-1"), " ")
	dev, bind := strings.Index(got, "--dev /dev"), strings.Index(got, "--ro-bind /dev/shm/ok-secrets-1 /dev/shm/ok-secrets-1")
	if dev < 0 || bind < dev || !strings.Contains(got, "--setenv OK_SECRETS_DIR /dev/shm/ok-secrets-1") {
		t.Errorf("sandbox %q", got)
	}
	if strings.Contains(strings.Join(sandbox("/usr/bin/bwrap", nil, "/s", "", ""), " "), "OK_SECRETS_DIR") {
		t.Error("a run without secrets was told of them")
	}
}

// fakeRunSecrets is ok secrets: hunter2 at secret/prod/db for run 7's lease, refusing the rest.
type fakeRunSecrets struct {
	asks []*secretspb.ReadRunSecretRequest
}

func (f *fakeRunSecrets) ReadRunSecret(_ context.Context, in *secretspb.ReadRunSecretRequest, _ ...grpc.CallOption) (*secretspb.ReadSecretResponse, error) {
	f.asks = append(f.asks, in)
	if in.GetRunId() != 7 || string(in.GetRunKey()) != "key" {
		return nil, status.Error(codes.PermissionDenied, "run 7 is not running under that lease")
	}
	if in.GetPath() != "secret/prod/db" {
		return nil, status.Errorf(codes.PermissionDenied, "main's secret/acl.yaml does not give the job //%s", in.GetPath())
	}
	return &secretspb.ReadSecretResponse{Value: []byte("hunter2")}, nil
}

// A post run job reads what its stanza at the run's commit names, with its lease's key; other
// runs read nothing; a test naming secrets, ok secrets' refusal, and no ok secrets fail the run.
func TestRunSecrets(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, ".ok/workflow"), 0o755)
	os.WriteFile(filepath.Join(dir, ".ok/workflow/ci.yaml"), []byte("post:\n  push:\n    target: //a:push\n    secrets: [//secret/prod/db]\n"+
		"  other:\n    target: //a:other\n    secrets: [//secret/other]\n  plain:\n    target: //a:plain\n"), 0o644)
	sec := &fakeRunSecrets{}
	cfg := Config{RunSecrets: sec, checkout: fakeCheckout{dir: dir}, runKey: []byte("key")}
	log := newLogStream(nil, 7, "w")
	run := func(job string, kind automationpb.Job_Kind, phase automationpb.Origin_Phase) *automationpb.Run {
		return &automationpb.Run{Id: 7, RepositoryId: 3, Job: &automationpb.Job{Kind: kind}, Origin: &automationpb.Origin{Phase: phase, Job: job}}
	}
	v, refused, err := runSecrets(ctx, cfg, run("//.ok/workflow/ci.yaml:post:push", automationpb.Job_KIND_RUN, automationpb.Origin_PHASE_POST), log)
	if err != nil || refused != "" || string(v["secret/prod/db"]) != "hunter2" || len(v) != 1 {
		t.Fatalf("the job's secrets: %v, %q, %v", v, refused, err)
	}
	if a := sec.asks[0]; a.GetRepositoryId() != 3 || a.GetRunId() != 7 || string(a.GetRunKey()) != "key" {
		t.Errorf("asked %v", a)
	}
	for name, tc := range map[string]struct {
		run  *automationpb.Run
		cfg  Config
		want string
	}{
		"a job naming none":  {run("//.ok/workflow/ci.yaml:post:plain", automationpb.Job_KIND_RUN, automationpb.Origin_PHASE_POST), cfg, ""},
		"a pre run":          {run("//.ok/workflow/ci.yaml:pre:push", automationpb.Job_KIND_RUN, automationpb.Origin_PHASE_PRE), cfg, ""},
		"a test":             {run("//.ok/workflow/ci.yaml:post:push", automationpb.Job_KIND_TEST, automationpb.Origin_PHASE_POST), cfg, "its target is no binary"},
		"a secret not given": {run("//.ok/workflow/ci.yaml:post:other", automationpb.Job_KIND_RUN, automationpb.Origin_PHASE_POST), cfg, "does not give the job"},
		"no ok secrets":      {run("//.ok/workflow/ci.yaml:post:push", automationpb.Job_KIND_RUN, automationpb.Origin_PHASE_POST), Config{checkout: cfg.checkout}, "reaches no ok secrets"},
	} {
		v, refused, err := runSecrets(ctx, tc.cfg, tc.run, log)
		if err != nil || len(v) != 0 || !strings.Contains(refused, tc.want) || (tc.want == "") != (refused == "") {
			t.Errorf("%s: %v, %q, %v", name, v, refused, err)
		}
	}
}
