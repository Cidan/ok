package e2e

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/secrets"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

// secretsFixture is the fixture keeping secrets: //secret, which ops reads and writes, gives the
// post job push its secrets; //app:pusher reads the token in its sandbox and prints it.
func secretsFixture() map[string]*string {
	f := fixture()
	f[".ok/repository.yaml"] = ptr(*f[".ok/repository.yaml"] + "secrets: {}\n")
	f["secret/acl.yaml"] = ptr(aclGiving("push"))
	f["app/BUILD.bazel"] = ptr(*f["app/BUILD.bazel"] + `
script_binary(
    name = "pusher",
    data = ["pusher.txt"],
    script = """set -e
test -n "$OK_SECRETS_DIR"
v=$(cat "$OK_SECRETS_DIR/token")
[ "$v" = hunter2-correct-horse ] || { echo "the token is wrong"; exit 1; }
if touch "$OK_SECRETS_DIR/written" 2>/dev/null; then echo "the secrets are writable"; exit 1; fi
echo "pushed with the token $v"
""",
)
`)
	f["app/pusher.txt"] = ptr("one\n")
	return f
}

// aclGiving is //secret's acl.yaml, giving the post jobs named their secrets.
func aclGiving(jobs ...string) string {
	var runs []string
	for _, j := range jobs {
		runs = append(runs, `"//.ok/workflow/ci.yaml:post:`+j+`"`)
	}
	return "readers: [ops]\nwriters: [ops]\nruns: [" + strings.Join(runs, ", ") + "]\n"
}

// ciWith is the fixture's workflow file with post jobs running //app:pusher, each naming the token.
func ciWith(jobs ...string) *string {
	ci := *fixture()[".ok/workflow/ci.yaml"] + "post:\n"
	for _, j := range jobs {
		ci += "  " + j + ":\n    target: //app:pusher\n    secrets: [//secret/token]\n"
	}
	return &ci
}

// TestPostSecrets runs a post job that reads a secret (docs/secrets.md, "Readers that are not
// people"): the worker reads it from ok secrets with its lease's key — ok secrets asking ok
// automation whose lease it is — and the job's binary finds it in its sandbox, read-only, as
// $OK_SECRETS_DIR; its log names the secret, never its value; and nothing of it stays on the
// worker. A job the folder does not give the secret fails, ok secrets saying why, and its
// submission does not land.
func TestPostSecrets(t *testing.T) {
	postSecrets(t, func(e *env) { e.startWorker("w1", 1) })
}

// TestMicroVMPostSecrets is TestPostSecrets on a worker that runs each run in a microVM of its own
// (docs/hosted.md, "Hosted automation, as built"): the secrets, shared into the guest read-only,
// are its job's sandbox's alone there too.
func TestMicroVMPostSecrets(t *testing.T) {
	postSecrets(t, func(e *env) { e.startMicroVMWorker("vm1", 1) })
}

func postSecrets(t *testing.T, startWorker func(e *env)) {
	e := startWithSecrets(t)
	a := e.person(ada)
	a.jj(".", "create", "keeps", "keeps")
	write(t, a.dir("keeps"), secretsFixture())
	a.jj("keeps", "describe", "-m", "keep secrets")
	repo := e.repository("keeps")
	e.land(repo, uploaded(a.jj("keeps", "cl", "--ready"))[0], 1)
	a.jj("keeps", "sync")
	w, err := secretspb.NewSecretsClient(as(t, e.secrets, ada)).WriteSecret(e.ctx, &secretspb.WriteSecretRequest{RepositoryId: repo,
		Path: "secret/token", Value: []byte("hunter2-correct-horse")})
	if err != nil {
		t.Fatal(err)
	}
	// The token's pointer, and the job naming it.
	e.land(repo, uploadedOn(t, a, "add the token", map[string]*string{"secret/token": ptr(string(w.GetPointer())), ".ok/workflow/ci.yaml": ciWith("push")}), 1)
	a.jj("keeps", "sync")
	startWorker(e)
	before, _ := filepath.Glob("/dev/shm/ok-secrets-*")

	submit := func(msg string, files map[string]*string) *automationpb.Submission {
		t.Helper()
		a.jj("keeps", "new", "main@ok", "-m", msg)
		write(t, a.dir("keeps"), files)
		cl := uploaded(a.jj("keeps", "cl", "--ready"))[0]
		sub, err := e.runs.Submit(e.ctx, &automationpb.SubmitRequest{RepositoryId: repo, ForPersonId: e.adaID,
			Cls: []*automationpb.SubmitRequest_CL{{Number: cl, Patchset: e.cl(repo, cl).GetCurrentPatchset()}}})
		if err != nil {
			t.Fatalf("submitting %s: %v", msg, err)
		}
		deadline := time.Now().Add(10 * time.Minute)
		for {
			s, err := e.runs.GetSubmission(e.ctx, &automationpb.GetSubmissionRequest{Id: sub.GetId()})
			if err != nil {
				t.Fatal(err)
			}
			if s.GetStatus() != automationpb.Submission_STATUS_ACTIVE {
				return s
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s did not finish: %v", msg, s)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	jobsOf := func(s *automationpb.Submission) map[string]*automationpb.Run {
		out := map[string]*automationpb.Run{}
		for _, at := range s.GetAttempts() {
			for _, c := range at.GetCandidates() {
				for _, j := range c.GetJobs() {
					out[j.GetOrigin().GetJob()] = j
				}
			}
		}
		return out
	}
	describe := func(s *automationpb.Submission) string {
		var b strings.Builder
		fmt.Fprintf(&b, "submission %d: %s (%s)\n", s.GetId(), s.GetStatus(), s.GetReason())
		for id, j := range jobsOf(s) {
			fmt.Fprintf(&b, "  job %s: %s (%s)\n%s", id, j.GetStatus(), j.GetReason(), e.logOf(j.GetId()))
		}
		return b.String()
	}

	// A change pusher depends on: post runs push, which reads the token.
	s := submit("change pusher", map[string]*string{"app/pusher.txt": ptr("two\n")})
	if s.GetStatus() != automationpb.Submission_STATUS_LANDED {
		t.Fatalf("the change did not land:\n%s", describe(s))
	}
	push := jobsOf(s)["//.ok/workflow/ci.yaml:post:push"]
	if push.GetStatus() != automationpb.Run_STATUS_PASSED {
		t.Fatalf("push: %s\n%s", push.GetStatus(), describe(s))
	}
	log := e.logOf(push.GetId())
	if !strings.Contains(log, "the job reads 1 secret(s), in $"+secrets.DirEnv+" within its sandbox: //secret/token") ||
		!strings.Contains(log, "pushed with the token [secret //secret/token]") || strings.Contains(log, "hunter2") {
		t.Errorf("push's log:\n%s", log)
	}
	after, _ := filepath.Glob("/dev/shm/ok-secrets-*")
	if left := slices.DeleteFunc(after, func(d string) bool { return slices.Contains(before, d) }); len(left) != 0 {
		t.Errorf("the worker left its secrets in %v", left)
	}

	// A job whose grant main took back fails, and its submission does not land.
	a.jj("keeps", "sync")
	e.land(repo, uploadedOn(t, a, "sneaky", map[string]*string{".ok/workflow/ci.yaml": ciWith("push", "sneaky"),
		"secret/acl.yaml": ptr(aclGiving("push", "sneaky"))}), 1)
	a.jj("keeps", "sync")
	e.land(repo, uploadedOn(t, a, "take it back", map[string]*string{"secret/acl.yaml": ptr(aclGiving("push"))}), 1)
	a.jj("keeps", "sync")
	s = submit("change pusher again", map[string]*string{"app/pusher.txt": ptr("three\n")})
	sneaky := jobsOf(s)["//.ok/workflow/ci.yaml:post:sneaky"]
	if s.GetStatus() == automationpb.Submission_STATUS_LANDED || sneaky.GetStatus() != automationpb.Run_STATUS_FAILED ||
		!strings.Contains(sneaky.GetReason(), "does not give //.ok/workflow/ci.yaml:post:sneaky the folder's secrets") {
		t.Errorf("sneaky: %s (%s)\n%s", sneaky.GetStatus(), sneaky.GetReason(), describe(s))
	}
}

// uploadedOn uploads a change of files on main as a CL, returning its number.
func uploadedOn(t *testing.T, p *person, msg string, files map[string]*string) int64 {
	t.Helper()
	p.jj("keeps", "new", "main@ok", "-m", msg)
	write(t, p.dir("keeps"), files)
	return uploaded(p.jj("keeps", "cl", "--ready"))[0]
}
