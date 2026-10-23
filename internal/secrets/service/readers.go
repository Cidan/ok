package service

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/workflow"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

// Readers that are not people (docs/secrets.md): a deploy's migrations, for a deployment of a
// commit of main, and a post job, for its run — each reading what its stanza at that commit names
// and its folder's acl.yaml on main gives it, with no person present and no unseal.

// Runs asks ok automation about a run: ok automation's SecretRuns.
type Runs interface {
	// CheckRun is the run, while it runs under the lease the key was minted for; NOT_FOUND
	// otherwise.
	CheckRun(ctx context.Context, run int64, key []byte) (*automationpb.Run, error)
}

// AutomationRuns asks ok automation's SecretRuns.
type AutomationRuns struct{ Client automationpb.SecretRunsClient }

func (a AutomationRuns) CheckRun(ctx context.Context, run int64, key []byte) (*automationpb.Run, error) {
	return a.Client.CheckRun(ctx, &automationpb.CheckRunRequest{RunId: run, RunKey: key})
}

// maxWorkflowFile bounds the workflow file a reader's stanza is read from, as upload does.
const maxWorkflowFile = 1 << 20

// grantee is a deploy's or a post job's identity, parsed: its workflow file and name.
type grantee struct {
	id, file, name string
	phase          workflow.Phase
}

func parseGrantee(id string, phase workflow.Phase) (grantee, bool) {
	file, name, ok := strings.Cut(strings.TrimPrefix(id, "//"), ":"+string(phase)+":")
	if !ok || !strings.HasPrefix(id, "//") || !workflow.IsWorkflowFile(file) || name == "" {
		return grantee{}, false
	}
	return grantee{id: id, file: file, name: name, phase: phase}, true
}

// granted reads a version for a reader that is not a person: the commit's stanza for the grantee
// must name path, the acl.yaml for its folder at head — main's, read just now, so a grant that just
// landed is seen — must give it the grantee, and the commit's pointer at path names the version
// opened. It returns the value, or a status saying why not.
func (c *core) granted(ctx context.Context, repo int64, commit, head []byte, g grantee, p string, op string) (*secretspb.ReadSecretResponse, error) {
	actor := ""
	if a, ok := authn.FromContext(ctx); ok {
		actor = a.ID
	}
	e := store.AuditEntry{Repository: repo, Actor: actor, For: g.id, Op: op, Path: p}
	files, err := c.Policy.main.Files(ctx, repo, commit, []string{g.file}, maxWorkflowFile)
	if err != nil {
		return nil, toStatus(err)
	}
	data, ok := files[g.file]
	if !ok {
		return nil, c.refuse(ctx, e, codes.FailedPrecondition, "the commit has no %s, which would declare %s", g.file, g.id)
	}
	wf, _ := workflow.Parse(g.file, data)
	var named []workflow.Secret
	found := false
	switch g.phase {
	case workflow.DeployPhase:
		for _, d := range wf.Deploys {
			if d.Name == g.name {
				found, named = true, d.Secrets
			}
		}
	default:
		for _, j := range wf.Jobs {
			if j.Phase == g.phase && j.Name == g.name {
				found, named = true, j.Secrets
			}
		}
	}
	if !found {
		return nil, c.refuse(ctx, e, codes.FailedPrecondition, "%s declares no %s at the commit", g.file, g.id)
	}
	if !nameSecret(named, p) {
		return nil, c.refuse(ctx, e, codes.PermissionDenied, "%s's stanza at the commit names no secret //%s (secrets:)", g.id, p)
	}
	f, err := c.Policy.FolderAt(ctx, repo, head, secrets.Folder(p))
	if err != nil {
		return nil, toStatus(err)
	}
	if !f.KeepsSecrets() {
		return nil, status.Error(codes.FailedPrecondition, noSecrets)
	}
	grants := f.Access.GrantsDeploy(g.id)
	what := "deploys"
	if g.phase != workflow.DeployPhase {
		grants, what = f.Access.GrantsRun(g.id), "runs"
	}
	if !f.Present || !grants {
		return nil, c.refuse(ctx, e, codes.PermissionDenied, "main's %s/%s does not give %s the folder's secrets (%s:)", f.Path, secrets.ACLFile, g.id, what)
	}
	ptr, err := c.Policy.main.Files(ctx, repo, commit, []string{p}, secrets.PointerLen)
	if err != nil {
		return nil, c.refuse(ctx, e, codes.FailedPrecondition, "//%s at the commit is no pointer", p)
	}
	held, ok := ptr[p]
	if !ok {
		return nil, c.refuse(ctx, e, codes.FailedPrecondition, "the commit holds no secret //%s", p)
	}
	d, err := secrets.ParsePointer(held)
	if err != nil {
		return nil, c.refuse(ctx, e, codes.FailedPrecondition, "//%s at the commit is no pointer", p)
	}
	e.Digest = d.Short()
	o, v, err := c.Store.Open(ctx, repo, p, d)
	if err != nil {
		return nil, c.refuse(ctx, e, codes.FailedPrecondition, "//%s's version %s: %v", p, d.Short(), err)
	}
	e.Outcome = "ok"
	if err := c.decide(ctx, e); err != nil {
		clear(v)
		return nil, toStatus(err)
	}
	return &secretspb.ReadSecretResponse{Value: v, Opening: o[:]}, nil
}

func nameSecret(named []workflow.Secret, p string) bool {
	for _, s := range named {
		if s.Path == p {
			return true
		}
	}
	return false
}

// ─── DeploySecrets: the deployer's ───

// DeploySecrets serves the deployer a deploy's secrets.
type DeploySecrets struct {
	secretspb.UnimplementedDeploySecretsServer
	*core
}

// NewDeploySecrets is the deployer's service.
func NewDeploySecrets(cfg Config) *DeploySecrets { return &DeploySecrets{core: newCore(cfg)} }

func (s *DeploySecrets) ReadDeploySecret(ctx context.Context, req *secretspb.ReadDeploySecretRequest) (*secretspb.ReadSecretResponse, error) {
	repo, p := req.GetRepositoryId(), req.GetPath()
	if err := validPath(p); err != nil {
		return nil, err
	}
	g, ok := parseGrantee(req.GetDeploy(), workflow.DeployPhase)
	if !ok || !secrets.ValidDeploy(req.GetDeploy()) {
		return nil, status.Errorf(codes.InvalidArgument, "%q is no deploy's identity, //<dir>/.ok/workflow/<file>.yaml:deploy:<name>", req.GetDeploy())
	}
	if len(req.GetCommitId()) != 32 {
		return nil, status.Error(codes.InvalidArgument, "a commit's ID is 32 bytes")
	}
	// main's head now, not as kept: a deployment of what just landed is of main.
	head, err := s.Policy.main.Head(ctx, repo)
	if err != nil {
		return nil, toStatus(err)
	}
	onMain := bytes.Equal(head, req.GetCommitId())
	if !onMain {
		if onMain, err = s.Policy.main.IsAncestor(ctx, repo, req.GetCommitId(), head); err != nil {
			return nil, toStatus(err)
		}
	}
	if !onMain {
		return nil, s.refuse(ctx, store.AuditEntry{Repository: repo, For: g.id, Op: "deploy-read", Path: p}, codes.FailedPrecondition,
			"commit %x is not on main: a deploy reads secrets for main's commits alone", req.GetCommitId()[:6])
	}
	return s.granted(ctx, repo, req.GetCommitId(), head, g, p, "deploy-read")
}

// ─── RunSecrets: workers' ───

// RunSecrets serves a worker its post run's secrets.
type RunSecrets struct {
	secretspb.UnimplementedRunSecretsServer
	*core
	runs Runs
}

// NewRunSecrets is workers' service, asking ok automation about runs through runs (nil: it refuses
// every request).
func NewRunSecrets(cfg Config, runs Runs) *RunSecrets {
	return &RunSecrets{core: newCore(cfg), runs: runs}
}

func (s *RunSecrets) ReadRunSecret(ctx context.Context, req *secretspb.ReadRunSecretRequest) (*secretspb.ReadSecretResponse, error) {
	repo, p := req.GetRepositoryId(), req.GetPath()
	if err := validPath(p); err != nil {
		return nil, err
	}
	if s.runs == nil {
		return nil, status.Error(codes.Unavailable, "this ok secrets reaches no ok automation (services.automation), which says whose a run is")
	}
	run, err := s.runs.CheckRun(ctx, req.GetRunId(), req.GetRunKey())
	if status.Code(err) == codes.NotFound || status.Code(err) == codes.InvalidArgument {
		return nil, s.refuse(ctx, store.AuditEntry{Repository: repo, Op: "run-read", Path: p, For: fmt.Sprintf("run %d", req.GetRunId())},
			codes.PermissionDenied, "run %d is not running under that lease", req.GetRunId())
	}
	if err != nil {
		return nil, toStatus(err)
	}
	job := run.GetOrigin().GetJob()
	g, ok := parseGrantee(job, workflow.Post)
	switch {
	case run.GetRepositoryId() != repo:
		return nil, status.Errorf(codes.InvalidArgument, "run %d is not repository %d's", run.GetId(), repo)
	case run.GetOrigin().GetPhase() != automationpb.Origin_PHASE_POST || !ok || !secrets.ValidRun(job):
		// Pre never reads a secret: it runs a CL's unreviewed code.
		return nil, s.refuse(ctx, store.AuditEntry{Repository: repo, Op: "run-read", Path: p, For: job}, codes.PermissionDenied,
			"run %d is no post job's: only post jobs, of submitted changes, read secrets", run.GetId())
	}
	head, err := s.Policy.main.Head(ctx, repo)
	if err != nil {
		return nil, toStatus(err)
	}
	return s.granted(ctx, repo, run.GetCommitId(), head, g, p, "run-read")
}
