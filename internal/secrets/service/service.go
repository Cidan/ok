package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/secrets/secretstore"
	"github.com/Cidan/ok/internal/secrets/store"
	"github.com/Cidan/ok/internal/serve/authn"
	secretspb "github.com/Cidan/ok/proto/ok/secrets/v1"
)

// Approvals asks people to approve with a passkey: ok identity's (docs/secrets.md, "Unsealing").
type Approvals interface {
	// Start asks a person to approve what scope says, described to them as description; it
	// returns the approval's ID, the page they approve it at, and when it expires.
	Start(ctx context.Context, principal, scope, description string) (id, url string, expires time.Time, err error)
	// Status says whether the person approved an approval of exactly scope: done is false while
	// they have not answered.
	Status(ctx context.Context, id, principal, scope string) (approved, done bool, err error)
}

// Config is the services' configuration.
type Config struct {
	Store  *store.Store
	Policy *Policy
	// Approvals, when set, asks for passkeys; nil, folders that ask for one cannot be unsealed.
	Approvals Approvals
	// UnsealTTL is how long an unseal waits for its read: store.DefaultUnsealTTL unless set.
	UnsealTTL time.Duration
	Log       *slog.Logger
}

// core is what every service shares.
type core struct{ Config }

func newCore(cfg Config) *core {
	if cfg.UnsealTTL <= 0 {
		cfg.UnsealTTL = store.DefaultUnsealTTL
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &core{cfg}
}

// person is a request's caller, when a person made it with their device credential: a personal
// token narrows a person to what it names, which no secret is.
func person(ctx context.Context) (authn.Caller, error) {
	c, ok := authn.FromContext(ctx)
	switch {
	case !ok:
		return c, status.Error(codes.Unauthenticated, "ok secrets needs a caller: sign in (jj auth login)")
	case !c.Human:
		return c, status.Error(codes.PermissionDenied, "ok secrets serves people their secrets; a robot reads only what its deploy or post job is given")
	case c.Via == authn.ViaPersonalToken || c.Token != nil:
		return c, status.Error(codes.PermissionDenied, "secrets are read and written with a device credential (jj auth login), not a personal token")
	}
	return c, nil
}

// decide audits a decision and logs it.
func (c *core) decide(ctx context.Context, e store.AuditEntry) error {
	c.Log.Info("ok secrets: secret", "op", e.Op, "repository", e.Repository, "path", e.Path, "digest", e.Digest,
		"actor", e.Actor, "for", e.For, "outcome", e.Outcome)
	return c.Store.Audit(ctx, e)
}

// refuse audits a refusal and returns it, as a status with code.
func (c *core) refuse(ctx context.Context, e store.AuditEntry, code codes.Code, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	e.Outcome = msg
	if err := c.decide(ctx, e); err != nil {
		return err
	}
	return status.Error(code, msg)
}

// toStatus is a store's error as a status.
func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrDestroyed), errors.Is(err, store.ErrNotUnsealed), errors.Is(err, store.ErrPurged),
		errors.Is(err, secretstore.ErrNoStore):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, secretstore.ErrTooLarge):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err // the SCM's, identity's, or a move's: a repository another cell holds, frozen
	}
	return status.Error(codes.Internal, err.Error())
}

func validPath(p string) error {
	if !secrets.ValidPath(p) {
		return status.Errorf(codes.InvalidArgument, "%q is not a secret's path (secret/<folder>/…/<name>)", p)
	}
	return nil
}

func digestOf(b []byte) (secrets.Digest, error) {
	var d secrets.Digest
	if len(b) != len(d) {
		return d, status.Errorf(codes.InvalidArgument, "a digest is %d bytes, not %d", len(d), len(b))
	}
	copy(d[:], b)
	return d, nil
}

// ─── Secrets: people's ───

// Secrets serves people their secrets.
type Secrets struct {
	secretspb.UnimplementedSecretsServer
	*core
}

// NewSecrets is people's service.
func NewSecrets(cfg Config) *Secrets { return &Secrets{core: newCore(cfg)} }

// noSecrets is the answer for a repository that keeps none.
const noSecrets = "the repository keeps no secrets: its .ok/repository.yaml on main has no secrets stanza (docs/secrets.md)"

func (s *Secrets) CheckAccess(ctx context.Context, req *secretspb.CheckAccessRequest) (*secretspb.CheckAccessResponse, error) {
	c, err := person(ctx)
	if err != nil {
		return nil, err
	}
	// A secret's folder, or — for a client asking whether the repository keeps secrets at all —
	// the tree's root.
	folder := secrets.Dir
	if req.GetPath() != secrets.Dir {
		if err := validPath(req.GetPath()); err != nil {
			return nil, err
		}
		folder = secrets.Folder(req.GetPath())
	}
	f, err := s.Policy.Folder(ctx, req.GetRepositoryId(), folder)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &secretspb.CheckAccessResponse{KeepsSecrets: f.KeepsSecrets(), Passkey: f.Access.Unseal == secrets.UnsealPasskey}
	if !f.KeepsSecrets() {
		out.WhyNotRead, out.WhyNotWrite = noSecrets, noSecrets
		return out, nil
	}
	if out.Read, err = s.Policy.Reader(ctx, f, c.ID); err != nil {
		return nil, toStatus(err)
	}
	if out.Write, err = s.Policy.Writer(ctx, f, c.ID); err != nil {
		return nil, toStatus(err)
	}
	if !out.Read {
		out.WhyNotRead = whyNot(f, "readers")
	}
	if !out.Write {
		out.WhyNotWrite = whyNot(f, "writers")
	}
	return out, nil
}

// whyNot says why someone is not among a folder's readers or writers.
func whyNot(f Folder, list string) string {
	if !f.Present {
		return fmt.Sprintf("main has no %s/%s yet: no one reads a folder still in review", f.Path, secrets.ACLFile)
	}
	sets := f.Access.Readers
	if list == "writers" {
		sets = f.Access.Writers
	}
	if len(sets) == 0 {
		return fmt.Sprintf("%s/%s names no %s", f.Path, secrets.ACLFile, list)
	}
	return fmt.Sprintf("you are in none of %s/%s's %s: %s", f.Path, secrets.ACLFile, list, strings.Join(sets, ", "))
}

func (s *Secrets) WriteSecret(ctx context.Context, req *secretspb.WriteSecretRequest) (*secretspb.WriteSecretResponse, error) {
	c, err := person(ctx)
	if err != nil {
		return nil, err
	}
	repo, p := req.GetRepositoryId(), req.GetPath()
	if err := validPath(p); err != nil {
		return nil, err
	}
	if len(req.GetValue()) > secrets.MaxValue {
		return nil, status.Errorf(codes.InvalidArgument, "a value of %d bytes: a secret holds at most %d", len(req.GetValue()), secrets.MaxValue)
	}
	e := store.AuditEntry{Repository: repo, Actor: c.ID, Op: "write", Path: p}
	f, err := s.Policy.Folder(ctx, repo, secrets.Folder(p))
	if err != nil {
		return nil, toStatus(err)
	}
	if !f.KeepsSecrets() {
		return nil, status.Error(codes.FailedPrecondition, noSecrets)
	}
	writer, err := s.Policy.Writer(ctx, f, c.ID)
	if err != nil {
		return nil, toStatus(err)
	}
	if !writer {
		return nil, s.refuse(ctx, e, codes.PermissionDenied, "you may not write %s: %s", p, whyNot(f, "writers"))
	}
	d, o, err := s.Store.Write(ctx, repo, p, req.GetValue(), c.ID, f.Store())
	if err != nil {
		return nil, toStatus(err)
	}
	e.Digest, e.Outcome = d.Short(), "ok"
	if err := s.decide(ctx, e); err != nil {
		return nil, toStatus(err)
	}
	return &secretspb.WriteSecretResponse{Digest: d[:], Opening: o[:], Pointer: secrets.Pointer(d)}, nil
}

func (s *Secrets) ReadSecret(ctx context.Context, req *secretspb.ReadSecretRequest) (*secretspb.ReadSecretResponse, error) {
	c, err := person(ctx)
	if err != nil {
		return nil, err
	}
	repo, p := req.GetRepositoryId(), req.GetPath()
	if err := validPath(p); err != nil {
		return nil, err
	}
	d, err := digestOf(req.GetDigest())
	if err != nil {
		return nil, err
	}
	e := store.AuditEntry{Repository: repo, Actor: c.ID, Op: "read", Path: p, Digest: d.Short()}
	may, why, err := s.mayRead(ctx, repo, p, d, c.ID)
	if err != nil {
		return nil, toStatus(err)
	}
	if why == noSecrets {
		return nil, status.Error(codes.FailedPrecondition, noSecrets)
	}
	if !may {
		return nil, s.refuse(ctx, e, codes.PermissionDenied, "you may not read %s: %s", p, why)
	}
	o, v, err := s.Store.OpenUnsealed(ctx, repo, p, d, c.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotUnsealed) {
			return nil, s.refuse(ctx, e, codes.FailedPrecondition, "%s is not unsealed: run jj secret unseal %s, then read it once", p, secrets.Ref(p))
		}
		return nil, toStatus(err)
	}
	e.Outcome = "ok"
	if err := s.decide(ctx, e); err != nil {
		clear(v)
		return nil, toStatus(err)
	}
	return &secretspb.ReadSecretResponse{Value: v, Opening: o[:]}, nil
}

// mayRead says whether a person may read a version: a reader of its folder on main; or its
// writer, while still a writer of the folder, reading back what they wrote.
func (s *Secrets) mayRead(ctx context.Context, repo int64, p string, d secrets.Digest, principal string) (bool, string, error) {
	f, err := s.Policy.Folder(ctx, repo, secrets.Folder(p))
	if err != nil {
		return false, "", err
	}
	if !f.KeepsSecrets() {
		return false, noSecrets, nil
	}
	reader, err := s.Policy.Reader(ctx, f, principal)
	if err != nil || reader {
		return reader, "", err
	}
	writer, err := s.Policy.Writer(ctx, f, principal)
	if err != nil {
		return false, "", err
	}
	if writer {
		v, err := s.Store.Version(ctx, repo, p, d)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return false, "", err
		}
		if err == nil && v.WrittenBy == principal {
			return true, "", nil
		}
	}
	return false, whyNot(f, "readers"), nil
}

// unsealScope is what an approval of unsealing paths approves, exactly.
func unsealScope(repo int64, paths []string) string {
	return "secrets/unseal/" + strconv.FormatInt(repo, 10) + "/" + strings.Join(paths, "\n")
}

func (s *Secrets) Unseal(ctx context.Context, req *secretspb.UnsealRequest) (*secretspb.UnsealResponse, error) {
	c, err := person(ctx)
	if err != nil {
		return nil, err
	}
	repo := req.GetRepositoryId()
	paths := slices.Clone(req.GetPaths())
	slices.Sort(paths)
	paths = slices.Compact(paths)
	if len(paths) == 0 || len(paths) > 100 {
		return nil, status.Error(codes.InvalidArgument, "name from 1 to 100 secrets to unseal")
	}
	passkey := false
	for _, p := range paths {
		if err := validPath(p); err != nil {
			return nil, err
		}
		e := store.AuditEntry{Repository: repo, Actor: c.ID, Op: "unseal", Path: p}
		f, err := s.Policy.Folder(ctx, repo, secrets.Folder(p))
		if err != nil {
			return nil, toStatus(err)
		}
		if !f.KeepsSecrets() {
			return nil, status.Error(codes.FailedPrecondition, noSecrets)
		}
		reader, err := s.Policy.Reader(ctx, f, c.ID)
		if err != nil {
			return nil, toStatus(err)
		}
		writer := false
		if !reader {
			if writer, err = s.Policy.Writer(ctx, f, c.ID); err != nil {
				return nil, toStatus(err)
			}
		}
		if !reader && !writer {
			return nil, s.refuse(ctx, e, codes.PermissionDenied, "you may not unseal %s: %s", p, whyNot(f, "readers"))
		}
		passkey = passkey || f.Access.Unseal == secrets.UnsealPasskey
	}
	scope := unsealScope(repo, paths)
	approval := req.GetApprovalId()
	if passkey && approval == "" {
		if s.Approvals == nil {
			return nil, status.Error(codes.FailedPrecondition, "a folder asks for a passkey to unseal its secrets, which this installation cannot ask for")
		}
		id, url, expires, err := s.Approvals.Start(ctx, c.ID, scope, fmt.Sprintf("Unseal %s in repository %d", strings.Join(paths, ", "), repo))
		if err != nil {
			return nil, toStatus(err)
		}
		return &secretspb.UnsealResponse{State: secretspb.UnsealResponse_STATE_APPROVE, ApprovalId: id, Url: url, ExpiresAt: timestamppb.New(expires)}, nil
	}
	if approval != "" {
		if s.Approvals == nil {
			return nil, status.Error(codes.FailedPrecondition, "this installation asks for no passkeys")
		}
		approved, done, err := s.Approvals.Status(ctx, approval, c.ID, scope)
		switch {
		case err != nil:
			return nil, toStatus(err)
		case !done:
			return &secretspb.UnsealResponse{State: secretspb.UnsealResponse_STATE_WAITING, ApprovalId: approval}, nil
		case !approved:
			return nil, s.refuse(ctx, store.AuditEntry{Repository: repo, Actor: c.ID, Op: "unseal", Path: strings.Join(paths, ",")},
				codes.PermissionDenied, "the approval was declined, or expired")
		}
	}
	if _, err := s.Store.Unseal(ctx, repo, paths, c.ID, approval, s.UnsealTTL); err != nil {
		return nil, toStatus(err)
	}
	for _, p := range paths {
		if err := s.decide(ctx, store.AuditEntry{Repository: repo, Actor: c.ID, Op: "unseal", Path: p, Outcome: "ok"}); err != nil {
			return nil, toStatus(err)
		}
	}
	return &secretspb.UnsealResponse{State: secretspb.UnsealResponse_STATE_GRANTED, ApprovalId: approval,
		ExpiresAt: timestamppb.New(time.Now().Add(s.UnsealTTL))}, nil
}

func (s *Secrets) ListUnseals(ctx context.Context, req *secretspb.ListUnsealsRequest) (*secretspb.ListUnsealsResponse, error) {
	c, err := person(ctx)
	if err != nil {
		return nil, err
	}
	us, err := s.Store.Unseals(ctx, req.GetRepositoryId(), c.ID)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &secretspb.ListUnsealsResponse{}
	for _, u := range us {
		out.Unseals = append(out.Unseals, &secretspb.Unseal{Path: u.Path, ExpiresAt: timestamppb.New(u.ExpiresAt)})
	}
	return out, nil
}

// ─── Versions: the SCM's ───

// Versions answers the SCM's upload checks.
type Versions struct {
	secretspb.UnimplementedVersionsServer
	*core
}

// NewVersions is the SCM's service.
func NewVersions(cfg Config) *Versions { return &Versions{core: newCore(cfg)} }

// maxChecked bounds the pointers or folders one check names.
const maxChecked = 10_000

func (v *Versions) CheckPointers(ctx context.Context, req *secretspb.CheckPointersRequest) (*secretspb.CheckPointersResponse, error) {
	if len(req.GetPointers()) > maxChecked {
		return nil, status.Errorf(codes.InvalidArgument, "%d pointers: at most %d", len(req.GetPointers()), maxChecked)
	}
	keys := make([]store.VersionKey, len(req.GetPointers()))
	for i, p := range req.GetPointers() {
		if err := validPath(p.GetPath()); err != nil {
			return nil, err
		}
		d, err := digestOf(p.GetDigest())
		if err != nil {
			return nil, err
		}
		keys[i] = store.VersionKey{Path: p.GetPath(), Digest: d}
	}
	kept, err := v.Store.Kept(ctx, req.GetRepositoryId(), keys)
	if err != nil {
		return nil, toStatus(err)
	}
	return &secretspb.CheckPointersResponse{Kept: kept}, nil
}

func (v *Versions) CheckWriters(ctx context.Context, req *secretspb.CheckWritersRequest) (*secretspb.CheckWritersResponse, error) {
	if len(req.GetFolders()) > maxChecked {
		return nil, status.Errorf(codes.InvalidArgument, "%d folders: at most %d", len(req.GetFolders()), maxChecked)
	}
	if req.GetPrincipalId() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the principal")
	}
	out := &secretspb.CheckWritersResponse{}
	for _, folder := range req.GetFolders() {
		f, err := v.Policy.Folder(ctx, req.GetRepositoryId(), folder)
		if err != nil {
			if !secrets.InTree(folder) {
				return nil, status.Error(codes.InvalidArgument, err.Error())
			}
			return nil, toStatus(err)
		}
		w, err := v.Policy.Writer(ctx, f, req.GetPrincipalId())
		if err != nil {
			return nil, toStatus(err)
		}
		out.Writer = append(out.Writer, w)
	}
	return out, nil
}
