package service

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/Cidan/ok/internal/authz"
	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/serve/authn"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Authorize in the SCM (docs/hosted.md, "Tenancy, as built", "Roles and actions"): every request a
// person or a robot makes names its repository, and Authorize decides whether they may do what the
// method does to it. ok's own roles act for a person they checked, and are not checked again here.
// A method anyone else may call is listed below; one that is not is refused them, so a new method
// is closed until it is given an action.

// actions are what each method people may call does to the repository its request names.
var actions = map[string]authz.Action{
	"/ok.scm.v1.Store/ReadObjects":            authz.ReadCode,
	"/ok.scm.v1.Store/ReadFiles":              authz.ReadCode,
	"/ok.scm.v1.Store/GetRelatedCopies":       authz.ReadCode,
	"/ok.scm.v1.Store/GetCopyRecords":         authz.ReadCode,
	"/ok.scm.v1.Store/PrefetchCommits":        authz.ReadCode,
	"/ok.scm.v1.Store/ReadTreeClosure":        authz.ReadCode,
	"/ok.scm.v1.Store/ReadTreeSizes":          authz.ReadCode,
	"/ok.scm.v1.Store/ReadDictionary":         authz.ReadCode,
	"/ok.scm.v1.Store/ReadCheckout":           authz.ReadCode,
	"/ok.scm.v1.Store/GetOpHeads":             authz.ReadCode,
	"/ok.scm.v1.Store/ReadOperations":         authz.ReadCode,
	"/ok.scm.v1.Store/PrefetchOperations":     authz.ReadCode,
	"/ok.scm.v1.Store/ResolveOperationPrefix": authz.ReadCode,
	"/ok.scm.v1.Store/CollectOperations":      authz.ReadCode,
	// Publishing writes to the person's own workspace, whose operation log is on the SCM: every jj
	// command a reader runs records one there. Proposing what they publish is Upload's.
	"/ok.scm.v1.Store/Publish":                 authz.ReadCode,
	"/ok.scm.v1.ChangeLists/Upload":            authz.Propose,
	"/ok.scm.v1.ChangeLists/GetChangeList":     authz.ReadCode,
	"/ok.scm.v1.ChangeLists/ListChangeLists":   authz.ReadCode,
	"/ok.scm.v1.ChangeLists/LookupChanges":     authz.ReadCode,
	"/ok.scm.v1.Browse/GetCommits":             authz.ReadCode,
	"/ok.scm.v1.Browse/Log":                    authz.ReadCode,
	"/ok.scm.v1.Browse/ReadTree":               authz.ReadCode,
	"/ok.scm.v1.Search/Code":                   authz.ReadCode,
	"/ok.scm.v1.Browse/ReadFileAt":             authz.ReadCode,
	"/ok.scm.v1.Browse/ReadFilesAt":            authz.ReadCode,
	"/ok.scm.v1.Browse/DiffTrees":              authz.ReadCode,
	"/ok.scm.v1.Browse/DiffFiles":              authz.ReadCode,
	"/ok.scm.v1.Browse/Annotate":               authz.ReadCode,
	"/ok.scm.v1.Browse/IsAncestor":             authz.ReadCode,
	"/ok.scm.v1.Browse/Export":                 authz.ReadCode,
	"/ok.scm.v1.Workspaces/Attach":             authz.ReadCode,
	"/ok.scm.v1.Workspaces/Renew":              authz.ReadCode,
	"/ok.scm.v1.Workspaces/Release":            authz.ReadCode,
	"/ok.scm.v1.Workspaces/RecordCheckpoint":   authz.ReadCode,
	"/ok.scm.v1.Workspaces/GetCheckpoint":      authz.ReadCode,
	"/ok.scm.v1.Workspaces/ListCheckpoints":    authz.ReadCode,
	"/ok.scm.v1.Workspaces/ListWorkspaces":     authz.ReadCode,
	"/ok.scm.v1.Workspaces/CreateWorkspace":    authz.ReadCode,
	"/ok.scm.v1.Repositories/GetRepository":    authz.ReadCode,
	"/ok.scm.v1.Repositories/CreateRepository": 0, // the directory decides who makes what
	"/ok.scm.v1.Repositories/ForkRepository":   0, // the method decides forking its upstream; the directory, making the fork
	"/ok.scm.v1.Repositories/SyncFork":         authz.Land,
}

// access decides people's requests. It is safe for concurrent use.
type access struct {
	checker *authz.Checker
	// cell is the cell the SCM serves (InCell): it refuses repositories other cells hold. "" serves
	// every repository, as a single installation's does.
	cell string
}

// repositoryOf is the repository a request names: its repository_id, or that of the message it
// carries its lease or its beginning in. ok is false for a request that names none.
func repositoryOf(m proto.Message) (int64, bool) {
	r := m.ProtoReflect()
	if id, ok := idField(r); ok {
		return id, true
	}
	var id int64
	var found bool
	r.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if f.Kind() == protoreflect.MessageKind && !f.IsList() && !f.IsMap() {
			id, found = idField(v.Message())
		}
		return !found
	})
	return id, found
}

func idField(r protoreflect.Message) (int64, bool) {
	f := r.Descriptor().Fields().ByName("repository_id")
	if f == nil || f.Kind() != protoreflect.Int64Kind {
		return 0, false
	}
	return r.Get(f).Int(), true
}

// check decides whether the caller may make a request to a method. ok's roles are refused only a
// repository another cell holds; a request no one is identified on — which the method refuses
// itself — is let through.
func (a *access) check(ctx context.Context, method string, req any) error {
	c, ok := authn.FromContext(ctx)
	if !ok {
		return nil
	}
	if c.IsRole() {
		return a.inCell(ctx, req)
	}
	action, listed := actions[method]
	if !listed {
		return status.Errorf(codes.PermissionDenied, "%s is for ok's own roles", method)
	}
	if action == 0 {
		return nil
	}
	m, ok := req.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "%s's request is no message", method)
	}
	repo, ok := repositoryOf(m)
	if g, isGet := m.(*scmpb.GetRepositoryRequest); isGet {
		if g.GetName() != "" {
			return nil // resolved through the directory as the person may see it
		}
		repo, ok = g.GetId(), true
	}
	if !ok || repo == 0 {
		return status.Errorf(codes.InvalidArgument, "%s names no repository", method)
	}
	d, err := a.checker.Decide(ctx, c.ID, c.Token, repo, action)
	if err == nil && a.elsewhere(d.Cell) {
		// A refusal is decided on fresh facts: those kept may predate a move here.
		a.checker.Forget(repo)
		d, err = a.checker.Decide(ctx, c.ID, c.Token, repo, action)
	}
	if err != nil {
		return status.Errorf(codes.Unavailable, "deciding who may %s repository %d: %v", action, repo, err)
	}
	switch {
	case !d.Visible:
		return status.Errorf(codes.NotFound, "no repository has the ID %d", repo)
	case a.elsewhere(d.Cell):
		return cells.Moved(repo, d.Cell)
	case !d.Allowed:
		return status.Error(codes.PermissionDenied, upperFirst(d.Why))
	}
	return nil
}

// inCell refuses a request of ok's roles about a repository another cell holds, which the
// directory says (the facts Authorize decides from, kept a few seconds); one the directory does not
// know, or is still making, is the method's to answer.
func (a *access) inCell(ctx context.Context, req any) error {
	if a.cell == "" {
		return nil
	}
	m, ok := req.(proto.Message)
	if !ok {
		return nil
	}
	repo, ok := repositoryOf(m)
	if g, isGet := m.(*scmpb.GetRepositoryRequest); isGet {
		repo, ok = g.GetId(), g.GetId() != 0
	}
	if !ok || repo == 0 {
		return nil
	}
	f, err := a.checker.Facts(ctx, "", repo)
	if err == nil && a.elsewhere(f.Repository.Cell) {
		// A refusal is decided on fresh facts: those kept may predate a move here.
		a.checker.Forget(repo)
		f, err = a.checker.Facts(ctx, "", repo)
	}
	if errors.Is(err, authz.ErrNotFound) {
		return nil
	}
	if err != nil {
		return status.Errorf(codes.Unavailable, "finding repository %d's cell: %v", repo, err)
	}
	if a.elsewhere(f.Repository.Cell) {
		return cells.Moved(repo, f.Repository.Cell)
	}
	return nil
}

// elsewhere reports whether a repository's cell is another than the one the SCM serves.
func (a *access) elsewhere(cell string) bool { return a.cell != "" && cell != "" && cell != a.cell }

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// registrar registers services so that each of their methods checks its request first: a unary
// one once the serving chain has identified its caller, a stream's on its first message.
type registrar struct {
	reg grpc.ServiceRegistrar
	a   *access
}

func (r registrar) RegisterService(desc *grpc.ServiceDesc, impl any) {
	d := *desc
	d.Methods = make([]grpc.MethodDesc, len(desc.Methods))
	for i, m := range desc.Methods {
		method := "/" + desc.ServiceName + "/" + m.MethodName
		inner := m.Handler
		m.Handler = func(srv any, ctx context.Context, dec func(any) error, chain grpc.UnaryServerInterceptor) (any, error) {
			wrapped := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
				checked := func(ctx context.Context, req any) (any, error) {
					if err := r.a.check(ctx, method, req); err != nil {
						return nil, err
					}
					return h(writesAbout(ctx, req), req)
				}
				if chain == nil {
					return checked(ctx, req)
				}
				return chain(ctx, req, info, checked)
			}
			return inner(srv, ctx, dec, wrapped)
		}
		d.Methods[i] = m
	}
	d.Streams = make([]grpc.StreamDesc, len(desc.Streams))
	for i, s := range desc.Streams {
		method := "/" + desc.ServiceName + "/" + s.StreamName
		inner := s.Handler
		s.Handler = func(srv any, ss grpc.ServerStream) error {
			return inner(srv, &checkedStream{ServerStream: ss, method: method, a: r.a})
		}
		d.Streams[i] = s
	}
	r.reg.RegisterService(&d, impl)
}

// checkedStream checks a stream's first message, which names its repository, and its context
// names the repository from then on (writesAbout).
type checkedStream struct {
	grpc.ServerStream
	method  string
	a       *access
	checked bool
	ctx     context.Context
}

func (s *checkedStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if s.checked {
		return nil
	}
	s.checked = true
	if err := s.a.check(s.Context(), s.method, m); err != nil {
		return err
	}
	s.ctx = writesAbout(s.ServerStream.Context(), m)
	return nil
}

func (s *checkedStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return s.ServerStream.Context()
}

// writesAbout is ctx naming the repository a request names, for the store's gate: each write
// transaction it runs reads its network's move first (docs/hosted.md, "Cells, as built").
func writesAbout(ctx context.Context, req any) context.Context {
	m, ok := req.(proto.Message)
	if !ok {
		return ctx
	}
	repo, ok := repositoryOf(m)
	if g, isGet := m.(*scmpb.GetRepositoryRequest); isGet {
		repo, ok = g.GetId(), g.GetId() != 0
	}
	if !ok {
		return ctx
	}
	return move.WithRepository(ctx, repo)
}
