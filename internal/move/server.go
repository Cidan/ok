package move

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/serve/authn"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Pages: how many rows an export reads at a time, and the bytes it answers with at most.
const (
	exportRows  = 1000
	exportBytes = 8 << 20
	listObjects = 1000
	objectChunk = 1 << 20
)

// Server serves a role's side of moves (ok.v1.Moves) over its Role: to the directory, and to the
// same role of another cell, which it reaches with Peer to copy from it. It is a node.Service's
// registrar's to register.
type Server struct {
	okpb.UnimplementedMovesServer
	role Role
	// self is the caller the role is, which may ask it as identity may.
	self okpb.Caller
	// peer is the role's Moves of another cell.
	peer func(ctx context.Context, cell string) (okpb.MovesClient, error)
}

// NewServer serves r, the role whose caller is self, reaching the same role of other cells with
// peer.
func NewServer(r Role, self okpb.Caller, peer func(ctx context.Context, cell string) (okpb.MovesClient, error)) *Server {
	return &Server{role: r, self: self, peer: peer}
}

// Register registers the role's side on reg: the service is registered once, each request going to
// the side of the role its scope names, as one process may serve several roles on one server.
func (s *Server) Register(reg grpc.ServiceRegistrar) {
	v, loaded := muxes.LoadOrStore(reg, &mux{roles: map[string]*Server{}})
	m := v.(*mux)
	m.mu.Lock()
	m.roles[s.role.Name()] = s
	m.mu.Unlock()
	if !loaded {
		okpb.RegisterMovesServer(reg, m)
	}
}

// muxes are the services registered, by the registrar each is on.
var muxes sync.Map

// mux serves the sides of moves of a process's roles on one server.
type mux struct {
	okpb.UnimplementedMovesServer
	mu    sync.RWMutex
	roles map[string]*Server
}

// side is the role's side a request's scope names.
func (m *mux) side(sc *okpb.MoveScope) (*Server, error) {
	m.mu.RLock()
	s, ok := m.roles[sc.GetRole()]
	m.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no %q side of moves is served here", sc.GetRole())
	}
	return s, nil
}

func (m *mux) Export(ctx context.Context, req *okpb.ExportRequest) (*okpb.ExportResponse, error) {
	s, err := m.side(req.GetScope())
	if err != nil {
		return nil, err
	}
	return s.Export(ctx, req)
}

func (m *mux) ListObjects(ctx context.Context, req *okpb.ListObjectsRequest) (*okpb.ListObjectsResponse, error) {
	s, err := m.side(req.GetScope())
	if err != nil {
		return nil, err
	}
	return s.ListObjects(ctx, req)
}

func (m *mux) ReadObject(req *okpb.ReadObjectRequest, stream okpb.Moves_ReadObjectServer) error {
	s, err := m.side(req.GetScope())
	if err != nil {
		return err
	}
	return s.ReadObject(req, stream)
}

func (m *mux) Freeze(ctx context.Context, req *okpb.MoveScope) (*okpb.FreezeResponse, error) {
	s, err := m.side(req)
	if err != nil {
		return nil, err
	}
	return s.Freeze(ctx, req)
}

func (m *mux) Thaw(ctx context.Context, req *okpb.MoveScope) (*okpb.ThawResponse, error) {
	s, err := m.side(req)
	if err != nil {
		return nil, err
	}
	return s.Thaw(ctx, req)
}

func (m *mux) Finish(ctx context.Context, req *okpb.FinishRequest) (*okpb.FinishResponse, error) {
	s, err := m.side(req.GetScope())
	if err != nil {
		return nil, err
	}
	return s.Finish(ctx, req)
}

func (m *mux) Import(ctx context.Context, req *okpb.ImportRequest) (*okpb.ImportResponse, error) {
	s, err := m.side(req.GetScope())
	if err != nil {
		return nil, err
	}
	return s.Import(ctx, req)
}

func (m *mux) Open(ctx context.Context, req *okpb.MoveScope) (*okpb.OpenResponse, error) {
	s, err := m.side(req)
	if err != nil {
		return nil, err
	}
	return s.Open(ctx, req)
}

func (m *mux) Drop(ctx context.Context, req *okpb.MoveScope) (*okpb.DropResponse, error) {
	s, err := m.side(req)
	if err != nil {
		return nil, err
	}
	return s.Drop(ctx, req)
}

// admit admits ok identity, which runs moves, and the same role of another cell.
func (s *Server) admit(ctx context.Context) error {
	c, ok := authn.FromContext(ctx)
	if ok && (c.Role == okpb.Caller_CALLER_IDENTITY || c.Role == s.self) {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "a %s's moves are ok identity's and its own role's", s.role.Name())
}

func (s *Server) Export(ctx context.Context, req *okpb.ExportRequest) (*okpb.ExportResponse, error) {
	if err := s.admit(ctx); err != nil {
		return nil, err
	}
	limit := int(req.GetLimit())
	if limit <= 0 || limit > exportRows {
		limit = exportRows
	}
	rows, err := s.role.Export(ctx, scopeOf(req.GetScope()), req.GetTable(), req.GetAfter(), limit)
	if err != nil {
		return nil, toStatus(err)
	}
	out := &okpb.ExportResponse{More: len(rows) == limit}
	size := 0
	for _, r := range rows {
		n := proto.Size(r)
		if size+n > exportBytes && len(out.Rows) > 0 {
			out.More = true
			break
		}
		size += n
		out.Rows = append(out.Rows, r)
	}
	return out, nil
}

func (s *Server) ListObjects(ctx context.Context, req *okpb.ListObjectsRequest) (*okpb.ListObjectsResponse, error) {
	if err := s.admit(ctx); err != nil {
		return nil, err
	}
	o := s.role.Objects()
	if o == nil {
		return &okpb.ListObjectsResponse{}, nil
	}
	limit := int(req.GetLimit())
	if limit <= 0 || limit > listObjects {
		limit = listObjects
	}
	objs, err := o.List(ctx, scopeOf(req.GetScope()), req.GetAfter(), limit)
	if err != nil {
		return nil, toStatus(err)
	}
	return &okpb.ListObjectsResponse{Objects: objs, More: len(objs) == limit}, nil
}

func (s *Server) ReadObject(req *okpb.ReadObjectRequest, stream okpb.Moves_ReadObjectServer) error {
	ctx := stream.Context()
	if err := s.admit(ctx); err != nil {
		return err
	}
	o := s.role.Objects()
	if o == nil {
		return status.Errorf(codes.NotFound, "a %s keeps no objects", s.role.Name())
	}
	r, err := o.Open(ctx, scopeOf(req.GetScope()), req.GetKey())
	if err != nil {
		return toStatus(err)
	}
	defer r.Close()
	buf := make([]byte, objectChunk)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if serr := stream.Send(&okpb.ReadObjectResponse{Data: buf[:n]}); serr != nil {
				return serr
			}
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		if err != nil {
			return toStatus(err)
		}
	}
}

func (s *Server) Freeze(ctx context.Context, req *okpb.MoveScope) (*okpb.FreezeResponse, error) {
	if err := s.admit(ctx); err != nil {
		return nil, err
	}
	return &okpb.FreezeResponse{}, toStatus(s.role.Freeze(ctx, scopeOf(req)))
}

func (s *Server) Thaw(ctx context.Context, req *okpb.MoveScope) (*okpb.ThawResponse, error) {
	if err := s.admit(ctx); err != nil {
		return nil, err
	}
	return &okpb.ThawResponse{}, toStatus(s.role.Thaw(ctx, scopeOf(req)))
}

func (s *Server) Finish(ctx context.Context, req *okpb.FinishRequest) (*okpb.FinishResponse, error) {
	if err := s.admit(ctx); err != nil {
		return nil, err
	}
	if req.GetDestination() == "" {
		return nil, status.Error(codes.InvalidArgument, "name the cell the network moved to")
	}
	return &okpb.FinishResponse{}, toStatus(s.role.Finish(ctx, scopeOf(req.GetScope()), req.GetDestination()))
}

func (s *Server) Open(ctx context.Context, req *okpb.MoveScope) (*okpb.OpenResponse, error) {
	if err := s.admit(ctx); err != nil {
		return nil, err
	}
	return &okpb.OpenResponse{}, toStatus(s.role.Open(ctx, scopeOf(req)))
}

func (s *Server) Drop(ctx context.Context, req *okpb.MoveScope) (*okpb.DropResponse, error) {
	if err := s.admit(ctx); err != nil {
		return nil, err
	}
	// Objects first: a role may find them through its rows.
	scope := scopeOf(req)
	if o := s.role.Objects(); o != nil {
		if err := o.Drop(ctx, scope); err != nil {
			return nil, toStatus(err)
		}
	}
	if err := s.role.Drop(ctx, scope); err != nil {
		return nil, toStatus(err)
	}
	return &okpb.DropResponse{}, nil
}

// Import copies the network from its source cell's same role: the objects this role lacks, and,
// for the second stage, every row of every table, parents first, a page at a time, then what
// numbers rows moved past them.
func (s *Server) Import(ctx context.Context, req *okpb.ImportRequest) (*okpb.ImportResponse, error) {
	if err := s.admit(ctx); err != nil {
		return nil, err
	}
	scope := req.GetScope()
	src, err := s.peer(ctx, req.GetSource())
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "reaching cell %s's %s: %v", req.GetSource(), s.role.Name(), err)
	}
	out := &okpb.ImportResponse{}
	if o := s.role.Objects(); o != nil {
		if err := copyObjects(ctx, src, o, scope, out); err != nil {
			return nil, toStatus(err)
		}
	}
	if req.GetStage() != okpb.ImportRequest_STAGE_ALL {
		return out, nil
	}
	// What it holds of the network already — a failed import's, or a residence before — goes first:
	// the source's rows are the network's, whole.
	if err := s.role.Drop(ctx, scopeOf(scope)); err != nil {
		return nil, toStatus(err)
	}
	tables, err := s.role.Tables(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	for _, t := range tables {
		var after *okpb.Row
		for {
			page, err := src.Export(ctx, &okpb.ExportRequest{Scope: scope, Table: t, After: after, Limit: exportRows})
			if err != nil {
				return nil, fmt.Errorf("exporting %s: %w", t, err)
			}
			rows := page.GetRows()
			if err := s.role.Import(ctx, scopeOf(scope), t, rows); err != nil {
				return nil, toStatus(fmt.Errorf("importing %s: %w", t, err))
			}
			out.Rows += int64(len(rows))
			if !page.GetMore() || len(rows) == 0 {
				break
			}
			after = rows[len(rows)-1]
		}
	}
	if err := s.role.Imported(ctx, scopeOf(scope)); err != nil {
		return nil, toStatus(err)
	}
	return out, nil
}

// copyObjects copies each of the scope's objects src holds and o lacks.
func copyObjects(ctx context.Context, src okpb.MovesClient, o Objects, scope *okpb.MoveScope, out *okpb.ImportResponse) error {
	after := ""
	for {
		page, err := src.ListObjects(ctx, &okpb.ListObjectsRequest{Scope: scope, After: after, Limit: listObjects})
		if err != nil {
			return fmt.Errorf("listing the objects: %w", err)
		}
		for _, obj := range page.GetObjects() {
			after = obj.GetKey()
			have, err := o.Has(ctx, obj.GetKey())
			if err != nil {
				return err
			}
			if have {
				continue
			}
			if err := copyObject(ctx, src, o, scope, obj); err != nil {
				return fmt.Errorf("copying %s: %w", obj.GetKey(), err)
			}
			out.Objects++
			out.Bytes += obj.GetSize()
		}
		if !page.GetMore() || len(page.GetObjects()) == 0 {
			return nil
		}
	}
}

func copyObject(ctx context.Context, src okpb.MovesClient, o Objects, scope *okpb.MoveScope, obj *okpb.MovedObject) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := src.ReadObject(ctx, &okpb.ReadObjectRequest{Scope: scope, Key: obj.GetKey()})
	if err != nil {
		return err
	}
	pr, pw := io.Pipe()
	go func() {
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				pw.Close()
				return
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, err := pw.Write(msg.GetData()); err != nil {
				return
			}
		}
	}()
	err = o.Put(ctx, obj.GetKey(), pr, obj.GetSize())
	pr.CloseWithError(err)
	return err
}

// toStatus is a role's error as a status: a gRPC status as it is, a frozen network's refusal as
// its own, anything else as it says.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}
