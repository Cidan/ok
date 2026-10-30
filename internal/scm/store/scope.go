package store

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/scm/store/backend"
)

// A request's scope holds what one request reads once and uses many times — a repository's
// segment manifests (manifest) — for the request's life alone: the store keeps nothing that
// changes from one request to the next (docs/state.md). Outside a request, each read reads again.
type requestScope struct {
	mu        sync.Mutex
	manifests map[manifestKey][]backend.Segment
}

type requestScopeKey struct{}

// WithRequestScope is ctx carrying a scope of its own.
func WithRequestScope(ctx context.Context) context.Context {
	return context.WithValue(ctx, requestScopeKey{}, &requestScope{})
}

func scopeOf(ctx context.Context) *requestScope {
	sc, _ := ctx.Value(requestScopeKey{}).(*requestScope)
	return sc
}

// ScopedRegistrar registers services whose every call runs in a request scope of its own.
func ScopedRegistrar(reg grpc.ServiceRegistrar) grpc.ServiceRegistrar { return scoped{reg} }

type scoped struct{ grpc.ServiceRegistrar }

func (s scoped) RegisterService(desc *grpc.ServiceDesc, impl any) {
	d := *desc
	d.Methods = make([]grpc.MethodDesc, len(desc.Methods))
	for i, m := range desc.Methods {
		h := m.Handler
		m.Handler = func(srv any, ctx context.Context, dec func(any) error, in grpc.UnaryServerInterceptor) (any, error) {
			return h(srv, WithRequestScope(ctx), dec, in)
		}
		d.Methods[i] = m
	}
	d.Streams = make([]grpc.StreamDesc, len(desc.Streams))
	for i, st := range desc.Streams {
		h := st.Handler
		st.Handler = func(srv any, stream grpc.ServerStream) error {
			return h(srv, scopedStream{stream, WithRequestScope(stream.Context())})
		}
		d.Streams[i] = st
	}
	s.ServiceRegistrar.RegisterService(&d, impl)
}

type scopedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s scopedStream) Context() context.Context { return s.ctx }
