package serve

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/serve/authn"
)

// Option configures Init.
type Option func(*options)

type options struct {
	unary            []grpc.UnaryServerInterceptor
	stream           []grpc.StreamServerInterceptor
	addr             string
	stopTimeout      time.Duration
	maxRecv, maxSend int
	metricsAddr      string
	profiling        bool
	listener         net.Listener
	local            net.Listener
	reflection       bool
	tls              *tls.Config
	middleware       func(http.Handler) http.Handler
}

// WithAuthn resolves every request's caller with r and holds it to its service's callers: gRPC and
// REST through its interceptors, web routes through its middleware.
func WithAuthn(r *authn.Resolver) Option {
	return func(o *options) {
		o.unary = append([]grpc.UnaryServerInterceptor{r.UnaryInterceptor()}, o.unary...)
		o.stream = append([]grpc.StreamServerInterceptor{r.StreamInterceptor()}, o.stream...)
		o.middleware = r.Middleware
	}
}

// WithTLS serves the port over TLS.
func WithTLS(c *tls.Config) Option { return func(o *options) { o.tls = c } }

// WithReflection turns gRPC server reflection on, which exposes the services' schemas: for
// development.
func WithReflection() Option { return func(o *options) { o.reflection = true } }

// WithListener serves l instead of listening on an address: tests bind an ephemeral port and
// know it up front.
func WithListener(l net.Listener) Option { return func(o *options) { o.listener = l } }

// WithLocal serves gRPC on l too: the process's own calls to its roles (inproc.Listener), which
// WithAuthn's resolver takes as the roles they name.
func WithLocal(l net.Listener) Option { return func(o *options) { o.local = l } }

// WithUnaryInterceptor installs unary gRPC server interceptors on the shared server, outermost
// first, after authentication's. It covers every Service's REST surface too: REST is served as
// calls through the same server.
func WithUnaryInterceptor(i ...grpc.UnaryServerInterceptor) Option {
	return func(o *options) { o.unary = append(o.unary, i...) }
}

// WithStreamInterceptor installs stream gRPC server interceptors, outermost first
// — the streaming half of the same auth seam.
func WithStreamInterceptor(i ...grpc.StreamServerInterceptor) Option {
	return func(o *options) { o.stream = append(o.stream, i...) }
}

// WithListenAddr sets the serve address (default ":8472").
func WithListenAddr(addr string) Option {
	return func(o *options) { o.addr = addr }
}

// WithStopTimeout sets how long every Application has to return once the binary is told to
// stop (default serving.DefaultStopTimeout); one still running then is abandoned as the
// process exits. Keep it within the pod's termination grace period.
func WithStopTimeout(d time.Duration) Option {
	return func(o *options) { o.stopTimeout = d }
}

// WithMaxMessageSize sets the largest message, in bytes, the shared gRPC server takes and sends
// (default gRPC's: 4 MiB in, unbounded out) — for a binary whose services carry files.
func WithMaxMessageSize(recv, send int) Option {
	return func(o *options) { o.maxRecv, o.maxSend = recv, send }
}

// WithMetricsAddr sets where the role serves its metrics (default: an ephemeral port on loopback,
// so roles a test runs side by side do not collide; an installation names ":8473").
func WithMetricsAddr(addr string) Option {
	return func(o *options) { o.metricsAddr = addr }
}

// WithProfiling serves Go's profiles (/debug/pprof/) beside the metrics.
func WithProfiling(on bool) Option {
	return func(o *options) { o.profiling = on }
}
