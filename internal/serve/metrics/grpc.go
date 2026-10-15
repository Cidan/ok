package metrics

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// The server's view of every RPC a role serves, gRPC and REST alike (REST is calls through the same
// server, over an in-memory connection: transport "rest").
var (
	serverHandled = NewCounter("ok_rpc_server_handled_total",
		"RPCs the server finished, by method and status.", "grpc_service", "grpc_method", "grpc_code", "transport")
	serverDuration = NewHistogram("ok_rpc_server_duration_seconds",
		"How long the server took to finish each RPC.", Seconds, "grpc_service", "grpc_method", "transport")
	serverInFlight = NewGauge("ok_rpc_server_in_flight",
		"RPCs the server is handling now.", "grpc_service", "grpc_method", "transport")
	serverReceivedBytes = NewHistogram("ok_rpc_server_received_bytes",
		"Bytes each RPC's requests carried to the server, as they crossed the wire.", Bytes, "grpc_service", "grpc_method", "transport")
	serverSentBytes = NewHistogram("ok_rpc_server_sent_bytes",
		"Bytes each RPC's responses carried from the server, as they crossed the wire.", Bytes, "grpc_service", "grpc_method", "transport")
	serverReceivedMessages = NewHistogram("ok_rpc_server_received_messages",
		"Messages each RPC sent the server.", Count, "grpc_service", "grpc_method", "transport")
	serverSentMessages = NewHistogram("ok_rpc_server_sent_messages",
		"Messages the server sent in each RPC.", Count, "grpc_service", "grpc_method", "transport")
)

// The caller's view of the RPCs a binary makes over a connection dialed with DialOption.
var (
	clientHandled = NewCounter("ok_rpc_client_handled_total",
		"RPCs this binary made that finished, by method and status.", "grpc_service", "grpc_method", "grpc_code")
	clientDuration = NewHistogram("ok_rpc_client_duration_seconds",
		"How long each RPC this binary made took, as it waited.", Seconds, "grpc_service", "grpc_method")
	clientInFlight = NewGauge("ok_rpc_client_in_flight",
		"RPCs this binary is waiting on now.", "grpc_service", "grpc_method")
	clientSentBytes = NewHistogram("ok_rpc_client_sent_bytes",
		"Bytes each RPC's requests carried, as they crossed the wire.", Bytes, "grpc_service", "grpc_method")
	clientReceivedBytes = NewHistogram("ok_rpc_client_received_bytes",
		"Bytes each RPC's responses carried back, as they crossed the wire.", Bytes, "grpc_service", "grpc_method")
)

// unknown labels an RPC for a method the server does not serve: its name is the caller's to
// choose, so it is never a label.
const unknown = "unknown"

// splitMethod splits "/package.Service/Method".
func splitMethod(full string) (service, method string) {
	full = strings.TrimPrefix(full, "/")
	if i := strings.LastIndexByte(full, '/'); i > 0 {
		return full[:i], full[i+1:]
	}
	return unknown, unknown
}

// wireLength is a payload's length on the wire, or, when the transport does not report it, its
// encoded length.
func wireLength(wire, length int) uint64 {
	if wire > 0 {
		return uint64(wire)
	}
	return uint64(length)
}

type rpcKey struct{}

type connKey struct{}

// rpc accumulates one RPC's messages and bytes until it ends.
type rpc struct {
	series                                   *rpcSeries
	recvMsgs, sentMsgs, recvBytes, sentBytes atomic.Uint64
}

// rpcSeries are one method's series, looked up once.
type rpcSeries struct {
	service, method, transport string
	duration                   HistogramSeries
	inFlight                   GaugeSeries
	recvBytes, sentBytes       HistogramSeries
	recvMsgs, sentMsgs         HistogramSeries // the server's only
}

// ServerHandler records every RPC a gRPC server handles: install it with grpc.StatsHandler, and
// give it the server's services once they are registered (Serve). the runtime installs one on its
// shared server; nothing else needs to.
type ServerHandler struct {
	known  atomic.Pointer[map[string]bool]
	series sync.Map // service/method/transport -> *rpcSeries
}

// NewServerHandler returns a handler that labels every RPC unknown until Serve names the methods.
func NewServerHandler() *ServerHandler { return &ServerHandler{} }

// Serve names the methods the server serves; an RPC for any other is labeled unknown.
func (h *ServerHandler) Serve(services map[string]grpc.ServiceInfo) {
	known := map[string]bool{}
	for svc, info := range services {
		for _, m := range info.Methods {
			known["/"+svc+"/"+m.Name] = true
		}
	}
	h.known.Store(&known)
}

func (h *ServerHandler) lookup(service, method, transport string) *rpcSeries {
	key := service + "/" + method + "/" + transport
	if s, ok := h.series.Load(key); ok {
		return s.(*rpcSeries)
	}
	s := &rpcSeries{
		service: service, method: method, transport: transport,
		duration:  serverDuration.With(service, method, transport),
		inFlight:  serverInFlight.With(service, method, transport),
		recvBytes: serverReceivedBytes.With(service, method, transport),
		sentBytes: serverSentBytes.With(service, method, transport),
		recvMsgs:  serverReceivedMessages.With(service, method, transport),
		sentMsgs:  serverSentMessages.With(service, method, transport),
	}
	actual, _ := h.series.LoadOrStore(key, s)
	return actual.(*rpcSeries)
}

// TagConn marks a connection from the REST surface, whose listener is in memory.
func (h *ServerHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	if info.LocalAddr != nil && info.LocalAddr.Network() == "bufconn" {
		return context.WithValue(ctx, connKey{}, "rest")
	}
	return ctx
}

// HandleConn records nothing: connections are the transport's.
func (h *ServerHandler) HandleConn(context.Context, stats.ConnStats) {}

// TagRPC attaches the RPC's series.
func (h *ServerHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	service, method := unknown, unknown
	if known := h.known.Load(); known != nil && (*known)[info.FullMethodName] {
		service, method = splitMethod(info.FullMethodName)
	}
	transport := "grpc"
	if t, ok := ctx.Value(connKey{}).(string); ok {
		transport = t
	}
	return context.WithValue(ctx, rpcKey{}, &rpc{series: h.lookup(service, method, transport)})
}

// HandleRPC records the RPC's events.
func (h *ServerHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	r, ok := ctx.Value(rpcKey{}).(*rpc)
	if !ok {
		return
	}
	switch e := s.(type) {
	case *stats.InHeader:
		// gRPC answers a method it does not serve Unimplemented with no Begin and no End: this is
		// the one event such an RPC has.
		if m := r.series; m.method == unknown {
			serverHandled.With(unknown, unknown, codes.Unimplemented.String(), m.transport).Inc()
		}
	case *stats.Begin:
		r.series.inFlight.Inc()
	case *stats.InPayload:
		r.recvMsgs.Add(1)
		r.recvBytes.Add(wireLength(e.WireLength, e.Length))
	case *stats.OutPayload:
		r.sentMsgs.Add(1)
		r.sentBytes.Add(wireLength(e.WireLength, e.Length))
	case *stats.End:
		m := r.series
		m.inFlight.Dec()
		m.duration.Observe(e.EndTime.Sub(e.BeginTime).Seconds())
		m.recvBytes.Observe(float64(r.recvBytes.Load()))
		m.sentBytes.Observe(float64(r.sentBytes.Load()))
		m.recvMsgs.Observe(float64(r.recvMsgs.Load()))
		m.sentMsgs.Observe(float64(r.sentMsgs.Load()))
		serverHandled.With(m.service, m.method, status.Code(e.Error).String(), m.transport).Inc()
	}
}

// clientHandler records the RPCs a binary makes.
type clientHandler struct {
	series sync.Map // service/method -> *rpcSeries
}

var defaultClientHandler = &clientHandler{}

// DialOption records every RPC made over a connection dialed with it, by method: add it where a
// binary dials another service. The methods are the binary's own to name, so every one is a label.
func DialOption() grpc.DialOption { return grpc.WithStatsHandler(defaultClientHandler) }

func (h *clientHandler) lookup(service, method string) *rpcSeries {
	key := service + "/" + method
	if s, ok := h.series.Load(key); ok {
		return s.(*rpcSeries)
	}
	s := &rpcSeries{
		service: service, method: method,
		duration:  clientDuration.With(service, method),
		inFlight:  clientInFlight.With(service, method),
		recvBytes: clientReceivedBytes.With(service, method),
		sentBytes: clientSentBytes.With(service, method),
	}
	actual, _ := h.series.LoadOrStore(key, s)
	return actual.(*rpcSeries)
}

func (h *clientHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *clientHandler) HandleConn(context.Context, stats.ConnStats) {}

func (h *clientHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	service, method := splitMethod(info.FullMethodName)
	return context.WithValue(ctx, rpcKey{}, &rpc{series: h.lookup(service, method)})
}

func (h *clientHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	r, ok := ctx.Value(rpcKey{}).(*rpc)
	if !ok {
		return
	}
	switch e := s.(type) {
	case *stats.Begin:
		r.series.inFlight.Inc()
	case *stats.OutPayload:
		r.sentBytes.Add(wireLength(e.WireLength, e.Length))
	case *stats.InPayload:
		r.recvBytes.Add(wireLength(e.WireLength, e.Length))
	case *stats.End:
		m := r.series
		m.inFlight.Dec()
		m.duration.Observe(e.EndTime.Sub(e.BeginTime).Seconds())
		m.sentBytes.Observe(float64(r.sentBytes.Load()))
		m.recvBytes.Observe(float64(r.recvBytes.Load()))
		clientHandled.With(m.service, m.method, status.Code(e.Error).String()).Inc()
	}
}
