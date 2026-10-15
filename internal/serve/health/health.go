// Package health reports a role's liveness and readiness, both as the
// standard gRPC health service (grpc.health.v1, registered off the shelf so no
// health.proto is defined here) and as plain HTTP probes for Kubernetes.
//
// Liveness ("am I up") is the assembly's own health. Readiness ("should I take
// traffic") additionally requires that Setup has completed and the server is
// listening — the runtime calls MarkReady once serving begins.
package health

import (
	"context"
	"net/http"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Reporter answers health/readiness for the binary. It implements
// healthpb.HealthServer and provides HTTP handlers backed by the same state.
type Reporter struct {
	healthpb.UnimplementedHealthServer
	healthy func(ctx context.Context) bool
	ready   atomic.Bool
}

// New builds a Reporter whose liveness is the given healthy func (typically "are
// all nodes healthy"). The Reporter starts not-ready; call MarkReady once the
// server is listening.
func New(healthy func(ctx context.Context) bool) *Reporter {
	return &Reporter{healthy: healthy}
}

// MarkReady flips the binary to ready. Idempotent.
func (r *Reporter) MarkReady() { r.ready.Store(true) }

// Ready reports whether MarkReady has been called: setup is done and the server is listening.
func (r *Reporter) Ready() bool { return r.ready.Load() }

func (r *Reporter) live(ctx context.Context) bool    { return r.healthy(ctx) }
func (r *Reporter) serving(ctx context.Context) bool { return r.ready.Load() && r.healthy(ctx) }

// Check implements grpc.health.v1 Health/Check: SERVING once ready and healthy.
func (r *Reporter) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	st := healthpb.HealthCheckResponse_NOT_SERVING
	if r.serving(ctx) {
		st = healthpb.HealthCheckResponse_SERVING
	}
	return &healthpb.HealthCheckResponse{Status: st}, nil
}

// Watch is not supported — health is point-in-time, polled by probes.
func (r *Reporter) Watch(_ *healthpb.HealthCheckRequest, _ healthpb.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "health: watch is not supported")
}

// Healthz is the HTTP liveness handler: 200 when the assembly is healthy.
func (r *Reporter) Healthz(w http.ResponseWriter, req *http.Request) {
	if r.live(req.Context()) {
		writeText(w, http.StatusOK, "ok")
		return
	}
	writeText(w, http.StatusServiceUnavailable, "unhealthy")
}

// Readyz is the HTTP readiness handler: 200 when ready and healthy.
func (r *Reporter) Readyz(w http.ResponseWriter, req *http.Request) {
	if r.serving(req.Context()) {
		writeText(w, http.StatusOK, "ready")
		return
	}
	writeText(w, http.StatusServiceUnavailable, "not ready")
}

func writeText(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(msg + "\n"))
}
