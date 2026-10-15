package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func probe(h http.HandlerFunc) int {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	return rec.Code
}

func grpcStatus(t *testing.T, r *Reporter) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	resp, err := r.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	return resp.GetStatus()
}

func TestReadinessLifecycle(t *testing.T) {
	r := New(func(context.Context) bool { return true })

	// Before MarkReady: live but not ready.
	if code := probe(r.Healthz); code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", code)
	}
	if code := probe(r.Readyz); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before ready = %d, want 503", code)
	}
	if st := grpcStatus(t, r); st != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("grpc Check before ready = %v, want NOT_SERVING", st)
	}

	r.MarkReady()

	if code := probe(r.Readyz); code != http.StatusOK {
		t.Fatalf("readyz after ready = %d, want 200", code)
	}
	if st := grpcStatus(t, r); st != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("grpc Check after ready = %v, want SERVING", st)
	}
}

func TestUnhealthyFailsProbes(t *testing.T) {
	r := New(func(context.Context) bool { return false })
	r.MarkReady()

	if code := probe(r.Healthz); code != http.StatusServiceUnavailable {
		t.Fatalf("healthz unhealthy = %d, want 503", code)
	}
	if code := probe(r.Readyz); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz unhealthy = %d, want 503", code)
	}
	if st := grpcStatus(t, r); st != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("grpc Check unhealthy = %v, want NOT_SERVING", st)
	}
}
