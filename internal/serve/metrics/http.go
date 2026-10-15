package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// What the runtime's router answers: web routes, REST, and probes, by the route that matched — its
// pattern, never the path.
var (
	httpRequests = NewCounter("ok_http_requests_total",
		"HTTP requests the router answered, by route, method, and status.", "route", "method", "code")
	httpDuration = NewHistogram("ok_http_duration_seconds",
		"How long the router took to answer each request.", Seconds, "route", "method")
	httpResponseBytes = NewHistogram("ok_http_response_bytes",
		"Bytes of each response's body.", Bytes, "route", "method")
)

// unmatched labels a request no route matched.
const unmatched = "unmatched"

// httpMethod is a request's method as a label: one HTTP defines, or OTHER.
func httpMethod(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return m
	}
	return "OTHER"
}

// HTTPMiddleware records every request a chi router answers. Install it on the router before
// its routes (chi's Use).
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		route := unmatched
		if rc := chi.RouteContext(r.Context()); rc != nil {
			if p := rc.RoutePattern(); p != "" {
				route = p
			}
		}
		method := httpMethod(r.Method)
		code := ww.Status()
		if code == 0 {
			code = http.StatusOK
		}
		httpRequests.With(route, method, strconv.Itoa(code)).Inc()
		httpDuration.With(route, method).Since(start)
		httpResponseBytes.With(route, method).Observe(float64(ww.BytesWritten()))
	})
}
