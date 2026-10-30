// Package serving is the serving runtime's network half: it takes the resolved node graph and
// stands up the single-port surface every role exposes.
//
// gRPC, every Service's REST surface (REST: calls through the same gRPC server), the web routers,
// and the health and probe endpoints are multiplexed on one port: over TLS when the role has a
// certificate, else cleartext HTTP/2 (h2c), for a network that secures it otherwise — a service
// mesh, a terminating load balancer — or loopback. Applications run under a suture supervision
// tree.
package serving

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/thejerf/suture/v4"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/Cidan/ok/internal/serve/health"
	"github.com/Cidan/ok/internal/serve/interceptor"
	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/serve/node"
)

// DefaultAddr is the single port a role serves everything on.
const DefaultAddr = ":8472"

// MetricsAddr is where an installation's roles serve their metrics: a port of its own, so that
// only the scraper need be let in.
const MetricsAddr = ":8473"

const shutdownGrace = 15 * time.Second

// DefaultStopTimeout is how long Applications have to return once Serve's context ends,
// unless Config.StopTimeout says otherwise.
const DefaultStopTimeout = 10 * time.Second

// Config is the fully-resolved input to Serve: every node in the graph, the
// interceptor chain (auth + application), the health reporter, and the listen
// address.
type Config struct {
	// StopTimeout is how long every Application has to return once ctx ends: Serve waits
	// for them that long, then abandons any still running. 0 means DefaultStopTimeout.
	StopTimeout time.Duration
	Nodes       []node.Node
	Chain       *interceptor.Chain
	Health      *health.Reporter
	Addr        string // defaults to DefaultAddr
	// Reflection enables gRPC server reflection, which exposes the services' schemas: for
	// development.
	Reflection bool
	// Listener, when set, is served instead of listening on Addr — mainly so tests
	// can bind an ephemeral port and know it up front.
	Listener net.Listener
	// Local, when set, is served gRPC alone, besides: the process's own calls (inproc.Listener).
	Local net.Listener
	// MaxRecvMsgSize and MaxSendMsgSize bound the gRPC server's messages, in bytes; 0 is
	// gRPC's default.
	MaxRecvMsgSize, MaxSendMsgSize int
	// MetricsAddr is where the binary's metrics (metrics.Handler) are served; "" serves them
	// nowhere unless MetricsListener is set, which is served instead.
	MetricsAddr     string
	MetricsListener net.Listener
	// Profiling serves Go's profiles (/debug/pprof/) beside the metrics.
	Profiling bool
	// TLS, when set, serves the port over TLS (HTTP/1.1 and HTTP/2); without it the port is
	// cleartext, for a network that secures it otherwise or for loopback.
	TLS *tls.Config
	// Middleware, when set, wraps every web server's routes: how a binary resolves web requests'
	// callers. REST is not wrapped: its calls pass through the gRPC interceptors, which resolve
	// them, and a credential's proof is good for one check.
	Middleware func(http.Handler) http.Handler
}

// readiness is the reporter ok_ready samples: the last one Serve was given.
var readiness atomic.Pointer[health.Reporter]

func init() {
	metrics.NewGaugeFunc("ok_ready", "1 once the binary's setup is done and it serves, 0 before.", func(report func(float64, ...string)) {
		if r := readiness.Load(); r != nil {
			v := 0.0
			if r.Ready() {
				v = 1
			}
			report(v)
		}
	})
}

var applicationRestarts = metrics.NewCounter("ok_application_restarts_total",
	"Times an application's Start returned or panicked and the supervisor started it again.", "application")

// Serve builds the gRPC server (registering every Service, and serving each over REST),
// mounts every WebServer on the shared router, supervises every Application,
// exposes health, multiplexes it all over h2c on cfg.Addr, and serves until ctx
// is cancelled — then gracefully shuts down. It returns nil on a clean shutdown
// or the first fatal serving error.
func Serve(ctx context.Context, cfg Config) error {
	addr := cfg.Addr
	if addr == "" {
		addr = DefaultAddr
	}
	chain := cfg.Chain
	if chain == nil {
		chain = interceptor.New()
	}

	rpcStats := metrics.NewServerHandler()
	serverOpts := append(chain.ServerOptions(), grpc.StatsHandler(rpcStats))
	if cfg.MaxRecvMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(cfg.MaxRecvMsgSize))
	}
	if cfg.MaxSendMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxSendMsgSize(cfg.MaxSendMsgSize))
	}
	grpcSrv := grpc.NewServer(serverOpts...)
	if cfg.Reflection {
		reflection.Register(grpcSrv)
	}
	healthpb.RegisterHealthServer(grpcSrv, cfg.Health)
	builtin := grpcSrv.GetServiceInfo()
	readiness.Store(cfg.Health)

	router := chi.NewRouter()
	router.Use(metrics.HTTPMiddleware)

	var services, webservers int
	var apps []node.Application
	var front http.Handler
	for _, n := range cfg.Nodes {
		if f, ok := n.(node.Front); ok {
			if front != nil {
				return fmt.Errorf("serve: %s is a second front", n.Name())
			}
			front = f.Front()
		}
		if svc, ok := n.(node.Service); ok {
			if err := svc.RegisterRPC(ctx, grpcSrv); err != nil {
				return fmt.Errorf("serve: register service %s: %w", n.Name(), err)
			}
			services++
		}
		if ws, ok := n.(node.WebServer); ok {
			sub := chi.NewRouter()
			if _, own := n.(node.SelfAuthenticated); cfg.Middleware != nil && !own {
				sub.Use(cfg.Middleware)
			}
			route, err := ws.Mount(sub)
			if err != nil {
				return fmt.Errorf("serve: mount webserver %s: %w", n.Name(), err)
			}
			router.Mount(route, sub)
			webservers++
		}
		if app, ok := n.(node.Application); ok {
			apps = append(apps, app)
		}
	}

	if front != nil && (services > 0 || webservers > 0) {
		return errors.New("serve: a front serves alone: no service or web server beside it")
	}
	rpcStats.Serve(grpcSrv.GetServiceInfo())

	router.Get("/healthz", cfg.Health.Healthz)
	router.Get("/readyz", cfg.Health.Readyz)
	// Every service the nodes registered is served over REST too, through the same
	// server and so the same interceptors (rest.go) — but not the runtime's own health and
	// reflection services. Each service's REST is routed at its own prefix,
	// /<package>.<Service>/, which no web route can mean otherwise: a static prefix outranks
	// a WebServer mounted at the site root, so a site that claims every path cannot shadow
	// it. Anything else no web route or probe claims falls to REST as well (NotFound), which
	// answers it as an unknown method.
	var served []string
	for name := range grpcSrv.GetServiceInfo() {
		if _, ok := builtin[name]; !ok {
			served = append(served, name)
		}
	}
	sort.Strings(served)
	rest, err := newREST(grpcSrv, served)
	if err != nil {
		return err
	}
	defer rest.Close()
	for _, name := range served {
		router.Handle("/"+name+"/*", rest)
	}
	router.NotFound(rest.ServeHTTP)

	// stopped is closed once every Application has returned, or been abandoned, after ctx ends.
	stopped := make(chan struct{})
	if len(apps) > 0 {
		stopTimeout := cfg.StopTimeout
		if stopTimeout <= 0 {
			stopTimeout = DefaultStopTimeout
		}
		sup := suture.New("ok-applications", suture.Spec{
			Timeout: stopTimeout,
			EventHook: func(e suture.Event) {
				if t, ok := e.(suture.EventServiceTerminate); ok && t.Restarting {
					applicationRestarts.With(t.ServiceName).Inc()
				}
				slog.Warn("serve: " + e.String())
			},
		})
		for _, app := range apps {
			sup.Add(&appService{app: app})
		}
		done := sup.ServeBackground(ctx)
		go func() { <-done; close(stopped) }()
	} else {
		close(stopped)
	}

	lis := cfg.Listener
	if lis == nil {
		var err error
		if lis, err = net.Listen("tcp", addr); err != nil {
			return fmt.Errorf("serve: listen %s: %w", addr, err)
		}
	}
	metricsLis := cfg.MetricsListener
	if metricsLis == nil && cfg.MetricsAddr != "" {
		var err error
		if metricsLis, err = net.Listen("tcp", cfg.MetricsAddr); err != nil {
			return fmt.Errorf("serve: listen %s for metrics: %w", cfg.MetricsAddr, err)
		}
	}

	// One port: HTTP/1.1 for REST, web, and probes, and HTTP/2 for gRPC — over TLS when the
	// binary has a certificate, else cleartext with HTTP/2 by prior knowledge (net/http's
	// unencrypted HTTP/2).
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	handler := grpcMux(grpcSrv, router)
	if front != nil {
		handler = frontMux(cfg.Health, front)
	}
	httpSrv := &http.Server{Handler: handler, Protocols: &protocols, ReadHeaderTimeout: 30 * time.Second}
	if cfg.TLS != nil {
		protocols.SetHTTP2(true)
		httpSrv.TLSConfig = cfg.TLS.Clone()
	} else {
		protocols.SetUnencryptedHTTP2(true)
	}

	serveErr := make(chan error, 3)
	if cfg.Local != nil {
		go func() {
			if err := grpcSrv.Serve(cfg.Local); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				serveErr <- err
			}
		}()
	}
	go func() {
		var err error
		if cfg.TLS != nil {
			err = httpSrv.ServeTLS(lis, "", "")
		} else {
			err = httpSrv.Serve(lis)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	var metricsSrv *http.Server
	if metricsLis != nil {
		var h http.Handler = metrics.Handler()
		if cfg.Profiling {
			mux := http.NewServeMux()
			mux.Handle("/metrics", h)
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
			h = mux
		}
		metricsSrv = &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := metricsSrv.Serve(metricsLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErr <- err
			}
		}()
		slog.Info("serve: metrics", "addr", metricsLis.Addr().String())
	}

	cfg.Health.MarkReady()
	slog.Info("serve: serving", "addr", lis.Addr().String(), "services", services,
		"webservers", webservers, "applications", len(apps))

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		slog.Info("serve: shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
		grpcSrv.Stop()
		if metricsSrv != nil {
			_ = metricsSrv.Shutdown(shutdownCtx)
		}
		// The process exits when Serve returns: an Application still finishing — giving
		// work back, closing what it holds — would be cut off.
		<-stopped
		return nil
	}
}

// grpcMux routes a request to the gRPC server when it is an HTTP/2 gRPC call — a POST
// (gRPC's one method) of an application/grpc body — and to the HTTP handler (REST + web +
// probes) otherwise: a GET carrying a gRPC client's content-type, as Traefik's ForwardAuth
// check of a gRPC call does, is a web request.
func grpcMux(grpcSrv *grpc.Server, httpHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && r.Method == http.MethodPost && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcSrv.ServeHTTP(w, r)
			return
		}
		httpHandler.ServeHTTP(w, r)
	})
}

// frontMux serves the probes, and sends every other request to front.
func frontMux(h *health.Reporter, front http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			switch r.URL.Path {
			case "/healthz":
				h.Healthz(w, r)
				return
			case "/readyz":
				h.Readyz(w, r)
				return
			}
		}
		front.ServeHTTP(w, r)
	})
}

// appService adapts a node.Application to a suture.Service so the supervision tree
// can run and restart it.
type appService struct{ app node.Application }

func (a *appService) Serve(ctx context.Context) error { return a.app.Start(ctx) }

// String names the application in the supervisor's events.
func (a *appService) String() string { return a.app.Name() }
