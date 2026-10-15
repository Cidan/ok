package serving

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/serve/health"
	"github.com/Cidan/ok/internal/serve/interceptor"
	"github.com/Cidan/ok/internal/serve/node"
	echo "github.com/Cidan/ok/internal/serve/serving/echotest"
)

// --- test nodes ---

type echoService struct{ echo.UnimplementedEchoServer }

func (echoService) Name() string                 { return "//test/echo" }
func (echoService) Dependencies() []node.Node    { return nil }
func (echoService) Healthy(context.Context) bool { return true }
func (echoService) Setup(context.Context) error  { return nil }

func (s echoService) RegisterRPC(_ context.Context, reg grpc.ServiceRegistrar) error {
	echo.RegisterEchoServer(reg, s)
	return nil
}

func (echoService) Say(_ context.Context, req *echo.SayRequest) (*echo.SayResponse, error) {
	return &echo.SayResponse{Text: "echo: " + req.GetText()}, nil
}

func (echoService) Count(req *echo.CountRequest, stream grpc.ServerStreamingServer[echo.CountResponse]) error {
	for i := int32(1); i <= req.GetN(); i++ {
		if req.GetN() >= 100 && i == 3 {
			return status.Error(codes.ResourceExhausted, "counted too far")
		}
		if err := stream.Send(&echo.CountResponse{N: i}); err != nil {
			return err
		}
	}
	return nil
}

func (echoService) Collect(stream grpc.ClientStreamingServer[echo.SayRequest, echo.SayResponse]) error {
	var words []string
	for {
		req, err := stream.Recv()
		if err != nil {
			return stream.SendAndClose(&echo.SayResponse{Words: words, WordCount: int32(len(words))})
		}
		words = append(words, req.GetText())
	}
}

func (echoService) Fail(_ context.Context, req *echo.FailRequest) (*echo.SayResponse, error) {
	return nil, status.Error(codes.Code(req.GetCode()), req.GetMessage())
}

type webNode struct{}

func (webNode) Name() string                 { return "//test/web" }
func (webNode) Dependencies() []node.Node    { return nil }
func (webNode) Healthy(context.Context) bool { return true }
func (webNode) Setup(context.Context) error  { return nil }
func (webNode) Mount(r *chi.Mux) (string, error) {
	r.Get("/ping", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("pong")) })
	return "/web", nil
}

// siteNode owns the site root, as a web app does: every path is its.
type siteNode struct{}

func (siteNode) Name() string                 { return "//test/site" }
func (siteNode) Dependencies() []node.Node    { return nil }
func (siteNode) Healthy(context.Context) bool { return true }
func (siteNode) Setup(context.Context) error  { return nil }
func (siteNode) Mount(r *chi.Mux) (string, error) {
	r.HandleFunc("/*", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("site: " + req.URL.Path))
	})
	return "/", nil
}

type appNode struct{ started chan struct{} }

func (a *appNode) Name() string                 { return "//test/app" }
func (a *appNode) Dependencies() []node.Node    { return nil }
func (a *appNode) Healthy(context.Context) bool { return true }
func (a *appNode) Setup(context.Context) error  { return nil }
func (a *appNode) Start(ctx context.Context) error {
	close(a.started)
	<-ctx.Done()
	return ctx.Err()
}

// --- harness ---

func startServer(t *testing.T, nodes ...node.Node) (addr string, stop func()) {
	return startServerChain(t, interceptor.New(), nodes...)
}

func startServerChain(t *testing.T, chain *interceptor.Chain, nodes ...node.Node) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, Config{
			Nodes:    nodes,
			Chain:    chain,
			Health:   health.New(func(context.Context) bool { return true }),
			Listener: lis,
		})
	}()
	return lis.Addr().String(), func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve returned error: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Error("Serve did not shut down in time")
		}
	}
}

func getStatus(t *testing.T, url string) (int, string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp.StatusCode, string(body)
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET %s: %v", url, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- tests ---

func TestServeGRPCOverH2C(t *testing.T) {
	addr, stop := startServer(t, echoService{})
	defer stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := echo.NewEchoClient(conn).Say(ctx, &echo.SayRequest{Text: "hi"})
	if err != nil {
		t.Fatalf("Say: %v", err)
	}
	if resp.GetText() != "echo: hi" {
		t.Fatalf("Say = %q, want %q", resp.GetText(), "echo: hi")
	}
}

// The server's message limits are the configured ones: a message past gRPC's 4 MiB default is
// refused by default and taken once the limit allows it.
func TestServeMaxMessageSize(t *testing.T) {
	big := strings.Repeat("x", 5<<20)
	for _, c := range []struct {
		name string
		recv int
		want codes.Code
	}{{"default", 0, codes.ResourceExhausted}, {"raised", 8 << 20, codes.OK}} {
		t.Run(c.name, func(t *testing.T) {
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- Serve(ctx, Config{Nodes: []node.Node{echoService{}}, Chain: interceptor.New(),
					Health: health.New(func(context.Context) bool { return true }), Listener: lis,
					MaxRecvMsgSize: c.recv, MaxSendMsgSize: c.recv})
			}()
			defer func() { cancel(); <-done }()
			conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20), grpc.MaxCallSendMsgSize(16<<20)))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer callCancel()
			resp, err := echo.NewEchoClient(conn).Say(callCtx, &echo.SayRequest{Text: big}, grpc.WaitForReady(true))
			if status.Code(err) != c.want {
				t.Fatalf("a 5 MiB message: %v, want %v", status.Code(err), c.want)
			}
			if c.want == codes.OK && len(resp.GetText()) != len(big)+len("echo: ") {
				t.Fatalf("answer of %d bytes", len(resp.GetText()))
			}
		})
	}
}

func TestServeHealthEndpoints(t *testing.T) {
	addr, stop := startServer(t, echoService{})
	defer stop()

	for _, path := range []string{"/healthz", "/readyz"} {
		code, _ := getStatus(t, "http://"+addr+path)
		if code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, code)
		}
	}
}

func TestServeWebRoute(t *testing.T) {
	addr, stop := startServer(t, echoService{}, webNode{})
	defer stop()

	code, body := getStatus(t, "http://"+addr+"/web/ping")
	if code != http.StatusOK || body != "pong" {
		t.Fatalf("GET /web/ping = %d %q, want 200 \"pong\"", code, body)
	}
}

func TestServeApplicationRuns(t *testing.T) {
	started := make(chan struct{})
	_, stop := startServer(t, &appNode{started: started})
	defer stop()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("application Start was not invoked")
	}
}

// stopper is an Application that takes a while to stop once told to — giving its work back —
// or, with ignore set, never stops on its own.
type stopper struct {
	started chan struct{}
	finish  time.Duration
	ignore  chan struct{}
	stopped chan struct{}
}

func (a *stopper) Name() string                 { return "//test/stopper" }
func (a *stopper) Dependencies() []node.Node    { return nil }
func (a *stopper) Healthy(context.Context) bool { return true }
func (a *stopper) Setup(context.Context) error  { return nil }
func (a *stopper) Start(ctx context.Context) error {
	close(a.started)
	<-ctx.Done()
	if a.ignore != nil {
		<-a.ignore
	}
	time.Sleep(a.finish)
	close(a.stopped)
	return nil
}

// serveUntilStopped serves app with a stop timeout until it starts, then ends Serve's context
// and returns how long Serve took to return.
func serveUntilStopped(t *testing.T, app *stopper, stopTimeout time.Duration) time.Duration {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, Config{Nodes: []node.Node{app}, Health: health.New(func(context.Context) bool { return true }),
			Listener: lis, StopTimeout: stopTimeout})
	}()
	select {
	case <-app.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the application did not start")
	}
	began := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Serve did not return")
	}
	return time.Since(began)
}

// Serve returns only once its Applications have: one still giving its work back when the
// binary is told to stop finishes before the process can exit.
func TestShutdownWaitsForApplications(t *testing.T) {
	app := &stopper{started: make(chan struct{}), finish: 500 * time.Millisecond, stopped: make(chan struct{})}
	took := serveUntilStopped(t, app, 5*time.Second)
	select {
	case <-app.stopped:
	default:
		t.Fatalf("Serve returned after %s, before the application stopped", took)
	}
}

// An Application that does not stop within the stop timeout is abandoned: Serve returns.
func TestShutdownAbandonsAnApplicationPastItsTimeout(t *testing.T) {
	app := &stopper{started: make(chan struct{}), ignore: make(chan struct{}), stopped: make(chan struct{})}
	defer close(app.ignore)
	if took := serveUntilStopped(t, app, 300*time.Millisecond); took > 5*time.Second {
		t.Fatalf("Serve took %s to give up on an application past its 300ms stop timeout", took)
	}
	select {
	case <-app.stopped:
		t.Fatal("the application stopped though it ignores its context")
	default:
	}
}

// A call made while the server shuts down gets an answer a gRPC client can act on: it is
// served, or refused as Unavailable (the connection's GOAWAY, then the closed listener) —
// never an empty HTTP response, which a client reads as a malformed reply (Unknown) and
// cannot tell is retryable. An RPC in flight holds the connection open through it.
func TestShutdownRefusesNewCalls(t *testing.T) {
	addr, stop := startServer(t, echoService{})
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer conn.Close()
	client := echo.NewEchoClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.Say(ctx, &echo.SayRequest{Text: "up"}); err != nil {
		t.Fatalf("Say before shutdown: %v", err)
	}
	inflight, err := client.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if err := inflight.Send(&echo.SayRequest{Text: "held"}); err != nil {
		t.Fatalf("Collect Send: %v", err)
	}

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	time.Sleep(300 * time.Millisecond) // the server is now shutting down
	for i := range 5 {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := client.Say(callCtx, &echo.SayRequest{Text: "during"})
		callCancel()
		if err != nil && status.Code(err) != codes.Unavailable {
			t.Errorf("call %d during shutdown: %v (code %s), want served or Unavailable", i, err, status.Code(err))
		}
	}
	<-stopped
}

// A role serves its metrics on a port of its own — never on its serving port — and they record its gRPC
// and REST calls, its web routes, and that it is ready.
func TestServeMetrics(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	metricsLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, Config{
			Nodes:           []node.Node{echoService{}, webNode{}},
			Health:          health.New(func(context.Context) bool { return true }),
			Listener:        lis,
			MetricsListener: metricsLis,
		})
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()
	addr := lis.Addr().String()
	if code, _ := getStatus(t, "http://"+addr+"/web/ping"); code != http.StatusOK {
		t.Fatalf("GET /web/ping: %d", code)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := echo.NewEchoClient(conn).Say(context.Background(), &echo.SayRequest{Text: "hi"}); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+addr+"/ok.echo.v1.Echo/Say", "application/json", strings.NewReader(`{"text":"rest"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	code, body := getStatus(t, "http://"+metricsLis.Addr().String()+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("GET /metrics: %d", code)
	}
	for _, want := range []string{
		`ok_rpc_server_handled_total{grpc_service="ok.echo.v1.Echo",grpc_method="Say",grpc_code="OK",transport="grpc"} `,
		`ok_rpc_server_handled_total{grpc_service="ok.echo.v1.Echo",grpc_method="Say",grpc_code="OK",transport="rest"} `,
		`ok_http_requests_total{route="/web/ping",method="GET",code="200"} `,
		`ok_http_requests_total{route="/ok.echo.v1.Echo/*",method="POST",code="200"} `,
		"ok_ready 1\n",
		"go_goroutines ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	if code, _ := getStatus(t, "http://"+addr+"/metrics"); code == http.StatusOK {
		t.Error("the serving port serves /metrics")
	}
}

// Only an HTTP/2 POST of an application/grpc body is a gRPC call: a GET carrying a gRPC client's
// content-type — Traefik's ForwardAuth check of a gRPC call — and an HTTP/1.1 request reach the
// HTTP handler.
func TestGRPCMuxMethod(t *testing.T) {
	var to string
	grpcSrv := grpc.NewServer()
	h := grpcMux(grpcSrv, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { to = "http" }))
	for _, c := range []struct {
		method string
		major  int
		want   string
	}{
		{http.MethodGet, 2, "http"},
		{http.MethodGet, 1, "http"},
		{http.MethodPost, 1, "http"},
		{http.MethodPost, 2, "grpc"},
	} {
		to = "grpc"
		r := httptest.NewRequest(c.method, "/ok.example.v1.Things/Get", nil)
		r.ProtoMajor = c.major
		r.Header.Set("Content-Type", "application/grpc")
		h.ServeHTTP(httptest.NewRecorder(), r)
		if to != c.want {
			t.Errorf("HTTP/%d %s went to %s, want %s", c.major, c.method, to, c.want)
		}
	}
}

// The web middleware wraps the web servers' routes and never REST, whose calls the interceptors
// authenticate: a credential's proof is good for one check, so checking a REST call twice would
// refuse it.
func TestMiddlewareWrapsWebOnly(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	var mu sync.Mutex
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, Config{
			Nodes:    []node.Node{echoService{}, siteNode{}},
			Chain:    interceptor.New(),
			Health:   health.New(func(context.Context) bool { return true }),
			Listener: lis,
			Middleware: func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					seen = append(seen, r.URL.Path)
					mu.Unlock()
					next.ServeHTTP(w, r)
				})
			},
		})
	}()
	defer func() { cancel(); <-done }()
	base := "http://" + lis.Addr().String()
	if resp, body := post(t, base, "/ok.echo.v1.Echo/Say", `{"text":"hi"}`); resp.StatusCode != 200 {
		t.Fatalf("REST: %d %s", resp.StatusCode, body)
	}
	resp, err := http.Get(base + "/page")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(seen, []string{"/page"}) {
		t.Errorf("the middleware saw %v, want only the web route", seen)
	}
}
