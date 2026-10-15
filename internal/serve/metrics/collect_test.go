package metrics

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	echo "github.com/Cidan/ok/internal/serve/serving/echotest"
)

type echoService struct{ echo.UnimplementedEchoServer }

func (echoService) Say(_ context.Context, req *echo.SayRequest) (*echo.SayResponse, error) {
	return &echo.SayResponse{Text: "echo: " + req.GetText()}, nil
}

func (echoService) Count(req *echo.CountRequest, stream grpc.ServerStreamingServer[echo.CountResponse]) error {
	for i := int32(1); i <= req.GetN(); i++ {
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
			return stream.SendAndClose(&echo.SayResponse{Words: words})
		}
		words = append(words, req.GetText())
	}
}

func (echoService) Fail(_ context.Context, req *echo.FailRequest) (*echo.SayResponse, error) {
	return nil, status.Error(codes.Code(req.GetCode()), req.GetMessage())
}

// value reads a series of the binary's registry.
func value(name string, labels ...string) Reading { return Default().Read(name, labels...) }

// serveEcho serves the echo service with a ServerHandler on lis.
func serveEcho(t *testing.T, lis net.Listener) {
	t.Helper()
	h := NewServerHandler()
	srv := grpc.NewServer(grpc.StatsHandler(h))
	echo.RegisterEchoServer(srv, echoService{})
	h.Serve(srv.GetServiceInfo())
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
}

// The server records every RPC by method and status, with its time, messages, and bytes; a method
// it does not serve is unknown; and a client dialed with DialOption records its side.
func TestRPCs(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveEcho(t, lis)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), DialOption())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := echo.NewEchoClient(conn)
	ctx := context.Background()

	const svc = "ok.echo.v1.Echo"
	okSay := value("ok_rpc_server_handled_total", svc, "Say", "OK", "grpc")
	notFound := value("ok_rpc_server_handled_total", svc, "Fail", "NotFound", "grpc")
	unknownRPCs := value("ok_rpc_server_handled_total", unknown, unknown, "Unimplemented", "grpc")
	countSent := value("ok_rpc_server_sent_messages", svc, "Count", "grpc")
	collectRecv := value("ok_rpc_server_received_messages", svc, "Collect", "grpc")
	clientSay := value("ok_rpc_client_handled_total", svc, "Say", "OK")
	clientNope := value("ok_rpc_client_handled_total", svc, "Nope", "Unimplemented")

	for range 2 {
		if _, err := client.Say(ctx, &echo.SayRequest{Text: "hello"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.Fail(ctx, &echo.FailRequest{Code: int32(codes.NotFound)}); status.Code(err) != codes.NotFound {
		t.Fatalf("Fail: %v", err)
	}
	stream, err := client.Count(ctx, &echo.CountRequest{N: 3})
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := stream.Recv(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	collect, err := client.Collect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []string{"a", "b"} {
		if err := collect.Send(&echo.SayRequest{Text: w}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := collect.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Invoke(ctx, "/"+svc+"/Nope", &echo.SayRequest{}, &echo.SayResponse{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("an unknown method: %v", err)
	}

	if d := value("ok_rpc_server_handled_total", svc, "Say", "OK", "grpc").Value - okSay.Value; d != 2 {
		t.Errorf("Say OK counted %v, want 2", d)
	}
	if d := value("ok_rpc_server_handled_total", svc, "Fail", "NotFound", "grpc").Value - notFound.Value; d != 1 {
		t.Errorf("Fail NotFound counted %v, want 1", d)
	}
	if d := value("ok_rpc_server_handled_total", unknown, unknown, "Unimplemented", "grpc").Value - unknownRPCs.Value; d != 1 {
		t.Errorf("the unknown method counted %v, want 1 under unknown", d)
	}
	if r := value("ok_rpc_server_handled_total", svc, "Nope", "Unimplemented", "grpc"); r.Found {
		t.Error("a method the server does not serve became a label")
	}
	sent := value("ok_rpc_server_sent_messages", svc, "Count", "grpc")
	if sent.Count-countSent.Count != 1 || sent.Sum-countSent.Sum != 3 {
		t.Errorf("Count's messages: %d calls, %v messages; want 1 and 3", sent.Count-countSent.Count, sent.Sum-countSent.Sum)
	}
	recv := value("ok_rpc_server_received_messages", svc, "Collect", "grpc")
	if recv.Sum-collectRecv.Sum != 2 {
		t.Errorf("Collect received %v messages, want 2", recv.Sum-collectRecv.Sum)
	}
	if b := value("ok_rpc_server_sent_bytes", svc, "Say", "grpc"); b.Count < 2 || b.Sum <= 0 {
		t.Errorf("Say's bytes: %+v", b)
	}
	if d := value("ok_rpc_server_duration_seconds", svc, "Say", "grpc"); d.Count < 2 {
		t.Errorf("Say's durations: %+v", d)
	}
	if g := value("ok_rpc_server_in_flight", svc, "Say", "grpc"); g.Value != 0 {
		t.Errorf("%v Say RPCs still in flight", g.Value)
	}
	if d := value("ok_rpc_client_handled_total", svc, "Say", "OK").Value - clientSay.Value; d != 2 {
		t.Errorf("the client counted %v Say, want 2", d)
	}
	if d := value("ok_rpc_client_handled_total", svc, "Nope", "Unimplemented").Value - clientNope.Value; d != 1 {
		t.Errorf("the client counted %v Nope, want 1: a client names its own methods", d)
	}
}

// RPCs over an in-memory connection — the REST surface — are labeled transport "rest".
func TestRESTTransport(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	serveEcho(t, lis)
	conn, err := grpc.NewClient("passthrough:///rest",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	before := value("ok_rpc_server_handled_total", "ok.echo.v1.Echo", "Say", "OK", "rest")
	if _, err := echo.NewEchoClient(conn).Say(context.Background(), &echo.SayRequest{Text: "x"}); err != nil {
		t.Fatal(err)
	}
	if d := value("ok_rpc_server_handled_total", "ok.echo.v1.Echo", "Say", "OK", "rest").Value - before.Value; d != 1 {
		t.Errorf("REST's Say counted %v, want 1", d)
	}
}

// The router's requests are recorded by route pattern, method, and status, with their bodies'
// bytes; a request no route matched is unmatched, and one a mounted router matched nothing inside
// is its mount's pattern.
func TestHTTPMiddleware(t *testing.T) {
	r := chi.NewRouter()
	r.Use(HTTPMiddleware)
	r.Get("/items/{id}", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("an item")) })
	sub := chi.NewRouter()
	sub.Post("/cl/{n}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusCreated) })
	r.Mount("/r/{repo}", sub)

	items := value("ok_http_requests_total", "/items/{id}", "GET", "200")
	cls := value("ok_http_requests_total", "/r/{repo}/cl/{n}", "POST", "201")
	inMount := value("ok_http_requests_total", "/r/{repo}/*", "GET", "404")
	missing := value("ok_http_requests_total", unmatched, "GET", "404")
	odd := value("ok_http_requests_total", unmatched, "OTHER", "405")
	body := value("ok_http_response_bytes", "/items/{id}", "GET")
	for _, c := range []struct{ method, path string }{
		{"GET", "/items/1"}, {"GET", "/items/2"}, {"POST", "/r/acme/cl/7"}, {"GET", "/r/acme/nothing"},
		{"GET", "/nothing/here/at/all"}, {"BREW", "/items/1"},
	} {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(c.method, c.path, nil))
	}
	for _, c := range []struct {
		before Reading
		labels []string
		want   float64
	}{
		{items, []string{"/items/{id}", "GET", "200"}, 2},
		{cls, []string{"/r/{repo}/cl/{n}", "POST", "201"}, 1},
		{inMount, []string{"/r/{repo}/*", "GET", "404"}, 1},
		{missing, []string{unmatched, "GET", "404"}, 1},
		{odd, []string{unmatched, "OTHER", "405"}, 1},
	} {
		if d := value("ok_http_requests_total", c.labels...).Value - c.before.Value; d != c.want {
			t.Errorf("%v counted %v, want %v", c.labels, d, c.want)
		}
	}
	if b := value("ok_http_response_bytes", "/items/{id}", "GET"); b.Sum-body.Sum != 14 {
		t.Errorf("the items' bodies were %v bytes, want 14", b.Sum-body.Sum)
	}
	if r := value("ok_http_requests_total", "/items/1", "GET", "200"); r.Found {
		t.Error("a path became a label")
	}
}
