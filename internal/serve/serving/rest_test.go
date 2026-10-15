package serving

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/serve/interceptor"
)

// post sends body to a REST path and returns the answer.
func post(t *testing.T, base, path, body string, headers ...string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	getStatus(t, base+"/healthz")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

// REST answers in grpc-gateway's JSON: lowerCamelCase names, unpopulated fields
// emitted, unknown fields ignored, an empty body an empty request.
func TestRESTUnary(t *testing.T) {
	addr, stop := startServer(t, echoService{})
	defer stop()
	base := "http://" + addr
	resp, body := post(t, base, "/ok.echo.v1.Echo/Say", `{"text":"hi","notAField":1}`)
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("Say: %d %s", resp.StatusCode, body)
	}
	var got map[string]any
	json.Unmarshal([]byte(body), &got)
	if got["text"] != "echo: hi" || got["wordCount"] != float64(0) {
		t.Errorf("Say answered %s", body)
	}
	if words, ok := got["words"].([]any); !ok || len(words) != 0 {
		t.Errorf("an unpopulated repeated field: %s", body)
	}
	if resp, body := post(t, base, "/ok.echo.v1.Echo/Say", ""); resp.StatusCode != 200 || !strings.Contains(body, `"text":"echo: "`) {
		t.Errorf("an empty body: %d %s", resp.StatusCode, body)
	}
}

// A WebServer that owns the site root claims every path but the services' own: their REST is
// still served, and everything else is the site's.
func TestRESTBesideASiteAtTheRoot(t *testing.T) {
	addr, stop := startServer(t, echoService{}, siteNode{})
	defer stop()
	base := "http://" + addr
	if resp, body := post(t, base, "/ok.echo.v1.Echo/Say", `{"text":"hi"}`); resp.StatusCode != 200 || !strings.Contains(body, `"text":"echo: hi"`) {
		t.Errorf("REST beside a site at the root: %d %s", resp.StatusCode, body)
	}
	if resp, body := post(t, base, "/ok.echo.v1.Echo/Nope", `{}`); resp.StatusCode != 404 || !strings.Contains(body, `"code"`) {
		t.Errorf("an unknown method of a served service: %d %s", resp.StatusCode, body)
	}
	if code, body := getStatus(t, base+"/some/page"); code != http.StatusTeapot || body != "site: /some/page" {
		t.Errorf("the site's own page: %d %q", code, body)
	}
}

// A REST call passes the same interceptors as a gRPC call: its headers are the call's
// metadata under their own names (as identity headers must be), and what an interceptor
// refuses is refused.
func TestRESTInterceptors(t *testing.T) {
	who := func(ctx context.Context) (string, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if v := md.Get("ok-dev-principal"); len(v) == 1 {
			grpc.SetHeader(ctx, metadata.Pairs("x-seen", v[0], "x-trace", strings.Join(md.Get("trace"), ",")))
			return v[0], nil
		}
		return "", status.Error(codes.PermissionDenied, "no caller")
	}
	chain := interceptor.New().
		UseUnary(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			if _, err := who(ctx); err != nil {
				return nil, err
			}
			return h(ctx, req)
		}).
		UseStream(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			if _, err := who(ss.Context()); err != nil {
				return err
			}
			return h(srv, ss)
		})
	addr, stop := startServerChain(t, chain, echoService{})
	defer stop()
	base := "http://" + addr

	resp, body := post(t, base, "/ok.echo.v1.Echo/Say", `{"text":"hi"}`, "Ok-Dev-Principal", "alice.human.example.com", "Trace", "t-1")
	if resp.StatusCode != 200 || resp.Header.Get("Grpc-Metadata-X-Seen") != "alice.human.example.com" || resp.Header.Get("Grpc-Metadata-X-Trace") != "t-1" {
		t.Errorf("an admitted call: %d %s, headers %v", resp.StatusCode, body, resp.Header)
	}
	resp, body = post(t, base, "/ok.echo.v1.Echo/Say", `{"text":"hi"}`)
	var st map[string]any
	json.Unmarshal([]byte(body), &st)
	if resp.StatusCode != 403 || st["code"] != float64(codes.PermissionDenied) || st["message"] != "no caller" {
		t.Errorf("a refused call: %d %s", resp.StatusCode, body)
	}
	if _, ok := st["details"]; !ok {
		t.Errorf("the error carries no details field: %s", body)
	}
	// No header is renamed into an identity key: a proxy strips the header it trusts, not
	// Grpc-Metadata-<it>, so honoring the prefix would let a caller forge one.
	for _, forged := range []string{"Grpc-Metadata-Ok-Dev-Principal", "grpc-metadata-ok-dev-principal"} {
		if resp, body := post(t, base, "/ok.echo.v1.Echo/Say", `{"text":"hi"}`, forged, "mallory.human.example.com"); resp.StatusCode != 403 {
			t.Errorf("%s reached the interceptor as a caller: %d %s", forged, resp.StatusCode, body)
		}
	}
	// The stream interceptor too.
	if resp, body := post(t, base, "/ok.echo.v1.Echo/Count", `{"n":2}`); resp.StatusCode != 403 {
		t.Errorf("a refused stream: %d %s", resp.StatusCode, body)
	}
	if resp, body := post(t, base, "/ok.echo.v1.Echo/Count", `{"n":2}`, "Ok-Dev-Principal", "alice.human.example.com"); resp.StatusCode != 200 || resp.Header.Get("Grpc-Metadata-X-Seen") != "alice.human.example.com" {
		t.Errorf("an admitted stream: %d %s", resp.StatusCode, body)
	}
}

// A server stream answers one {"result": …} per line, and an error after results one
// {"error": …}; an error before any is the answer's status.
func TestRESTServerStream(t *testing.T) {
	addr, stop := startServer(t, echoService{})
	defer stop()
	base := "http://" + addr
	lines := func(body string) []map[string]map[string]any {
		var out []map[string]map[string]any
		sc := bufio.NewScanner(strings.NewReader(body))
		for sc.Scan() {
			var l map[string]map[string]any
			if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
				t.Fatalf("line %q: %v", sc.Text(), err)
			}
			out = append(out, l)
		}
		return out
	}
	resp, body := post(t, base, "/ok.echo.v1.Echo/Count", `{"n":3}`)
	got := lines(body)
	if resp.StatusCode != 200 || len(got) != 3 || got[2]["result"]["n"] != float64(3) {
		t.Fatalf("Count 3: %d %s", resp.StatusCode, body)
	}
	resp, body = post(t, base, "/ok.echo.v1.Echo/Count", `{"n":100}`)
	got = lines(body)
	if resp.StatusCode != 200 || len(got) != 3 || got[1]["result"]["n"] != float64(2) || got[2]["error"]["code"] != float64(codes.ResourceExhausted) {
		t.Fatalf("Count failing midway: %d %s", resp.StatusCode, body)
	}
	if resp, body := post(t, base, "/ok.echo.v1.Echo/Count", `{"n":0}`); resp.StatusCode != 200 || body != "" {
		t.Errorf("an empty stream: %d %q", resp.StatusCode, body)
	}
}

func TestRESTRefusals(t *testing.T) {
	addr, stop := startServer(t, echoService{})
	defer stop()
	base := "http://" + addr
	for _, c := range []struct {
		name, path, body string
		status           int
		code             codes.Code
	}{
		{"unknown method", "/ok.echo.v1.Echo/Shout", "{}", 404, codes.NotFound},
		{"unknown service", "/nope.v1.Nope/Say", "{}", 404, codes.NotFound},
		{"the runtime's health", "/grpc.health.v1.Health/Check", "{}", 404, codes.NotFound},
		{"not JSON", "/ok.echo.v1.Echo/Say", "{", 400, codes.InvalidArgument},
		{"a wrong field type", "/ok.echo.v1.Echo/Say", `{"text":5}`, 400, codes.InvalidArgument},
		{"a client stream", "/ok.echo.v1.Echo/Collect", `{"text":"x"}`, 501, codes.Unimplemented},
		{"the service's error", "/ok.echo.v1.Echo/Fail", `{"code":6,"message":"it exists"}`, 409, codes.AlreadyExists},
		{"too large", "/ok.echo.v1.Echo/Say", `{"text":"` + strings.Repeat("x", maxRESTBody) + `"}`, 413, codes.ResourceExhausted},
	} {
		resp, body := post(t, base, c.path, c.body)
		var st map[string]any
		json.Unmarshal([]byte(body), &st)
		if resp.StatusCode != c.status || st["code"] != float64(c.code) {
			t.Errorf("%s: %d %.200s, want %d with code %s", c.name, resp.StatusCode, body, c.status, c.code)
		}
	}
	resp, err := http.Get(base + "/ok.echo.v1.Echo/Say")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 405 || resp.Header.Get("Allow") != "POST" {
		t.Errorf("a GET: %d, Allow %q", resp.StatusCode, resp.Header.Get("Allow"))
	}
}

func TestCallMetadata(t *testing.T) {
	h := http.Header{}
	h.Set("Ok-Dev-Principal", "alice.human.example.com")
	h.Set("X-Forwarded-Client-Cert", "By=spiffe://a;URI=spiffe://b")
	h.Set("Grpc-Metadata-Ok-Dev-Principal", "mallory.human.example.com")
	h.Set("Grpc-Timeout", "1S")
	h.Set("Content-Type", "application/json")
	h.Set("Trace-Bin", "AAEC") // base64 of 00 01 02
	h.Set("Broken-Bin", "not base64!")
	h.Set("Weird", "tab\there")
	md := callMetadata(h)
	want := metadata.MD{
		"ok-dev-principal":        {"alice.human.example.com"},
		"x-forwarded-client-cert": {"By=spiffe://a;URI=spiffe://b"},
		"trace-bin":               {"\x00\x01\x02"},
	}
	if len(md) != len(want) {
		t.Fatalf("metadata %v, want %v", md, want)
	}
	for k, v := range want {
		if got := md.Get(k); len(got) != 1 || got[0] != v[0] {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
}

// A browser's request from another site is refused before any service sees it: its session
// cookie must not make calls for a page elsewhere.
func TestRESTRefusesCrossSite(t *testing.T) {
	addr, stop := startServer(t, echoService{})
	defer stop()
	base := "http://" + addr
	for _, h := range [][]string{
		{"Sec-Fetch-Site", "cross-site"},
		{"Sec-Fetch-Site", "same-site"},
		{"Origin", "https://evil.example.org"},
	} {
		if resp, body := post(t, base, "/ok.echo.v1.Echo/Say", `{"text":"hi"}`, h...); resp.StatusCode != 403 {
			t.Errorf("%s: %s: %d %s", h[0], h[1], resp.StatusCode, body)
		}
	}
	for _, h := range [][]string{
		{"Sec-Fetch-Site", "same-origin"},
		{"Sec-Fetch-Site", "none"},
		{"Origin", base},
		{"X-Nothing", "x"},
	} {
		if resp, body := post(t, base, "/ok.echo.v1.Echo/Say", `{"text":"hi"}`, h...); resp.StatusCode != 200 {
			t.Errorf("%s: %s: %d %s", h[0], h[1], resp.StatusCode, body)
		}
	}
}
