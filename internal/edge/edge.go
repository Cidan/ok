// Package edge is ok's edge (docs/hosted.md, "Cells, as built", "The edge"): stateless replicas at
// ok's public address, in every region, in front of the cells. Each authenticates a request as the
// roles do, refusing a credential that fails, limits each principal's and each address's rate, and
// forwards it — its credentials untouched, as they are all metadata, so the role behind checks them
// again and trusts nothing new — to the role and cell that serve it: identity's to the directory,
// and another role's to the cell of the repository the request names. A cell that answers that
// another holds the repository (ok.v1.Moved, or a page's 421) has the request sent again there,
// whole.
package edge

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/config"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/metrics"
	"github.com/Cidan/ok/internal/serve/node"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Config is what an Edge serves with.
type Config struct {
	// Resolver authenticates requests as the roles do.
	Resolver *authn.Resolver
	// Cells is the directory's, which lists the cells and says where each repository is.
	Cells identitypb.CellsClient
	// Directory is where ok identity is reached, host:port.
	Directory string
	// UpstreamTLS is how the roles are reached: nil, cleartext HTTP/2 on a network the installation
	// trusts; otherwise TLS, its RootCAs the CA their certificates are under.
	UpstreamTLS *tls.Config
	// Edge is the configuration's edge section: its region, the client's address, and its limits.
	Edge config.Edge
}

// replayLimit bounds what a request's body is kept for, to send again to another cell; and so how
// far the edge reads ahead of the cell. A publish is split into batches well under it.
const replayLimit = 16 << 20

// maxAttempts bounds how many cells one request is sent to: a repository moving twice while a
// request is under way is told to try again.
const maxAttempts = 3

// Edge serves every request at ok's public address. It is a node.Front, and an Application that
// keeps nothing.
type Edge struct {
	cfg       Config
	cells     *cellList
	routes    *routes
	up        *upstreams
	principal *limiter
	address   *limiter
}

var (
	_ node.Front       = (*Edge)(nil)
	_ node.Application = (*Edge)(nil)
)

var requests = metrics.NewCounter("ok_edge_requests_total",
	"Requests the edge answered, by the role it sent them to and how: forwarded, resent (to another cell first), refused (a credential that failed), limited, or unavailable.",
	"role", "outcome")

// New is an edge over cfg.
func New(cfg Config) *Edge {
	return &Edge{cfg: cfg, cells: &cellList{client: cfg.Cells, region: cfg.Edge.Region}, routes: &routes{client: cfg.Cells, m: map[string]route{}},
		up: newUpstreams(cfg.UpstreamTLS), principal: newLimiter(cfg.Edge.Limits.Principal), address: newLimiter(cfg.Edge.Limits.Address)}
}

func (*Edge) Name() string                 { return "ok/edge" }
func (*Edge) Dependencies() []node.Node    { return nil }
func (*Edge) Setup(context.Context) error  { return nil }
func (*Edge) Healthy(context.Context) bool { return true }
func (e *Edge) Front() http.Handler        { return e }

// Start lists the cells once ahead of the first request, and lets the roles' connections go when
// ctx ends.
func (e *Edge) Start(ctx context.Context) error {
	if _, err := e.cells.get(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("edge: listing the cells", slog.Any("err", err))
	}
	<-ctx.Done()
	e.up.close()
	return nil
}

func (e *Edge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	isRPC := r.ProtoMajor == 2 && r.Method == http.MethodPost && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc")
	role := roleOf(r.URL.Path)
	fail := func(outcome string, code codes.Code, httpCode int, msg string) {
		requests.With(role, outcome).Inc()
		if isRPC {
			grpcFail(w, code, msg)
			return
		}
		if httpCode == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "1")
		}
		http.Error(w, msg, httpCode)
	}

	c, ok, err := e.cfg.Resolver.Resolve(r.Context(), metadataOf(r.Header), r.URL.Path)
	switch {
	case grpcstatus.Code(err) == codes.Unauthenticated:
		fail("refused", codes.Unauthenticated, http.StatusUnauthorized, grpcstatus.Convert(err).Message())
		return
	case err != nil:
		fail("unavailable", codes.Unavailable, http.StatusServiceUnavailable, grpcstatus.Convert(err).Message())
		return
	}
	now := time.Now()
	if ok && !e.principal.allow(c.ID, now) {
		fail("limited", codes.ResourceExhausted, http.StatusTooManyRequests, fmt.Sprintf("%s is making too many requests: slow down", c.Principal))
		return
	}
	if !e.address.allow(e.clientAddress(r), now) {
		fail("limited", codes.ResourceExhausted, http.StatusTooManyRequests, "too many requests from this address: slow down")
		return
	}

	cells, err := e.cells.get(r.Context())
	if err != nil {
		fail("unavailable", codes.Unavailable, http.StatusServiceUnavailable, err.Error())
		return
	}
	t := e.targetOf(r.Context(), r, cells)
	e.forward(w, r, t, cells, isRPC, fail)
}

// forward sends a request to its target and copies the answer back — again to another cell, while
// its body is held, when the cell answers that the other holds the repository.
func (e *Edge) forward(w http.ResponseWriter, r *http.Request, t target, cells *cellTable, isRPC bool,
	fail func(outcome string, code codes.Code, httpCode int, msg string)) {
	tp := newTap(r.Body, replayLimit)
	defer tp.stop()
	go tp.pump()
	outcome := "forwarded"
	for attempt := 1; ; attempt++ {
		addr := e.cfg.Directory
		if t.role != roleIdentity {
			var err error
			if addr, err = cells.address(t.cell, t.role); err != nil {
				fail("unavailable", codes.Unavailable, http.StatusServiceUnavailable, err.Error())
				return
			}
		}
		hostport, rt, scheme, err := e.up.pick(r.Context(), addr, t.repo)
		if err != nil {
			fail("unavailable", codes.Unavailable, http.StatusServiceUnavailable, err.Error())
			return
		}
		body, ok := tp.reader()
		if !ok {
			fail("unavailable", codes.Unavailable, http.StatusServiceUnavailable,
				fmt.Sprintf("repository %s moved to cell %s while the request was under way: try again", t.key, t.cell))
			return
		}
		up := r.Clone(r.Context())
		up.RequestURI = ""
		up.URL.Scheme, up.URL.Host = scheme, hostport
		up.Body = body
		stripHopByHop(up.Header)
		if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
			up.Header.Set("X-Forwarded-For", prior+", "+e.clientAddress(r))
		} else {
			up.Header.Set("X-Forwarded-For", e.clientAddress(r))
		}
		resp, err := rt.RoundTrip(up)
		if err != nil {
			if r.Context().Err() != nil {
				return
			}
			fail("unavailable", codes.Unavailable, http.StatusBadGateway, fmt.Sprintf("reaching the %s: %v", t.role, err))
			return
		}
		// A call's status is in its trailers, which come after its messages: read to the first, or
		// the end, before deciding. The request's body flows to the role meanwhile.
		var first []byte
		var firstErr error
		if isRPC {
			first, firstErr = peek(resp.Body)
		}
		if id, cell, moved := movedOf(resp, isRPC, len(first) == 0 && errors.Is(firstErr, io.EOF)); moved && cell != t.cell {
			e.routes.moved(t.key, id, cell)
			if attempt < maxAttempts && tp.replayable() {
				resp.Body.Close()
				body.Close()
				t.cell, outcome = cell, "resent"
				if id != 0 {
					t.repo = id
				}
				continue
			}
			if isRPC {
				resp.Body.Close()
				fail("unavailable", codes.Unavailable, http.StatusServiceUnavailable,
					fmt.Sprintf("the repository moved to cell %s while the request was under way: try again", cell))
				return
			}
			// A page's 421 goes to the browser, which asks again on a new connection.
		}
		tp.commit()
		requests.With(t.role, outcome).Inc()
		copyResponse(w, resp, isRPC, first, firstErr)
		return
	}
}

// peek reads a body to its first bytes, or its end.
func peek(body io.Reader) ([]byte, error) {
	buf := make([]byte, 32<<10)
	for {
		n, err := body.Read(buf)
		if n > 0 || err != nil {
			return buf[:n], err
		}
	}
}

// movedOf is what a cell's answer says when it refuses a request about a repository another cell
// holds: a gRPC call's FAILED_PRECONDITION with an ok.v1.Moved detail and no message — in its
// trailers, which ended (an empty body) has read, or its headers, for a trailers-only answer; or a
// page's or REST call's 421, naming the cell.
func movedOf(resp *http.Response, isRPC, ended bool) (int64, string, bool) {
	if !isRPC {
		cell := resp.Header.Get(cells.Header)
		return 0, cell, resp.StatusCode == http.StatusMisdirectedRequest && cell != ""
	}
	st := resp.Header
	if st.Get("Grpc-Status") == "" && ended {
		st = resp.Trailer
	}
	if st.Get("Grpc-Status") != strconv.Itoa(int(codes.FailedPrecondition)) {
		return 0, "", false
	}
	b, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(st.Get("Grpc-Status-Details-Bin"), "="))
	if err != nil {
		return 0, "", false
	}
	s := &status.Status{}
	if proto.Unmarshal(b, s) != nil {
		return 0, "", false
	}
	for _, d := range s.GetDetails() {
		m := &okpb.Moved{}
		if d.MessageIs(m) && d.UnmarshalTo(m) == nil && m.GetCell() != "" {
			return m.GetRepositoryId(), m.GetCell(), true
		}
	}
	return 0, "", false
}

// grpcStatusHeaders are a gRPC call's status, which a trailers-only answer carries in its headers.
var grpcStatusHeaders = []string{"Grpc-Status", "Grpc-Message", "Grpc-Status-Details-Bin"}

// copyResponse copies a role's answer to the client: its headers, its body as it comes — first,
// which was read already, ending with firstErr, then the rest, each part flushed, for a stream —
// and its trailers. A gRPC call's trailers-only answer goes as headers and trailers, which every
// gRPC client reads.
func copyResponse(w http.ResponseWriter, resp *http.Response, isRPC bool, first []byte, firstErr error) {
	defer resp.Body.Close()
	h := w.Header()
	for k, vs := range resp.Header {
		h[k] = vs
	}
	stripHopByHop(h)
	status := map[string][]string{}
	if isRPC {
		for _, k := range grpcStatusHeaders {
			if vs, ok := h[k]; ok {
				status[k] = vs
				delete(h, k)
			}
		}
	}
	w.WriteHeader(resp.StatusCode)
	rc := http.NewResponseController(w)
	if len(first) > 0 {
		if _, err := w.Write(first); err != nil {
			return
		}
		rc.Flush()
	}
	buf := make([]byte, 32<<10)
	for err := firstErr; err == nil; {
		var n int
		n, err = resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			rc.Flush()
		}
		if err != nil && !errors.Is(err, io.EOF) {
			slog.Debug("edge: reading a role's answer", slog.Any("err", err))
		}
	}
	for k, vs := range resp.Trailer {
		h[http.TrailerPrefix+k] = vs
	}
	for k, vs := range status {
		h[http.TrailerPrefix+k] = vs
	}
}

// grpcFail answers a gRPC call with a status, as trailers.
func grpcFail(w http.ResponseWriter, code codes.Code, msg string) {
	h := w.Header()
	h.Set("Content-Type", "application/grpc")
	h.Set(http.TrailerPrefix+"Grpc-Status", strconv.Itoa(int(code)))
	h.Set(http.TrailerPrefix+"Grpc-Message", encodeGRPCMessage(msg))
	w.WriteHeader(http.StatusOK)
}

// encodeGRPCMessage percent-encodes a status message as gRPC's wire carries it.
func encodeGRPCMessage(msg string) string {
	var b strings.Builder
	for i := 0; i < len(msg); i++ {
		c := msg[i]
		if c >= ' ' && c <= '~' && c != '%' {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

// hopByHop are the headers that belong to one connection, not the request.
var hopByHop = []string{"Connection", "Keep-Alive", "Proxy-Connection", "Proxy-Authenticate", "Proxy-Authorization", "Transfer-Encoding", "Upgrade"}

func stripHopByHop(h http.Header) {
	for _, f := range h.Values("Connection") {
		for _, k := range strings.Split(f, ",") {
			h.Del(strings.TrimSpace(k))
		}
	}
	for _, k := range hopByHop {
		h.Del(k)
	}
}

// clientAddress is the address rate limits count a request by: the configured header a proxy in
// front writes, or the connection's.
func (e *Edge) clientAddress(r *http.Request) string {
	if h := e.cfg.Edge.AddressHeader; h != "" {
		if v := r.Header.Get(h); v != "" {
			first, _, _ := strings.Cut(v, ",")
			return strings.TrimSpace(first)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// metadataOf is a request's headers as the gRPC metadata authn reads: names lowercased, -bin
// values decoded.
func metadataOf(h http.Header) metadata.MD {
	md := metadata.MD{}
	for k, vs := range h {
		k = strings.ToLower(k)
		if strings.HasSuffix(k, "-bin") {
			for _, v := range vs {
				if b, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(v, "=")); err == nil {
					md.Append(k, string(b))
				}
			}
			continue
		}
		md.Append(k, vs...)
	}
	return md
}
