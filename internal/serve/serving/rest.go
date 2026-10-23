package serving

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/Cidan/ok/internal/cells"
)

// REST is every Service's JSON surface: POST /<package>.<Service>/<Method> with the
// request as JSON, answered with the response as JSON. It is a transport, not a second
// implementation: each request is made into a gRPC call on an in-memory connection to
// the role's own server, so it passes the same interceptor chain as any gRPC call — the
// caller authn resolves, and whatever else the chain enforces. No service writes REST code.
//
// The request's headers become the call's metadata under their own names, lower-cased, so
// a credential, a session cookie, or a trusted proxy's header reaches the interceptors
// exactly as it would on a gRPC call. No header is renamed into another key: grpc-gateway's
// Grpc-Metadata-<Key> convention would let a caller write a trusted header under a name a
// proxy does not strip, so grpc-* headers (gRPC's reserved names) are dropped instead. A
// browser's request from another site is refused (crossSite).
// The JSON is protojson's, as grpc-gateway wrote it: field names in lowerCamelCase,
// unpopulated fields emitted, unknown fields ignored. A server-streaming method answers
// one {"result": …} object per line, and an error mid-stream one {"error": …}; client
// and bidirectional streams are not served. Errors are google.rpc.Status objects under
// the HTTP status that corresponds to their code.
type REST struct {
	conn    *grpc.ClientConn
	methods map[string]restMethod // by "/<service>/<method>"
	lis     *bufconn.Listener
}

type restMethod struct {
	in, out      protoreflect.MessageType
	serverStream bool
	clientStream bool
}

// maxRESTBody bounds a request body, as gRPC's default bounds a message.
const maxRESTBody = 4 << 20

// newREST serves the named services' methods over REST through srv, which it also
// starts serving on an in-memory listener.
func newREST(srv *grpc.Server, services []string) (*REST, error) {
	r := &REST{methods: map[string]restMethod{}, lis: bufconn.Listen(1 << 20)}
	info := srv.GetServiceInfo()
	for _, name := range services {
		d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name))
		if err != nil {
			return nil, fmt.Errorf("serve: REST for service %s: its descriptor is not registered: %w", name, err)
		}
		sd, ok := d.(protoreflect.ServiceDescriptor)
		if !ok {
			return nil, fmt.Errorf("serve: REST for %s: not a service", name)
		}
		for _, m := range info[name].Methods {
			md := sd.Methods().ByName(protoreflect.Name(m.Name))
			if md == nil {
				return nil, fmt.Errorf("serve: REST for %s/%s: no such method in its descriptor", name, m.Name)
			}
			in, err := protoregistry.GlobalTypes.FindMessageByName(md.Input().FullName())
			if err != nil {
				return nil, fmt.Errorf("serve: REST for %s/%s: %w", name, m.Name, err)
			}
			out, err := protoregistry.GlobalTypes.FindMessageByName(md.Output().FullName())
			if err != nil {
				return nil, fmt.Errorf("serve: REST for %s/%s: %w", name, m.Name, err)
			}
			r.methods["/"+name+"/"+m.Name] = restMethod{in: in, out: out, serverStream: m.IsServerStream, clientStream: m.IsClientStream}
		}
	}
	go srv.Serve(r.lis)
	conn, err := grpc.NewClient("passthrough:///ok-rest",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return r.lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// An answer is bounded by the server's send limit, as it is for a gRPC client
		// that asks for it; REST cannot ask.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, err
	}
	r.conn = conn
	return r, nil
}

// Close ends the in-memory connection.
func (r *REST) Close() {
	r.conn.Close()
	r.lis.Close()
}

var (
	restMarshal   = protojson.MarshalOptions{EmitUnpopulated: true}
	restUnmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// notForwarded are request headers that describe the HTTP request itself, or that gRPC
// reserves, rather than anything a service reads.
var notForwarded = map[string]bool{
	"accept-encoding": true, "connection": true, "content-length": true, "content-type": true,
	"host": true, "keep-alive": true, "proxy-authorization": true, "proxy-connection": true,
	"te": true, "trailer": true, "transfer-encoding": true, "upgrade": true, "user-agent": true,
}

// crossSite reports whether a browser sent req from another site: Sec-Fetch-Site says so, or
// Origin names another host. A browser's session cookie reaches REST, so a page elsewhere must not
// be able to make calls with it. Clients that are not browsers send neither header.
func crossSite(req *http.Request) bool {
	switch req.Header.Get("Sec-Fetch-Site") {
	case "", "same-origin", "none":
	default:
		return true
	}
	origin := req.Header.Get("Origin")
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	return err != nil || u.Host != req.Host
}

// callMetadata is the metadata a request's headers become: each under its own name.
// A -bin header carries base64, as it does on the wire; its value is decoded, and one
// that does not decode is dropped.
func callMetadata(h http.Header) metadata.MD {
	md := metadata.MD{}
	for k, vs := range h {
		key := strings.ToLower(k)
		if notForwarded[key] || strings.HasPrefix(key, "grpc-") || strings.HasPrefix(key, ":") {
			continue
		}
		for _, v := range vs {
			if strings.HasSuffix(key, "-bin") {
				b, err := base64.StdEncoding.DecodeString(v)
				if err != nil {
					b, err = base64.RawStdEncoding.DecodeString(v)
				}
				if err == nil {
					md.Append(key, string(b))
				}
				continue
			}
			if printable(v) {
				md.Append(key, v)
			}
		}
	}
	return md
}

// printable reports whether v may be the value of a text metadata key.
func printable(v string) bool {
	for i := 0; i < len(v); i++ {
		if v[i] < 0x20 || v[i] > 0x7e {
			return false
		}
	}
	return true
}

func (r *REST) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	m, ok := r.methods[req.URL.Path]
	if !ok {
		writeStatus(w, status.New(codes.NotFound, "Not Found"))
		return
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeStatusCode(w, http.StatusMethodNotAllowed, status.New(codes.Unimplemented, "Method Not Allowed: "+req.URL.Path+" takes POST"))
		return
	}
	if crossSite(req) {
		writeStatus(w, status.New(codes.PermissionDenied, "a request from another site is refused"))
		return
	}
	if m.clientStream {
		writeStatus(w, status.New(codes.Unimplemented, req.URL.Path+" streams its requests; call it over gRPC"))
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxRESTBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeStatusCode(w, http.StatusRequestEntityTooLarge, status.Newf(codes.ResourceExhausted, "the request exceeds %d bytes", maxRESTBody))
			return
		}
		writeStatus(w, status.New(codes.InvalidArgument, err.Error()))
		return
	}
	in := m.in.New().Interface()
	if len(strings.TrimSpace(string(body))) > 0 {
		if err := restUnmarshal.Unmarshal(body, in); err != nil {
			writeStatus(w, status.Newf(codes.InvalidArgument, "the request is not a %s: %v", m.in.Descriptor().FullName(), err))
			return
		}
	}
	ctx := metadata.NewOutgoingContext(req.Context(), callMetadata(req.Header))
	var header metadata.MD
	if !m.serverStream {
		out := m.out.New().Interface()
		err := r.conn.Invoke(ctx, req.URL.Path, in, out, grpc.Header(&header))
		writeHeaderMetadata(w, header)
		if err != nil {
			writeStatus(w, status.Convert(err))
			return
		}
		writeMessage(w, http.StatusOK, out)
		return
	}
	stream, err := r.conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, req.URL.Path)
	if err == nil {
		err = stream.SendMsg(in)
	}
	if err == nil {
		err = stream.CloseSend()
	}
	if err != nil {
		writeStatus(w, status.Convert(err))
		return
	}
	first := true
	for {
		out := m.out.New().Interface()
		err := stream.RecvMsg(out)
		if first {
			if header, herr := stream.Header(); herr == nil {
				writeHeaderMetadata(w, header)
			}
			if err != nil && !errors.Is(err, io.EOF) {
				writeStatus(w, status.Convert(err))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			first = false
		}
		if errors.Is(err, io.EOF) {
			return
		}
		line := map[string]json.RawMessage{}
		if err != nil {
			b, _ := restMarshal.Marshal(status.Convert(err).Proto())
			line["error"] = b
		} else {
			b, merr := restMarshal.Marshal(out)
			if merr != nil {
				slog.Error("serve: REST: encoding a streamed response", slog.String("method", req.URL.Path), slog.Any("err", merr))
				return
			}
			line["result"] = b
		}
		b, _ := json.Marshal(line)
		w.Write(append(b, '\n'))
		http.NewResponseController(w).Flush()
		if err != nil {
			return
		}
	}
}

func writeHeaderMetadata(w http.ResponseWriter, md metadata.MD) {
	for k, vs := range md {
		for _, v := range vs {
			w.Header().Add("Grpc-Metadata-"+k, v)
		}
	}
}

func writeMessage(w http.ResponseWriter, code int, m proto.Message) {
	b, err := restMarshal.Marshal(m)
	if err != nil {
		writeStatus(w, status.Newf(codes.Internal, "encoding the response: %v", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
}

// writeStatus answers a failed call: a request about a repository another cell holds 421,
// Misdirected Request, naming the cell, for the edge in front to send it there.
func writeStatus(w http.ResponseWriter, s *status.Status) {
	if m, ok := cells.MovedOf(s.Err()); ok {
		w.Header().Set(cells.Header, m.GetCell())
		writeStatusCode(w, http.StatusMisdirectedRequest, s)
		return
	}
	writeStatusCode(w, httpStatus(s.Code()), s)
}

func writeStatusCode(w http.ResponseWriter, code int, s *status.Status) {
	writeMessage(w, code, s.Proto())
}

// httpStatus is the HTTP status of a gRPC code (as grpc-gateway maps it).
func httpStatus(c codes.Code) int {
	switch c {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		return 499
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
