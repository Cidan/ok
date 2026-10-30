// Package dial connects ok's roles, and its command-line tools, to the services they call, as the
// principal a credential proves: every call carries the credential's certificate and a fresh
// proof (package credential). A development principal names the caller instead, only to a service
// on loopback, which a development server accepts.
package dial

import (
	"context"
	"crypto"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/serve/authn"
	"github.com/Cidan/ok/internal/serve/metrics"
)

// MaxMessage bounds a message either way: files travel in large frames.
const MaxMessage = 64 << 20

// Credential is what a caller proves itself with: its certificate (DER) and the key it was issued
// for, current at the time of the call.
type Credential interface {
	Current(ctx context.Context) (cert []byte, key crypto.Signer, err error)
}

// Static is a credential that does not change.
type Static struct {
	Cert []byte
	Key  crypto.Signer
}

func (s Static) Current(context.Context) ([]byte, crypto.Signer, error) { return s.Cert, s.Key, nil }

// Options are how to reach a service.
type Options struct {
	// Credential proves the caller; nil calls as no one (a service open to anyone).
	Credential Credential
	// DevPrincipal names the caller to a development server on loopback instead.
	DevPrincipal string
	// Token, when set, is a bearer token each call presents instead of a credential: a robot's
	// issuer's token, with which it certifies a key (identity's Credentials.Certify).
	Token func(ctx context.Context) (string, error)
	// TLS secures the connection; nil is plaintext, which only Insecure or a loopback address
	// allows.
	TLS *tls.Config
	// Insecure allows plaintext to an address that is not loopback: a network the installation
	// trusts, such as a service mesh's.
	Insecure bool
	// InProcess, when set, reaches a role in this process over its listener (inproc.Listener's
	// Dial), each call naming the role's robot it is, InProcessRobot (authn.InProcessKey): no
	// credential, no TLS — nothing leaves the process. addr is not dialed.
	InProcess      func(ctx context.Context, addr string) (net.Conn, error)
	InProcessRobot string
	// WaitForReady has a call wait for a connection, for as long as its context allows, rather than
	// fail while there is none — while the service's replicas roll, say; the call is sent once,
	// when there is one. For a client with its own deadline that would rather wait a restart out:
	// a role calling another fails at once instead, and serves what it can without it.
	WaitForReady bool
}

// Dial connects to the service at addr.
func Dial(addr string, o Options) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessage), grpc.MaxCallSendMsgSize(MaxMessage), grpc.WaitForReady(o.WaitForReady)),
		metrics.DialOption(),
	}
	if o.InProcess != nil {
		if o.InProcessRobot == "" {
			return nil, errors.New("dial: a call within the process names the role's robot it is")
		}
		opts = append(opts, grpc.WithContextDialer(o.InProcess), grpc.WithTransportCredentials(insecure.NewCredentials()))
		opts = append(opts, naming(authn.InProcessKey, o.InProcessRobot)...)
		return grpc.NewClient("passthrough:///inproc", opts...)
	}
	loopback := isLoopback(addr)
	switch {
	case o.TLS != nil:
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(o.TLS)))
	case o.Insecure || loopback:
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	default:
		return nil, fmt.Errorf("%s: plaintext is only for loopback, or a network the installation names insecure", addr)
	}
	switch {
	case o.DevPrincipal != "":
		if !loopback {
			return nil, fmt.Errorf("a development principal is only for a loopback service, not %s", addr)
		}
		opts = append(opts, naming(authn.DevPrincipalKey, o.DevPrincipal)...)
	case o.Credential != nil:
		opts = append(opts, grpc.WithPerRPCCredentials(perRPC{cred: o.Credential, secure: o.TLS != nil}))
	case o.Token != nil:
		opts = append(opts, grpc.WithPerRPCCredentials(bearer{token: o.Token, secure: o.TLS != nil}))
	}
	return grpc.NewClient(addr, opts...)
}

// perRPC adds the credential's certificate and a proof for the call's method to each call.
type perRPC struct {
	cred   Credential
	secure bool
}

func (p perRPC) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	info, ok := credentials.RequestInfoFromContext(ctx)
	if !ok {
		return nil, errors.New("dial: no request information to prove")
	}
	cert, key, err := p.cred.Current(ctx)
	if err != nil {
		return nil, fmt.Errorf("dial: the credential: %w", err)
	}
	proof, err := credential.Prove(key, info.Method, time.Now())
	if err != nil {
		return nil, err
	}
	return map[string]string{credential.CertificateKey: string(cert), credential.ProofKey: proof}, nil
}

// RequireTransportSecurity is false for a plaintext connection the options allowed: the proof
// still binds each call to the key, and the installation chose the network.
func (p perRPC) RequireTransportSecurity() bool { return p.secure }

// bearer adds a bearer token to each call.
type bearer struct {
	token  func(context.Context) (string, error)
	secure bool
}

func (b bearer) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	tok, err := b.token(ctx)
	if err != nil {
		return nil, fmt.Errorf("dial: the token: %w", err)
	}
	return map[string]string{"authorization": "Bearer " + tok}, nil
}

// RequireTransportSecurity is false for a plaintext connection the options allowed: a network the
// installation trusts.
func (b bearer) RequireTransportSecurity() bool { return b.secure }

// isLoopback reports whether addr (host:port) is a loopback address.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// naming has each call carry the caller's name under key.
func naming(key, name string) []grpc.DialOption {
	with := func(ctx context.Context) context.Context { return metadata.AppendToOutgoingContext(ctx, key, name) }
	return []grpc.DialOption{
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, co ...grpc.CallOption) error {
			return invoker(with(ctx), method, req, reply, cc, co...)
		}),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, co ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(with(ctx), desc, cc, method, co...)
		}),
	}
}
