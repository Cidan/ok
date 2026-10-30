// Package inproc carries a process's calls to its own roles (ok serve): a listener only code in the
// process can reach, whose connections name Addr as their peer, which no connection from outside
// it can — so a call over one is the process's own, the role it names, with no credential to
// check against the directory.
package inproc

import (
	"context"
	"net"

	"google.golang.org/grpc/test/bufconn"
)

// Listener is a process's listener for its own calls.
type Listener struct{ l *bufconn.Listener }

// Listen makes a process's listener for its own calls.
func Listen() *Listener { return &Listener{l: bufconn.Listen(1 << 20)} }

// Accept is the next of the process's own connections.
func (l *Listener) Accept() (net.Conn, error) {
	c, err := l.l.Accept()
	if err != nil {
		return nil, err
	}
	return conn{c}, nil
}

func (l *Listener) Close() error   { return l.l.Close() }
func (l *Listener) Addr() net.Addr { return Addr{} }

// Dial connects to the listener, from within the process: a gRPC client's dialer.
func (l *Listener) Dial(ctx context.Context, _ string) (net.Conn, error) { return l.l.DialContext(ctx) }

// conn is a connection the listener accepted, its peer the process itself.
type conn struct{ net.Conn }

func (conn) RemoteAddr() net.Addr { return Addr{} }
func (conn) LocalAddr() net.Addr  { return Addr{} }

// Addr is the peer of a call from within the process.
type Addr struct{}

func (Addr) Network() string { return "inproc" }
func (Addr) String() string  { return "inproc" }

// Is reports whether a call's peer is the process itself: it came over a Listener.
func Is(a net.Addr) bool {
	_, ok := a.(Addr)
	return ok
}
