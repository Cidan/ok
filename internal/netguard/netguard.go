// Package netguard reaches what a tenant points ok at — an organization's OIDC issuer, its LDAP
// directory — and nothing else: public unicast addresses only, checked on the address each
// connection is made to, after DNS, so a name that resolves somewhere private later is caught; and,
// over HTTP, TLS only, no proxy, a few redirects, bounded bodies, short timeouts. A development
// server (Allow) reaches anywhere.
package netguard

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"
)

// special are ranges no tenant's server is in, beyond what netip classifies: shared address space
// (CGNAT), IETF protocol assignments, benchmarking, NAT64 and 6to4 (which embed IPv4 addresses),
// and documentation.
var special = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("2001:db8::/32"),
}

// Public reports whether an address is a public unicast one: not loopback, private, link-local,
// multicast, unspecified, or one of special's.
func Public(a netip.Addr) bool {
	a = a.Unmap()
	if !a.IsGlobalUnicast() || a.IsPrivate() {
		return false
	}
	for _, p := range special {
		if p.Contains(a) {
			return false
		}
	}
	return true
}

// control refuses a connection to an address that is not public.
func control(_, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("refusing %s: not an address", address)
	}
	if !Public(ap.Addr()) {
		return fmt.Errorf("refusing %s: a tenant's server is reached at a public address", ap.Addr())
	}
	return nil
}

// Dialer dials public addresses only — or, allowPrivate, any.
func Dialer(allowPrivate bool) *net.Dialer {
	d := &net.Dialer{Timeout: 10 * time.Second}
	if !allowPrivate {
		d.Control = control
	}
	return d
}

// HTTPClient reaches public addresses over HTTPS only, its bodies at most maxBody bytes — or,
// allowPrivate, anywhere over anything.
func HTTPClient(allowPrivate bool, maxBody int64) *http.Client {
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           Dialer(allowPrivate).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
	}
	return &http.Client{
		Transport: guarded{next: tr, allowPrivate: allowPrivate, maxBody: maxBody},
		Timeout:   20 * time.Second,
		CheckRedirect: func(_ *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return errors.New("redirected more than 3 times")
			}
			return nil
		},
	}
}

// guarded refuses plaintext requests (but allowPrivate's), and bounds what a response may hold.
type guarded struct {
	next         http.RoundTripper
	allowPrivate bool
	maxBody      int64
}

func (g guarded) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" && !g.allowPrivate {
		return nil, fmt.Errorf("refusing %s: a tenant's server is reached over HTTPS", r.URL.Redacted())
	}
	resp, err := g.next.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	resp.Body = &limited{ReadCloser: resp.Body, left: g.maxBody, max: g.maxBody}
	return resp, nil
}

// limited fails a body longer than its limit, rather than cutting it short.
type limited struct {
	io.ReadCloser
	left, max int64
}

func (l *limited) Read(p []byte) (int, error) {
	if l.left <= 0 {
		// At the limit: the body may end here, or say more than it may.
		var one [1]byte
		if n, err := l.ReadCloser.Read(one[:]); n == 0 && err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("a response over %d bytes", l.max)
	}
	if int64(len(p)) > l.left {
		p = p[:l.left]
	}
	n, err := l.ReadCloser.Read(p)
	l.left -= int64(n)
	return n, err
}
