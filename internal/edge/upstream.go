package edge

import (
	"context"
	"crypto/tls"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// upstreams reaches the roles: an HTTP/2 transport per host — cleartext by prior knowledge, or TLS
// with the host's name checked — dialing the addresses its name resolves to, so a repository's
// requests go to one of them by rendezvous hashing of its ID and its caches stay hot
// (docs/hosted.md, "The edge").
type upstreams struct {
	tls *tls.Config // nil: cleartext HTTP/2

	mu    sync.Mutex
	hosts map[string]*upstreamHost
}

type upstreamHost struct {
	rt   *http.Transport
	next atomic.Uint64 // round robin, for requests about no known repository
}

func newUpstreams(tc *tls.Config) *upstreams {
	return &upstreams{tls: tc, hosts: map[string]*upstreamHost{}}
}

// pick is the address (ip:port) an attempt about repo (0: none known) at a role's address dials,
// and the transport that dials it, with the scheme it speaks.
func (u *upstreams) pick(ctx context.Context, hostport string, repo int64) (string, http.RoundTripper, string, error) {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return "", nil, "", fmt.Errorf("the address %s: %w", hostport, err)
	}
	h := u.host(host)
	ips, err := h.resolve(ctx, host)
	if err != nil {
		return "", nil, "", err
	}
	var ip string
	if repo != 0 {
		ip = rendezvous(ips, repo)
	} else {
		ip = ips[h.next.Add(1)%uint64(len(ips))]
	}
	scheme := "http"
	if u.tls != nil {
		scheme = "https"
	}
	return net.JoinHostPort(ip, port), h.rt, scheme, nil
}

func (u *upstreams) host(host string) *upstreamHost {
	u.mu.Lock()
	defer u.mu.Unlock()
	if h, ok := u.hosts[host]; ok {
		return h
	}
	var protocols http.Protocols
	t := &http.Transport{Protocols: &protocols, ForceAttemptHTTP2: true, MaxIdleConnsPerHost: 8, IdleConnTimeout: 90 * time.Second,
		DialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext}
	if u.tls != nil {
		protocols.SetHTTP2(true)
		tc := u.tls.Clone()
		tc.ServerName = host
		t.TLSClientConfig = tc
	} else {
		protocols.SetUnencryptedHTTP2(true)
	}
	h := &upstreamHost{rt: t}
	u.hosts[host] = h
	return h
}

// resolve is a host's addresses as the resolver answers now: a replica added or gone takes its
// repositories at once, and the edge keeps no answer (docs/state.md).
func (h *upstreamHost) resolve(ctx context.Context, host string) ([]string, error) {
	if net.ParseIP(host) != nil {
		return []string{host}, nil
	}
	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil || len(ips) == 0 {
		return nil, fmt.Errorf("resolving %s: %v", host, err)
	}
	return ips, nil
}

// rendezvous is the address of ips a repository's requests go to: the one whose score with its ID
// is highest, so adding or losing a replica moves only the repositories that were its.
func rendezvous(ips []string, repo int64) string {
	id := fmix64(uint64(repo))
	best, bestScore := ips[0], uint64(0)
	for _, ip := range ips {
		f := fnv.New64a()
		f.Write([]byte(ip))
		if s := fmix64(f.Sum64() ^ id); s >= bestScore {
			best, bestScore = ip, s
		}
	}
	return best
}

// fmix64 is MurmurHash3's finalizer: every bit of x moves every bit of the result, as FNV's do not.
func fmix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

func (u *upstreams) close() {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, h := range u.hosts {
		h.rt.CloseIdleConnections()
	}
}
