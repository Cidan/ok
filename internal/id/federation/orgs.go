package federation

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/netguard"
	"github.com/coreos/go-oidc/v3/oidc"
)

// Organizations' issuers. An organization names its issuers, so what ok fetches from them is
// guarded as anything a tenant points ok at: HTTPS only, at public addresses only — checked on the
// address each connection is made to, after DNS, so a name that resolves elsewhere later is caught
// — no proxy, a few redirects, small bodies, short timeouts. What ok learns of an issuer — its keys'
// URL — is kept a day, a failure a minute.

const (
	orgIssuerTTL     = 24 * time.Hour
	orgIssuerFailTTL = time.Minute
	// maxOrgIssuers bounds what is kept: only issuers some organization trusts are fetched.
	maxOrgIssuers = 10_000
	// maxIssuerBody bounds a discovery document's or a key set's size.
	maxIssuerBody = 1 << 20
)

type orgIssuers struct {
	client *http.Client
	ctx    context.Context // what key sets fetch under, for as long as they are kept
	mu     sync.Mutex
	known  map[string]*orgIssuer
	now    func() time.Time
}

type orgIssuer struct {
	mu      sync.Mutex
	keys    oidc.KeySet
	err     error
	fetched time.Time
}

func newOrgIssuers(allowPrivate bool) *orgIssuers {
	c := netguard.HTTPClient(allowPrivate, maxIssuerBody)
	return &orgIssuers{client: c, ctx: oidc.ClientContext(context.Background(), c), known: map[string]*orgIssuer{}, now: time.Now}
}

// keys are an organization's issuer's signing keys, discovered on first use and again a day on.
func (o *orgIssuers) keys(ctx context.Context, issuer string) (oidc.KeySet, error) {
	o.mu.Lock()
	e, ok := o.known[issuer]
	if !ok {
		if len(o.known) >= maxOrgIssuers {
			o.evictOldest()
		}
		e = &orgIssuer{}
		o.known[issuer] = e
	}
	o.mu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	age := o.now().Sub(e.fetched)
	if !e.fetched.IsZero() && (e.err == nil && age < orgIssuerTTL || e.err != nil && age < orgIssuerFailTTL) {
		return e.keys, e.err
	}
	e.keys, e.err = o.discover(ctx, issuer)
	e.fetched = o.now()
	if e.err != nil && e.keys == nil {
		return nil, e.err
	}
	return e.keys, e.err
}

func (o *orgIssuers) evictOldest() {
	var oldest string
	var at time.Time
	for iss, e := range o.known {
		if oldest == "" || e.fetched.Before(at) {
			oldest, at = iss, e.fetched
		}
	}
	delete(o.known, oldest)
}

// discover reads an issuer's discovery document — which must name the issuer itself — for its keys'
// URL, fetched through the guarded client too.
func (o *orgIssuers) discover(ctx context.Context, issuer string) (oidc.KeySet, error) {
	p, err := oidc.NewProvider(oidc.ClientContext(ctx, o.client), issuer)
	if err != nil {
		return nil, err
	}
	var meta struct {
		JWKSURI string `json:"jwks_uri"`
	}
	if err := p.Claims(&meta); err != nil {
		return nil, err
	}
	if meta.JWKSURI == "" {
		return nil, errors.New("its discovery document names no jwks_uri")
	}
	return oidc.NewRemoteKeySet(o.ctx, meta.JWKSURI), nil
}
