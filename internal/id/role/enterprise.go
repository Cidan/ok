package role

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/id/enterprise"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/serve/node"
)

// enterpriseSync is the node syncing organizations' identity providers.
type enterpriseSync struct{ s *enterprise.Syncer }

var _ node.Application = enterpriseSync{}

func (enterpriseSync) Name() string                      { return "ok/identity/enterprise" }
func (enterpriseSync) Dependencies() []node.Node         { return nil }
func (enterpriseSync) Setup(context.Context) error       { return nil }
func (enterpriseSync) Healthy(context.Context) bool      { return true }
func (e enterpriseSync) Start(ctx context.Context) error { return e.s.Start(ctx) }

// orgSCIM serves each organization's SCIM at /id/scim/v2/orgs/<handle>: the bearer token is the
// organization's, and what is provisioned is the organization's alone.
type orgSCIM struct {
	st   *store.Store
	sync *enterprise.Syncer
	base string // ok's public URL

	mu      sync.Mutex
	pending map[string]bool // organizations whose sync a provisioning has asked for
}

var _ node.SelfAuthenticated = (*orgSCIM)(nil)

func (*orgSCIM) Name() string                 { return "ok/identity/scim/orgs" }
func (*orgSCIM) Dependencies() []node.Node    { return nil }
func (*orgSCIM) Healthy(context.Context) bool { return true }
func (*orgSCIM) Setup(context.Context) error  { return nil }
func (*orgSCIM) AuthenticatesItself()         {}

func (o *orgSCIM) Mount(r *chi.Mux) (string, error) {
	r.HandleFunc("/{handle}/*", o.serve)
	r.HandleFunc("/{handle}", o.serve)
	return "/id/scim/v2/orgs", nil
}

func (o *orgSCIM) serve(w http.ResponseWriter, r *http.Request) {
	handle := chi.URLParam(r, "handle")
	tok, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	org, err := o.st.OrganizationByHandle(r.Context(), handle)
	var of string
	if err == nil {
		of, err = o.st.SCIMTokenOrganization(r.Context(), strings.TrimSpace(tok))
	}
	if err != nil || of != org.ID || !org.Active() {
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			http.Error(w, "ok could not check the token", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("WWW-Authenticate", `Bearer realm="ok scim"`)
		http.Error(w, "present the organization's SCIM bearer token", http.StatusUnauthorized)
		return
	}
	h, err := scim.NewHandler(o.st.SCIMOf(org.ID), strings.TrimSpace(tok), o.base+"/id/scim/v2/orgs/"+handle, func() { o.changed(org.ID) })
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	r2 := r.Clone(r.Context())
	r2.URL.Path = "/id/scim/v2/" + chi.URLParam(r, "*")
	h.ServeHTTP(w, r2)
}

// changed syncs an organization soon after what its provider pushed, once for a burst of pushes.
func (o *orgSCIM) changed(org string) {
	o.mu.Lock()
	if o.pending[org] {
		o.mu.Unlock()
		return
	}
	o.pending[org] = true
	o.mu.Unlock()
	go func() {
		time.Sleep(2 * time.Second)
		o.mu.Lock()
		delete(o.pending, org)
		o.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		o.sync.Sync(ctx, org)
	}()
}
