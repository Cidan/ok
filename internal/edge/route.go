package edge

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	okcells "github.com/Cidan/ok/internal/cells"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// The roles a request goes to.
const (
	roleIdentity   = "identity"
	roleSCM        = "scm"
	roleControl    = "control"
	roleAutomation = "automation"
	roleDeploy     = "deploy"
	roleSecrets    = "secrets"
)

// RepositoryHeader names the repository a request is about — its ID, or its full name — as ok's
// clients set it on every request that names one (jj's store, jj cl).
const RepositoryHeader = "Ok-Repository"

// roleOf is the role that serves a path: identity's pages (/id/…) and services, the SCM's services,
// ok automation's and the deployer's, and ok control's for everything else — its pages, REST, and
// services — as an installation's one public address routes them (docs/configuration.md, "One
// public address").
func roleOf(path string) string {
	if path == "/id" || strings.HasPrefix(path, "/id/") {
		return roleIdentity
	}
	svc, _, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	switch {
	case strings.HasPrefix(svc, "ok.identity."):
		return roleIdentity
	case strings.HasPrefix(svc, "ok.scm."):
		return roleSCM
	case strings.HasPrefix(svc, "ok.automation."):
		return roleAutomation
	case strings.HasPrefix(svc, "ok.deploy."):
		return roleDeploy
	case strings.HasPrefix(svc, "ok.secrets."):
		return roleSecrets
	}
	return roleControl
}

// pageKeys are the repositories a page's path may be about, most likely first: /owner/name/…, then
// an installation's bare /name/….
func pageKeys(path string) []string {
	if strings.HasPrefix(path, "/ok.") {
		return nil // a service's REST, which names its repository in its header
	}
	segs := strings.SplitN(strings.Trim(path, "/"), "/", 3)
	var keys []string
	if len(segs) >= 2 {
		if _, _, err := tenancy.ParseFullName(segs[0] + "/" + segs[1]); err == nil {
			keys = append(keys, segs[0]+"/"+segs[1])
		}
	}
	if len(segs) >= 1 && segs[0] != "" {
		if _, _, err := tenancy.ParseFullName(segs[0]); err == nil {
			keys = append(keys, segs[0])
		}
	}
	return keys
}

// cellTable is the cells as the directory last listed them.
type cellTable struct {
	cells map[string]*identitypb.Cell
	home  string // where a request that names no repository goes
}

// cellsTTL is how long the edge keeps the directory's list of cells before it asks again.
const cellsTTL = 30 * time.Second

type cellList struct {
	client identitypb.CellsClient
	region string

	table atomic.Pointer[cellTable]
	at    atomic.Int64 // when it was listed, Unix nanoseconds
	sf    singleflight.Group
}

// get is the table, listed again when it is older than cellsTTL; the last one when the directory
// does not answer.
func (l *cellList) get(ctx context.Context) (*cellTable, error) {
	t := l.table.Load()
	if t != nil && time.Since(time.Unix(0, l.at.Load())) < cellsTTL {
		return t, nil
	}
	v, err, _ := l.sf.Do("cells", func() (any, error) {
		resp, err := l.client.ListCells(ctx, &identitypb.ListCellsRequest{})
		if err != nil {
			return nil, err
		}
		nt := &cellTable{cells: map[string]*identitypb.Cell{}}
		var anyOpen string
		for _, c := range resp.GetCells() {
			nt.cells[c.GetName()] = c
			if !c.GetPlacement() {
				continue
			}
			if anyOpen == "" {
				anyOpen = c.GetName()
			}
			if nt.home == "" && c.GetRegion() == l.region {
				nt.home = c.GetName()
			}
		}
		if nt.home == "" {
			nt.home = anyOpen
		}
		if nt.home == "" && len(resp.GetCells()) > 0 {
			nt.home = resp.GetCells()[0].GetName()
		}
		l.table.Store(nt)
		l.at.Store(time.Now().UnixNano())
		return nt, nil
	})
	if err != nil {
		if t != nil {
			return t, nil
		}
		return nil, fmt.Errorf("listing the cells: %w", err)
	}
	return v.(*cellTable), nil
}

// address is where a cell's role is reached.
func (t *cellTable) address(cell, role string) (string, error) {
	c, ok := t.cells[cell]
	if !ok {
		return "", fmt.Errorf("the directory knows no cell %s", cell)
	}
	addr := map[string]string{roleSCM: c.GetScm(), roleControl: c.GetControl(), roleAutomation: c.GetAutomation(), roleDeploy: c.GetDeploy(),
		roleSecrets: c.GetSecrets()}[role]
	if addr == "" {
		return "", fmt.Errorf("cell %s names no address for its %s", cell, role)
	}
	return addr, nil
}

// route is where a repository is, as the directory said: its ID and cell; or that it has none
// (found false), held for less long.
type route struct {
	id    int64
	cell  string
	found bool
	at    time.Time
}

// routesTTL is how long the edge keeps where a repository is: a request a stale route sends to the
// wrong cell is refused there (Moved) and sent again, so this bounds only how often that happens
// after a move. A repository there is none of is held for missTTL, as one may be made.
const (
	routesTTL  = 30 * time.Second
	missTTL    = 2 * time.Second
	maxRoutes  = 200_000
	routeAsked = 5 * time.Second // how long a lookup may take
)

// routes are where repositories are, by what requests name them by — an ID, or a full name — kept
// routesTTL. They are safe for concurrent use.
type routes struct {
	client identitypb.CellsClient

	mu sync.Mutex
	m  map[string]route
	sf singleflight.Group
}

// lookup is where the repository key names is; ok false for one there is none of, or when the
// directory does not answer — the request goes to the edge's home cell, which sends it on.
func (r *routes) lookup(ctx context.Context, key string) (route, bool) {
	now := time.Now()
	r.mu.Lock()
	rt, held := r.m[key]
	r.mu.Unlock()
	if held && (rt.found && now.Sub(rt.at) < routesTTL || !rt.found && now.Sub(rt.at) < missTTL) {
		return rt, rt.found
	}
	v, _, _ := r.sf.Do(key, func() (any, error) {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), routeAsked)
		defer cancel()
		resp, err := r.client.Route(ctx, &identitypb.RouteRequest{Repository: key})
		switch {
		case status.Code(err) == codes.NotFound:
			rt := route{at: time.Now()}
			r.put(key, rt)
			return rt, nil
		case err != nil:
			return route{}, nil
		}
		rt := route{id: resp.GetId(), cell: resp.GetCell(), found: true, at: time.Now()}
		r.put(key, rt)
		r.put(strconv.FormatInt(rt.id, 10), rt)
		return rt, nil
	})
	rt = v.(route)
	return rt, rt.found
}

// moved records that a cell answered a repository is in cell, as the request named it by key (an
// ID, when the answer names none).
func (r *routes) moved(key string, id int64, cell string) {
	rt := route{id: id, cell: cell, found: true, at: time.Now()}
	if key != "" {
		r.mu.Lock()
		if old, ok := r.m[key]; ok && id == 0 {
			rt.id = old.id
		}
		r.mu.Unlock()
		r.put(key, rt)
	}
	if rt.id != 0 {
		r.put(strconv.FormatInt(rt.id, 10), rt)
	}
}

func (r *routes) put(key string, rt route) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.m) >= maxRoutes {
		clear(r.m)
	}
	r.m[key] = rt
}

// target is where one attempt at a request goes.
type target struct {
	role string
	cell string // "" for identity's
	repo int64  // the repository it is about, for affinity; 0 when unknown
	key  string // what the request named it by, to correct on a Moved answer
}

// targetOf is where a request goes: identity's to the directory; another role's to the cell of the
// repository its header names, or its page's path, and to the edge's home cell when it names none
// the directory knows.
func (e *Edge) targetOf(ctx context.Context, r *http.Request, cells *cellTable) target {
	t := target{role: roleOf(r.URL.Path)}
	if t.role == roleIdentity {
		return t
	}
	keys := pageKeys(r.URL.Path)
	if h := r.Header.Get(RepositoryHeader); h != "" {
		keys = []string{h}
	}
	for _, k := range keys {
		if rt, ok := e.routes.lookup(ctx, k); ok {
			t.cell, t.repo, t.key = rt.cell, rt.id, k
			return t
		}
	}
	t.cell = cells.home
	if len(keys) > 0 {
		t.key = keys[0]
	} else if c := r.Header.Get(okcells.Header); c != "" {
		// A request about no repository that names its cell — a deploy agent's connection to that
		// cell's deployer (docs/hosted.md, "Deploy agents, as built") — goes there.
		t.cell = c
	}
	return t
}
