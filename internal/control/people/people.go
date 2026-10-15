// Package people names the people ok control shows: principals under the installation's domain
// (docs/identity.md), displayed by handle — ada.human.example.com is "ada" — and the directory of
// people the reviewer search offers, read from ok identity.
package people

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/identity"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// The installation's names, and the domain a person's handle is an address at: set once, by Use,
// before anything is shown.
var (
	names       = identity.MustNames("example.com")
	emailDomain = "example.com"
)

// Use sets the installation's names, and the domain a person's handle is an address at (the
// commits ok writes carry <handle>@<emailDomain>, as the SCM's do). Call it once, at start.
func Use(n identity.Names, email string) {
	names = n
	emailDomain = email
	if emailDomain == "" {
		emailDomain = n.Domain()
	}
}

// Names are the installation's.
func Names() identity.Names { return names }

func humanSuffix() string { return ".human." + names.Domain() }

// Handle is how a principal is shown: a person's handle, or any other principal whole.
func Handle(principal string) string {
	if h, ok := strings.CutSuffix(principal, humanSuffix()); ok {
		return h
	}
	return principal
}

// Principal is the principal a person names: a handle ("ada"), an address
// ("ada@example.com"), or a principal's full name.
func Principal(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if h, ok := strings.CutSuffix(name, "@"+emailDomain); ok {
		return names.Human(h)
	}
	if strings.HasSuffix(name, "."+names.Domain()) {
		return name
	}
	return names.Human(name)
}

// IsHuman reports whether a principal names a person.
func IsHuman(principal string) bool { return strings.HasSuffix(principal, humanSuffix()) }

// Email is the address a person's commits carry, <handle>@<email domain>.
func Email(principal string) string { return Handle(principal) + "@" + emailDomain }

// FromEmail is the person an address at the email domain names, or "" for any other address.
func FromEmail(email string) string {
	h, ok := strings.CutSuffix(strings.ToLower(strings.TrimSpace(email)), "@"+emailDomain)
	if !ok || h == "" || strings.ContainsAny(h, "@ ") {
		return ""
	}
	return names.Human(h)
}

// DirectoryClient is the part of ok identity's Directory the people need; identitypb's client is
// one.
type DirectoryClient interface {
	GetPrincipal(ctx context.Context, in *identitypb.GetPrincipalRequest, opts ...grpc.CallOption) (*identitypb.GetPrincipalResponse, error)
	ListPrincipals(ctx context.Context, in *identitypb.ListPrincipalsRequest, opts ...grpc.CallOption) (*identitypb.ListPrincipalsResponse, error)
	ListRetiredNames(ctx context.Context, in *identitypb.ListRetiredNamesRequest, opts ...grpc.CallOption) (*identitypb.ListRetiredNamesResponse, error)
	ExpandSet(ctx context.Context, in *identitypb.ExpandSetRequest, opts ...grpc.CallOption) (*identitypb.ExpandSetResponse, error)
	CheckAdmin(ctx context.Context, in *identitypb.CheckAdminRequest, opts ...grpc.CallOption) (*identitypb.CheckAdminResponse, error)
	ListAdmins(ctx context.Context, in *identitypb.ListAdminsRequest, opts ...grpc.CallOption) (*identitypb.ListAdminsResponse, error)
	ListSets(ctx context.Context, in *identitypb.ListSetsRequest, opts ...grpc.CallOption) (*identitypb.ListSetsResponse, error)
}

// Directory lists the active people in ok identity's directory, cached briefly: people join and
// leave rarely, and the reviewer search asks on every keystroke.
type Directory struct {
	client DirectoryClient
	ttl    time.Duration

	aclMu sync.Mutex
	acls  map[string]cached // ACL name -> its principals

	adminMu   sync.Mutex
	admins    map[string]cached // principal -> whether they administer ok (one member when so)
	adminList cached            // every administrator

	syncedMu sync.Mutex
	synced   map[string][]string // the system of record's groups, by name: their members
	syncedAt time.Time

	mu      sync.Mutex
	humans  []string
	fetched time.Time
}

// NewDirectory reads people from ok identity's Directory.
func NewDirectory(client DirectoryClient) *Directory {
	return &Directory{client: client, ttl: time.Minute}
}

// Humans returns every active person, sorted.
func (d *Directory) Humans(ctx context.Context) ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.humans != nil && time.Since(d.fetched) < d.ttl {
		return d.humans, nil
	}
	var out []string
	token := ""
	for {
		resp, err := d.client.ListPrincipals(ctx, &identitypb.ListPrincipalsRequest{Kind: identitypb.Kind_KIND_HUMAN, PageSize: 500, PageToken: token})
		if err != nil {
			return nil, err
		}
		for _, p := range resp.GetPrincipals() {
			if p.GetStatus() == identitypb.Status_STATUS_ACTIVE {
				out = append(out, p.GetName())
			}
		}
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	sort.Strings(out)
	d.humans, d.fetched = out, time.Now()
	return out, nil
}

// IsActiveHuman reports whether a principal is an active person in the directory.
func (d *Directory) IsActiveHuman(ctx context.Context, principal string) (bool, error) {
	humans, err := d.Humans(ctx)
	if err != nil {
		return false, err
	}
	i := sort.SearchStrings(humans, principal)
	return i < len(humans) && humans[i] == principal, nil
}

// Search returns up to limit active people whose handle contains keyword, handles that start
// with it first, skipping those in exclude.
func (d *Directory) Search(ctx context.Context, keyword string, exclude map[string]bool, limit int) ([]string, error) {
	humans, err := d.Humans(ctx)
	if err != nil {
		return nil, err
	}
	keyword = strings.ToLower(strings.TrimSpace(keyword))
	if keyword == "" {
		return nil, nil
	}
	var prefix, inside []string
	for _, p := range humans {
		if exclude[p] {
			continue
		}
		h := Handle(p)
		switch {
		case strings.HasPrefix(h, keyword):
			prefix = append(prefix, p)
		case strings.Contains(h, keyword):
			inside = append(inside, p)
		}
	}
	out := append(prefix, inside...)
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// IsPrincipal reports whether a name is a principal's — of any kind, active or not — in ok
// identity's directory: what a member of a set in the groups-and-ACLs tree must be.
func (d *Directory) IsPrincipal(ctx context.Context, name string) (bool, error) {
	_, err := d.client.GetPrincipal(ctx, &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Name{Name: name}})
	switch status.Code(err) {
	case codes.OK:
		return true, nil
	case codes.NotFound:
		return false, nil
	}
	return false, err
}

// RetiredSetNames are the names of the groups and ACLs ok identity has retired, which are never
// declared again.
func (d *Directory) RetiredSetNames(ctx context.Context) (map[string]bool, error) {
	resp, err := d.client.ListRetiredNames(ctx, &identitypb.ListRetiredNamesRequest{})
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, r := range resp.GetNames() {
		if k, ok := names.KindOf(r.GetName()); ok && (k == identity.GroupMember || k == identity.ACLMember) {
			out[r.GetName()] = true
		}
	}
	return out, nil
}

type cached struct {
	members []string // sorted
	fetched time.Time
}

// ACLMembers returns every principal in an ACL, through the sets it holds, sorted, cached as
// briefly as the people: an ACL that is no ACL has none.
func (d *Directory) ACLMembers(ctx context.Context, acl string) ([]string, error) {
	d.aclMu.Lock()
	if e, ok := d.acls[acl]; ok && time.Since(e.fetched) < d.ttl {
		d.aclMu.Unlock()
		return e.members, nil
	}
	d.aclMu.Unlock()
	resp, err := d.client.ExpandSet(ctx, &identitypb.ExpandSetRequest{Name: acl})
	var members []string
	switch status.Code(err) {
	case codes.OK:
		members = append(members, resp.GetPrincipals()...)
		sort.Strings(members)
	case codes.NotFound:
	default:
		return nil, err
	}
	d.aclMu.Lock()
	if d.acls == nil {
		d.acls = map[string]cached{}
	}
	d.acls[acl] = cached{members: members, fetched: time.Now()}
	d.aclMu.Unlock()
	return members, nil
}

// InACL reports whether a principal is in an ACL.
func (d *Directory) InACL(ctx context.Context, acl, principal string) (bool, error) {
	members, err := d.ACLMembers(ctx, acl)
	if err != nil {
		return false, err
	}
	i := sort.SearchStrings(members, principal)
	return i < len(members) && members[i] == principal, nil
}

// IsAdmin reports whether a principal administers ok — every repository among it — as ok identity
// decides: a member of admins.acl.<domain>, or, before any tree declares it, a bootstrap
// administrator. Cached as briefly as the people.
func (d *Directory) IsAdmin(ctx context.Context, principal string) (bool, error) {
	d.adminMu.Lock()
	if e, ok := d.admins[principal]; ok && time.Since(e.fetched) < d.ttl {
		d.adminMu.Unlock()
		return len(e.members) > 0, nil
	}
	d.adminMu.Unlock()
	resp, err := d.client.CheckAdmin(ctx, &identitypb.CheckAdminRequest{Principal: principal})
	if err != nil {
		return false, err
	}
	e := cached{fetched: time.Now()}
	if resp.GetAdmin() {
		e.members = []string{principal}
	}
	d.adminMu.Lock()
	if d.admins == nil {
		d.admins = map[string]cached{}
	}
	d.admins[principal] = e
	d.adminMu.Unlock()
	return resp.GetAdmin(), nil
}

// Admins lists the principals who administer ok, sorted, cached as briefly as the people.
func (d *Directory) Admins(ctx context.Context) ([]string, error) {
	d.adminMu.Lock()
	if !d.adminList.fetched.IsZero() && time.Since(d.adminList.fetched) < d.ttl {
		defer d.adminMu.Unlock()
		return d.adminList.members, nil
	}
	d.adminMu.Unlock()
	resp, err := d.client.ListAdmins(ctx, &identitypb.ListAdminsRequest{})
	if err != nil {
		return nil, err
	}
	d.adminMu.Lock()
	d.adminList = cached{members: resp.GetPrincipals(), fetched: time.Now()}
	d.adminMu.Unlock()
	return resp.GetPrincipals(), nil
}

// SyncedGroups are the groups the system of record syncs, by name, with their members as
// declared: what a tree's ACLs may hold. Cached as briefly as the people.
func (d *Directory) SyncedGroups(ctx context.Context) (map[string][]string, error) {
	d.syncedMu.Lock()
	defer d.syncedMu.Unlock()
	if d.synced != nil && time.Since(d.syncedAt) < d.ttl {
		return d.synced, nil
	}
	resp, err := d.client.ListSets(ctx, &identitypb.ListSetsRequest{})
	if err != nil {
		return nil, err
	}
	out := map[string][]string{}
	for _, set := range resp.GetSets() {
		if set.GetSource() == identitypb.SetSource_SET_SOURCE_SYNCED {
			out[set.GetName()] = set.GetMembers()
		}
	}
	d.synced, d.syncedAt = out, time.Now()
	return out, nil
}
