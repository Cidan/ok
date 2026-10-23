// Package people names the people ok control shows and answers who they are. ok control records
// every principal by stable ID (docs/hosted.md, "Tenancy, as built") and shows each by their
// current name — a person by handle, ada.human.example.com as "ada" — resolved through ok
// identity; it also holds the directory of people the reviewer search offers, ACLs' members, and
// ok's administrators, all by ID.
package people

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/names"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// The installation's names, the domain a person's handle is an address at, and how principals'
// IDs are named: set once, by Use, before anything is shown.
var (
	installation = identity.MustNames("example.com")
	emailDomain  = "example.com"
	resolver     *names.Resolver
	hosted       bool
)

// Use sets the installation's names, the domain a person's handle is an address at (the commits
// ok writes carry <handle>@<emailDomain>, as the SCM's do), the resolver of principals' IDs, and
// whether ok is a hosted service, which shows names as one does (@ada). Call it once, at start.
// With no resolver (tests), a principal's ID is its name.
func Use(n identity.Names, email string, r *names.Resolver, isHosted bool) {
	installation = n
	hosted = isHosted
	emailDomain = email
	if emailDomain == "" {
		emailDomain = n.Domain()
	}
	resolver = r
}

// Names are the installation's.
func Names() identity.Names { return installation }

func humanSuffix() string { return ".human." + installation.Domain() }

// HandleOf is how a principal's name is shown, as the mode has it (identity.Names.Present): an
// installation's people by handle and any other principal whole; a hosted service's @ada,
// @acme/eng, acme/ci.
func HandleOf(name string) string { return installation.Present(name, hosted) }

// Name is the current name of the principal with a stable ID; one the directory cannot name now
// is shown by its ID.
func Name(ctx context.Context, id string) string {
	if resolver == nil || id == "" {
		return id
	}
	n, err := resolver.Name(ctx, id)
	if err != nil {
		return id
	}
	return n
}

// Handle is how the principal with a stable ID is shown: HandleOf its current name.
func Handle(ctx context.Context, id string) string { return HandleOf(Name(ctx, id)) }

// Handles are Handle of each ID, in order.
func Handles(ctx context.Context, ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = Handle(ctx, id)
	}
	return out
}

// Principal is the principal's name a person writes: a handle ("ada"), an address
// ("ada@example.com"), or a principal's full name.
func Principal(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if h, ok := strings.CutSuffix(name, "@"+emailDomain); ok && !strings.HasPrefix(name, "@") {
		return installation.Human(h)
	}
	if p, ok := installation.ParsePrincipal(name); ok {
		return p
	}
	return installation.Human(name)
}

// Reason is why something ended, as people read it: by is who ended it, by stable ID, which the
// reason never names — "canceled by ada", or "not wanted (canceled by ada)"; "" when no one did.
func Reason(ctx context.Context, reason, by string) string {
	if by == "" {
		return reason
	}
	who := "canceled by " + Handle(ctx, by)
	if reason == "" || reason == "canceled" {
		return who
	}
	return reason + " (" + who + ")"
}

// HaltReason is why a deployment ended or stands, as people read it: haltBy is who asked it to
// halt, by stable ID, which the reason never names; "" when no one did.
func HaltReason(ctx context.Context, reason, haltBy string) string {
	if haltBy == "" {
		return reason
	}
	who := "halted by " + Handle(ctx, haltBy)
	switch reason {
	case "", "halted":
		return who
	case "halted before it started":
		return who + " before it started"
	}
	return reason + " (" + who + ")"
}

// ErrUnknown is a name no principal has.
var ErrUnknown = names.ErrUnknown

// ID is the stable ID of the principal a person writes (Principal's forms); ErrUnknown for none.
func ID(ctx context.Context, written string) (string, error) {
	name := Principal(written)
	if resolver == nil {
		return name, nil
	}
	return resolver.ID(ctx, name)
}

// IDOfName is the stable ID of the principal with a full name; ErrUnknown for none.
func IDOfName(ctx context.Context, name string) (string, error) {
	if resolver == nil {
		return name, nil
	}
	return resolver.ID(ctx, name)
}

// IsHuman reports whether a principal's name names a person.
func IsHuman(name string) bool { return strings.HasSuffix(name, humanSuffix()) }

// IsHumanID reports whether the principal with a stable ID is a person.
func IsHumanID(ctx context.Context, id string) bool {
	if resolver == nil {
		return IsHuman(id)
	}
	p, err := resolver.ByID(ctx, id)
	return err == nil && p.Human
}

// Email is the address a person's commits carry, <handle>@<email domain>, by stable ID.
func Email(ctx context.Context, id string) string { return Handle(ctx, id) + "@" + emailDomain }

// EmailHandle is the handle an address at the email domain names, as written — a commit's author,
// which no rename changes — or "" for any other address.
func EmailHandle(email string) string {
	h, ok := strings.CutSuffix(strings.ToLower(strings.TrimSpace(email)), "@"+emailDomain)
	if !ok || h == "" || strings.ContainsAny(h, "@ ") {
		return ""
	}
	return h
}

// FromEmail is the stable ID of the person an address at the email domain names, or "" for any
// other address, or one no person has.
func FromEmail(ctx context.Context, email string) string {
	h := EmailHandle(email)
	if h == "" {
		return ""
	}
	id, err := IDOfName(ctx, installation.Human(h))
	if err != nil {
		return ""
	}
	return id
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

// Directory lists the active people in ok identity's directory, by stable ID, cached briefly:
// people join and leave rarely, and the reviewer search asks on every keystroke.
type Directory struct {
	client DirectoryClient
	ttl    time.Duration

	aclMu sync.Mutex
	acls  map[string]cached // ACL name -> its principals' IDs

	adminMu   sync.Mutex
	admins    map[string]cached // a principal's ID -> whether they administer ok (one member when so)
	adminList cached            // every administrator's ID

	syncedMu sync.Mutex
	synced   map[string][]string // the system of record's groups, by name: their members' names
	syncedAt time.Time

	mu      sync.Mutex
	humans  []person // by name
	byID    map[string]bool
	fetched time.Time
}

// person is an active person: their stable ID, and their name now.
type person struct{ id, name string }

// NewDirectory reads people from ok identity's Directory.
func NewDirectory(client DirectoryClient) *Directory {
	return &Directory{client: client, ttl: time.Minute}
}

func (d *Directory) people(ctx context.Context) ([]person, map[string]bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.humans != nil && time.Since(d.fetched) < d.ttl {
		return d.humans, d.byID, nil
	}
	var out []person
	token := ""
	for {
		resp, err := d.client.ListPrincipals(ctx, &identitypb.ListPrincipalsRequest{Kind: identitypb.Kind_KIND_HUMAN, PageSize: 500, PageToken: token})
		if err != nil {
			return nil, nil, err
		}
		for _, p := range resp.GetPrincipals() {
			if p.GetStatus() == identitypb.Status_STATUS_ACTIVE {
				out = append(out, person{id: p.GetId(), name: p.GetName()})
			}
		}
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	byID := make(map[string]bool, len(out))
	for _, p := range out {
		byID[p.id] = true
	}
	d.humans, d.byID, d.fetched = out, byID, time.Now()
	return out, byID, nil
}

// Humans returns every active person's ID, in their names' order.
func (d *Directory) Humans(ctx context.Context) ([]string, error) {
	ps, _, err := d.people(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.id
	}
	return out, nil
}

// IsActiveHuman reports whether the principal with a stable ID is an active person.
func (d *Directory) IsActiveHuman(ctx context.Context, id string) (bool, error) {
	_, byID, err := d.people(ctx)
	if err != nil {
		return false, err
	}
	return byID[id], nil
}

// Search returns the IDs of up to limit active people whose handle contains keyword, handles that
// start with it first, skipping the IDs in exclude.
func (d *Directory) Search(ctx context.Context, keyword string, exclude map[string]bool, limit int) ([]string, error) {
	ps, _, err := d.people(ctx)
	if err != nil {
		return nil, err
	}
	keyword = strings.ToLower(strings.TrimSpace(keyword))
	if keyword == "" {
		return nil, nil
	}
	var prefix, inside []string
	for _, p := range ps {
		if exclude[p.id] {
			continue
		}
		h := HandleOf(p.name)
		switch {
		case strings.HasPrefix(h, keyword):
			prefix = append(prefix, p.id)
		case strings.Contains(h, keyword):
			inside = append(inside, p.id)
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
		if k, ok := installation.KindOf(r.GetName()); ok && (k == identity.GroupMember || k == identity.ACLMember) {
			out[r.GetName()] = true
		}
	}
	return out, nil
}

type cached struct {
	members []string // sorted
	fetched time.Time
}

// ACLMembers returns the stable ID of every active principal in an ACL, through the sets it
// holds, sorted, cached as briefly as the people: an ACL that is no ACL has none.
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
		members, err = idsOf(resp.GetPrincipals(), resp.GetPrincipalIds())
		if err != nil {
			return nil, err
		}
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

// idsOf are the IDs ok identity answered beside the names, which it always does.
func idsOf(names, ids []string) ([]string, error) {
	if len(ids) != len(names) {
		return nil, errors.New("ok identity answered principals' names without their IDs")
	}
	return append([]string(nil), ids...), nil
}

// InACL reports whether the principal with a stable ID is in an ACL.
func (d *Directory) InACL(ctx context.Context, acl, id string) (bool, error) {
	members, err := d.ACLMembers(ctx, acl)
	if err != nil {
		return false, err
	}
	i := sort.SearchStrings(members, id)
	return i < len(members) && members[i] == id, nil
}

// IsAdmin reports whether the principal with a stable ID administers ok — every repository among
// it — as ok identity decides: a member of admins.acl.<domain>, or, before any tree declares it, a
// bootstrap administrator. Cached as briefly as the people.
func (d *Directory) IsAdmin(ctx context.Context, id string) (bool, error) {
	d.adminMu.Lock()
	if e, ok := d.admins[id]; ok && time.Since(e.fetched) < d.ttl {
		d.adminMu.Unlock()
		return len(e.members) > 0, nil
	}
	d.adminMu.Unlock()
	resp, err := d.client.CheckAdmin(ctx, &identitypb.CheckAdminRequest{Key: &identitypb.CheckAdminRequest_PrincipalId{PrincipalId: id}})
	if err != nil {
		return false, err
	}
	e := cached{fetched: time.Now()}
	if resp.GetAdmin() {
		e.members = []string{id}
	}
	d.adminMu.Lock()
	if d.admins == nil {
		d.admins = map[string]cached{}
	}
	d.admins[id] = e
	d.adminMu.Unlock()
	return resp.GetAdmin(), nil
}

// Admins lists the stable IDs of the principals who administer ok, in their names' order, cached
// as briefly as the people.
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
	ids, err := idsOf(resp.GetPrincipals(), resp.GetPrincipalIds())
	if err != nil {
		return nil, err
	}
	d.adminMu.Lock()
	d.adminList = cached{members: ids, fetched: time.Now()}
	d.adminMu.Unlock()
	return ids, nil
}

// SyncedGroups are the groups the system of record syncs, by name, with their members as
// declared, by name: what a tree's ACLs may hold. Cached as briefly as the people.
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
