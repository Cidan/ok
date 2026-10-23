package service_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// tenancyEnv is the Tenancy service as ada, bea, cal, ok control, the SCM, and the deployer call it.
type tenancyEnv struct {
	*env
	ada, bea, cal, control, scm, deploy identitypb.TenancyClient
	adaID, beaID, calID                 string
}

func newTenancyEnv(t *testing.T, hosted bool) *tenancyEnv {
	e := newEnvWith(t, hosted)
	id := func(name string) string {
		t.Helper()
		p, err := e.store.Principal(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		return p.ID
	}
	c := func(who string) identitypb.TenancyClient { return identitypb.NewTenancyClient(e.as(who)) }
	return &tenancyEnv{env: e, ada: c(ada), bea: c(bea), cal: c("cal.human.example.com"), control: c(control), scm: c(scm), deploy: c(deploy),
		adaID: id(ada), beaID: id(bea), calID: id("cal.human.example.com")}
}

// A hosted service: people make organizations and own them; members, teams, and settings are
// owners'; repositories' entries are the SCM's to make for those who may; Authorize decides who
// sees, changes, moves, deletes, and restores each.
func TestTenancyHosted(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)

	// A person makes an organization as themselves, or ok control for a person it names.
	acme, err := e.ada.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"})
	if err != nil || acme.Handle != "acme" || acme.BaseRole != "read" || acme.Installation {
		t.Fatalf("CreateOrganization = %v, %v", acme, err)
	}
	if _, err := e.control.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "beta", ForPersonId: e.beaID}); err != nil {
		t.Errorf("ok control making an organization for bea: %v", err)
	}
	for name, tc := range map[string]struct {
		c    identitypb.TenancyClient
		req  *identitypb.CreateOrganizationRequest
		code codes.Code
	}{
		"bea, for ada":             {e.bea, &identitypb.CreateOrganizationRequest{Handle: "x", ForPersonId: e.adaID}, codes.PermissionDenied},
		"ok control, for no one":   {e.control, &identitypb.CreateOrganizationRequest{Handle: "x"}, codes.InvalidArgument},
		"the deployer":             {e.deploy, &identitypb.CreateOrganizationRequest{Handle: "x", ForPersonId: e.adaID}, codes.PermissionDenied},
		"a person's handle":        {e.ada, &identitypb.CreateOrganizationRequest{Handle: "bea"}, codes.AlreadyExists},
		"a reserved handle":        {e.ada, &identitypb.CreateOrganizationRequest{Handle: "settings"}, codes.InvalidArgument},
		"a base role there is not": {e.ada, &identitypb.CreateOrganizationRequest{Handle: "x", BaseRole: "boss"}, codes.InvalidArgument},
	} {
		if _, err := tc.c.CreateOrganization(ctx, tc.req); code(err) != tc.code {
			t.Errorf("%s: %v, want %v", name, err, tc.code)
		}
	}

	// Settings and members are its owners'; its members list them; its last owner stays.
	if _, err := e.bea.UpdateOrganization(ctx, &identitypb.UpdateOrganizationRequest{Handle: "acme", BaseRole: "admin"}); code(err) != codes.PermissionDenied {
		t.Errorf("a stranger changing an organization: %v", err)
	}
	if _, err := e.ada.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "acme", PrincipalId: e.beaID, Role: "member"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "acme", PrincipalId: e.calID, Role: "member"}); code(err) != codes.PermissionDenied {
		t.Errorf("a member setting members: %v", err)
	}
	if _, err := e.cal.ListMembers(ctx, &identitypb.ListMembersRequest{Handle: "acme"}); code(err) != codes.PermissionDenied {
		t.Errorf("a stranger listing members: %v", err)
	}
	if ms, err := e.bea.ListMembers(ctx, &identitypb.ListMembersRequest{Handle: "acme"}); err != nil || len(ms.Members) != 2 {
		t.Errorf("a member listing members = %v, %v", ms, err)
	}
	if _, err := e.ada.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "acme", PrincipalId: e.adaID, Role: "member"}); code(err) != codes.FailedPrecondition {
		t.Errorf("the last owner made a member: %v", err)
	}
	if o, err := e.ada.UpdateOrganization(ctx, &identitypb.UpdateOrganizationRequest{Handle: "acme", BaseRole: "read"}); err != nil || o.MembersCreate {
		t.Errorf("UpdateOrganization = %v, %v", o, err)
	}

	// Teams are its owners' to make and fill, of its members.
	if _, err := e.bea.CreateTeam(ctx, &identitypb.CreateTeamRequest{Handle: "acme", Team: "eng"}); code(err) != codes.PermissionDenied {
		t.Errorf("a member making a team: %v", err)
	}
	eng, err := e.ada.CreateTeam(ctx, &identitypb.CreateTeamRequest{Handle: "acme", Team: "eng"})
	if err != nil || eng.Name != "eng.acme.team.example.com" {
		t.Fatalf("CreateTeam = %v, %v", eng, err)
	}
	if got, err := e.ada.AddTeamMember(ctx, &identitypb.TeamMemberRequest{Team: eng.Name, PrincipalId: e.beaID}); err != nil ||
		len(got.Members) != 1 || got.Members[0].Principal != bea {
		t.Errorf("AddTeamMember = %v, %v", got, err)
	}
	if _, err := e.ada.AddTeamMember(ctx, &identitypb.TeamMemberRequest{Team: eng.Name, PrincipalId: e.calID}); code(err) != codes.InvalidArgument {
		t.Errorf("a stranger in a team: %v", err)
	}
	if ts, err := e.bea.ListTeams(ctx, &identitypb.ListTeamsRequest{Handle: "acme"}); err != nil || len(ts.Teams) != 1 {
		t.Errorf("ListTeams = %v, %v", ts, err)
	}

	// Entries are the SCM's to make, for a person who may make the owner's repositories.
	reserve := &identitypb.ReserveRepositoryRequest{Owner: "acme", Name: "app", Cell: "c1", ForPersonId: e.adaID}
	if _, err := e.ada.ReserveRepository(ctx, reserve); code(err) != codes.PermissionDenied {
		t.Errorf("a person reserving: %v", err)
	}
	if _, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "acme", Name: "app", ForPersonId: e.beaID}); code(err) != codes.PermissionDenied {
		t.Errorf("a member making a repository where members do not: %v", err)
	}
	if _, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "bea", Name: "x", ForPersonId: e.adaID}); code(err) != codes.PermissionDenied {
		t.Errorf("making another person's repository: %v", err)
	}
	app, err := e.scm.ReserveRepository(ctx, reserve)
	if err != nil || app.State != "creating" || app.Visibility != "private" || app.Owner != "acme" || app.Cell != "c1" || app.Role != "admin" {
		t.Fatalf("ReserveRepository = %v, %v", app, err)
	}
	if _, err := e.ada.ResolveRepository(ctx, &identitypb.ResolveRepositoryRequest{Owner: "acme", Name: "app"}); code(err) != codes.NotFound {
		t.Errorf("an entry being created: %v", err)
	}
	if _, err := e.control.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: app.Id}); code(err) != codes.PermissionDenied {
		t.Errorf("ok control activating: %v", err)
	}
	if app, err = e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: app.Id}); err != nil || app.State != "active" {
		t.Fatalf("ActivateRepository = %v, %v", app, err)
	}

	// Who sees it, and as what: its maker and owner, admin; a member, by the base role; a stranger,
	// nothing; the SCM, asking for itself, everything.
	resolve := func(c identitypb.TenancyClient, owner, name string) (*identitypb.RepositoryEntry, error) {
		return c.ResolveRepository(ctx, &identitypb.ResolveRepositoryRequest{Owner: owner, Name: name})
	}
	for who, tc := range map[string]struct {
		c    identitypb.TenancyClient
		role string
		code codes.Code
	}{"ada": {e.ada, "admin", codes.OK}, "bea": {e.bea, "read", codes.OK}, "cal": {e.cal, "", codes.NotFound}} {
		got, err := resolve(tc.c, "acme", "app")
		if code(err) != tc.code || (err == nil && got.Role != tc.role) {
			t.Errorf("%s resolving acme/app = %v, %v; want %s, %v", who, got, err, tc.role, tc.code)
		}
	}
	if _, err := resolve(e.scm, "acme", "app"); err != nil {
		t.Errorf("the SCM resolving for itself: %v", err)
	}
	if _, err := e.bea.UpdateRepository(ctx, &identitypb.UpdateRepositoryRequest{Id: app.Id, Visibility: "public"}); code(err) != codes.PermissionDenied {
		t.Errorf("a reader changing visibility: %v", err)
	}
	if _, err := e.cal.UpdateRepository(ctx, &identitypb.UpdateRepositoryRequest{Id: app.Id, Visibility: "public"}); code(err) != codes.NotFound {
		t.Errorf("a stranger changing visibility: %v", err)
	}

	// Grants are its admins': a team's grant raises its members.
	if _, err := e.bea.ListGrants(ctx, &identitypb.RepositoryRef{Id: app.Id}); code(err) != codes.PermissionDenied {
		t.Errorf("a reader listing grants: %v", err)
	}
	if _, err := e.ada.SetGrant(ctx, &identitypb.SetGrantRequest{Id: app.Id, Grant: &identitypb.RepositoryGrant{Subject: eng.Name, Role: "maintain"}}); err != nil {
		t.Fatal(err)
	}
	if got, err := resolve(e.bea, "acme", "app"); err != nil || got.Role != "maintain" {
		t.Errorf("bea through eng's grant = %v, %v", got, err)
	}
	if got, err := e.bea.UpdateRepository(ctx, &identitypb.UpdateRepositoryRequest{Id: app.Id, Propose: "write"}); err != nil || got.Propose != "write" {
		t.Errorf("a maintainer changing who proposes = %v, %v", got, err)
	}
	if gs, err := e.ada.ListGrants(ctx, &identitypb.RepositoryRef{Id: app.Id}); err != nil || len(gs.Grants) != 2 {
		t.Errorf("ListGrants (its maker's admin, eng's maintain) = %v, %v", gs, err)
	}

	// A fork is of what its maker reads, in its network, as visible as its parent, whatever is asked,
	// and its visibility does not change while it is a fork.
	if _, err := e.ada.UpdateRepository(ctx, &identitypb.UpdateRepositoryRequest{Id: app.Id, Visibility: "internal"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "cal", Name: "app", ParentId: app.Id, ForPersonId: e.calID}); code(err) != codes.NotFound {
		t.Errorf("forking what one cannot see: %v", err)
	}
	beaFork, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "bea", Name: "app", ParentId: app.Id, Visibility: "public", ForPersonId: e.beaID})
	if err != nil || beaFork.NetworkId != app.NetworkId || beaFork.ParentId != app.Id || beaFork.Visibility != "internal" {
		t.Fatalf("a fork = %v, %v; want network %d, internal", beaFork, err, app.NetworkId)
	}
	if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: beaFork.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.UpdateRepository(ctx, &identitypb.UpdateRepositoryRequest{Id: beaFork.Id, Visibility: "public"}); code(err) != codes.FailedPrecondition {
		t.Errorf("a fork's visibility changed on its own: %v", err)
	}
	if ls, err := e.bea.ListRepositoryEntries(ctx, &identitypb.ListRepositoryEntriesRequest{Owner: "bea"}); err != nil || len(ls.Repositories) != 1 {
		t.Errorf("bea listing her repositories = %v, %v", ls, err)
	}
	if ls, err := e.cal.ListRepositoryEntries(ctx, &identitypb.ListRepositoryEntriesRequest{Owner: "bea"}); err != nil || len(ls.Repositories) != 0 {
		t.Errorf("cal listing bea's = %v, %v", ls, err)
	}

	// A rename redirects its old name; a transfer is to an owner its admin may make repositories of.
	if _, err := e.bea.RenameRepository(ctx, &identitypb.RenameRepositoryEntryRequest{Id: app.Id, Name: "service"}); code(err) != codes.PermissionDenied {
		t.Errorf("a maintainer renaming: %v", err)
	}
	if got, err := e.ada.RenameRepository(ctx, &identitypb.RenameRepositoryEntryRequest{Id: app.Id, Name: "service"}); err != nil || got.Name != "service" {
		t.Fatalf("RenameRepository = %v, %v", got, err)
	}
	if got, err := resolve(e.bea, "acme", "app"); err != nil || !got.Redirected || got.Name != "service" {
		t.Errorf("the old name = %v, %v", got, err)
	}
	if _, err := e.ada.TransferRepository(ctx, &identitypb.TransferRepositoryRequest{Id: app.Id, Owner: "beta"}); code(err) != codes.PermissionDenied {
		t.Errorf("a transfer to an organization one is not in: %v", err)
	}
	if got, err := e.control.TransferRepository(ctx, &identitypb.TransferRepositoryRequest{Id: app.Id, Owner: "ada", ForPersonId: e.adaID}); err != nil || got.Owner != "ada" {
		t.Fatalf("TransferRepository = %v, %v", got, err)
	}
	if got, err := resolve(e.ada, "acme", "service"); err != nil || !got.Redirected || got.Owner != "ada" {
		t.Errorf("the old owner's name = %v, %v", got, err)
	}
	// eng's grant stayed with acme: bea, no member of ada's, sees ada's private repository not.
	if _, err := e.ada.UpdateRepository(ctx, &identitypb.UpdateRepositoryRequest{Id: app.Id, Visibility: "private"}); err != nil {
		t.Fatal(err)
	}
	if _, err := resolve(e.bea, "ada", "service"); code(err) != codes.NotFound {
		t.Errorf("bea after the transfer: %v", err)
	}

	// Deleted, it is its admins' alone, to restore.
	if got, err := e.ada.DeleteRepository(ctx, &identitypb.RepositoryRef{Id: app.Id}); err != nil || got.State != "deleted" {
		t.Fatalf("DeleteRepository = %v, %v", got, err)
	}
	if got, err := resolve(e.ada, "ada", "service"); err != nil || got.State != "deleted" || got.Role != "admin" {
		t.Errorf("its admin seeing it deleted = %v, %v", got, err)
	}
	if _, err := e.cal.RestoreRepository(ctx, &identitypb.RepositoryRef{Id: app.Id}); code(err) != codes.NotFound {
		t.Errorf("a stranger restoring: %v", err)
	}
	if _, err := e.ada.RenameRepository(ctx, &identitypb.RenameRepositoryEntryRequest{Id: app.Id, Name: "x"}); code(err) != codes.PermissionDenied {
		t.Errorf("renaming a deleted repository: %v", err)
	}
	if got, err := e.ada.RestoreRepository(ctx, &identitypb.RepositoryRef{Id: app.Id}); err != nil || got.State != "active" {
		t.Errorf("RestoreRepository = %v, %v", got, err)
	}

	if got, err := resolve(e.ada, "ada", "service"); err != nil || got.FullName != "ada/service" {
		t.Errorf("a hosted repository's full name = %v, %v", got, err)
	}

	// A person leaves an organization themselves; only its owners remove others.
	if _, err := e.bea.RemoveMember(ctx, &identitypb.RemoveMemberRequest{Handle: "acme", PrincipalId: e.adaID}); code(err) != codes.PermissionDenied {
		t.Errorf("a member removing an owner: %v", err)
	}
	if got, err := e.bea.RemoveMember(ctx, &identitypb.RemoveMemberRequest{Handle: "acme", PrincipalId: e.beaID}); err != nil || !got.Removed {
		t.Errorf("a member leaving = %v, %v", got, err)
	}
}

// An installation: one organization, whose owners are its administrators and whose members are its
// people, every one of whom makes its repositories, which are internal; a person's own are private.
func TestTenancyInstallation(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, false)
	if _, err := e.ada.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); code(err) != codes.FailedPrecondition {
		t.Errorf("an organization made in an installation: %v", err)
	}
	if o, err := e.bea.GetOrganization(ctx, &identitypb.GetOrganizationRequest{Handle: "example"}); err != nil || !o.Installation || o.BaseRole != "write" {
		t.Errorf("the installation's organization = %v, %v", o, err)
	}
	for name, err := range map[string]error{
		"setting a member": func() error {
			_, err := e.ada.SetMember(ctx, &identitypb.SetMemberRequest{Handle: "example", PrincipalId: e.beaID, Role: "owner"})
			return err
		}(),
		"making a team": func() error {
			_, err := e.ada.CreateTeam(ctx, &identitypb.CreateTeamRequest{Handle: "example", Team: "eng"})
			return err
		}(),
		"listing members": func() error {
			_, err := e.ada.ListMembers(ctx, &identitypb.ListMembersRequest{Handle: "example"})
			return err
		}(),
	} {
		if code(err) != codes.FailedPrecondition {
			t.Errorf("%s of the installation's organization: %v", name, err)
		}
	}

	r, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Name: "app", ForPersonId: e.beaID})
	if err != nil || r.Owner != "example" || r.Visibility != "internal" || r.FullName != "app" {
		t.Fatalf("a member making the installation's repository = %v, %v", r, err)
	}
	if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: r.Id}); err != nil {
		t.Fatal(err)
	}
	for who, tc := range map[string]struct {
		c    identitypb.TenancyClient
		role string
	}{"ada, an administrator": {e.ada, "admin"}, "bea, its maker": {e.bea, "admin"}, "cal, a member": {e.cal, "write"}} {
		if got, err := tc.c.ResolveRepository(ctx, &identitypb.ResolveRepositoryRequest{Name: "app"}); err != nil || got.Role != tc.role {
			t.Errorf("%s = %v, %v; want %s", who, got, err, tc.role)
		}
	}
	if got, err := e.scm.ResolveRepository(ctx, &identitypb.ResolveRepositoryRequest{Owner: "example", Name: "app"}); err != nil || got.Id != r.Id {
		t.Errorf("by the organization's handle = %v, %v", got, err)
	}
	own, err := e.scm.ReserveRepository(ctx, &identitypb.ReserveRepositoryRequest{Owner: "cal", Name: "notes", ForPersonId: e.calID})
	if err != nil || own.Visibility != "private" || own.FullName != "cal/notes" {
		t.Fatalf("a person's own repository = %v, %v", own, err)
	}
	if _, err := e.scm.ActivateRepository(ctx, &identitypb.RepositoryRef{Id: own.Id}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.bea.ResolveRepository(ctx, &identitypb.ResolveRepositoryRequest{Owner: "cal", Name: "notes"}); code(err) != codes.NotFound {
		t.Errorf("another's private repository: %v", err)
	}
}

// The directory places a new repository in the open cell holding the fewest — those made before
// there were cells counting as the first's, a tie going to the earlier — never in a closed one; a
// fork joins its network's cell; and a cell no one configured is refused (docs/hosted.md, "Cells,
// as built").
func TestPlacement(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	reserve := func(name string, parent int64, cell string) (store.Repository, error) {
		return e.store.ReserveRepository(ctx, store.NewRepository{Owner: e.adaID, Name: name, Visibility: tenancy.Private, Propose: tenancy.Read,
			Parent: parent, Cell: cell, CreatedBy: e.adaID})
	}
	old, err := reserve("old", 0, "")
	if err != nil || old.Cell != "" {
		t.Fatalf("a repository made before there were cells: %+v, %v", old, err)
	}
	e.store.SetCells([]store.Cell{{Name: "a", Open: true}, {Name: "b", Open: true}, {Name: "c"}}, "")
	if got := e.store.CellOf(old); got != "a" {
		t.Errorf("the cell of one made before there were cells: %q, want the first, a", got)
	}
	if pb, err := e.store.Entry(ctx, old); err != nil || pb.GetCell() != "a" {
		t.Errorf("its entry: %v, %v; want it in a", pb, err)
	}
	var last store.Repository
	for i, want := range []string{"b", "a", "b", "a"} {
		if last, err = reserve(fmt.Sprint("r", i), 0, ""); err != nil || last.Cell != want {
			t.Fatalf("repository %d: %+v, %v; want it in %s", i, last, err, want)
		}
	}
	// Each cell holds three now; the fork goes with its parent, in a, wherever the fewest are.
	if _, err := e.store.ActivateRepository(ctx, last.ID); err != nil {
		t.Fatal(err)
	}
	if f, err := reserve("fork", last.ID, ""); err != nil || f.Cell != "a" {
		t.Errorf("a fork of a repository in a: %+v, %v", f, err)
	}
	if _, err := reserve("nowhere", 0, "zzz"); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a cell no one configured: %v", err)
	}
}

// An owner's new repositories go to an open cell of its region — the one it set, or the directory's
// default — any open cell when none is open there; a person sets their own region, an
// organization's owners its, to one a cell is in.
func TestRegions(t *testing.T) {
	ctx := context.Background()
	e := newTenancyEnv(t, true)
	e.store.SetCells([]store.Cell{{Name: "us-1", Region: "us", Open: true}, {Name: "us-2", Region: "us", Open: true},
		{Name: "eu-1", Region: "eu", Open: true}, {Name: "ap-1", Region: "ap"}}, "us")
	if _, err := e.ada.CreateOrganization(ctx, &identitypb.CreateOrganizationRequest{Handle: "acme"}); err != nil {
		t.Fatal(err)
	}
	acme, err := e.store.OrganizationByHandle(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if r, err := e.ada.GetRegion(ctx, &identitypb.GetRegionRequest{Owner: "ada"}); err != nil || r.GetRegion() != "" || r.GetPlacedIn() != "us" {
		t.Errorf("ada's region before she sets one: %v, %v", r, err)
	}
	for name, tc := range map[string]struct {
		as     identitypb.TenancyClient
		owner  string
		region string
		want   codes.Code
	}{
		"her own":            {e.ada, "ada", "eu", codes.OK},
		"another's":          {e.bea, "ada", "us", codes.PermissionDenied},
		"no cell's":          {e.ada, "ada", "mars", codes.InvalidArgument},
		"her organization's": {e.ada, "acme", "ap", codes.OK},
		"not an owner's":     {e.bea, "acme", "us", codes.PermissionDenied},
	} {
		if _, err := tc.as.SetRegion(ctx, &identitypb.SetRegionRequest{Owner: tc.owner, Region: tc.region}); status.Code(err) != tc.want {
			t.Errorf("%s: %v, want %s", name, err, tc.want)
		}
	}
	reserve := func(owner, name string) string {
		t.Helper()
		r, err := e.store.ReserveRepository(ctx, store.NewRepository{Owner: owner, Name: name, Visibility: tenancy.Private, Propose: tenancy.Read,
			CreatedBy: e.adaID})
		if err != nil {
			t.Fatal(err)
		}
		return r.Cell
	}
	if got := reserve(e.adaID, "a1"); got != "eu-1" {
		t.Errorf("ada's repository went to %s, want eu-1, her region's", got)
	}
	// acme's region, ap, has no open cell: any open one, the fewest.
	if got := reserve(acme.ID, "a2"); got != "us-1" {
		t.Errorf("acme's repository went to %s, want us-1, the first of the open ones holding fewest", got)
	}
	// bea named none: the default region's cells, the fewest.
	for i, want := range []string{"us-2", "us-1", "us-2"} {
		if got := reserve(e.beaID, fmt.Sprint("b", i)); got != want {
			t.Errorf("bea's repository %d went to %s, want %s", i, got, want)
		}
	}
	if _, err := e.ada.SetRegion(ctx, &identitypb.SetRegionRequest{Owner: "ada", Region: ""}); err != nil {
		t.Fatal(err)
	}
	if r, err := e.ada.GetRegion(ctx, &identitypb.GetRegionRequest{Owner: "ada"}); err != nil || r.GetRegion() != "" || r.GetPlacedIn() != "us" {
		t.Errorf("ada's region once forgotten: %v, %v", r, err)
	}
}
