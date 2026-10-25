// Package storetest is the conformance suite every backend of identity's store
// (internal/id/store) passes: run it with Run, giving it a new, empty backend for each test.
package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/directory/scim/scimtest"
	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/identity"
)

var names = identity.MustNames("example.com")

// Run runs the suite, each test on a store over a new, empty backend that open makes.
func Run(t *testing.T, open func(t *testing.T) store.Backend) {
	for _, tc := range []struct {
		name string
		fn   func(t *testing.T, s *store.Store)
	}{
		{"Robots", testRobots},
		{"Sync", testSync},
		{"SyncAtScale", testSyncAtScale},
		{"Tree", testTree},
		{"DeviceLogin", testDeviceLogin},
		{"Credentials", testCredentials},
		{"SessionsAndCeremonies", testSessionsAndCeremonies},
		{"Approvals", testApprovals},
		{"SecretsRoots", testSecretsRoots},
		{"Tenancy", testTenancy},
		{"DirectoryLog", testDirectoryLog},
		{"Names", testNames},
		{"Tokens", testTokens},
		{"OrganizationRobots", testOrganizationRobots},
		{"Apps", testApps},
		{"Accounts", testAccounts},
		{"Enterprise", testEnterprise},
		{"ForkRules", testForkRules},
		{"Deletions", testDeletions},
		{"MoveIn", testMoveIn},
		{"NetworkMoves", testNetworkMoves},
		{"OwnerRegions", testOwnerRegions},
		{"Quotas", testQuotas},
		{"Search", testSearch},
		{"Inbox", testInbox},
		{"Notifications", testNotifications},
		{"DeployAgents", testDeployAgents},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, store.New(open(t), names)) })
	}
	for _, tc := range []struct {
		name string
		fn   func(t *testing.T, b store.Backend)
	}{
		{"Sets", testSets},
		{"SetsAtScale", testSetsAtScale},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, open(t)) })
	}
	t.Run("SCIM", func(t *testing.T) {
		scimtest.Run(t, func(t *testing.T) scim.Store { return store.New(open(t), names).SCIM() })
	})
}

// source is a system of record in memory.
type source struct{ snap *directory.Snapshot }

func (s *source) Snapshot(context.Context) (*directory.Snapshot, error) { return s.snap, nil }
func (s *source) Changes(context.Context, string) (*directory.Delta, error) {
	return nil, directory.ErrNoDelta
}

func person(id, handle string, active bool) directory.Person {
	return directory.Person{ExternalID: id, Handle: handle, Email: handle + "@example.com", Name: handle, Active: active}
}

func sync(t *testing.T, s *store.Store, src *source) *directory.Plan {
	t.Helper()
	opts, err := s.SyncOptions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := directory.Sync(context.Background(), names, src, s, opts)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func testRobots(t *testing.T, s *store.Store) {
	ctx := context.Background()
	r, err := s.CreateRobot(ctx, "ci.robot.example.com")
	if err != nil || r.Kind != store.Robot || !r.Active() {
		t.Fatalf("CreateRobot = %+v, %v", r, err)
	}
	if _, err := s.CreateRobot(ctx, "ci.robot.example.com"); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a robot twice: %v", err)
	}
	if _, err := s.CreateRobot(ctx, "ada.human.example.com"); err == nil {
		t.Error("a person's name made a robot")
	}
	if got, err := s.EnsureRobot(ctx, "ci.robot.example.com"); err != nil || got.ID != r.ID {
		t.Errorf("EnsureRobot of an existing robot = %+v, %v", got, err)
	}
	b := store.Binding{Issuer: "https://kubernetes.default.svc", Subject: "system:serviceaccount:ci:runner"}
	if err := s.Bind(ctx, "ci.robot.example.com", b); err != nil {
		t.Fatal(err)
	}
	if got, err := s.BoundRobot(ctx, b); err != nil || got.ID != r.ID {
		t.Errorf("BoundRobot = %+v, %v", got, err)
	}
	other, _ := s.CreateRobot(ctx, "deploy.ok.robot.example.com")
	if err := s.Bind(ctx, other.Name, b); err == nil {
		t.Error("one binding named two robots")
	}
	robots, err := s.Robots(ctx)
	if err != nil || len(robots) != 2 || !reflect.DeepEqual(robots[0].Bindings, []store.Binding{b}) {
		t.Errorf("Robots = %+v, %v", robots, err)
	}
	c, err := s.EnrollKey(ctx, r.ID, "runner", []byte("key-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteRobot(ctx, "ci.robot.example.com"); err != nil {
		t.Fatal(err)
	}
	if c, _ = s.Credential(ctx, c.ID); c.Standing() {
		t.Error("a deleted robot's credential stands")
	}
	if _, err := s.BoundRobot(ctx, b); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a deleted robot's binding: %v", err)
	}
	if _, err := s.CreateRobot(ctx, "ci.robot.example.com"); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a deleted robot's name given again: %v", err)
	}
	if _, err := s.EnsureRobot(ctx, "ci.robot.example.com"); err == nil {
		t.Error("EnsureRobot brought a deleted robot back")
	}
}

func testSync(t *testing.T, s *store.Store) {
	ctx := context.Background()
	if _, err := s.CreateRobot(ctx, "build.robot.example.com"); err != nil {
		t.Fatal(err)
	}
	src := &source{snap: &directory.Snapshot{
		People: []directory.Person{person("1", "ada", true), person("2", "bea", true)},
		Groups: []directory.Group{{ExternalID: "g1", Name: "eng", Members: []directory.Member{{ExternalID: "1"}, {ExternalID: "2"}}}},
	}}
	sync(t, s, src)
	ada, err := s.Principal(ctx, "ada.human.example.com")
	if err != nil || ada.Kind != store.Human || ada.ExternalID != "1" {
		t.Fatalf("ada = %+v, %v", ada, err)
	}
	groups, _ := s.SyncedGroups(ctx)
	if want := map[string][]string{"eng.group.example.com": {"ada.human.example.com", "bea.human.example.com"}}; !reflect.DeepEqual(groups, want) {
		t.Errorf("SyncedGroups = %v", groups)
	}
	session, _ := s.CreateSession(ctx, ada.ID, "oidc", time.Hour, time.Hour)
	key, _ := s.EnrollKey(ctx, ada.ID, "laptop", []byte("ada-key"))
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	beaKey, _ := s.EnrollKey(ctx, bea.ID, "laptop", []byte("bea-key"))

	// ada is renamed and suspended; bea is deleted; a newcomer wants the robot's name.
	src.snap = &directory.Snapshot{
		People: []directory.Person{person("1", "ada-l", false), {ExternalID: "3", Handle: "build", Active: true}},
		Groups: []directory.Group{{ExternalID: "g1", Name: "eng", Members: []directory.Member{{ExternalID: "1"}}}},
	}
	plan := sync(t, s, src)
	if len(plan.Waiting) != 0 {
		// build.human differs from build.robot: kinds keep names apart.
		t.Errorf("waiting: %v", plan.Waiting)
	}
	got, err := s.PrincipalByID(ctx, ada.ID)
	if err != nil || got.Name != "ada-l.human.example.com" || got.Status != directory.Suspended {
		t.Errorf("ada after = %+v, %v", got, err)
	}
	if _, err := s.Session(ctx, session, time.Hour); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a suspended person's session: %v", err)
	}
	if c, _ := s.Credential(ctx, key.ID); c.Standing() || !c.Revoked.IsZero() {
		t.Errorf("a suspended person's credential: standing %v, revoked %v", c.Standing(), c.Revoked)
	}
	if c, _ := s.Credential(ctx, beaKey.ID); c.Revoked.IsZero() {
		t.Error("a deleted person's credential was not revoked")
	}
	retired, _ := s.RetiredNames(ctx)
	var rn []string
	for _, r := range retired {
		rn = append(rn, r.Name)
	}
	if want := []string{"ada.human.example.com", "bea.human.example.com"}; !reflect.DeepEqual(rn, want) {
		t.Errorf("retired = %v, want %v", rn, want)
	}

	// An administrator resolves a newcomer's collision with a retired name.
	src.snap.People = append(src.snap.People, person("4", "ada", true))
	plan = sync(t, s, src)
	if len(plan.Waiting) != 1 || plan.Waiting[0].ExternalID != "4" {
		t.Fatalf("waiting = %v", plan.Waiting)
	}
	if st, _ := s.SyncStatus(ctx); st.LastRun.IsZero() || len(st.Waiting) != 1 {
		t.Errorf("SyncStatus = %+v", st)
	}
	if err := s.SetName(ctx, "4", false, "ada-b"); err != nil {
		t.Fatal(err)
	}
	if plan = sync(t, s, src); len(plan.Waiting) != 0 {
		t.Errorf("still waiting: %v", plan.Waiting)
	}
	if _, err := s.Principal(ctx, "ada-b.human.example.com"); err != nil {
		t.Error(err)
	}
	if err := s.SyncFailed(ctx, errors.New("the source is down")); err != nil {
		t.Fatal(err)
	}
	if st, _ := s.SyncStatus(ctx); st.Error != "the source is down" {
		t.Errorf("SyncStatus after a failure = %+v", st)
	}
}

func tree(t *testing.T, files map[string]string, synced map[string][]string) *identity.Tree {
	t.Helper()
	fs := map[string][]byte{}
	for p, c := range files {
		fs[p] = []byte(c)
	}
	tr, err := identity.LoadFiles(names, fs, identity.WithSynced(synced))
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func testTree(t *testing.T, s *store.Store) {
	ctx := context.Background()
	sync(t, s, &source{snap: &directory.Snapshot{
		People: []directory.Person{person("1", "ada", true)},
		Groups: []directory.Group{{ExternalID: "g1", Name: "it", Members: []directory.Member{{ExternalID: "1"}}}},
	}})
	synced, _ := s.SyncedGroups(ctx)
	first := tree(t, map[string]string{
		"acl.yaml":           "members: [ada.human.example.com]\n",
		"eng/acl.yaml":       "members: [it.group.example.com]\n",
		"eng/ops/group.yaml": "members: []\n",
	}, synced)
	if err := s.ReplaceTree(ctx, store.Applied{}, "c1", 1, first); err != nil {
		t.Fatal(err)
	}
	sets, applied, err := s.Tree(ctx)
	if err != nil || applied.Commit != "c1" || len(sets) != 3 {
		t.Fatalf("Tree = %+v, %+v, %v", sets, applied, err)
	}
	if sets[1].Name != "eng" || sets[1].Owner != "admins.acl.example.com" || !reflect.DeepEqual(sets[1].Members, []string{"it.group.example.com"}) {
		t.Errorf("eng = %+v", sets[1])
	}
	if opts, _ := s.SyncOptions(ctx); !opts.Declared["ops.group.example.com"] {
		t.Errorf("the tree's names are not declared to the sync: %v", opts.Declared)
	}

	second := tree(t, map[string]string{"acl.yaml": "members: [ada.human.example.com]\n"}, synced)
	if err := s.ReplaceTree(ctx, store.Applied{}, "c2", 2, second); !errors.Is(err, store.ErrStale) {
		t.Errorf("an apply over a tree it did not see: %v", err)
	}
	if err := s.ReplaceTree(ctx, applied, "c0", 0, second); !errors.Is(err, store.ErrStale) {
		t.Errorf("an older commit: %v", err)
	}
	if err := s.ReplaceTree(ctx, applied, "c2", 2, second); err != nil {
		t.Fatal(err)
	}
	retired, _ := s.RetiredNames(ctx)
	if len(retired) != 2 || retired[0].Name != "eng.acl.example.com" || retired[1].Name != "ops.group.example.com" {
		t.Errorf("retired = %+v", retired)
	}
	_, applied, _ = s.Tree(ctx)
	if err := s.ReplaceTree(ctx, applied, "c3", 3, first); !errors.Is(err, store.ErrTaken) {
		t.Errorf("a retired name declared again: %v", err)
	}
}

func testDeviceLogin(t *testing.T, s *store.Store) {
	ctx := context.Background()
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true)}}})
	ada, _ := s.Principal(ctx, "ada.human.example.com")

	device, code, expires, err := s.StartDeviceLogin(ctx, []byte("laptop-key"), "laptop", "203.0.113.7")
	if err != nil || len(code) != 9 || code[4] != '-' || time.Until(expires) < 9*time.Minute {
		t.Fatalf("Start = %q, %q, %v, %v", device, code, expires, err)
	}
	if state, _, _ := s.PollDeviceLogin(ctx, device); state != store.PollPending {
		t.Errorf("first poll: %s", state)
	}
	if state, _, _ := s.PollDeviceLogin(ctx, device); state != store.PollSlowDown {
		t.Errorf("a fast poll: %s", state)
	}
	typed := " " + code[:4] + code[5:] + " "
	d, err := s.DeviceLoginByUserCode(ctx, typed)
	if err != nil || d.Label != "laptop" || d.ClientAddress != "203.0.113.7" {
		t.Fatalf("by user code %q: %+v, %v", typed, d, err)
	}
	if err := s.DecideDeviceLogin(ctx, code, ada.ID, true); err != nil {
		t.Fatal(err)
	}
	if err := s.DecideDeviceLogin(ctx, code, ada.ID, false); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("deciding twice: %v", err)
	}
	state, d, err := s.PollDeviceLogin(ctx, device)
	if err != nil || state != store.PollApproved || d.PrincipalID != ada.ID || string(d.PublicKey) != "laptop-key" {
		t.Fatalf("an approved poll: %s, %+v, %v", state, d, err)
	}
	c, err := s.EnrollKey(ctx, ada.ID, d.Label, d.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteDeviceLogin(ctx, device, c.ID); err != nil {
		t.Fatal(err)
	}
	if state, _, _ := s.PollDeviceLogin(ctx, device); state != store.PollExpired {
		t.Errorf("a poll after completion: %s", state)
	}
	if state, _, _ := s.PollDeviceLogin(ctx, "no-such-code"); state != store.PollExpired {
		t.Errorf("an unknown device code: %s", state)
	}

	device, code, _, _ = s.StartDeviceLogin(ctx, []byte("other-key"), "phone", "")
	s.DecideDeviceLogin(ctx, code, ada.ID, false)
	if state, _, _ := s.PollDeviceLogin(ctx, device); state != store.PollDenied {
		t.Errorf("a denied poll: %s", state)
	}

	device, code, _, _ = s.StartDeviceLogin(ctx, []byte("late-key"), "late", "")
	s.SetClock(func() time.Time { return time.Now().Add(store.DeviceLoginLifetime + time.Minute) })
	if state, _, _ := s.PollDeviceLogin(ctx, device); state != store.PollExpired {
		t.Errorf("an expired poll: %s", state)
	}
}

func testCredentials(t *testing.T, s *store.Store) {
	ctx := context.Background()
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true), person("2", "bea", true)}}})
	ada, _ := s.Principal(ctx, "ada.human.example.com")
	bea, _ := s.Principal(ctx, "bea.human.example.com")
	c, err := s.EnrollKey(ctx, ada.ID, "laptop", []byte("k1"))
	if err != nil || !c.Standing() || c.Principal.Name != ada.Name {
		t.Fatalf("EnrollKey = %+v, %v", c, err)
	}
	if again, err := s.EnrollKey(ctx, ada.ID, "laptop", []byte("k1")); err != nil || again.ID != c.ID {
		t.Errorf("the same key again: %+v, %v", again, err)
	}
	if _, err := s.EnrollKey(ctx, bea.ID, "laptop", []byte("k1")); !errors.Is(err, store.ErrKeyEnrolled) {
		t.Errorf("another's key: %v", err)
	}
	notAfter := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	if err := s.Issued(ctx, c.ID, notAfter); err != nil {
		t.Fatal(err)
	}
	if err := s.Revoke(ctx, c.ID); err != nil {
		t.Fatal(err)
	}
	c, _ = s.Credential(ctx, c.ID)
	if c.Standing() || c.LastIssued.IsZero() || !c.NotAfter.Equal(notAfter) {
		t.Errorf("after revoking: %+v", c)
	}
	if _, err := s.EnrollKey(ctx, ada.ID, "laptop", []byte("k1")); !errors.Is(err, store.ErrKeyEnrolled) {
		t.Errorf("a revoked key again: %v", err)
	}
	if err := s.Revoke(ctx, "crd_nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("revoking nothing: %v", err)
	}
	list, _ := s.CredentialsOf(ctx, ada.ID)
	if len(list) != 1 {
		t.Errorf("CredentialsOf = %+v", list)
	}
}

func testSessionsAndCeremonies(t *testing.T, s *store.Store) {
	ctx := context.Background()
	sync(t, s, &source{snap: &directory.Snapshot{People: []directory.Person{person("1", "ada", true)}}})
	ada, _ := s.Principal(ctx, "ada.human.example.com")
	tok, err := s.CreateSession(ctx, ada.ID, "passkey", time.Hour, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if p, err := s.Session(ctx, tok, 10*time.Minute); err != nil || p.ID != ada.ID {
		t.Errorf("Session = %+v, %v", p, err)
	}
	if _, err := s.Session(ctx, "forged", time.Hour); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a forged session: %v", err)
	}
	if err := s.EndSession(ctx, tok); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Session(ctx, tok, time.Hour); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("an ended session: %v", err)
	}

	type state struct{ Nonce, Return string }
	id, err := s.BeginCeremony(ctx, "oidc", state{"n1", "/cl/7"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var got state
	if err := s.EndCeremony(ctx, id, "webauthn", &got); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("another kind's ceremony: %v", err)
	}
	if err := s.EndCeremony(ctx, id, "oidc", &got); err != nil || got != (state{"n1", "/cl/7"}) {
		t.Errorf("EndCeremony = %+v, %v", got, err)
	}
	if err := s.EndCeremony(ctx, id, "oidc", &got); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a ceremony ended twice: %v", err)
	}

	if err := s.AddPasskey(ctx, store.Passkey{ID: []byte("pk1"), PrincipalID: ada.ID, Data: []byte(`{"n":1}`), Label: "phone"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PasskeyUsed(ctx, []byte("pk1"), []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	pk, err := s.PasskeyByID(ctx, []byte("pk1"))
	if err != nil || !sameJSON(pk.Data, `{"n":2}`) || pk.LastUsed.IsZero() {
		t.Errorf("PasskeyByID = %+v (%s), %v", pk, pk.Data, err)
	}
	if err := s.RemovePasskey(ctx, ada.ID, []byte("pk1")); err != nil {
		t.Fatal(err)
	}
	if list, _ := s.Passkeys(ctx, ada.ID); len(list) != 0 {
		t.Errorf("Passkeys after removing = %+v", list)
	}
}

// sameJSON reports whether stored JSON is want's value: a database keeps JSON's value, not its
// spacing or the order of its keys.
func sameJSON(got []byte, want string) bool {
	var g, w any
	return json.Unmarshal(got, &g) == nil && json.Unmarshal([]byte(want), &w) == nil && reflect.DeepEqual(g, w)
}
