package identity

import (
	"reflect"
	"strings"
	"testing"
)

// changed returns estate's files with edits applied: a path mapped to "" is removed.
func changed(edits map[string]string) map[string]string {
	files := estate()
	for p, c := range edits {
		if c == "" {
			delete(files, p)
			continue
		}
		files[p] = c
	}
	return files
}

func TestDiff(t *testing.T) {
	before := mustLoad(t, estate())
	after := mustLoad(t, changed(map[string]string{
		"platform/acl.yaml":    members(cal, bea, "deploy.acl.example.com"), // adds cal
		"platform/group.yaml":  members(cal, "eng.group.example.com", bea),  // reordered only
		"eng/infra/acl.yaml":   "",
		"eng/infra/group.yaml": "",
		"ops/acl.yaml":         members(cal),           // created
		"ops/infra/acl.yaml":   members(cal),           // infra moves from eng/ to ops/
		"ops/infra/group.yaml": members(cal),           // and so does its group
		"eng/acl.yaml":         members(bea),           // eng/ gains an ACL: its group snaps to it
		"group.yaml":           members(ada, bea, cal), // adds cal
	}))
	got := map[string]Change{}
	for _, c := range Diff(before, after) {
		got[c.Set] = c
	}
	want := map[string]Change{
		"admins.group.example.com": {Set: "admins.group.example.com", Kind: Group, Added: []string{cal}, FromOwner: "admins.acl.example.com", ToOwner: "admins.acl.example.com"},
		"platform.acl.example.com": {Set: "platform.acl.example.com", Kind: ACL, Added: []string{cal}, FromOwner: "admins.acl.example.com", ToOwner: "admins.acl.example.com", FromPath: "platform", ToPath: "platform"},
		"ops.acl.example.com":      {Set: "ops.acl.example.com", Kind: ACL, Created: true, Added: []string{cal}, ToOwner: "admins.acl.example.com", ToPath: "ops"},
		"eng.acl.example.com":      {Set: "eng.acl.example.com", Kind: ACL, Created: true, Added: []string{bea}, ToOwner: "admins.acl.example.com", ToPath: "eng"},
		"eng.group.example.com":    {Set: "eng.group.example.com", Kind: Group, FromOwner: "admins.acl.example.com", ToOwner: "eng.acl.example.com", FromPath: "eng", ToPath: "eng"},
		"infra.acl.example.com":    {Set: "infra.acl.example.com", Kind: ACL, FromOwner: "admins.acl.example.com", ToOwner: "ops.acl.example.com", FromPath: "eng/infra", ToPath: "ops/infra"},
		"infra.group.example.com":  {Set: "infra.group.example.com", Kind: Group, FromOwner: "infra.acl.example.com", ToOwner: "infra.acl.example.com", FromPath: "eng/infra", ToPath: "ops/infra"},
	}
	if len(got) != len(want) {
		t.Errorf("Diff changed %d sets; want %d: %v", len(got), len(want), got)
	}
	for n, w := range want {
		g, ok := got[n]
		if !ok {
			t.Errorf("Diff lacks %s", n)
			continue
		}
		if !reflect.DeepEqual(nilEmpty(g), nilEmpty(w)) {
			t.Errorf("Diff[%s] =\n %+v\nwant\n %+v", n, g, w)
		}
	}
	if c := got["eng.group.example.com"]; c.Moved() || !c.Reowned() {
		t.Errorf("eng.group snapped: Moved=%v Reowned=%v", c.Moved(), c.Reowned())
	}
	if c := got["infra.group.example.com"]; !c.Moved() || c.Reowned() {
		t.Errorf("infra.group moved with its owner: Moved=%v Reowned=%v", c.Moved(), c.Reowned())
	}
	for n, c := range got {
		if c.String() == "" {
			t.Errorf("%s describes as nothing", n)
		}
	}
	if s := got["infra.acl.example.com"].String(); !strings.Contains(s, "move from eng/infra/ to ops/infra/") || !strings.Contains(s, "owner admins.acl.example.com → ops.acl.example.com") {
		t.Errorf("move describes as %q", s)
	}
}

func nilEmpty(c Change) Change {
	if len(c.Added) == 0 {
		c.Added = nil
	}
	if len(c.Removed) == 0 {
		c.Removed = nil
	}
	return c
}

func TestDiffGenesisAndDeletion(t *testing.T) {
	tr := mustLoad(t, estate())
	changes := Diff(nil, tr)
	if len(changes) != len(tr.Sets()) {
		t.Fatalf("genesis diff has %d changes; want one per set", len(changes))
	}
	for _, c := range changes {
		if !c.Created {
			t.Errorf("genesis: %s not created", c.Set)
		}
	}
	after := mustLoad(t, changed(map[string]string{"platform/group.yaml": ""}))
	changes = Diff(tr, after)
	if len(changes) != 1 || !changes[0].Deleted || changes[0].Set != "platform.group.example.com" || changes[0].FromOwner != "platform.acl.example.com" {
		t.Fatalf("Diff = %+v; want platform.group deleted", changes)
	}
	if !strings.Contains(changes[0].String(), "its name is retired") {
		t.Errorf("deletion describes as %q", changes[0].String())
	}
	if d := Diff(tr, mustLoad(t, estate())); len(d) != 0 {
		t.Errorf("an unchanged tree diffs as %v", d)
	}
}

func TestCheckDeletions(t *testing.T) {
	before := mustLoad(t, estate())
	cases := []struct {
		name  string
		edits map[string]string
		want  string // "" for allowed
	}{
		{"an ACL whose group remains", map[string]string{"eng/infra/acl.yaml": ""}, "infra.acl.example.com is deleted, but infra.group.example.com would remain"},
		{"an ACL whose ACL beneath remains", map[string]string{"platform/acl.yaml": "", "platform/group.yaml": members(bea)}, "platform.acl.example.com is deleted, but deploy.acl.example.com, platform.group.example.com would remain"},
		{"an ACL with everything it owns", map[string]string{"eng/infra/acl.yaml": "", "eng/infra/group.yaml": ""}, ""},
		{"an ACL whose sets moved away", map[string]string{"platform/acl.yaml": "", "platform/group.yaml": "", "platform/deploy/acl.yaml": "", "eng/deploy/acl.yaml": members(deployR), "eng/platform/group.yaml": members(bea, cal, "eng.group.example.com")}, ""},
		{"a group", map[string]string{"platform/group.yaml": ""}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			after, err := Load(testNames, fsOf(changed(tc.edits)))
			if err != nil {
				t.Fatalf("Load after: %v", err)
			}
			err = Check(before, after)
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("Check: %v; want it allowed", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("Check: %v; want %q", err, tc.want)
			}
		})
	}
	if err := Check(nil, before); err != nil {
		t.Errorf("Check(genesis) = %v", err)
	}
}

// decide authorizes the change from estate to estate with edits, as approvers.
func decide(t *testing.T, edits map[string]string, approvers ...string) *Decision {
	t.Helper()
	return Authorize(mustLoad(t, estate()), mustLoad(t, changed(edits)), approvers)
}

func TestAuthorizeByOwner(t *testing.T) {
	// admins owns platform.acl: its member may change platform.acl's members.
	d := decide(t, map[string]string{"platform/acl.yaml": members(bea, cal, "deploy.acl.example.com")}, ada)
	if !d.Approved || d.Genesis || len(d.Parts) != 1 {
		t.Fatalf("Decision = %+v; want one accepted part", d)
	}
	if p := d.Parts[0]; p.By != "admins.acl.example.com" || p.Approver != ada || p.Action != Members {
		t.Errorf("part = %+v", p)
	}
	// Someone in no owning ACL may not.
	d = decide(t, map[string]string{"platform/acl.yaml": members(bea, cal, "deploy.acl.example.com")}, cal)
	if d.Approved || len(d.Refused()) != 1 {
		t.Errorf("cal approved a change to platform.acl: %+v", d)
	}
	// No approvers, no approval.
	if d := decide(t, map[string]string{"platform/acl.yaml": members(bea)}); d.Approved {
		t.Error("a change with no approvers is approved")
	}
}

func TestAuthorizeIsNotTransitive(t *testing.T) {
	// deploy.acl is owned by platform.acl, which admins owns: an admin is not platform.acl's member.
	d := decide(t, map[string]string{"platform/deploy/acl.yaml": members(deployR, cal)}, ada)
	if d.Approved {
		t.Fatalf("an admin changed deploy.acl through ownership of its owner: %+v", d)
	}
	if r := d.Refused(); len(r) != 1 || r[0].By != "platform.acl.example.com" {
		t.Errorf("refused = %+v; want the part needing platform.acl", r)
	}
	// platform.acl's members may, including the robot, which is in platform.acl through deploy.acl.
	if d := decide(t, map[string]string{"platform/deploy/acl.yaml": members(deployR, cal)}, bea); !d.Approved {
		t.Errorf("platform.acl's member could not change deploy.acl: %+v", d.Refused())
	}
	if d := decide(t, map[string]string{"platform/deploy/acl.yaml": members(deployR, cal)}, deployR); !d.Approved {
		t.Errorf("platform.acl's member through deploy.acl could not change deploy.acl: %+v", d.Refused())
	}
}

func TestAuthorizeSelfGrantDownTheTree(t *testing.T) {
	// The admin adds themselves to platform.acl, and in the same change changes deploy.acl, which
	// platform.acl owns: the first part's membership accepts the second.
	edits := map[string]string{
		"platform/acl.yaml":        members(bea, "deploy.acl.example.com", ada),
		"platform/deploy/acl.yaml": members(deployR, cal),
	}
	d := decide(t, edits, ada)
	if !d.Approved {
		t.Fatalf("refused: %+v", d.Refused())
	}
	for _, p := range d.Parts {
		if p.Approver != ada {
			t.Errorf("part %+v accepted by %q", p, p.Approver)
		}
	}
	// Without the self-grant, the second part is refused.
	d = decide(t, map[string]string{"platform/deploy/acl.yaml": members(deployR, cal)}, ada)
	if d.Approved {
		t.Error("deploy.acl changed without a grant")
	}
}

func TestAuthorizeNoEscalation(t *testing.T) {
	// cal adds themselves to admins and uses it: authority traces to memberships before.
	edits := map[string]string{
		"acl.yaml":                 members(ada, cal),
		"platform/acl.yaml":        members(bea, "deploy.acl.example.com", cal),
		"platform/deploy/acl.yaml": members(deployR, cal),
	}
	d := decide(t, edits, cal)
	if d.Approved || len(d.Refused()) != 3 {
		t.Fatalf("cal escalated: %+v", d)
	}
	// Added through a group they are in: still nothing before.
	d = decide(t, map[string]string{"acl.yaml": members(ada, "eng.group.example.com")}, cal)
	if d.Approved {
		t.Error("cal reached admins through eng.group")
	}
}

func TestAuthorizeCreatesBeneathNewACLs(t *testing.T) {
	// A new folder with an ACL and a group: the ACL's creation is admins'; the group's is the new
	// ACL's, whose members count once its creation is accepted.
	edits := map[string]string{
		"ops/acl.yaml":   members(ada),
		"ops/group.yaml": members(cal),
	}
	d := decide(t, edits, ada)
	if !d.Approved {
		t.Fatalf("refused: %+v", d.Refused())
	}
	by := map[string]string{}
	for _, p := range d.Parts {
		by[p.Set] = p.By
	}
	if by["ops.acl.example.com"] != "admins.acl.example.com" || by["ops.group.example.com"] != "ops.acl.example.com" {
		t.Errorf("owners asked = %v", by)
	}
	// If the new ACL holds someone else, its group needs them.
	edits["ops/acl.yaml"] = members(cal)
	d = decide(t, edits, ada)
	if d.Approved || len(d.Refused()) != 1 || d.Refused()[0].Set != "ops.group.example.com" {
		t.Errorf("decision = %+v; want only ops.group refused", d)
	}
	if d := decide(t, edits, ada, cal); !d.Approved {
		t.Errorf("with cal approving too: %+v", d.Refused())
	}
}

func TestAuthorizeRemovalKeepsAuthority(t *testing.T) {
	// An admin removes bea from platform.acl and, in the same change, bea changes deploy.acl, which
	// platform.acl owns: her membership before the change still counts.
	edits := map[string]string{
		"platform/acl.yaml":        members("deploy.acl.example.com"),
		"platform/deploy/acl.yaml": members(deployR, cal),
	}
	d := decide(t, edits, ada, bea)
	if !d.Approved {
		t.Fatalf("refused: %+v", d.Refused())
	}
	for _, p := range d.Parts {
		if p.Set == "deploy.acl.example.com" && p.Approver != bea {
			t.Errorf("deploy.acl's change accepted by %s; want bea", p.Approver)
		}
	}
}

func TestAuthorizeDeleteMoveAndSnap(t *testing.T) {
	// Deleting platform.group asks its owner before: platform.acl.
	d := decide(t, map[string]string{"platform/group.yaml": ""}, ada)
	if d.Approved || d.Parts[0].By != "platform.acl.example.com" || d.Parts[0].Action != Delete {
		t.Errorf("delete: %+v", d)
	}
	if d := decide(t, map[string]string{"platform/group.yaml": ""}, bea); !d.Approved {
		t.Errorf("platform.acl's member could not delete platform.group: %+v", d.Refused())
	}

	// Moving infra (an ACL and its group) from beneath admins to beneath ops (a new ACL holding
	// cal): infra.acl needs its old owner, admins, and its new owner, ops.
	move := map[string]string{
		"eng/infra/acl.yaml":   "",
		"eng/infra/group.yaml": "",
		"ops/acl.yaml":         members(cal),
		"ops/infra/acl.yaml":   members(cal),
		"ops/infra/group.yaml": members(cal),
	}
	d = decide(t, move, ada)
	refused := d.Refused()
	if len(refused) != 1 || refused[0].By != "ops.acl.example.com" || refused[0].Action != MoveIn || refused[0].Set != "infra.acl.example.com" {
		t.Errorf("move approved by admins alone: %+v", refused)
	}
	if d := decide(t, move, ada, cal); !d.Approved {
		t.Errorf("move with both owners: %+v", d.Refused())
	}

	// eng/ gains an ACL holding bea: eng.group, and infra.acl beneath it, snap to it, which their
	// old owner, admins, approves.
	snap := map[string]string{"eng/acl.yaml": members(bea)}
	d = decide(t, snap, ada)
	if !d.Approved || len(d.Parts) != 3 {
		t.Fatalf("snap: %+v", d)
	}
	handed := map[string]bool{}
	for _, p := range d.Parts {
		if p.By != "admins.acl.example.com" {
			t.Errorf("snap part %v asks %s", p, p.By)
		}
		if p.Action == Handover {
			if p.To != "eng.acl.example.com" {
				t.Errorf("%v hands to %s", p, p.To)
			}
			handed[p.Set] = true
		}
	}
	if !handed["eng.group.example.com"] || !handed["infra.acl.example.com"] {
		t.Errorf("handed over %v; want eng.group and infra.acl", handed)
	}
	if s := d.Parts[1].String(); s != "hand eng.group.example.com to eng.acl.example.com: a member of admins.acl.example.com (ada.human.example.com)" {
		t.Errorf("part describes as %q", s)
	}
}

func TestAuthorizeAdminsOwnsItself(t *testing.T) {
	if d := decide(t, map[string]string{"acl.yaml": members(ada, bea)}, ada); !d.Approved {
		t.Errorf("an admin could not add an admin: %+v", d.Refused())
	}
	if d := decide(t, map[string]string{"acl.yaml": members(bea)}, ada); !d.Approved {
		t.Errorf("an admin could not hand admins over: %+v", d.Refused())
	}
	if d := decide(t, map[string]string{"acl.yaml": members(ada, bea)}, bea); d.Approved {
		t.Error("bea added herself to admins")
	}
}

func TestAuthorizeGenesis(t *testing.T) {
	d := Authorize(nil, mustLoad(t, estate()), []string{ada})
	if !d.Genesis || d.Approved {
		t.Fatalf("genesis decision = %+v", d)
	}
	if len(d.Parts) != len(mustLoad(t, estate()).Sets()) {
		t.Errorf("genesis lists %d parts", len(d.Parts))
	}
	for _, p := range d.Parts {
		if p.Accepted {
			t.Errorf("genesis accepted %+v by membership", p)
		}
	}
}

func TestAuthorizeNoChange(t *testing.T) {
	d := decide(t, nil, cal)
	if !d.Approved || len(d.Parts) != 0 {
		t.Errorf("an empty change: %+v", d)
	}
}

func TestAuthorizeEligible(t *testing.T) {
	// The admin adds themselves to platform.acl and changes deploy.acl: deploy.acl's part is eligible
	// to platform.acl's members — bea, the admin the change adds, and through deploy.acl, whose
	// accepted change counts too, the robot and cal.
	d := decide(t, map[string]string{
		"platform/acl.yaml":        members(bea, "deploy.acl.example.com", ada),
		"platform/deploy/acl.yaml": members(deployR, cal),
	}, ada)
	for _, p := range d.Parts {
		var want []string
		switch p.Set {
		case "platform.acl.example.com":
			want = []string{ada}
		case "deploy.acl.example.com":
			want = []string{ada, bea, cal, deployR}
		}
		if !reflect.DeepEqual(p.Eligible, want) {
			t.Errorf("%v eligible %v; want %v", p, p.Eligible, want)
		}
	}
	// Refused parts name who could approve them.
	d = decide(t, map[string]string{"platform/deploy/acl.yaml": members(deployR, cal)}, cal)
	if r := d.Refused(); len(r) != 1 || !reflect.DeepEqual(r[0].Eligible, []string{bea, deployR}) {
		t.Errorf("refused %+v", r)
	}
	// The genesis names each part's owner's members in the new tree.
	g := Authorize(nil, mustLoad(t, estate()), nil)
	for _, p := range g.Parts {
		if p.Set == "deploy.acl.example.com" && !reflect.DeepEqual(p.Eligible, []string{bea, deployR}) {
			t.Errorf("genesis: %v eligible %v", p, p.Eligible)
		}
	}
}
