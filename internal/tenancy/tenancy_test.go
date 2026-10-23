package tenancy

import "testing"

func TestHandles(t *testing.T) {
	for _, ok := range []string{"a", "ada", "a-b", "a1-b2-c3", "0", "abcdefghijklmnopqrstuvwxyz0123456789abc"} {
		if err := ValidHandle(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "-a", "a-", "a--b", "Ada", "a_b", "a.b", "abcdefghijklmnopqrstuvwxyz0123456789abcd", "settings", "api"} {
		if err := ValidHandle(bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}

func TestRepositoryNames(t *testing.T) {
	for _, ok := range []string{"a", "app", "my.app", "my_app-2", "0x"} {
		if err := ValidRepositoryName(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", ".a", "a.", "App", "a b", "assets", string(make([]byte, 65))} {
		if err := ValidRepositoryName(bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}

func TestParse(t *testing.T) {
	for r := None; r <= Admin; r++ {
		if got, err := ParseRole(r.String()); err != nil || got != r {
			t.Errorf("role %s: %v, %v", r, got, err)
		}
	}
	for v := Private; v <= Public; v++ {
		if got, err := ParseVisibility(v.String()); err != nil || got != v {
			t.Errorf("visibility %s: %v, %v", v, got, err)
		}
	}
	for _, m := range []Membership{Member, Owner} {
		if got, err := ParseMembership(m.String()); err != nil || got != m {
			t.Errorf("membership %s: %v, %v", m, got, err)
		}
	}
	if _, err := ParseMembership("none"); err == nil {
		t.Error("none is no membership one is given")
	}
	if _, err := ParseRole("owner"); err == nil {
		t.Error("owner is no role")
	}
	if !(Read < Triage && Triage < Write && Write < Maintain && Maintain < Admin) {
		t.Error("roles are ordered")
	}
}

func TestFullNames(t *testing.T) {
	for full, want := range map[string][2]string{"acme/app": {"acme", "app"}, "app": {"", "app"}, "ada/my.app": {"ada", "my.app"}, "settings": {"", "settings"}} {
		owner, name, err := ParseFullName(full)
		if err != nil || owner != want[0] || name != want[1] {
			t.Errorf("ParseFullName(%q) = %q, %q, %v", full, owner, name, err)
		}
		if got := FullName(owner, name, owner == ""); owner != "" && got != full {
			t.Errorf("FullName(%q, %q) = %q", owner, name, got)
		}
	}
	for _, bad := range []string{"", "/app", "acme/", "a/b/c", "Acme/app", "acme/App", "-x/app"} {
		if o, n, err := ParseFullName(bad); err == nil {
			t.Errorf("ParseFullName(%q) = %q, %q", bad, o, n)
		}
	}
	if FullName("example", "app", true) != "app" || FullName("acme", "app", false) != "acme/app" {
		t.Error("FullName")
	}
	if URLPath("app", "example") != "/example/app" || URLPath("acme/app", "example") != "/acme/app" || URLPath("acme/app", "") != "/acme/app" {
		t.Error("URLPath")
	}
}
