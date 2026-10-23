package identity

import "testing"

// Each kind of name is shown as each mode has it, and what either mode writes parses to the
// directory's name; what names nothing does not parse.
func TestForms(t *testing.T) {
	n := MustNames("example.com")
	for name, want := range map[string][2]string{ // installation, hosted
		"ada.human.example.com":     {"ada", "@ada"},
		"eng.acme.team.example.com": {"eng.acme.team.example.com", "@acme/eng"},
		"ci.acme.robot.example.com": {"ci.acme.robot.example.com", "acme/ci"},
		"scm.ok.robot.example.com":  {"scm.ok.robot.example.com", "ok/scm"},
		"ci.robot.example.com":      {"ci.robot.example.com", "ci"},
		"acme.org.example.com":      {"acme.org.example.com", "acme"},
		"eng.acl.example.com":       {"eng.acl.example.com", "eng"},
		"ada.human.elsewhere.org":   {"ada.human.elsewhere.org", "ada.human.elsewhere.org"},
	} {
		if got := n.Present(name, false); got != want[0] {
			t.Errorf("Present(%s, installation) = %q, want %q", name, got, want[0])
		}
		if got := n.Present(name, true); got != want[1] {
			t.Errorf("Present(%s, hosted) = %q, want %q", name, got, want[1])
		}
	}
	for form, want := range map[string]string{
		"@ada": "ada.human.example.com", "Ada": "ada.human.example.com", "ada.human.example.com": "ada.human.example.com",
		"acme/ci": "ci.acme.robot.example.com", "ok/scm": "scm.ok.robot.example.com",
		"ci.robot.example.com": "ci.robot.example.com",
	} {
		if got, ok := n.ParsePrincipal(form); !ok || got != want {
			t.Errorf("ParsePrincipal(%q) = %q, %v; want %q", form, got, ok, want)
		}
	}
	for _, bad := range []string{"", "@", "a b", "acme/", "/ci", "a/b/c", "@acme/eng"} {
		if got, ok := n.ParsePrincipal(bad); ok {
			t.Errorf("ParsePrincipal(%q) = %q", bad, got)
		}
	}
	for form, want := range map[string]string{
		"eng": "eng.acl.example.com", "@acme/eng": "eng.acme.team.example.com", "acme/eng": "eng.acme.team.example.com",
		"eng.group.example.com": "eng.group.example.com", "eng.acme.team.example.com": "eng.acme.team.example.com",
	} {
		if got, ok := n.ParseSet(form); !ok || got != want {
			t.Errorf("ParseSet(%q) = %q, %v; want %q", form, got, ok, want)
		}
	}
	for _, bad := range []string{"", "@ada", "ada.human.example.com", "a/b/c", "Not A Name"} {
		if got, ok := n.ParseSet(bad); ok {
			t.Errorf("ParseSet(%q) = %q", bad, got)
		}
	}
}
