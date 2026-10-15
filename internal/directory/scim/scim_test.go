package scim

import "testing"

func TestParseFilter(t *testing.T) {
	f, err := parseFilter(`emails[type eq "work"].value eq "a@b.c" and active eq true`)
	if err != nil || len(f) != 2 || f[0].attr != "emails.value" || f[1].value != "true" {
		t.Errorf("parseFilter = %+v, %v", f, err)
	}
	for _, bad := range []string{``, `userName`, `userName eq`, `(userName eq "a")`, `userName gt "a"`, `userName eq "a" and`, `userName eq "unterminated`} {
		if _, err := parseFilter(bad); err == nil {
			t.Errorf("parseFilter(%q) accepted it", bad)
		}
	}
}
