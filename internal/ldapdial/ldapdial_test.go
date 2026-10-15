package ldapdial

import "testing"

func TestGUIDString(t *testing.T) {
	b := []byte{0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
	if got := GUIDString(b); got != "00112233-4455-6677-8899-aabbccddeeff" {
		t.Errorf("GUIDString = %s", got)
	}
}

func TestNormalizeDN(t *testing.T) {
	for in, want := range map[string]string{
		"UID=Ada, OU=People ,DC=Example,DC=com": "uid=ada,ou=people,dc=example,dc=com",
		"cn=a+sn=b,dc=x":                        "cn=a+sn=b,dc=x",
		"not a dn":                              "not a dn",
	} {
		if got := NormalizeDN(in); got != want {
			t.Errorf("NormalizeDN(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDialerRefusesPlaintext(t *testing.T) {
	if _, err := Dialer(Config{URL: "ldap://ldap.example.com"}); err == nil {
		t.Error("plaintext LDAP was accepted")
	}
	if _, err := Dialer(Config{URL: "ldap://ldap.example.com", StartTLS: true}); err != nil {
		t.Errorf("StartTLS: %v", err)
	}
	if _, err := Dialer(Config{URL: "ldaps://ldap.example.com", CAFile: "/nonexistent"}); err == nil {
		t.Error("a missing CA file was accepted")
	}
}
