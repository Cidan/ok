package password

import (
	"context"
	"strings"
	"testing"
)

// A hash checks its password and no other, each hash salted apart; what is not an argon2id hash, or
// holds settings out of bounds, checks nothing.
func TestHash(t *testing.T) {
	ctx := context.Background()
	h, err := Hash(ctx, "correct horse battery staple")
	if err != nil || !strings.HasPrefix(h, "$argon2id$v=19$m=65536,t=3,p=4$") {
		t.Fatalf("Hash = %q, %v", h, err)
	}
	h2, _ := Hash(ctx, "correct horse battery staple")
	if h == h2 {
		t.Error("two hashes of one password are the same: no salt")
	}
	if ok, err := Verify(ctx, h, "correct horse battery staple"); err != nil || !ok {
		t.Errorf("Verify the password = %v, %v", ok, err)
	}
	if ok, _ := Verify(ctx, h, "correct horse battery stapler"); ok {
		t.Error("Verify another password")
	}
	for _, bad := range []string{"", "$2a$10$abc", "$argon2id$v=19$m=1,t=1,p=1$c2FsdHNhbHQ$a2V5a2V5a2V5a2V5a2V5", strings.Replace(h, "v=19", "v=16", 1)} {
		if ok, err := Verify(ctx, bad, "x"); ok || err == nil {
			t.Errorf("Verify %q = %v, %v", bad, ok, err)
		}
	}
}

func TestCheck(t *testing.T) {
	for pw, ok := range map[string]bool{
		"short":                          false,
		strings.Repeat("a", 1025):        false,
		"twelve chars":                   true,
		"ada-likes-long-passwords":       false, // the handle
		"lovelace is my password!":       false, // the email's name
		"Correct Horse Battery Staple ✓": true,
	} {
		if err := Check(pw, "ada", "lovelace@example.com"); (err == nil) != ok {
			t.Errorf("Check(%q) = %v, want ok %v", pw, err, ok)
		}
	}
}
