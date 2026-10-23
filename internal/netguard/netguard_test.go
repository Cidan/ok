package netguard

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func TestPublic(t *testing.T) {
	for addr, want := range map[string]bool{
		"8.8.8.8": true, "2606:4700::1111": true, "140.82.112.3": true,
		"127.0.0.1": false, "10.1.2.3": false, "172.16.0.1": false, "192.168.1.1": false, "169.254.169.254": false,
		"100.64.0.1": false, "0.0.0.0": false, "224.0.0.1": false, "198.18.0.1": false, "192.0.0.1": false,
		"::1": false, "fe80::1": false, "fd00::1": false, "::ffff:127.0.0.1": false, "::ffff:10.0.0.1": false,
		"64:ff9b::a00:1": false, "2002:a00:1::": false, "2001:db8::1": false, "ff02::1": false,
	} {
		if got := Public(netip.MustParseAddr(addr)); got != want {
			t.Errorf("Public(%s) = %v, want %v", addr, got, want)
		}
	}
}

// The guarded client refuses a loopback server over plaintext, and over HTTPS; a development
// server's reaches it; a body over the limit fails rather than being cut short, and one at the limit
// reads whole.
func TestHTTPClient(t *testing.T) {
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "hello") }))
	defer plain.Close()
	if _, err := HTTPClient(false, 1<<20).Get(plain.URL); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Errorf("plaintext: %v", err)
	}
	tls := httptest.NewTLSServer(http.NotFoundHandler())
	defer tls.Close()
	if _, err := HTTPClient(false, 1<<20).Get(tls.URL); err == nil || !strings.Contains(err.Error(), "public address") {
		t.Errorf("loopback over HTTPS: %v", err)
	}
	read := func(max int64) (string, error) {
		resp, err := HTTPClient(true, max).Get(plain.URL)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return string(b), err
	}
	if got, err := read(5); err != nil || got != "hello" {
		t.Errorf("a body at the limit = %q, %v", got, err)
	}
	if _, err := read(4); err == nil {
		t.Error("a body over the limit was read")
	}
}
