package e2e

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
)

// TestWebhooks drives a repository's webhook through ok control (docs/hosted.md, "ok control at scale,
// as built"): its administrator adds it, its secret shown once; it is pinged; a CL's events reach it
// signed with that secret, a failing answer retried; and no one else manages it.
func TestWebhooks(t *testing.T) {
	e := newEnv(t)
	e.post(alice, "/repositories/new", url.Values{"name": {"hooked"}})
	var mu sync.Mutex
	type got struct {
		event, sig string
		body       []byte
	}
	var gots []got
	failFirst := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		gots = append(gots, got{r.Header.Get("X-Ok-Event"), r.Header.Get("X-Ok-Signature-256"), b})
		if r.Header.Get("X-Ok-Event") == "cl" && failFirst {
			failFirst = false
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	base := "/example/hooked/webhooks"
	if !e.get(alice, "/example/hooked").has(`id="webhooks-link"`) || e.get(bob, "/example/hooked").has(`id="webhooks-link"`) {
		t.Error("the repository's page links its webhooks for its administrator alone")
	}
	if p := e.do(bob, http.MethodGet, base, nil, nil); p.status != http.StatusForbidden {
		t.Errorf("a non-administrator's webhooks page: %d", p.status)
	}
	p := e.post(alice, base, url.Values{"url": {srv.URL + "/ok"}, "event": {"cl"}})
	m := regexp.MustCompile(`(?s)id="webhook-secret".*?font-mono[^>]*>([0-9a-f]{64})<`).FindStringSubmatch(p.body)
	if m == nil {
		t.Fatalf("no secret shown: %s", excerpt(p.body))
	}
	secret := m[1]
	if strings.Contains(e.get(alice, base).body, secret) {
		t.Error("the secret is shown again")
	}
	if p := e.do(alice, http.MethodPost, base, url.Values{"url": {"ftp://nowhere"}, "event": {"cl"}}, nil); p.status != http.StatusUnprocessableEntity {
		t.Errorf("a webhook of an ftp URL: %d", p.status)
	}

	a := e.person(alice)
	a.jj("", "clone", "hooked")
	a.write("hooked/notes.md", "hello\n")
	a.jj("hooked", "describe", "-m", "hooked change")
	cl := uploaded(a.jj("hooked", "cl"))[0]
	e.post(alice, clPath("hooked", cl)+"/request-review", url.Values{"reviewer": {e.id(bob)}})
	signed := func(g got) bool {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(g.body)
		return g.sig == "sha256="+hex.EncodeToString(mac.Sum(nil))
	}
	e.waitFor("the ping and the CL's review request delivered", func() bool {
		mu.Lock()
		defer mu.Unlock()
		var ping, requested bool
		for _, g := range gots {
			if !signed(g) {
				continue
			}
			var b struct{ Event, Action string }
			_ = json.Unmarshal(g.body, &b)
			ping = ping || b.Event == "ping"
			requested = requested || (b.Event == "cl" && b.Action == "review_requested")
		}
		return ping && requested
	})
	mu.Lock()
	for _, g := range gots {
		if !signed(g) {
			t.Errorf("an unsigned delivery: %s %s", g.event, g.body)
		}
	}
	mu.Unlock()
	// The page lists the deliveries, the failed one among them with what the endpoint answered.
	if p := e.get(alice, base); !strings.Contains(p.body, "HTTP 502") || !strings.Contains(p.body, `data-state="delivered"`) {
		t.Errorf("the deliveries listed: %s", excerpt(p.body))
	}
}
