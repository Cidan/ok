package static

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
)

// Every asset the pages and the font stylesheet name is served, the vendored ones included.
func TestServesWhatIsReferenced(t *testing.T) {
	css, err := os.ReadFile("fonts.css")
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{"/htmx.min.js", "/hydrate.js", "/nav.js", "/infinite.js", "/livefilter.js", "/clipboard.js", "/fonts.css", "/favicon.svg"}
	for _, m := range regexp.MustCompile(`url\('/assets(/[^']+)'\)`).FindAllStringSubmatch(string(css), -1) {
		paths = append(paths, m[1])
	}
	if len(paths) < 14 {
		t.Fatalf("fonts.css names %d fonts", len(paths)-8)
	}
	h := Handler()
	for _, p := range paths {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		body, _ := io.ReadAll(rec.Body)
		if rec.Code != http.StatusOK || len(body) == 0 {
			t.Errorf("%s: %d, %d bytes", p, rec.Code, len(body))
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/htmx.min.js", nil))
	if !strings.HasPrefix(rec.Body.String(), "var htmx=") || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("htmx: %.40q, %v", rec.Body.String(), rec.Header())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/missing.js", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("a missing asset: %d", rec.Code)
	}
}
