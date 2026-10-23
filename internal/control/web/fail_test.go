package web

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Cidan/ok/internal/cells"
)

// A page about a repository another cell holds answers 421, naming the cell, for the edge in front
// to send the request there.
func TestFailMoved(t *testing.T) {
	w := httptest.NewRecorder()
	fail(w, httptest.NewRequest(http.MethodGet, "/acme/app", nil), cells.Moved(7, "eu-1"))
	if w.Code != http.StatusMisdirectedRequest || w.Header().Get(cells.Header) != "eu-1" {
		t.Errorf("the answer: %d, %s %q", w.Code, cells.Header, w.Header().Get(cells.Header))
	}
}
