package web

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/wait/waittest"
)

// waitsDB is a store whose registrations are in memory.
type waitsDB struct {
	db.Store
	waits *waittest.Store
}

func (d waitsDB) Waits() wait.Store { return d.waits }

// events reads a stream's events, by name, as they come.
func events(t *testing.T, url string) <-chan string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type %q", ct)
	}
	out := make(chan string, 16)
	go func() {
		defer close(out)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if name, ok := strings.CutPrefix(sc.Text(), "event: "); ok {
				out <- name
			}
		}
	}()
	return out
}

func next(t *testing.T, evs <-chan string, within time.Duration) (string, bool) {
	t.Helper()
	select {
	case e, ok := <-evs:
		return e, ok
	case <-time.After(within):
		return "", false
	}
}

// A page's change events say "change" as the stream opens — what changed since the page rendered —
// then once for each change of what it registered on, and nothing for another's; a registration a
// change calls for is made, and its changes come too. Closed, the stream leaves no registration.
func TestLiveEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ws := waittest.New()
	go wait.Run(ctx, ws, ws.Jobs().Queue, nil)
	s := &Server{db: waitsDB{waits: ws}}
	landed := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.liveEvents(w, r, []wait.Reg{{Store: ws, Subjects: []string{"cl"}}}, func(context.Context) ([]wait.Reg, error) {
			if landed {
				return nil, nil
			}
			landed = true
			return []wait.Reg{{Store: ws, Subjects: []string{"commit"}}}, nil
		})
	}))
	defer func() {
		srv.CloseClientConnections()
		srv.Close()
	}()
	evs := events(t, srv.URL)
	if e, ok := next(t, evs, 5*time.Second); !ok || e != "change" {
		t.Fatalf("the first event: %q, %v; want change", e, ok)
	}
	ws.Change(ctx, "other")
	if e, ok := next(t, evs, 300*time.Millisecond); ok {
		t.Fatalf("an event %q for what the page did not register on", e)
	}
	ws.Change(ctx, "cl")
	if e, ok := next(t, evs, 5*time.Second); !ok || e != "change" {
		t.Fatalf("after the CL changed: %q, %v", e, ok)
	}
	if ws.Registered("commit") != 1 {
		t.Fatal("the registration the change called for was not made")
	}
	ws.Change(ctx, "commit")
	if e, ok := next(t, evs, 5*time.Second); !ok || e != "change" {
		t.Fatalf("after the commit changed: %q, %v", e, ok)
	}
	srv.CloseClientConnections()
	deadline := time.Now().Add(5 * time.Second)
	for ws.Registered("cl")+ws.Registered("commit") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("registrations left after the stream closed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
