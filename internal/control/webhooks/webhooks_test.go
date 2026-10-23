package webhooks

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/db/postgres"
	"github.com/Cidan/ok/internal/control/outbox"
	"github.com/Cidan/ok/internal/id/seal"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/testdb"
)

// A webhook's URL is https with a host and no credentials, at a public address unless private
// networks are allowed, when http is too.
func TestCheck(t *testing.T) {
	strict, open := &Hooks{}, &Hooks{AllowPrivate: true}
	for _, tc := range []struct {
		url          string
		strict, open bool
	}{
		{"https://hooks.example.com/ok", true, true},
		{"http://hooks.example.com/ok", false, true},
		{"https://127.0.0.1/ok", false, true},
		{"https://10.1.2.3/ok", false, true},
		{"https://[::1]/ok", false, true},
		{"https://169.254.169.254/latest", false, true},
		{"https://user:pw@hooks.example.com/", false, false},
		{"ftp://hooks.example.com/", false, false},
		{"https:///nohost", false, false},
		{"https://hooks.example.com/" + strings.Repeat("a", maxURL), false, false},
	} {
		if err := strict.Check(tc.url); (err == nil) != tc.strict {
			t.Errorf("Check(%q), private networks refused: %v", tc.url, err)
		}
		if err := open.Check(tc.url); (err == nil) != tc.open {
			t.Errorf("Check(%q), private networks allowed: %v", tc.url, err)
		}
	}
	for addr, want := range map[string]bool{
		"8.8.8.8": true, "2606:4700::1111": true, "127.0.0.1": false, "10.0.0.1": false, "172.16.5.4": false, "192.168.1.1": false,
		"169.254.169.254": false, "100.64.0.1": false, "0.0.0.0": false, "::1": false, "fe80::1": false, "fc00::1": false,
		"::ffff:127.0.0.1": false, "64:ff9b::a00:1": false, "224.0.0.1": false, "198.51.100.7": false,
	} {
		if got := public(netip.MustParseAddr(addr)); got != want {
			t.Errorf("public(%s) = %v", addr, got)
		}
	}
}

// A signature is the body's HMAC-SHA256 under the secret, in hex, after sha256=.
func TestSign(t *testing.T) {
	m := hmac.New(sha256.New, []byte("s3cret"))
	m.Write([]byte(`{"event":"ping"}`))
	if got, want := Sign([]byte("s3cret"), []byte(`{"event":"ping"}`)), "sha256="+hex.EncodeToString(m.Sum(nil)); got != want {
		t.Errorf("Sign = %s, want %s", got, want)
	}
}

// endpoint is a webhook endpoint: what it got, and what it answers.
type endpoint struct {
	mu     sync.Mutex
	got    []*http.Request
	bodies [][]byte
	status int
}

func (e *endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.got, e.bodies = append(e.got, r), append(e.bodies, b)
	w.WriteHeader(e.status)
}

// A webhook made has a secret of its own, sealed, and is pinged; its deliveries are signed, retried
// after a doubling backoff while its endpoint fails, kept dead after a day, and its events are what
// it wants of a repository's outbox, once each however often queued.
func TestDelivery(t *testing.T) {
	ctx := move.WithRepository(context.Background(), 1)
	s, err := postgres.Open(context.Background(), testdb.Pool(t))
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := seal.New(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	ep := &endpoint{status: http.StatusOK}
	srv := httptest.NewServer(ep)
	defer srv.Close()
	now := time.Unix(1_800_000_000, 0)
	h := &Hooks{DB: s, Sealer: sealer, AllowPrivate: true, PublicURL: "https://ok.example.com", Installation: "acme", Now: func() time.Time { return now }}

	if _, _, err := h.Create(ctx, 1, srv.URL, 0, "ada"); !errors.Is(err, ErrRefused) {
		t.Errorf("a webhook of no events: %v", err)
	}
	hook, secret, err := h.Create(ctx, 1, srv.URL+"/ok", db.WebhookCL, "ada")
	if err != nil || len(secret) != 64 {
		t.Fatalf("Create = %+v, %q, %v", hook, secret, err)
	}
	if strings.Contains(string(hook.Secret), secret) {
		t.Error("the secret is kept in the clear")
	}
	if err := h.Send(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if len(ep.got) != 1 || ep.got[0].Header.Get("X-Ok-Event") != "ping" ||
		ep.got[0].Header.Get("X-Ok-Signature-256") != Sign([]byte(secret), ep.bodies[0]) {
		t.Fatalf("the ping: %d requests, %v", len(ep.got), ep.got)
	}

	// A CL's event and a push, a second on: the webhook wants CLs alone; queued twice, it is
	// delivered once.
	now = now.Add(time.Second)
	cl := db.ChangeList{Repo: 1, Number: 4, ChangeID: []byte{4}, Author: "ada", Status: db.StatusOpen, CurrentPatchset: 2, Description: "fix it",
		CreatedAt: now, UpdatedAt: now}
	if _, err := s.UpsertChangeList(ctx, cl); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordEvent(ctx, 1, 4, db.Event{Actor: "bea", Kind: db.EventLabelGranted, Label: db.LabelLGTM, Patchset: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.QueueHeadMoved(ctx, 1, 7, []byte{0xab, 0xcd}); err != nil {
		t.Fatal(err)
	}
	es, err := s.Outbox(ctx, 1, 10)
	if err != nil || len(es) != 2 {
		t.Fatalf("the outbox: %+v, %v", es, err)
	}
	r := outbox.Repository{ID: 1, Name: "app"}
	for range 2 {
		if err := h.Consume(ctx, r, es); err != nil {
			t.Fatal(err)
		}
	}
	ep.status = http.StatusServiceUnavailable
	if err := h.Send(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if len(ep.got) != 2 || ep.got[1].Header.Get("X-Ok-Event") != "cl" {
		t.Fatalf("the CL's event: %d requests", len(ep.got))
	}
	var body struct {
		Event, Action, Actor string
		CL                   struct {
			Number int64
			URL    string
			Status string
		}
		Detail struct{ Label string }
	}
	if err := json.Unmarshal(ep.bodies[1], &body); err != nil || body.Action != "label_granted" || body.Actor != "bea" || body.CL.Number != 4 ||
		body.CL.URL != "https://ok.example.com/acme/app/cl/4" || body.CL.Status != "open" || body.Detail.Label != "LGTM" {
		t.Errorf("the CL's payload: %s, %v", ep.bodies[1], err)
	}
	ds, err := s.Deliveries(ctx, 1, hook.ID, 10)
	if err != nil || len(ds) != 2 || ds[0].State != db.DeliveryWaiting || ds[0].LastStatus != 503 || !ds[0].NextTry.Equal(now.Add(30*time.Second)) {
		t.Fatalf("after a failure: %+v, %v", ds, err)
	}
	// Not yet due: not sent. Due: sent again, the backoff doubling.
	if err := h.Send(ctx, 1); err != nil || len(ep.got) != 2 {
		t.Errorf("a retry before its time: %d requests, %v", len(ep.got), err)
	}
	now = now.Add(30 * time.Second)
	if err := h.Send(ctx, 1); err != nil || len(ep.got) != 3 {
		t.Fatalf("the retry: %d requests, %v", len(ep.got), err)
	}
	if ds, _ = s.Deliveries(ctx, 1, hook.ID, 10); ds[0].Attempts != 2 || !ds[0].NextTry.Equal(now.Add(time.Minute)) {
		t.Errorf("after a second failure: %+v", ds[0])
	}
	// A day on, failing still: dead.
	now = now.Add(GiveUp)
	if err := h.Send(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if ds, _ = s.Deliveries(ctx, 1, hook.ID, 10); ds[0].State != db.DeliveryDead {
		t.Errorf("after a day of failures: %+v", ds[0])
	}
	// Redelivered once the endpoint answers: a delivery of its own, delivered.
	ep.status = http.StatusNoContent
	if err := h.Redeliver(ctx, 1, ds[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := h.Send(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if ds, _ = s.Deliveries(ctx, 1, hook.ID, 10); len(ds) != 3 || ds[0].State != db.DeliveryDelivered || ds[0].Event != "cl" {
		t.Errorf("the redelivery: %+v", ds)
	}
}

// Without private networks allowed, a private address is refused as it is dialed — after a name
// is resolved, so a name that resolves to one is refused too.
func TestPrivateRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()
	h := &Hooks{}
	_, port, _ := strings.Cut(strings.TrimPrefix(srv.URL, "http://"), ":")
	urls := []string{srv.URL + "/"}
	// A sandbox may resolve no names at all.
	if addrs, err := net.DefaultResolver.LookupHost(context.Background(), "localhost"); err == nil && len(addrs) > 0 {
		urls = append(urls, "http://localhost:"+port+"/")
	}
	for _, u := range urls {
		req, _ := http.NewRequest(http.MethodPost, u, nil)
		if _, err := h.httpClient().Do(req); err == nil || !errors.Is(err, ErrRefused) {
			t.Errorf("dialing %s: %v", u, err)
		}
	}
}
