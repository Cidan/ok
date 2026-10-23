// Package webhooks delivers repositories' events to their webhooks (docs/hosted.md, "ok control at
// scale, as built"): each event a webhook wants queued as a delivery when its repository's outbox
// drains, sent as keyed work by repository — a POST signed with the webhook's secret, retried after
// 30 seconds doubling to an hour, kept dead after a day — and the last hundred of each webhook kept.
package webhooks

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"syscall"
	"time"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/data"
	"github.com/Cidan/ok/internal/id/seal"
	"github.com/Cidan/ok/internal/move"
	"github.com/Cidan/ok/internal/work"
)

// Kept is how many of a webhook's deliveries are kept once done.
const Kept = 100

// GiveUp is how long a delivery is tried before it is kept, dead.
const GiveUp = 24 * time.Hour

// timeout bounds one delivery's request.
const timeout = 15 * time.Second

// maxURL bounds a webhook's URL.
const maxURL = 2000

// Hooks are repositories' webhooks: creating them, and delivering to them.
type Hooks struct {
	DB     db.Store
	Sealer *seal.Sealer
	// AllowPrivate lets webhooks reach private, loopback, and link-local addresses, over http too.
	AllowPrivate bool
	// PublicURL is ok control's, which payloads link CLs under.
	PublicURL string
	// Installation is the installation's organization's handle ("" for a hosted service).
	Installation string
	Log          *slog.Logger
	// Look is how often marked deliveries are looked for: two seconds unless set.
	Look time.Duration
	// Now is the clock; unset, time.Now.
	Now    func() time.Time
	client *http.Client
}

func (h *Hooks) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

// purpose binds a webhook's sealed secret to it: a sealed secret moved to another row does not open.
func purpose(repo, id int64) string { return fmt.Sprintf("webhook:%d:%d", repo, id) }

// ErrRefused is a webhook ok will not make, or deliver to.
var ErrRefused = errors.New("webhooks")

// Check refuses a URL ok will not deliver to: not https (or http, where private networks are
// allowed), or no host.
func (h *Hooks) Check(raw string) error {
	if len(raw) > maxURL {
		return fmt.Errorf("%w: a URL of at most %d characters", ErrRefused, maxURL)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" {
		return fmt.Errorf("%w: %q is no URL with a host", ErrRefused, raw)
	}
	switch {
	case u.Scheme == "https":
	case u.Scheme == "http" && h.AllowPrivate:
	default:
		return fmt.Errorf("%w: a webhook's URL is https", ErrRefused)
	}
	if u.User != nil {
		return fmt.Errorf("%w: a webhook's URL carries no credentials: its secret signs each delivery", ErrRefused)
	}
	if !h.AllowPrivate {
		if a, err := netip.ParseAddr(u.Hostname()); err == nil && !public(a) {
			return fmt.Errorf("%w: %s is not a public address", ErrRefused, a)
		}
	}
	return nil
}

// Create makes a webhook of a repository, for events, with a secret of its own, which it returns —
// shown once — and queues a ping to it.
func (h *Hooks) Create(ctx context.Context, repo int64, rawURL string, events int64, by string) (db.Webhook, string, error) {
	if h.Sealer == nil {
		return db.Webhook{}, "", fmt.Errorf("%w: this installation keeps no webhooks (control.sealing_key_file)", ErrRefused)
	}
	if err := h.Check(rawURL); err != nil {
		return db.Webhook{}, "", err
	}
	if events&(db.WebhookCL|db.WebhookPush) == 0 || events&^(db.WebhookCL|db.WebhookPush) != 0 {
		return db.Webhook{}, "", fmt.Errorf("%w: a webhook wants cl events, push events, or both", ErrRefused)
	}
	raw := make([]byte, 32)
	rand.Read(raw)
	secret := hex.EncodeToString(raw)
	w := db.Webhook{Repository: repo, ID: data.NewID(), URL: rawURL, Events: events, CreatedBy: by, CreatedAt: h.now()}
	w.Secret = h.Sealer.Seal([]byte(secret), purpose(repo, w.ID))
	ctx = move.WithRepository(ctx, repo)
	if err := h.DB.CreateWebhook(ctx, w); err != nil {
		return db.Webhook{}, "", err
	}
	ping, err := payload(map[string]any{"event": "ping", "webhook": strconv.FormatInt(w.ID, 10), "repository": map[string]any{"id": repo}})
	if err != nil {
		return db.Webhook{}, "", err
	}
	if err := h.DB.QueueDeliveries(ctx, repo, []db.Delivery{{Repository: repo, ID: data.NewID(), Webhook: w.ID, Event: "ping", Payload: ping,
		NextTry: h.now(), CreatedAt: h.now()}}); err != nil {
		return db.Webhook{}, "", err
	}
	return w, secret, nil
}

// Redeliver queues a delivery's payload again, as a delivery of its own.
func (h *Hooks) Redeliver(ctx context.Context, repo, id int64) error {
	d, ok, err := h.DB.GetDelivery(ctx, repo, id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: no delivery %d", ErrRefused, id)
	}
	again := db.Delivery{Repository: repo, ID: data.NewID(), Webhook: d.Webhook, Event: d.Event, Payload: d.Payload, NextTry: h.now(),
		CreatedAt: h.now()}
	return h.DB.QueueDeliveries(move.WithRepository(ctx, repo), repo, []db.Delivery{again})
}

// Run sends deliveries as they are marked, and as their retries fall due, until ctx ends; a
// repository with deliveries waiting that no mark names — moved in — is marked every minute.
func (h *Hooks) Run(ctx context.Context) error {
	look := h.Look
	if look <= 0 {
		look = 2 * time.Second
	}
	var swept time.Time
	return (&work.Runner{
		Kind:  db.WebhookWork,
		Store: h.DB.Work(),
		Do:    h.Send,
		Sweep: func(ctx context.Context) error {
			if time.Since(swept) < time.Minute {
				return nil
			}
			swept = time.Now()
			repos, err := h.DB.WaitingDeliveries(ctx)
			if err != nil {
				return err
			}
			for _, r := range repos {
				if err := h.DB.Work().Mark(ctx, db.WebhookWork, r); err != nil {
					return err
				}
			}
			return nil
		},
		Idle: look,
		Log:  h.Log,
	}).Run(ctx)
}

// Send sends a repository's due deliveries, and marks its work for the next to fall due.
func (h *Hooks) Send(ctx context.Context, repo int64) error {
	ctx = move.WithRepository(ctx, repo)
	hooks, err := h.DB.Webhooks(ctx, repo)
	if err != nil {
		return err
	}
	byID := map[int64]db.Webhook{}
	for _, w := range hooks {
		byID[w.ID] = w
	}
	touched := map[int64]bool{}
	for {
		due, err := h.DB.DueDeliveries(ctx, repo, h.now(), 50)
		if err != nil {
			return err
		}
		for _, d := range due {
			touched[d.Webhook] = true
			if err := h.DB.UpdateDelivery(ctx, h.deliver(ctx, byID, d)); err != nil {
				return err
			}
		}
		if len(due) < 50 {
			break
		}
	}
	for w := range touched {
		if err := h.DB.PruneDeliveries(ctx, repo, w, Kept); err != nil {
			return err
		}
	}
	next, err := h.DB.NextDelivery(ctx, repo)
	if err != nil || next.IsZero() {
		return err
	}
	return h.DB.Work().MarkAt(ctx, db.WebhookWork, repo, next)
}

// deliver tries a delivery once, and says what came of it.
func (h *Hooks) deliver(ctx context.Context, hooks map[int64]db.Webhook, d db.Delivery) db.Delivery {
	d.Attempts++
	w, ok := hooks[d.Webhook]
	if !ok {
		d.State, d.LastError = db.DeliveryDead, "its webhook was removed"
		return d
	}
	status, err := h.post(ctx, w, d)
	d.LastStatus = status
	switch {
	case err == nil:
		d.State, d.LastError = db.DeliveryDelivered, ""
		return d
	case h.now().Sub(d.CreatedAt) >= GiveUp:
		d.State, d.LastError = db.DeliveryDead, err.Error()
	default:
		backoff := 30 * time.Second << min(d.Attempts-1, 7)
		d.NextTry, d.LastError = h.now().Add(min(backoff, time.Hour)), err.Error()
	}
	return d
}

// post sends a delivery to its webhook, signed with the webhook's secret: a 2xx answer is delivered.
func (h *Hooks) post(ctx context.Context, w db.Webhook, d db.Delivery) (int, error) {
	if err := h.Check(w.URL); err != nil {
		return 0, err
	}
	secret, err := h.Sealer.Open(w.Secret, purpose(w.Repository, w.ID))
	if err != nil {
		return 0, fmt.Errorf("its secret does not open: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(d.Payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ok-webhooks")
	req.Header.Set("X-Ok-Event", d.Event)
	req.Header.Set("X-Ok-Delivery", strconv.FormatInt(d.ID, 10))
	req.Header.Set("X-Ok-Signature-256", Sign(secret, d.Payload))
	resp, err := h.httpClient().Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, fmt.Errorf("the endpoint answered %s", resp.Status)
	}
	return resp.StatusCode, nil
}

// Sign is a payload's signature with a secret: sha256= and its HMAC-SHA256, in hex.
func Sign(secret, payload []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write(payload)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

// httpClient follows no redirect, and dials only public addresses — checked as it connects, so a
// name that resolves anew reaches no private one — unless private networks are allowed.
func (h *Hooks) httpClient() *http.Client {
	if h.client != nil {
		return h.client
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	if !h.AllowPrivate {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			ap, err := netip.ParseAddrPort(address)
			if err != nil {
				return err
			}
			if !public(ap.Addr()) {
				return fmt.Errorf("%w: %s is not a public address", ErrRefused, ap.Addr())
			}
			return nil
		}
	}
	h.client = &http.Client{
		Transport: &http.Transport{DialContext: dialer.DialContext, TLSHandshakeTimeout: 10 * time.Second, MaxIdleConnsPerHost: 2,
			IdleConnTimeout: 90 * time.Second, Proxy: nil},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return h.client
}

// public reports whether an address is one on the internet: not loopback, private, link-local,
// unspecified, multicast, or the shared or documentation ranges.
func public(a netip.Addr) bool {
	a = a.Unmap()
	if !a.IsValid() || a.IsLoopback() || a.IsPrivate() || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() || a.IsInterfaceLocalMulticast() ||
		a.IsMulticast() || a.IsUnspecified() {
		return false
	}
	for _, p := range nonPublic {
		if p.Contains(a) {
			return false
		}
	}
	return true
}

var nonPublic = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),   // shared address space (carrier NAT)
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),    // documentation
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking
	netip.MustParsePrefix("198.51.100.0/24"), // documentation
	netip.MustParsePrefix("203.0.113.0/24"),  // documentation
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved
	netip.MustParsePrefix("64:ff9b::/96"),    // NAT64, which can reach private IPv4
	netip.MustParsePrefix("2001:db8::/32"),   // documentation
}

// deliveryID is the delivery of an outbox entry to a webhook: the same however often it is queued.
func deliveryID(entry, webhook int64) int64 {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(entry))
	binary.BigEndian.PutUint64(b[8:], uint64(webhook))
	sum := sha256.Sum256(b[:])
	return int64(binary.BigEndian.Uint64(sum[:8]) &^ (1 << 63))
}
