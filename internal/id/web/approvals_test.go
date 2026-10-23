package web_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"strings"
	"testing"
	"time"

	"github.com/go-webauthn/webauthn/protocol/webauthncbor"
	"github.com/go-webauthn/webauthn/protocol/webauthncose"
	"github.com/go-webauthn/webauthn/webauthn"

	"github.com/Cidan/ok/internal/id/store"
)

// authenticator is a passkey in software: an ES256 key, its credential ID, and its counter.
type authenticator struct {
	key     *ecdsa.PrivateKey
	id      []byte
	counter uint32
}

// newAuthenticator makes a passkey for a principal and keeps it in the store, as registering one
// would.
func newAuthenticator(t *testing.T, st *store.Store, principalID string) *authenticator {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	a := &authenticator{key: key, id: make([]byte, 16)}
	rand.Read(a.id)
	x, y := make([]byte, 32), make([]byte, 32)
	key.PublicKey.X.FillBytes(x)
	key.PublicKey.Y.FillBytes(y)
	cose, err := webauthncbor.Marshal(webauthncose.EC2PublicKeyData{
		PublicKeyData: webauthncose.PublicKeyData{KeyType: int64(webauthncose.EllipticKey), Algorithm: int64(webauthncose.AlgES256)},
		Curve:         1, XCoord: x, YCoord: y,
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(webauthn.Credential{ID: a.id, PublicKey: cose, AttestationType: "none",
		Flags: webauthn.CredentialFlags{UserPresent: true, UserVerified: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AddPasskey(context.Background(), store.Passkey{ID: a.id, PrincipalID: principalID, Data: data, Label: "test"}); err != nil {
		t.Fatal(err)
	}
	return a
}

var b64u = base64.RawURLEncoding.EncodeToString

// assert answers a ceremony's options, as the browser would send it: an assertion for rpID and
// origin, verifying the user or not.
func (a *authenticator) assert(t *testing.T, options []byte, rpID, origin, userHandle string, verified bool) []byte {
	t.Helper()
	var begun struct {
		Options struct {
			PublicKey struct {
				Challenge string `json:"challenge"`
			} `json:"publicKey"`
		} `json:"options"`
	}
	if err := json.Unmarshal(options, &begun); err != nil {
		t.Fatalf("the options %s: %v", options, err)
	}
	client, _ := json.Marshal(map[string]string{"type": "webauthn.get", "challenge": begun.Options.PublicKey.Challenge, "origin": origin})
	rp := sha256.Sum256([]byte(rpID))
	flags := byte(0x01) // user present
	if verified {
		flags |= 0x04
	}
	a.counter++
	auth := append(append(rp[:], flags), binary.BigEndian.AppendUint32(nil, a.counter)...)
	ch := sha256.Sum256(client)
	digest := sha256.Sum256(append(append([]byte{}, auth...), ch[:]...))
	sig, err := ecdsa.SignASN1(rand.Reader, a.key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]any{"id": b64u(a.id), "rawId": b64u(a.id), "type": "public-key",
		"response": map[string]string{"clientDataJSON": b64u(client), "authenticatorData": b64u(auth), "signature": b64u(sig), "userHandle": b64u([]byte(userHandle))}})
	return body
}

// postJSON posts a JSON body from the page's origin.
func (e *env) postJSON(t *testing.T, path string, body []byte) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.base+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", e.base)
	resp, err := e.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// approve runs an approval's passkey ceremony, returning the finish's status and body.
func (e *env) approve(t *testing.T, id string, a *authenticator, userHandle string, verified bool) (int, string) {
	t.Helper()
	code, begun := e.postJSON(t, "/id/approve/"+id+"/begin", []byte("{}"))
	if code != http.StatusOK {
		return code, string(begun)
	}
	var b struct{ Ceremony string }
	json.Unmarshal(begun, &b)
	code, body := e.postJSON(t, "/id/approve/"+id+"/finish?ceremony="+b.Ceremony, a.assert(t, begun, "localhost", e.base, userHandle, verified))
	return code, string(body)
}

// A person approves what ok secrets asked them with their own passkey, verifying themselves; an
// assertion that does not verify them, another person, a passkey of someone else's, a ceremony
// begun for another approval, and an approval answered or expired approve nothing; and declining
// is a form.
func TestApprovals(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	ada, _ := e.store.Principal(ctx, "ada.human.example.com")
	bea, _ := e.store.Principal(ctx, "bea.human.example.com")
	adas := newAuthenticator(t, e.store, ada.ID)
	beas := newAuthenticator(t, e.store, bea.ID)
	ask := func(who store.Principal) store.Approval {
		t.Helper()
		a, err := e.store.StartApproval(ctx, who.ID, "secrets/unseal/7/secret/prod/db", "Unseal secret/prod/db in acme/app", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	decision := func(a store.Approval) string {
		t.Helper()
		got, err := e.store.Approval(ctx, a.ID)
		if err != nil {
			t.Fatal(err)
		}
		return got.Decision
	}

	a := ask(ada)
	if code, _, u := e.get(t, "/id/approve/"+a.ID); !strings.HasPrefix(u.Path, "/id/login") || code != http.StatusOK {
		t.Errorf("the page, signed out: %d at %s", code, u)
	}
	e.signIn(t, map[string]any{"email": "ada@example.com", "email_verified": true})
	code, body, _ := e.get(t, "/id/approve/"+a.ID)
	if code != http.StatusOK || !strings.Contains(body, "Unseal secret/prod/db in acme/app") || !strings.Contains(body, `id="passkey-approve"`) {
		t.Fatalf("the page: %d\n%s", code, body)
	}

	// Not verified: refused, and still waiting.
	if code, body := e.approve(t, a.ID, adas, ada.ID, false); code != http.StatusUnauthorized || decision(a) != "" {
		t.Errorf("an assertion not verifying ada: %d %s, decision %q", code, body, decision(a))
	}
	// Bea's passkey, in ada's session: not among ada's, refused.
	if code, body := e.approve(t, a.ID, beas, bea.ID, true); code != http.StatusUnauthorized || decision(a) != "" {
		t.Errorf("bea's passkey: %d %s, decision %q", code, body, decision(a))
	}
	// A ceremony begun for another approval finishes nothing here.
	other := ask(ada)
	_, begun := e.postJSON(t, "/id/approve/"+other.ID+"/begin", []byte("{}"))
	var b struct{ Ceremony string }
	json.Unmarshal(begun, &b)
	if code, body := e.postJSON(t, "/id/approve/"+a.ID+"/finish?ceremony="+b.Ceremony, adas.assert(t, begun, "localhost", e.base, ada.ID, true)); code != http.StatusBadRequest || decision(a) != "" {
		t.Errorf("another approval's ceremony: %d %s", code, body)
	}
	// Approved, once.
	if code, body := e.approve(t, a.ID, adas, ada.ID, true); code != http.StatusOK || decision(a) != store.ApprovalApproved {
		t.Fatalf("ada approving: %d %s, decision %q", code, body, decision(a))
	}
	if code, _ := e.approve(t, a.ID, adas, ada.ID, true); code != http.StatusNotFound {
		t.Errorf("approving twice: %d", code)
	}
	if code, body, _ := e.get(t, "/id/approve/"+a.ID); code != http.StatusNotFound || !strings.Contains(body, "approved this already") {
		t.Errorf("the page once approved: %d", code)
	}

	// Declined, with the form.
	if code, body, _ := e.post(t, "/id/approve/"+other.ID+"/decline", nil, e.base); code != http.StatusOK || !strings.Contains(body, "Declined") ||
		decision(other) != store.ApprovalDeclined {
		t.Errorf("declining: %d, decision %q", code, decision(other))
	}

	// Bea's approval is nothing of ada's.
	bs := ask(bea)
	if code, body, _ := e.get(t, "/id/approve/"+bs.ID); code != http.StatusNotFound || !strings.Contains(body, "Nothing of yours") {
		t.Errorf("someone else's approval: %d", code)
	}
	if code, _ := e.approve(t, bs.ID, adas, ada.ID, true); code != http.StatusNotFound || decision(bs) != "" {
		t.Errorf("approving someone else's: %d, decision %q", code, decision(bs))
	}
	if code, _, _ := e.post(t, "/id/approve/"+bs.ID+"/decline", nil, e.base); code != http.StatusNotFound || decision(bs) != "" {
		t.Errorf("declining someone else's: %d, decision %q", code, decision(bs))
	}

	// Expired.
	x, err := e.store.StartApproval(ctx, ada.ID, "s", "d", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if code, body, _ := e.get(t, "/id/approve/"+x.ID); code != http.StatusNotFound || !strings.Contains(body, "expired") {
		t.Errorf("an expired approval: %d", code)
	}

	// Bea, signed in herself, approves hers.
	jar, _ := cookiejar.New(nil)
	e.client = &http.Client{Jar: jar}
	e.signIn(t, map[string]any{"email": "bea@example.com", "email_verified": true})
	if code, body := e.approve(t, bs.ID, beas, bea.ID, true); code != http.StatusOK || decision(bs) != store.ApprovalApproved {
		t.Errorf("bea approving hers: %d %s", code, body)
	}
}
