// Package auth is the command line's side of signing in to ok: the device login (OAuth's device
// authorization grant), the credential it yields — a key and its certificate, kept on the machine —
// and renewing that certificate before it expires.
package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/credential"
	"github.com/Cidan/ok/internal/dial"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Dir is where the credential is kept: $OK_CONFIG_DIR, or ~/.config/ok, then credential/.
func Dir() (string, error) {
	base := os.Getenv("OK_CONFIG_DIR")
	if base == "" {
		cfg, err := os.UserConfigDir()
		if err != nil {
			return "", err
		}
		base = filepath.Join(cfg, "ok")
	}
	return filepath.Join(base, "credential"), nil
}

// Server is where ok is: its public URL, and the address its services answer on.
type Server struct {
	URL  string `json:"url"`
	Addr string `json:"addr"`
	TLS  bool   `json:"tls"`
}

// ParseServer reads ok's public URL: https://ok.example.com is reached on 443 over TLS; http:// is
// for an ok on loopback.
func ParseServer(raw string) (Server, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return Server{}, fmt.Errorf("%q is not ok's address: https://ok.example.com", raw)
	}
	s := Server{URL: strings.TrimSuffix(u.Scheme+"://"+u.Host, "/"), TLS: u.Scheme == "https"}
	port := u.Port()
	if port == "" {
		port = "80"
		if s.TLS {
			port = "443"
		}
	}
	s.Addr = net.JoinHostPort(u.Hostname(), port)
	return s, nil
}

// Dial connects to the server with options as given, its TLS added.
func (s Server) Dial(o dial.Options) (*grpc.ClientConn, error) {
	if s.TLS {
		o.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return dial.Dial(s.Addr, o)
}

// files are the credential's files in its directory.
const (
	serverFile = "server.json"
	personFile = "person.json"
	certFile   = "cert.pem"
	keyFile    = "key.pem"
)

// Login signs this machine in: it makes a key, starts a device login, tells the person where to
// approve it (and opens that page, when open is set), and polls until they do. The credential is
// saved in dir.
func Login(ctx context.Context, srv Server, dir, label string, out io.Writer, open func(string) error) (string, error) {
	key, err := credential.NewKey()
	if err != nil {
		return "", err
	}
	spki, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", err
	}
	conn, err := srv.Dial(dial.Options{})
	if err != nil {
		return "", err
	}
	defer conn.Close()
	client := identitypb.NewDeviceLoginClient(conn)
	start, err := client.Start(ctx, &identitypb.StartRequest{PublicKey: spki, Label: label})
	if err != nil {
		return "", fmt.Errorf("starting the sign-in: %w", err)
	}
	fmt.Fprintf(out, "To sign in, open %s\nand confirm the code %s\n", start.GetVerificationUriComplete(), start.GetUserCode())
	if open != nil {
		if err := open(start.GetVerificationUriComplete()); err == nil {
			fmt.Fprintln(out, "(your browser is open there)")
		}
	}
	interval := time.Duration(start.GetInterval()) * time.Second
	deadline := time.Now().Add(time.Duration(start.GetExpiresIn()) * time.Second)
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(interval):
		}
		if time.Now().After(deadline) {
			return "", errors.New("the code expired before it was approved: sign in again")
		}
		poll, err := client.Poll(ctx, &identitypb.PollRequest{DeviceCode: start.GetDeviceCode()})
		if err != nil {
			return "", err
		}
		switch poll.GetState() {
		case identitypb.PollState_POLL_STATE_PENDING:
		case identitypb.PollState_POLL_STATE_SLOW_DOWN:
			interval += 5 * time.Second
		case identitypb.PollState_POLL_STATE_DENIED:
			return "", errors.New("the sign-in was denied")
		case identitypb.PollState_POLL_STATE_EXPIRED:
			return "", errors.New("the code expired before it was approved: sign in again")
		case identitypb.PollState_POLL_STATE_APPROVED:
			if err := save(dir, srv, poll.GetCertificate().GetChain()[0], key); err != nil {
				return "", err
			}
			if c, err := Load(dir); err == nil {
				if err := savePerson(ctx, c); err != nil {
					fmt.Fprintf(out, "(who signed in is not recorded: %v)\n", err)
				}
			}
			return poll.GetPrincipal(), nil
		}
	}
}

// Person is who signed in, as the directory says: what jj signs their commits with.
type Person struct {
	Name        string `json:"name"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
}

// savePerson records who a credential is, beside it.
func savePerson(ctx context.Context, c *Credential) error {
	conn, err := c.srv.Dial(dial.Options{Credential: c})
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := identitypb.NewDirectoryClient(conn).GetPrincipal(ctx, &identitypb.GetPrincipalRequest{Key: &identitypb.GetPrincipalRequest_Name{Name: c.Principal()}})
	if err != nil {
		return err
	}
	p := resp.GetPrincipal()
	data, err := json.MarshalIndent(Person{Name: p.GetName(), Email: p.GetEmail(), DisplayName: p.GetDisplayName()}, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(filepath.Join(c.dir, personFile), data, 0o644)
}

// Save writes a credential to dir — the key only its owner reads — for Load: a robot's an
// administrator certified (ok admin robot certify).
func Save(dir string, srv Server, cert []byte, key *ecdsa.PrivateKey) error {
	return save(dir, srv, cert, key)
}

// save writes the credential: the key only its owner reads.
func save(dir string, srv Server, cert []byte, key *ecdsa.PrivateKey) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	kd, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	sj, err := json.MarshalIndent(srv, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFile(filepath.Join(dir, keyFile), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kd}), 0o600); err != nil {
		return err
	}
	if err := writeFile(filepath.Join(dir, certFile), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}), 0o644); err != nil {
		return err
	}
	return writeFile(filepath.Join(dir, serverFile), sj, 0o644)
}

// writeFile replaces a file whole: a reader sees the old one or the new one.
func writeFile(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// ErrSignedOut is no credential, or one that expired.
var ErrSignedOut = errors.New("this machine is not signed in to ok: run ok auth login")

// Credential is the machine's credential, renewed as it nears expiry: a dial.Credential.
type Credential struct {
	dir string
	srv Server
	now func() time.Time

	mu   sync.Mutex
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

// RenewWithin is how near expiry a certificate is renewed.
const RenewWithin = 6 * time.Hour

// Load reads the credential in dir.
func Load(dir string) (*Credential, error) {
	sj, err := os.ReadFile(filepath.Join(dir, serverFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrSignedOut
	}
	if err != nil {
		return nil, err
	}
	c := &Credential{dir: dir, now: time.Now}
	if err := json.Unmarshal(sj, &c.srv); err != nil {
		return nil, err
	}
	if err := c.read(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Credential) read() error {
	cp, err := os.ReadFile(filepath.Join(c.dir, certFile))
	if err != nil {
		return ErrSignedOut
	}
	kp, err := os.ReadFile(filepath.Join(c.dir, keyFile))
	if err != nil {
		return ErrSignedOut
	}
	cb, _ := pem.Decode(cp)
	kb, _ := pem.Decode(kp)
	if cb == nil || kb == nil {
		return errors.New("the credential's files are damaged: run ok auth login")
	}
	cert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return err
	}
	k, err := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if err != nil {
		return err
	}
	key, ok := k.(*ecdsa.PrivateKey)
	if !ok {
		return errors.New("the credential's key is not ECDSA")
	}
	c.cert, c.key = cert, key
	return nil
}

// Server is where the credential signs in.
func (c *Credential) Server() Server { return c.srv }

// Principal is who the credential is.
func (c *Credential) Principal() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, _ := credential.Principal(c.cert)
	return p
}

// NotAfter is when the current certificate expires.
func (c *Credential) NotAfter() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cert.NotAfter
}

// Current is the certificate and key to call with, renewing the certificate first when it nears
// expiry.
func (c *Credential) Current(ctx context.Context) ([]byte, crypto.Signer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if now.After(c.cert.NotAfter) {
		return nil, nil, ErrSignedOut
	}
	if c.cert.NotAfter.Sub(now) < RenewWithin {
		if err := c.renew(ctx); err != nil && c.cert.NotAfter.Sub(now) < time.Minute {
			return nil, nil, fmt.Errorf("renewing the credential: %w", err)
		}
	}
	return c.cert.Raw, c.key, nil
}

// renew asks for a fresh certificate with the current one; c.mu is held.
func (c *Credential) renew(ctx context.Context) error {
	conn, err := c.srv.Dial(dial.Options{Credential: dial.Static{Cert: c.cert.Raw, Key: c.key}})
	if err != nil {
		return err
	}
	defer conn.Close()
	resp, err := identitypb.NewCredentialsClient(conn).Renew(ctx, &identitypb.RenewRequest{})
	if err != nil {
		return err
	}
	if err := save(c.dir, c.srv, resp.GetCertificate().GetChain()[0], c.key); err != nil {
		return err
	}
	return c.read()
}

// Logout revokes the credential and removes it from the machine.
func Logout(ctx context.Context, dir string) error {
	c, err := Load(dir)
	if errors.Is(err, ErrSignedOut) {
		return nil
	}
	if err != nil {
		return err
	}
	if conn, err := c.srv.Dial(dial.Options{Credential: c}); err == nil {
		id, _ := credential.ID(c.cert)
		_, err = identitypb.NewCredentialsClient(conn).Revoke(ctx, &identitypb.RevokeRequest{Id: id})
		conn.Close()
		if err != nil && !errors.Is(err, ErrSignedOut) {
			fmt.Fprintf(os.Stderr, "ok: revoking the credential: %v (it is removed from this machine anyway)\n", err)
		}
	}
	for _, f := range []string{keyFile, certFile, serverFile, personFile} {
		if err := os.Remove(filepath.Join(dir, f)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
