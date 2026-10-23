// Package mail sends ok's email: the links people follow to verify an address and to reset a
// password (docs/identity.md, "Accounts"). A message is plain text, sent over SMTP with STARTTLS —
// required, but to a server on loopback.
package mail

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"time"
)

// Sender sends a message.
type Sender interface {
	Send(ctx context.Context, to, subject, body string) error
}

// SMTP sends through an SMTP server.
type SMTP struct {
	// Addr is the server, host:port.
	Addr string
	// Username and Password authenticate (PLAIN), when set.
	Username, Password string
	// From is the sender: "ok <no-reply@example.com>".
	From string
	// TLS, when set, verifies the server's certificate (its CA, say); the system's roots otherwise.
	TLS *tls.Config
}

// timeout bounds a message's whole conversation.
const timeout = 30 * time.Second

// Send sends one plain-text message to one address.
func (s SMTP) Send(ctx context.Context, to, subject, body string) error {
	from, err := mail.ParseAddress(s.From)
	if err != nil {
		return fmt.Errorf("mail: the sender %q: %w", s.From, err)
	}
	rcpt, err := mail.ParseAddress(to)
	if err != nil || strings.ContainsAny(to, "\r\n") {
		return fmt.Errorf("mail: the address %q", to)
	}
	if strings.ContainsAny(subject, "\r\n") {
		return errors.New("mail: a subject is one line")
	}
	msg, err := message(from, rcpt, subject, body)
	if err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(s.Addr)
	if err != nil {
		return fmt.Errorf("mail: the server %q: %w", s.Addr, err)
	}
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("mail: %w", err)
	}
	deadline := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	conn.SetDeadline(deadline)
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		conn.Close()
		return fmt.Errorf("mail: %w", err)
	}
	defer c.Close()
	if ok, _ := c.Extension("STARTTLS"); ok {
		cfg := &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
		if s.TLS != nil {
			cfg = s.TLS.Clone()
			if cfg.ServerName == "" {
				cfg.ServerName = host
			}
		}
		if err := c.StartTLS(cfg); err != nil {
			return fmt.Errorf("mail: STARTTLS: %w", err)
		}
	} else if !loopback(host) {
		return fmt.Errorf("mail: %s offers no STARTTLS; ok sends mail over TLS but to a server on loopback", s.Addr)
	}
	if s.Username != "" {
		if err := c.Auth(smtp.PlainAuth("", s.Username, s.Password, host)); err != nil {
			return fmt.Errorf("mail: authenticating: %w", err)
		}
	}
	if err := c.Mail(from.Address); err != nil {
		return fmt.Errorf("mail: %w", err)
	}
	if err := c.Rcpt(rcpt.Address); err != nil {
		return fmt.Errorf("mail: %w", err)
	}
	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("mail: %w", err)
	}
	if _, err := w.Write(msg); err != nil {
		return fmt.Errorf("mail: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("mail: %w", err)
	}
	return c.Quit()
}

// message is a whole RFC 5322 message: headers, and the body quoted-printable.
func message(from, to *mail.Address, subject, body string) ([]byte, error) {
	var id [12]byte
	rand.Read(id[:])
	domain := from.Address[strings.LastIndexByte(from.Address, '@')+1:]
	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", from.String())
	fmt.Fprintf(&b, "To: %s\r\n", to.String())
	fmt.Fprintf(&b, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", subject))
	fmt.Fprintf(&b, "Date: %s\r\n", time.Now().UTC().Format(time.RFC1123Z))
	fmt.Fprintf(&b, "Message-ID: <%s@%s>\r\n", hex.EncodeToString(id[:]), domain)
	b.WriteString("MIME-Version: 1.0\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n")
	qp := quotedprintable.NewWriter(&b)
	if _, err := qp.Write([]byte(strings.ReplaceAll(body, "\n", "\r\n"))); err != nil {
		return nil, err
	}
	if err := qp.Close(); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
