package mail_test

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/id/mail"
	"github.com/Cidan/ok/internal/id/mail/mailtest"
)

// A message reaches the server whole — its sender, its recipient, its subject and body, the body's
// non-ASCII text intact — over plaintext to a server on loopback; no header is injected.
func TestSend(t *testing.T) {
	srv := mailtest.New(t)
	s := mail.SMTP{Addr: srv.Addr, From: "ok <no-reply@example.com>"}
	body := "Follow this link to verify your address:\n\nhttps://ok.example.com/id/signup/verify?token=abc\n\n— ok, née ok\n"
	if err := s.Send(context.Background(), "ada@example.com", "Verify your address", body); err != nil {
		t.Fatal(err)
	}
	m := srv.Next(t)
	if len(m.To) != 1 || m.To[0] != "ada@example.com" || m.From[0] != "no-reply@example.com" || m.Subject != "Verify your address" || m.Body != body {
		t.Errorf("the message taken = %+v", m)
	}
	for name, err := range map[string]error{
		"a header in the address": s.Send(context.Background(), "ada@example.com\r\nBcc: eve@example.com", "x", "x"),
		"a header in the subject": s.Send(context.Background(), "ada@example.com", "x\r\nBcc: eve@example.com", "x"),
		"no address":              s.Send(context.Background(), "", "x", "x"),
	} {
		if err == nil {
			t.Errorf("%s: sent", name)
		}
	}
	if len(srv.Messages()) != 1 {
		t.Errorf("messages taken: %d", len(srv.Messages()))
	}
}
