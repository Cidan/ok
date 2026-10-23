// Package mailtest is an SMTP server for tests: it takes every message sent to it, over plaintext
// on loopback, and hands it to the test.
package mailtest

import (
	"bufio"
	"io"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"strings"
	"sync"
	"testing"
	"time"
)

// Message is a message the server took.
type Message struct {
	From, To []string
	Subject  string
	Body     string // decoded
}

// Server is a running SMTP server; it stops when the test ends.
type Server struct {
	Addr string
	mu   sync.Mutex
	msgs []Message
	got  chan struct{}
}

// New starts a server.
func New(t testing.TB) *Server {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Addr: lis.Addr().String(), got: make(chan struct{}, 100)}
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go s.serve(c)
		}
	}()
	t.Cleanup(func() { lis.Close() })
	return s
}

func (s *Server) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	say := func(line string) { io.WriteString(c, line+"\r\n") }
	say("220 mailtest ready")
	var m Message
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
			say("250-mailtest")
			say("250 8BITMIME")
		case strings.HasPrefix(cmd, "MAIL FROM:"):
			m = Message{From: []string{addr(line)}}
			say("250 ok")
		case strings.HasPrefix(cmd, "RCPT TO:"):
			m.To = append(m.To, addr(line))
			say("250 ok")
		case cmd == "DATA":
			say("354 go on")
			var data strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if strings.TrimRight(l, "\r\n") == "." {
					break
				}
				data.WriteString(strings.TrimPrefix(l, "."))
			}
			if msg, err := mail.ReadMessage(strings.NewReader(data.String())); err == nil {
				m.Subject = msg.Header.Get("Subject")
				body, _ := io.ReadAll(quotedprintable.NewReader(msg.Body))
				m.Body = strings.ReplaceAll(string(body), "\r\n", "\n")
			}
			s.mu.Lock()
			s.msgs = append(s.msgs, m)
			s.mu.Unlock()
			s.got <- struct{}{}
			say("250 taken")
		case cmd == "QUIT":
			say("221 bye")
			return
		case cmd == "RSET", cmd == "NOOP":
			say("250 ok")
		default:
			say("502 not here")
		}
	}
}

// addr is the address in a MAIL or RCPT command: what is between its angle brackets, its
// parameters (BODY=8BITMIME) after them.
func addr(line string) string {
	_, a, _ := strings.Cut(line, "<")
	a, _, _ = strings.Cut(a, ">")
	return a
}

// Messages are every message taken so far.
func (s *Server) Messages() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Message(nil), s.msgs...)
}

// Next waits for the next message to arrive (a minute at most), and returns the last one taken.
func (s *Server) Next(t testing.TB) Message {
	t.Helper()
	select {
	case <-s.got:
	case <-time.After(time.Minute):
		t.Fatal("mailtest: no message arrived")
	}
	msgs := s.Messages()
	return msgs[len(msgs)-1]
}
