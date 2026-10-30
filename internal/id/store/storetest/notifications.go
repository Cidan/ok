package storetest

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/id/store"
	"github.com/Cidan/ok/internal/work/worktest"
)

// A notification is recorded once by recipient and source, its mail queued with it only then; a
// person's are listed latest first, unread counted, and marked read; preferences default until set;
// and mail is sent, retried after a backoff, and kept dead after a day of failures.
func testNotifications(t *testing.T, s *store.Store) {
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0).UTC()
	s.SetClock(func() time.Time { return now })
	note := func(to, source string, at time.Duration) store.Notification {
		return store.Notification{Principal: to, Source: source, Kind: 1, Repository: 7, Number: 1, RepositoryName: "acme/app", Title: "a change",
			Actor: "bea", Text: "asked you to review it", Link: "/acme/app/cl/1", At: now.Add(at)}
	}
	mail := &store.Mail{To: "ada@example.com", Subject: "[acme/app #1] a change", Body: "bea asked you to review it."}
	added, id, err := s.Notify(ctx, note("ada", "e:7:1", time.Minute), mail)
	if err != nil || !added || id == 0 {
		t.Fatalf("Notify = %v, %d, %v", added, id, err)
	}
	if added, again, err := s.Notify(ctx, note("ada", "e:7:1", time.Minute), mail); err != nil || added || again != 0 {
		t.Errorf("the same notification again: %v, %d, %v; want nothing new, no mail", added, again, err)
	}
	if _, _, err := s.Notify(ctx, note("ada", "e:7:2", 2*time.Minute), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Notify(ctx, note("bea", "e:7:2", 2*time.Minute), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Notify(ctx, store.Notification{Principal: "ada", Source: "x"}, nil); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a notification of no kind: %v", err)
	}
	sources := func(ns []store.Notification) []string {
		var out []string
		for _, n := range ns {
			out = append(out, n.Source)
		}
		return out
	}
	ns, unread, err := s.Notifications(ctx, "ada", false, 10)
	if err != nil || !slices.Equal(sources(ns), []string{"e:7:2", "e:7:1"}) || unread != 2 || ns[1].Text != "asked you to review it" {
		t.Fatalf("ada's notifications: %+v, %d, %v", ns, unread, err)
	}
	if err := s.MarkRead(ctx, "ada", []string{"e:7:1"}, false); err != nil {
		t.Fatal(err)
	}
	if ns, unread, err := s.Notifications(ctx, "ada", true, 10); err != nil || !slices.Equal(sources(ns), []string{"e:7:2"}) || unread != 1 {
		t.Errorf("ada's unread once one is read: %+v, %d, %v", ns, unread, err)
	}
	if err := s.MarkRead(ctx, "ada", nil, true); err != nil {
		t.Fatal(err)
	}
	if _, unread, _ := s.Notifications(ctx, "ada", false, 10); unread != 0 {
		t.Errorf("ada's unread once all are read: %d", unread)
	}
	if _, unread, _ := s.Notifications(ctx, "bea", false, 10); unread != 1 {
		t.Errorf("bea's unread, untouched by ada's: %d", unread)
	}

	if m, set, err := s.Mailed(ctx, "ada"); err != nil || set || m != store.DefaultMailed {
		t.Errorf("ada's default mailed kinds: %b, %v, %v", m, set, err)
	}
	if err := s.SetMailed(ctx, "ada", 1<<3); err != nil {
		t.Fatal(err)
	}
	if m, set, err := s.Mailed(ctx, "ada"); err != nil || !set || m != 1<<3 {
		t.Errorf("ada's mailed kinds once set: %b, %v, %v", m, set, err)
	}

	// The mail: it fails twice, a backoff apart; then it is sent.
	var tries int
	failing := func(context.Context, string, string, string) error { tries++; return errors.New("the server refused") }
	if err := s.SendMail(ctx, id, failing); err != nil {
		t.Fatal(err)
	}
	m, _, _ := s.MailOf(ctx, id)
	if m.State != store.MailWaiting || m.Attempts != 1 || !m.NextTry.Equal(now.Add(30*time.Second)) || m.LastError != "the server refused" {
		t.Fatalf("after a failure: %+v", m)
	}
	if err := s.SendMail(ctx, id, failing); err != nil || tries != 1 {
		t.Errorf("a retry before its time sent it: %d tries, %v", tries, err)
	}
	// Its sending is keyed work, its job enqueued with the message.
	if got := worktest.Ready(t, s.Work(), store.MailWork); !slices.Contains(got, id) {
		t.Errorf("the mail's jobs: %v; want %d's", got, id)
	}
	now = now.Add(30 * time.Second)
	if err := s.SendMail(ctx, id, failing); err != nil {
		t.Fatal(err)
	}
	if m, _, _ = s.MailOf(ctx, id); m.Attempts != 2 || !m.NextTry.Equal(now.Add(time.Minute)) {
		t.Errorf("after a second failure, the backoff doubles: %+v", m)
	}
	now = now.Add(time.Minute)
	var sent []string
	if err := s.SendMail(ctx, id, func(_ context.Context, to, subject, _ string) error { sent = append(sent, to+" "+subject); return nil }); err != nil {
		t.Fatal(err)
	}
	if m, _, _ = s.MailOf(ctx, id); m.State != store.MailSent || !slices.Equal(sent, []string{"ada@example.com [acme/app #1] a change"}) {
		t.Errorf("sent: %+v, %v", m, sent)
	}

	// Another fails until a day has passed: then it is kept, dead.
	_, dead, err := s.Notify(ctx, note("ada", "e:7:3", 0), mail)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(store.MailGiveUp)
	if err := s.SendMail(ctx, dead, failing); err != nil {
		t.Fatal(err)
	}
	if m, _, _ = s.MailOf(ctx, dead); m.State != store.MailDead || m.LastError == "" {
		t.Errorf("a message failing past a day: %+v", m)
	}
}
