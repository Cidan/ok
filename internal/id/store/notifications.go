package store

import (
	"context"
	"time"

	"github.com/Cidan/ok/internal/data"
)

// Notifications (docs/hosted.md, "ok control at scale, as built"): what asks something of a person,
// from every cell, each once by recipient and source; and the mail of those whose kinds they have
// mailed, sent as keyed work (MailWork), retried until MailGiveUp has passed, then kept, dead.

// Notification is one of a person's.
type Notification struct {
	Principal, Source string
	Kind              int
	Repository        int64
	Number            int64
	RepositoryName    string
	Title             string
	Actor             string
	Text              string
	Link              string
	At                time.Time
	Read              bool
}

// DefaultMailed are the kinds mailed to a person who has chosen none: reviews asked of them (1), and
// their CLs rolled back (6).
const DefaultMailed uint64 = 1<<1 | 1<<6

// MailWork is the keyed work that sends a message, keyed by its ID.
const MailWork = "mail"

// MailGiveUp is how long a message is tried before it is kept, dead.
const MailGiveUp = 24 * time.Hour

// Mail states.
const (
	MailWaiting = 0
	MailSent    = 1
	MailDead    = 2
)

// Mail is a message to send.
type Mail struct {
	ID        int64
	Principal string
	To        string
	Subject   string
	Body      string
	State     int
	Attempts  int
	NextTry   time.Time
	LastError string
	CreatedAt time.Time
}

// Notify records a notification unless its recipient has it from its source, queuing mail with it
// when it is new, in one transaction; it reports whether it was new, and the mail's ID (0: none).
func (s *Store) Notify(ctx context.Context, n Notification, mail *Mail) (bool, int64, error) {
	if n.Principal == "" || n.Source == "" || n.Kind <= 0 || n.Kind >= 64 {
		return false, 0, invalid("a notification names its recipient, its source, and its kind")
	}
	var added bool
	var id int64
	err := s.txn(ctx, func(tx Ops) error {
		var err error
		if added, err = tx.AddNotification(ctx, n); err != nil || !added || mail == nil {
			id = 0
			return err
		}
		m := *mail
		m.ID, m.Principal, m.State, m.NextTry, m.CreatedAt = data.NewID(), n.Principal, MailWaiting, s.now(), s.now()
		id = m.ID
		return tx.QueueMail(ctx, m)
	})
	if err != nil || id == 0 {
		return added, 0, err
	}
	return added, id, nil
}

// Notifications are up to limit of a person's latest, and how many are unread.
func (s *Store) Notifications(ctx context.Context, principal string, unread bool, limit int) ([]Notification, int64, error) {
	ns, err := s.b.NotificationsOf(ctx, principal, unread, limit)
	if err != nil {
		return nil, 0, err
	}
	n, err := s.b.UnreadNotifications(ctx, principal)
	return ns, n, err
}

// MarkRead marks some of a person's notifications read, or all of them.
func (s *Store) MarkRead(ctx context.Context, principal string, sources []string, all bool) error {
	return s.b.MarkNotificationsRead(ctx, principal, sources, all)
}

// Mailed are the kinds a person is mailed (a bit each), and whether they chose them.
func (s *Store) Mailed(ctx context.Context, principal string) (uint64, bool, error) {
	m, ok, err := s.b.NotificationPrefs(ctx, principal)
	if err != nil || !ok {
		return DefaultMailed, false, err
	}
	return m, true, nil
}

// SetMailed sets the kinds a person is mailed.
func (s *Store) SetMailed(ctx context.Context, principal string, mailed uint64) error {
	return s.b.SetNotificationPrefs(ctx, principal, mailed)
}

// Sender sends a message.
type Sender func(ctx context.Context, to, subject, body string) error

// SendMail sends a waiting message, through send: sent, it is done; failed, it is tried again
// after a backoff — 30 seconds, doubling to an hour — until MailGiveUp has passed since it was
// queued, when it is kept, dead.
func (s *Store) SendMail(ctx context.Context, id int64, send Sender) error {
	m, ok, err := s.b.MailByID(ctx, id)
	if err != nil || !ok || m.State != MailWaiting {
		return err
	}
	now := s.now()
	if m.NextTry.After(now) {
		return s.b.Work().MarkAt(ctx, MailWork, id, m.NextTry)
	}
	err = send(ctx, m.To, m.Subject, m.Body)
	m.Attempts++
	switch {
	case err == nil:
		m.State, m.LastError = MailSent, ""
	case now.Sub(m.CreatedAt) >= MailGiveUp:
		m.State, m.LastError = MailDead, err.Error()
	default:
		backoff := 30 * time.Second << min(m.Attempts-1, 7)
		m.NextTry, m.LastError = now.Add(min(backoff, time.Hour)), err.Error()
	}
	if uerr := s.b.UpdateMail(ctx, m); uerr != nil {
		return uerr
	}
	if m.State == MailWaiting {
		return s.b.Work().MarkAt(ctx, MailWork, id, m.NextTry)
	}
	return nil
}

// MailOf reads a message: tests, and administrators.
func (s *Store) MailOf(ctx context.Context, id int64) (Mail, bool, error) {
	return s.b.MailByID(ctx, id)
}
