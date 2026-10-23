package outbox

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/tenancy"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
)

// Notifier turns outbox entries into people's notifications in the directory (docs/hosted.md, "ok
// control at scale, as built"): what asks something of someone, never of the one who did it.
//
//   - A review asked of someone — a reviewer added to an open CL, or a draft's reviewers when its
//     author asks for review — once for each patchset.
//   - A label granted, the CL landed, or a rollback of it begun: its author.
//   - The CL abandoned: its reviewers.
//   - A comment: the CL's author, and everyone else in its thread before it.
type Notifier struct {
	DB       db.Ops
	Delivery identitypb.DeliveryClient
	// Installation is the installation's organization's handle ("" for a hosted service), which
	// links name CLs under.
	Installation string
}

// notifyBatch is how many notifications one request carries.
const notifyBatch = 500

// excerpt is how much of a comment a notification carries.
const excerpt = 140

// Consume notifies of what entries record.
func (n *Notifier) Consume(ctx context.Context, r Repository, entries []db.OutboxEntry) error {
	cls := map[int64]*db.ChangeList{}
	cl := func(number int64) (*db.ChangeList, error) {
		if c, ok := cls[number]; ok {
			return c, nil
		}
		c, ok, err := n.DB.GetChangeList(ctx, r.ID, number)
		if err != nil {
			return nil, err
		}
		if !ok {
			cls[number] = nil
			return nil, nil
		}
		cls[number] = &c
		return &c, nil
	}
	var out []*identitypb.Notification
	for _, e := range entries {
		c, err := cl(e.Number)
		if err != nil {
			return err
		}
		if c == nil {
			continue
		}
		var ns []*identitypb.Notification
		switch {
		case e.EventID != 0:
			ev, ok, err := n.DB.GetEvent(ctx, r.ID, e.Number, e.EventID)
			if err != nil {
				return err
			}
			if ok {
				if ns, err = n.ofEvent(ctx, r, *c, ev); err != nil {
					return err
				}
			}
		case e.CommentID != 0:
			if ns, err = n.ofComment(ctx, r, *c, e.CommentID); err != nil {
				return err
			}
		}
		out = append(out, ns...)
	}
	for len(out) > 0 {
		k := min(len(out), notifyBatch)
		if _, err := n.Delivery.Notify(ctx, &identitypb.NotifyRequest{Notifications: out[:k]}); err != nil {
			return fmt.Errorf("notifying: %w", err)
		}
		out = out[k:]
	}
	return nil
}

// notification is one of a CL's, to a person.
func (n *Notifier) notification(r Repository, c db.ChangeList, to, source string, kind identitypb.Notification_Kind, actor, text string,
	at *timestamppb.Timestamp) *identitypb.Notification {
	return &identitypb.Notification{Source: source, PrincipalId: to, Kind: kind, RepositoryId: r.ID, Number: c.Number, Repository: r.Name,
		Title: c.Title(), ActorId: actor, Text: text, Link: tenancy.URLPath(r.Name, n.Installation) + "/cl/" + strconv.FormatInt(c.Number, 10), At: at}
}

func (n *Notifier) ofEvent(ctx context.Context, r Repository, c db.ChangeList, ev db.Event) ([]*identitypb.Notification, error) {
	at := timestamppb.New(ev.At)
	source := fmt.Sprintf("e:%d:%d", r.ID, ev.ID)
	// A review asked of a reviewer, once for each patchset however it was asked.
	review := func(reviewer string) *identitypb.Notification {
		return n.notification(r, c, reviewer, fmt.Sprintf("review:%d:%d:%d:%s", r.ID, c.Number, ev.Patchset, reviewer),
			identitypb.Notification_KIND_REVIEW_REQUESTED, ev.Actor, "asked you to review it", at)
	}
	toAuthor := func(kind identitypb.Notification_Kind, text string) []*identitypb.Notification {
		if c.Author == "" || c.Author == ev.Actor {
			return nil
		}
		return []*identitypb.Notification{n.notification(r, c, c.Author, source, kind, ev.Actor, text, at)}
	}
	toReviewers := func(f func(reviewer string) *identitypb.Notification) ([]*identitypb.Notification, error) {
		rs, err := n.DB.Reviewers(ctx, r.ID, c.Number)
		if err != nil {
			return nil, err
		}
		var out []*identitypb.Notification
		for _, rv := range rs {
			if rv.Principal != ev.Actor && rv.Principal != c.Author {
				out = append(out, f(rv.Principal))
			}
		}
		return out, nil
	}
	switch ev.Kind {
	case db.EventReviewerAdded:
		// A draft's reviewers are asked when its author asks for review (EventReviewRequested).
		if c.Status != db.StatusOpen || ev.Subject == ev.Actor || ev.Subject == c.Author {
			return nil, nil
		}
		return []*identitypb.Notification{review(ev.Subject)}, nil
	case db.EventReviewRequested:
		return toReviewers(review)
	case db.EventLabelGranted:
		return toAuthor(identitypb.Notification_KIND_REPLIED, fmt.Sprintf("granted %s on patchset %d", ev.Label, ev.Patchset)), nil
	case db.EventSubmitted:
		return toAuthor(identitypb.Notification_KIND_SUBMITTED, "landed it on main"), nil
	case db.EventRolledBack:
		return toAuthor(identitypb.Notification_KIND_ROLLED_BACK, fmt.Sprintf("is rolling it back in #%d", ev.Related)), nil
	case db.EventAbandoned:
		text := "abandoned it"
		if ev.Message != "" {
			text += ": " + clip(ev.Message)
		}
		return toReviewers(func(reviewer string) *identitypb.Notification {
			return n.notification(r, c, reviewer, source, identitypb.Notification_KIND_ABANDONED, ev.Actor, text, at)
		})
	}
	return nil, nil
}

// ofComment notifies the CL's author and everyone else in the comment's thread before it.
func (n *Notifier) ofComment(ctx context.Context, r Repository, c db.ChangeList, id int64) ([]*identitypb.Notification, error) {
	cm, ok, err := n.DB.GetComment(ctx, r.ID, c.Number, id)
	if err != nil || !ok {
		return nil, err
	}
	root := cm.ID
	if cm.ParentID != 0 {
		root = cm.ParentID
	}
	all, err := n.DB.Comments(ctx, r.ID, c.Number)
	if err != nil {
		return nil, err
	}
	// Those in its thread before it: one who joined later saw it there.
	to := []string{c.Author}
	for _, o := range all {
		if (o.ID == root || o.ParentID == root) && !o.CreatedAt.After(cm.CreatedAt) && !slices.Contains(to, o.Author) {
			to = append(to, o.Author)
		}
	}
	text := "commented: " + clip(cm.Body)
	if cm.FilePath != "" {
		text = fmt.Sprintf("commented on %s: %s", cm.FilePath, clip(cm.Body))
	}
	var out []*identitypb.Notification
	for _, p := range to {
		if p == "" || p == cm.Author {
			continue
		}
		out = append(out, n.notification(r, c, p, fmt.Sprintf("c:%d:%d", r.ID, cm.ID), identitypb.Notification_KIND_COMMENTED, cm.Author, text,
			timestamppb.New(cm.CreatedAt)))
	}
	return out, nil
}

// clip is the start of a text, on one line, for a notification.
func clip(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) <= excerpt {
		return s
	}
	return string([]rune(s)[:excerpt]) + "…"
}
