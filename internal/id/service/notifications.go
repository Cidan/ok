package service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"

	"github.com/Cidan/ok/internal/id/store"
)

// Notifications (docs/hosted.md, "ok control at scale, as built"): ok control's cells notify; people
// list theirs, mark them read, and choose which kinds they are mailed.

// Notify records notifications, each once, and queues mail of each new one whose kind its recipient
// has mailed and who has an address.
func (s *Server) Notify(ctx context.Context, req *identitypb.NotifyRequest) (*identitypb.NotifyResponse, error) {
	out := &identitypb.NotifyResponse{}
	for _, pn := range req.GetNotifications() {
		n := store.Notification{Principal: pn.GetPrincipalId(), Source: pn.GetSource(), Kind: int(pn.GetKind()), Repository: pn.GetRepositoryId(),
			Number: pn.GetNumber(), RepositoryName: pn.GetRepository(), Title: pn.GetTitle(), Actor: pn.GetActorId(), Text: pn.GetText(),
			Link: pn.GetLink(), At: pn.GetAt().AsTime()}
		mail, err := s.mailFor(ctx, n)
		if err != nil {
			return nil, grpcError(err)
		}
		added, id, err := s.cfg.Store.Notify(ctx, n, mail)
		if err != nil {
			return nil, tenancyError(err)
		}
		if added {
			out.Added++
		}
		if id != 0 {
			out.Mailed++
		}
	}
	return out, nil
}

// mailFor is a notification's message, if its recipient has its kind mailed and has an address.
func (s *Server) mailFor(ctx context.Context, n store.Notification) (*store.Mail, error) {
	if !s.cfg.Mail {
		return nil, nil
	}
	mailed, _, err := s.cfg.Store.Mailed(ctx, n.Principal)
	if err != nil || mailed&(1<<uint(n.Kind)) == 0 {
		return nil, err
	}
	to, err := s.cfg.Store.PrincipalByID(ctx, n.Principal)
	if errors.Is(err, store.ErrNotFound) || err == nil && (to.Email == "" || !to.Active()) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	actor := "someone"
	if a, err := s.cfg.Store.PrincipalByID(ctx, n.Actor); err == nil {
		if h, ok := s.names.HandleOf(a.Name); ok {
			actor = h
		} else {
			actor = a.Name
		}
	}
	link := strings.TrimSuffix(s.cfg.PublicURL, "/") + n.Link
	subject := fmt.Sprintf("[%s #%d] %s", n.RepositoryName, n.Number, n.Title)
	body := fmt.Sprintf("%s %s.\n\n%s #%d: %s\n%s\n\n—\nChoose what you are mailed at %s/notifications\n", actor, n.Text, n.RepositoryName,
		n.Number, n.Title, link, strings.TrimSuffix(s.cfg.PublicURL, "/"))
	return &store.Mail{To: to.Email, Subject: subject, Body: body}, nil
}

// ListNotifications lists a person's latest, of repositories they may still see.
func (s *Server) ListNotifications(ctx context.Context, req *identitypb.ListNotificationsRequest) (*identitypb.ListNotificationsResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 50
	}
	ns, unread, err := s.cfg.Store.Notifications(ctx, p.ID, req.GetUnread(), min(limit, 200))
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.ListNotificationsResponse{Unread: unread}
	seen := map[int64]bool{}
	for _, n := range ns {
		visible, ok := seen[n.Repository]
		if !ok {
			if visible, err = s.mayRead(ctx, n.Repository, p.ID); err != nil {
				return nil, err
			}
			seen[n.Repository] = visible
		}
		if !visible {
			continue
		}
		out.Notifications = append(out.Notifications, &identitypb.Notification{Source: n.Source, PrincipalId: n.Principal,
			Kind: identitypb.Notification_Kind(n.Kind), RepositoryId: n.Repository, Number: n.Number, Repository: n.RepositoryName, Title: n.Title,
			ActorId: n.Actor, Text: n.Text, Link: n.Link, At: timestamppb.New(n.At), Read: n.Read})
	}
	return out, nil
}

// MarkRead marks some of a person's notifications read, or all of them.
func (s *Server) MarkRead(ctx context.Context, req *identitypb.MarkReadRequest) (*identitypb.MarkReadResponse, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	if err := s.cfg.Store.MarkRead(ctx, p.ID, req.GetSources(), req.GetAll()); err != nil {
		return nil, grpcError(err)
	}
	return &identitypb.MarkReadResponse{}, nil
}

// GetNotificationPreferences are the kinds a person is mailed.
func (s *Server) GetNotificationPreferences(ctx context.Context, req *identitypb.GetNotificationPreferencesRequest) (*identitypb.NotificationPreferences, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	return s.preferencesOut(ctx, p.ID)
}

// SetNotificationPreferences sets the kinds a person is mailed.
func (s *Server) SetNotificationPreferences(ctx context.Context, req *identitypb.NotificationPreferences) (*identitypb.NotificationPreferences, error) {
	p, err := s.actor(ctx, req.GetForPersonId())
	if err != nil {
		return nil, err
	}
	var mailed uint64
	for _, k := range req.GetMailed() {
		if k <= 0 || k >= 64 || identitypb.Notification_Kind_name[int32(k)] == "" {
			return nil, tenancyError(fmt.Errorf("%w: no notification is of kind %d", store.ErrInvalid, k))
		}
		mailed |= 1 << uint(k)
	}
	if err := s.cfg.Store.SetMailed(ctx, p.ID, mailed); err != nil {
		return nil, grpcError(err)
	}
	return s.preferencesOut(ctx, p.ID)
}

func (s *Server) preferencesOut(ctx context.Context, person string) (*identitypb.NotificationPreferences, error) {
	mailed, set, err := s.cfg.Store.Mailed(ctx, person)
	if err != nil {
		return nil, grpcError(err)
	}
	out := &identitypb.NotificationPreferences{ForPersonId: person, Default: !set}
	for k := range identitypb.Notification_Kind_name {
		if k > 0 && mailed&(1<<uint(k)) != 0 {
			out.Mailed = append(out.Mailed, identitypb.Notification_Kind(k))
		}
	}
	slices.Sort(out.Mailed)
	return out, nil
}
