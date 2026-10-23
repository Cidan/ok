package web

import (
	"net/http"
	"strconv"

	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"

	"github.com/Cidan/ok/internal/control/pages"
	"github.com/Cidan/ok/internal/control/people"
)

// mailKinds are the kinds of notification a person may have mailed, in the order the page offers
// them, and how it names them.
var mailKinds = []struct {
	kind  identitypb.Notification_Kind
	label string
}{
	{identitypb.Notification_KIND_REVIEW_REQUESTED, "Reviews asked of you"},
	{identitypb.Notification_KIND_REPLIED, "Labels granted on your changes"},
	{identitypb.Notification_KIND_COMMENTED, "Comments on your changes, and in your threads"},
	{identitypb.Notification_KIND_SUBMITTED, "Your changes landed"},
	{identitypb.Notification_KIND_ABANDONED, "Changes you review abandoned"},
	{identitypb.Notification_KIND_ROLLED_BACK, "Your changes rolled back"},
}

// notificationsShown is how many notifications the page lists.
const notificationsShown = 100

// notifications is the signed-in person's notifications page (docs/hosted.md, "ok control at scale,
// as built"), from the directory.
func (s *Server) notifications(w http.ResponseWriter, r *http.Request) {
	who := principal(r)
	list, err := s.notify.ListNotifications(r.Context(), &identitypb.ListNotificationsRequest{ForPersonId: who, Limit: notificationsShown})
	if err != nil {
		fail(w, r, err)
		return
	}
	prefs, err := s.notify.GetNotificationPreferences(r.Context(), &identitypb.GetNotificationPreferencesRequest{ForPersonId: who})
	if err != nil {
		fail(w, r, err)
		return
	}
	v := pages.NotificationsView{Unread: list.GetUnread()}
	for _, n := range list.GetNotifications() {
		ago, title := age(n.GetAt().AsTime())
		v.Items = append(v.Items, pages.NotificationItem{Who: people.Handle(r.Context(), n.GetActorId()), Text: n.GetText(), Repository: n.GetRepository(),
			Title: n.GetTitle(), Number: n.GetNumber(), Link: n.GetLink(), Read: n.GetRead(), Ago: ago, AgoTitle: title})
	}
	mailed := map[identitypb.Notification_Kind]bool{}
	for _, k := range prefs.GetMailed() {
		mailed[k] = true
	}
	for _, k := range mailKinds {
		v.Kinds = append(v.Kinds, pages.MailKind{Value: strconv.Itoa(int(k.kind)), Label: k.label, Mailed: mailed[k.kind]})
	}
	pages.ServeNotifications(w, r, v)
}

// markNotificationsRead marks every one of the person's notifications read.
func (s *Server) markNotificationsRead(w http.ResponseWriter, r *http.Request) {
	if _, err := s.notify.MarkRead(r.Context(), &identitypb.MarkReadRequest{ForPersonId: principal(r), All: true}); err != nil {
		fail(w, r, err)
		return
	}
	http.Redirect(w, r, "/notifications", http.StatusSeeOther)
}

// setMailed sets the kinds of notification the person is mailed.
func (s *Server) setMailed(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		pages.ServeError(w, r, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	req := &identitypb.NotificationPreferences{ForPersonId: principal(r)}
	for _, v := range r.PostForm["mail"] {
		k, err := strconv.Atoi(v)
		if err != nil {
			pages.ServeError(w, r, http.StatusBadRequest, "Bad Request", "no notification is of kind "+v)
			return
		}
		req.Mailed = append(req.Mailed, identitypb.Notification_Kind(k))
	}
	if _, err := s.notify.SetNotificationPreferences(r.Context(), req); err != nil {
		fail(w, r, err)
		return
	}
	http.Redirect(w, r, "/notifications", http.StatusSeeOther)
}
