package webhooks

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/control/db"
	"github.com/Cidan/ok/internal/control/outbox"
	"github.com/Cidan/ok/internal/tenancy"
)

// Consume queues, for each outbox entry, a delivery to each of its repository's webhooks that wants
// its event: a CL's events and comments (cl), and main's moves (push). It is outbox.Consumer.
func (h *Hooks) Consume(ctx context.Context, r outbox.Repository, entries []db.OutboxEntry) error {
	hooks, err := h.DB.Webhooks(ctx, r.ID)
	if err != nil || len(hooks) == 0 {
		return err
	}
	var ds []db.Delivery
	cls := map[int64]*db.ChangeList{}
	for _, e := range entries {
		event, body, err := h.payloadOf(ctx, r, e, cls)
		if err != nil {
			return err
		}
		if body == nil {
			continue
		}
		want := int64(db.WebhookCL)
		if event == "push" {
			want = db.WebhookPush
		}
		for _, w := range hooks {
			if w.Events&want == 0 {
				continue
			}
			ds = append(ds, db.Delivery{Repository: r.ID, ID: deliveryID(e.ID, w.ID), Webhook: w.ID, Event: event, Payload: body, NextTry: h.now(),
				CreatedAt: h.now()})
		}
	}
	return h.DB.QueueDeliveries(ctx, r.ID, ds)
}

func payload(v any) ([]byte, error) { return json.Marshal(v) }

// payloadOf is an outbox entry's event and payload; nil for one no webhook is sent.
func (h *Hooks) payloadOf(ctx context.Context, r outbox.Repository, e db.OutboxEntry, cls map[int64]*db.ChangeList) (string, []byte, error) {
	repo := map[string]any{"id": strconv.FormatInt(r.ID, 10), "name": r.Name, "url": h.link(r.Name, 0)}
	if e.Number == 0 {
		if e.HeadSeq == 0 {
			return "", nil, nil
		}
		body, err := payload(map[string]any{"event": "push", "repository": repo, "head": hex.EncodeToString(e.Head),
			"head_seq": e.HeadSeq, "at": e.QueuedAt.UTC().Format(time.RFC3339Nano)})
		return "push", body, err
	}
	c, ok := cls[e.Number]
	if !ok {
		got, found, err := h.DB.GetChangeList(ctx, r.ID, e.Number)
		if err != nil {
			return "", nil, err
		}
		if found {
			c = &got
		}
		cls[e.Number] = c
	}
	if c == nil {
		return "", nil, nil
	}
	cl := map[string]any{"number": c.Number, "title": c.Title(), "status": outbox.StatusName(c.Status), "author": c.Author,
		"patchset": c.CurrentPatchset, "url": h.link(r.Name, c.Number)}
	body := map[string]any{"event": "cl", "repository": repo, "cl": cl}
	switch {
	case e.EventID != 0:
		ev, found, err := h.DB.GetEvent(ctx, r.ID, e.Number, e.EventID)
		if err != nil || !found {
			return "", nil, err
		}
		body["action"] = eventAction[ev.Kind]
		body["actor"] = ev.Actor
		body["at"] = ev.At.UTC().Format(time.RFC3339Nano)
		detail := map[string]any{"patchset": ev.Patchset}
		switch ev.Kind {
		case db.EventReviewerAdded, db.EventReviewerRemoved:
			detail["reviewer"] = ev.Subject
		case db.EventLabelGranted, db.EventLabelRevoked:
			detail["label"] = ev.Label.String()
		case db.EventSubmitted:
			detail["commit"] = hex.EncodeToString(ev.Commit)
		case db.EventAbandoned:
			detail["reason"] = ev.Message
		case db.EventRolledBack:
			detail["rollback"] = ev.Related
		}
		body["detail"] = detail
	case e.CommentID != 0:
		cm, found, err := h.DB.GetComment(ctx, r.ID, e.Number, e.CommentID)
		if err != nil || !found {
			return "", nil, err
		}
		body["action"] = "commented"
		body["actor"] = cm.Author
		body["at"] = cm.CreatedAt.UTC().Format(time.RFC3339Nano)
		comment := map[string]any{"id": strconv.FormatInt(cm.ID, 10), "patchset": cm.Patchset, "body": cm.Body}
		if cm.ParentID != 0 {
			comment["in_reply_to"] = strconv.FormatInt(cm.ParentID, 10)
		}
		if cm.FilePath != "" {
			comment["file"] = cm.FilePath
			if cm.Line != 0 {
				comment["line"] = cm.Line
			}
		}
		body["comment"] = comment
	default:
		return "", nil, nil
	}
	out, err := payload(body)
	return "cl", out, err
}

// eventAction is how a payload names an event.
var eventAction = map[db.EventKind]string{
	db.EventUploaded: "uploaded", db.EventReviewRequested: "review_requested", db.EventReturnedToDraft: "returned_to_draft",
	db.EventAbandoned: "abandoned", db.EventRestored: "restored", db.EventSubmitted: "submitted", db.EventReviewerAdded: "reviewer_added",
	db.EventReviewerRemoved: "reviewer_removed", db.EventLabelGranted: "label_granted", db.EventLabelRevoked: "label_revoked",
	db.EventApproved: "approved", db.EventApprovalLost: "approval_lost", db.EventRolledBack: "rolled_back",
}

// link is a repository's URL on ok control, or one of its CL's.
func (h *Hooks) link(repo string, number int64) string {
	u := strings.TrimSuffix(h.PublicURL, "/") + tenancy.URLPath(repo, h.Installation)
	if number != 0 {
		u += "/cl/" + strconv.FormatInt(number, 10)
	}
	return u
}
