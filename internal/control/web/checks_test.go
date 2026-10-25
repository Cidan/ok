package web

import (
	"context"
	"testing"

	"github.com/Cidan/ok/internal/control/db"
	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// A CL's post says it landed once the SCM landed it, though automation has yet to record its
// submission so — the SCM's feed can bring the landing first — and until then, while it has not, it
// is submitting.
func TestPostLandedFirst(t *testing.T) {
	ctx := context.Background()
	active := submissions{list: []*automationpb.Submission{{Status: automationpb.Submission_STATUS_ACTIVE}}}
	s := &Server{}
	if p := s.post(ctx, nil, db.ChangeList{Status: db.StatusOpen}, active); p.Status != "Submitting" {
		t.Errorf("an open CL with an active submission: %q", p.Status)
	}
	if p := s.post(ctx, nil, db.ChangeList{Status: db.StatusSubmitted}, active); p.Status != "Landed" {
		t.Errorf("a CL the SCM landed, its submission active still: %q", p.Status)
	}
	landed := submissions{list: []*automationpb.Submission{{Status: automationpb.Submission_STATUS_LANDED}}}
	if p := s.post(ctx, nil, db.ChangeList{Status: db.StatusSubmitted}, landed); p.Status != "Landed" {
		t.Errorf("a CL landed and recorded so: %q", p.Status)
	}
}
