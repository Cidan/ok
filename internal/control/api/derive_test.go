package api

import (
	"context"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Cidan/ok/internal/serve/authn"
	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
)

func cl(s controlpb.CL_Status) *controlpb.CL {
	return &controlpb.CL{Repository: "acme", Number: 7, Author: "ann", Status: s, Patchset: 2, LandedCommit: "416da42e0123456789"}
}

func pre(v controlpb.Checks_Verdict, passed, total int32) *controlpb.Checks {
	return &controlpb.Checks{Verdict: v, Passed: passed, Total: total, Patchset: 2}
}

func deployed(planned bool, statuses ...controlpb.Deployment_Status) *controlpb.Deployments {
	d := &controlpb.Deployments{Planned: planned}
	for i, s := range statuses {
		stage := "roll"
		if s == controlpb.Deployment_STATUS_SUCCEEDED {
			stage = "done"
		}
		d.Items = append(d.Items, &controlpb.Deployment{Id: int64(i + 1), Name: []string{"api", "scm", "hazardous"}[i], Status: s, Stage: stage})
	}
	return d
}

// Every stage, from what a CL's status holds: who it waits on, and what its sentence says.
func TestDerive(t *testing.T) {
	passed := pre(controlpb.Checks_VERDICT_PASSED, 7, 7)
	approved := &controlpb.Review{Approved: true, Lgtm: true}
	for name, tc := range map[string]struct {
		st    *controlpb.CLStatus
		stage controlpb.Stage
		on    controlpb.WaitingOn
		who   []string
		says  string
	}{
		"abandoned": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_ABANDONED)},
			controlpb.Stage_STAGE_ABANDONED, controlpb.WaitingOn_WAITING_ON_NOBODY, nil, "Abandoned"},
		"pre not planned yet": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: pre(controlpb.Checks_VERDICT_NONE, 0, 0)},
			controlpb.Stage_STAGE_PRE_RUNNING, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, "Pre is being planned"},
		"a draft, which runs no pre": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_DRAFT), Pre: pre(controlpb.Checks_VERDICT_NONE, 0, 0)},
			controlpb.Stage_STAGE_DRAFT, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{"ann"}, "pre runs once it is sent for review (jj cl ready), or now (jj cl pre)"},
		"under its stack's top, in review": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: pre(controlpb.Checks_VERDICT_NONE, 0, 0), PreAt: 9,
			Review: &controlpb.Review{Reviewers: []string{"bea"}}},
			controlpb.Stage_STAGE_IN_REVIEW, controlpb.WaitingOn_WAITING_ON_REVIEWERS, []string{"bea"}, "In review; pre runs at CL 9, the top of its stack"},
		"under its stack's top, approved, its own old pre failed": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: pre(controlpb.Checks_VERDICT_FAILED, 1, 7), PreAt: 9,
			Review: approved},
			controlpb.Stage_STAGE_SUBMITTABLE, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{"ann"}, "submit the stack from there (jj cl submit --stack 9)"},
		"pre running": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: pre(controlpb.Checks_VERDICT_PENDING, 3, 7)},
			controlpb.Stage_STAGE_PRE_RUNNING, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, "Pre is running: 3/7 passed"},
		"pre unreadable": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: pre(controlpb.Checks_VERDICT_UNSPECIFIED, 0, 0), Unavailable: []string{unavailableAutomation}},
			controlpb.Stage_STAGE_PRE_RUNNING, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, "ok automation is unavailable"},
		"pre failed": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: pre(controlpb.Checks_VERDICT_FAILED, 5, 7)},
			controlpb.Stage_STAGE_PRE_FAILED, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{"ann"}, "Pre failed (5/7 passed)"},
		"draft": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_DRAFT), Pre: passed},
			controlpb.Stage_STAGE_DRAFT, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{"ann"}, "jj cl ready"},
		"in review": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: passed, Review: &controlpb.Review{
			Reviewers: []string{"bea"}, OwnerSets: []*controlpb.OwnerSet{{Config: "auth/OWNERS", Owners: []string{"user: cy", "acl: admins"}}},
			Unresolved: []*controlpb.Thread{{Id: 1}}}},
			controlpb.Stage_STAGE_IN_REVIEW, controlpb.WaitingOn_WAITING_ON_REVIEWERS, []string{"bea", "cy", "ann"},
			"needs an LGTM, Approval from //auth/OWNERS, 1 unresolved thread resolved"},
		"OWNERS that do not resolve": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: passed, Review: &controlpb.Review{Error: "no such ACL"}},
			controlpb.Stage_STAGE_IN_REVIEW, controlpb.WaitingOn_WAITING_ON_REVIEWERS, nil, "OWNERS that resolve (no such ACL)"},
		"submittable": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: passed, Review: approved},
			controlpb.Stage_STAGE_SUBMITTABLE, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{"ann"}, "jj cl submit"},
		"submittable, but in conflict": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: passed, Review: &controlpb.Review{Approved: true, Blocked: "it conflicts with main"}},
			controlpb.Stage_STAGE_SUBMITTABLE, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{"ann"}, "cannot submit: it conflicts with main"},
		"submitted": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: passed, Review: approved,
			Post: &controlpb.Post{Status: controlpb.Post_STATUS_ACTIVE, Note: "testing post", Passed: 2, Total: 7, Patchset: 2}},
			controlpb.Stage_STAGE_SUBMITTED, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, "Submitted: testing post; post 2/7 passed"},
		"post failed": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: passed, Review: approved,
			Post: &controlpb.Post{Status: controlpb.Post_STATUS_FAILED, Reason: "post failed: //x:test", Patchset: 2}},
			controlpb.Stage_STAGE_POST_FAILED, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{"ann"}, "Submission failed: post failed: //x:test"},
		"an older patchset's failed submission": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: passed, Review: approved,
			Post: &controlpb.Post{Status: controlpb.Post_STATUS_FAILED, Patchset: 1}},
			controlpb.Stage_STAGE_SUBMITTABLE, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{"ann"}, "submit it"},
		"its submission landed after the CL was read": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Pre: passed, Review: approved,
			Post: &controlpb.Post{Status: controlpb.Post_STATUS_LANDED, Patchset: 2}},
			controlpb.Stage_STAGE_LANDED, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, "Landed"},
		"landed, deployer unreadable": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_SUBMITTED)},
			controlpb.Stage_STAGE_LANDED, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, "the deployer is unavailable"},
		"landed, not planned": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_SUBMITTED), Deployments: deployed(false)},
			controlpb.Stage_STAGE_LANDED, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, "Landed as 416da42e0123; the deployer has yet to plan"},
		"nothing to deploy": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_SUBMITTED), Deployments: deployed(true)},
			controlpb.Stage_STAGE_DEPLOYED, controlpb.WaitingOn_WAITING_ON_NOBODY, nil, "nothing to deploy"},
		"deploying": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_SUBMITTED), Deployments: deployed(true, controlpb.Deployment_STATUS_SUCCEEDED, controlpb.Deployment_STATUS_RUNNING)},
			controlpb.Stage_STAGE_DEPLOYING, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, "deploying 1/2: scm: roll"},
		"a hazard to allow": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_SUBMITTED), Deployments: &controlpb.Deployments{Planned: true,
			Items: []*controlpb.Deployment{{Id: 1, Name: "db", Status: controlpb.Deployment_STATUS_RUNNING, Stage: "hazard"}}}},
			controlpb.Stage_STAGE_DEPLOYING, controlpb.WaitingOn_WAITING_ON_ADMINS, []string{"root"}, "waits for a repository admin"},
		"deployed": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_SUBMITTED), Deployments: deployed(true, controlpb.Deployment_STATUS_SUCCEEDED, controlpb.Deployment_STATUS_SUCCEEDED)},
			controlpb.Stage_STAGE_DEPLOYED, controlpb.WaitingOn_WAITING_ON_NOBODY, nil, "deployed (2/2)"},
		"deploy failed": {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_SUBMITTED), Deployments: deployed(true, controlpb.Deployment_STATUS_SUCCEEDED, controlpb.Deployment_STATUS_FAILED, controlpb.Deployment_STATUS_RUNNING)},
			controlpb.Stage_STAGE_DEPLOY_FAILED, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{"ann"}, "deployment failed: scm (1/3 deployed)"},
	} {
		t.Run(name, func(t *testing.T) {
			derive(tc.st, []string{"root"})
			if tc.st.GetStage() != tc.stage || tc.st.GetWaitingOn() != tc.on || !slices.Equal(tc.st.GetWaitingFor(), tc.who) || !strings.Contains(tc.st.GetSummary(), tc.says) {
				t.Errorf("got %v, waiting on %v %v, %q\nwant %v, waiting on %v %v, saying %q",
					tc.st.GetStage(), tc.st.GetWaitingOn(), tc.st.GetWaitingFor(), tc.st.GetSummary(), tc.stage, tc.on, tc.who, tc.says)
			}
			if done := Finished(tc.st); done != (tc.stage == controlpb.Stage_STAGE_DEPLOYED || tc.stage == controlpb.Stage_STAGE_ABANDONED) {
				t.Errorf("Finished = %v at %v", done, tc.stage)
			}
		})
	}
}

// Only a person acts here, and never by a request from another site.
func TestCaller(t *testing.T) {
	a := &API{base: "https://ok.example.com"}
	person := authn.Caller{Principal: "ann.human.example.com", ID: "prn_ann", Human: true}
	for name, tc := range map[string]struct {
		caller *authn.Caller
		md     []string
		ok     bool
	}{
		"a person, from the command line": {&person, nil, true},
		"a person, from ok's own page":    {&person, []string{"origin", "https://ok.example.com", "sec-fetch-site", "same-origin"}, true},
		"no one":                          {nil, nil, false},
		"a workload":                      {&authn.Caller{Principal: "control.ok.robot.example.com"}, nil, false},
		"another site":                    {&person, []string{"sec-fetch-site", "cross-site"}, false},
		"a sibling site":                  {&person, []string{"sec-fetch-site", "same-site"}, false},
		"a foreign origin":                {&person, []string{"origin", "https://evil.example"}, false},
		"ok over plain http":              {&person, []string{"origin", "http://ok.example.com"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(tc.md...))
			if tc.caller != nil {
				ctx = authn.NewContext(ctx, *tc.caller)
			}
			me, err := a.caller(ctx)
			if tc.ok != (err == nil) || (!tc.ok && status.Code(err) != codes.PermissionDenied) || (tc.ok && me != "prn_ann") {
				t.Errorf("caller = %q, %v", me, err)
			}
		})
	}
}

// A status read across a landing, or while main moved, is read again.
func TestSettled(t *testing.T) {
	head, moved := []byte{1}, []byte{2}
	open := &controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Post: &controlpb.Post{Status: controlpb.Post_STATUS_ACTIVE}}
	landedOpen := &controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_OPEN), Post: &controlpb.Post{Status: controlpb.Post_STATUS_LANDED}}
	landed := &controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_SUBMITTED), Post: &controlpb.Post{Status: controlpb.Post_STATUS_LANDED}}
	for name, tc := range map[string]struct {
		st     *controlpb.CLStatus
		before []byte
		after  []byte
		want   bool
	}{
		"nothing moved":                      {open, head, head, true},
		"main moved while it was read":       {open, head, moved, false},
		"its submission landed after the CL": {landedOpen, head, head, false},
		"landed, read whole":                 {landed, head, head, true},
		"no submission":                      {&controlpb.CLStatus{Cl: cl(controlpb.CL_STATUS_DRAFT)}, head, head, true},
	} {
		if got := settled(tc.st, tc.before, tc.after); got != tc.want {
			t.Errorf("%s: settled = %v, want %v", name, got, tc.want)
		}
	}
}
