package api

import (
	"fmt"
	"slices"
	"strings"

	controlpb "github.com/Cidan/ok/proto/ok/control/v1"
)

// derive sets where a CL stands — its stage, who it waits on, and a sentence saying so — from
// what its status already holds: the CL, its review, pre, post, and deployments. admins are the
// repository's admins, by handle, for a deployment waiting on one.
//
// The order is the lifecycle's: a closed CL is decided by how it closed; an active one by its
// submission when it has one going, then its pre, then its review.
func derive(st *controlpb.CLStatus, admins []string) {
	cl := st.GetCl()
	switch cl.GetStatus() {
	case controlpb.CL_STATUS_ABANDONED:
		set(st, controlpb.Stage_STAGE_ABANDONED, controlpb.WaitingOn_WAITING_ON_NOBODY, nil, "Abandoned")
		return
	case controlpb.CL_STATUS_SUBMITTED:
		deriveLanded(st, admins)
		return
	}
	if st.GetPost().GetStatus() == controlpb.Post_STATUS_LANDED {
		// Its submission landed after its CL was read (statusOf reads it again): it is landed.
		set(st, controlpb.Stage_STAGE_LANDED, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, "Landed; reading where it stands")
		return
	}

	if p := st.GetPost(); p != nil {
		switch p.GetStatus() {
		case controlpb.Post_STATUS_ACTIVE:
			what := "in the landing queue"
			if p.GetNote() != "" {
				what = p.GetNote()
			}
			if p.GetTotal() > 0 {
				what = fmt.Sprintf("%s; post %d/%d passed", what, p.GetPassed(), p.GetTotal())
			}
			set(st, controlpb.Stage_STAGE_SUBMITTED, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, "Submitted: "+what)
			return
		case controlpb.Post_STATUS_FAILED:
			if postIsCurrent(st) {
				set(st, controlpb.Stage_STAGE_POST_FAILED, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{cl.GetAuthor()},
					"Submission failed: "+orElse(p.GetReason(), "post failed"))
				return
			}
		}
	}

	pre := st.GetPre()
	switch pre.GetVerdict() {
	case controlpb.Checks_VERDICT_FAILED:
		set(st, controlpb.Stage_STAGE_PRE_FAILED, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{cl.GetAuthor()},
			fmt.Sprintf("Pre failed (%d/%d passed)", pre.GetPassed(), pre.GetTotal()))
		return
	case controlpb.Checks_VERDICT_PASSED:
	default:
		what := "Pre is being planned"
		if pre.GetVerdict() == controlpb.Checks_VERDICT_PENDING && pre.GetTotal() > 0 {
			what = fmt.Sprintf("Pre is running: %d/%d passed", pre.GetPassed(), pre.GetTotal())
		}
		if slices.Contains(st.GetUnavailable(), unavailableAutomation) {
			what = "Pre could not be read: ok automation is unavailable"
		}
		set(st, controlpb.Stage_STAGE_PRE_RUNNING, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil, what)
		return
	}

	passed := fmt.Sprintf("pre passed (%d/%d)", pre.GetPassed(), pre.GetTotal())
	if cl.GetStatus() == controlpb.CL_STATUS_DRAFT {
		set(st, controlpb.Stage_STAGE_DRAFT, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{cl.GetAuthor()},
			"Draft; "+passed+": send it for review (jj cl ready)")
		return
	}
	rv := st.GetReview()
	if !rv.GetApproved() {
		set(st, controlpb.Stage_STAGE_IN_REVIEW, controlpb.WaitingOn_WAITING_ON_REVIEWERS, reviewWaitsFor(st),
			"In review; "+passed+"; needs "+needs(rv))
		return
	}
	summary := "Approved and " + passed + ": submit it (jj cl submit)"
	if rv.GetBlocked() != "" {
		summary = "Approved and " + passed + ", but it cannot submit: " + rv.GetBlocked()
	}
	set(st, controlpb.Stage_STAGE_SUBMITTABLE, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{cl.GetAuthor()}, summary)
}

// deriveLanded decides a landed CL by its commit's deployments: each deploy's latest deployment
// covering the commit decides that deploy.
func deriveLanded(st *controlpb.CLStatus, admins []string) {
	commit := short(st.GetCl().GetLandedCommit())
	d := st.GetDeployments()
	switch {
	case d == nil:
		set(st, controlpb.Stage_STAGE_LANDED, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil,
			"Landed as "+commit+"; its deployments could not be read: the deployer is unavailable")
		return
	case !d.GetPlanned():
		set(st, controlpb.Stage_STAGE_LANDED, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil,
			"Landed as "+commit+"; the deployer has yet to plan its deployments")
		return
	case len(d.GetItems()) == 0:
		set(st, controlpb.Stage_STAGE_DEPLOYED, controlpb.WaitingOn_WAITING_ON_NOBODY, nil,
			"Landed as "+commit+"; nothing to deploy")
		return
	}
	var done, failed, hazard int
	var failedNames, unfinished []string
	for _, it := range d.GetItems() {
		switch it.GetStatus() {
		case controlpb.Deployment_STATUS_SUCCEEDED:
			done++
		case controlpb.Deployment_STATUS_FAILED, controlpb.Deployment_STATUS_HALTED, controlpb.Deployment_STATUS_CANCELED:
			failed++
			failedNames = append(failedNames, it.GetName())
		default:
			if it.GetStage() == "hazard" {
				hazard++
			}
			unfinished = append(unfinished, it.GetName()+": "+it.GetStage())
		}
	}
	total := len(d.GetItems())
	switch {
	case failed > 0:
		set(st, controlpb.Stage_STAGE_DEPLOY_FAILED, controlpb.WaitingOn_WAITING_ON_AUTHOR, []string{st.GetCl().GetAuthor()},
			fmt.Sprintf("Landed as %s; deployment failed: %s (%d/%d deployed)", commit, strings.Join(failedNames, ", "), done, total))
	case hazard > 0:
		set(st, controlpb.Stage_STAGE_DEPLOYING, controlpb.WaitingOn_WAITING_ON_ADMINS, admins,
			fmt.Sprintf("Landed as %s; a migration waits for a repository admin to allow its hazards (%d/%d deployed)", commit, done, total))
	case len(unfinished) > 0:
		set(st, controlpb.Stage_STAGE_DEPLOYING, controlpb.WaitingOn_WAITING_ON_AUTOMATION, nil,
			fmt.Sprintf("Landed as %s; deploying %d/%d: %s", commit, done, total, strings.Join(unfinished, ", ")))
	default:
		set(st, controlpb.Stage_STAGE_DEPLOYED, controlpb.WaitingOn_WAITING_ON_NOBODY, nil,
			fmt.Sprintf("Landed as %s; deployed (%d/%d)", commit, done, total))
	}
}

// postIsCurrent reports whether the CL's latest submission was of its current patchset: a failed
// submission of an older one says nothing of the CL now.
func postIsCurrent(st *controlpb.CLStatus) bool {
	return st.GetPost().GetPatchset() == st.GetCl().GetPatchset()
}

// needs words what review still needs.
func needs(rv *controlpb.Review) string {
	var n []string
	if rv.GetError() != "" {
		return "OWNERS that resolve (" + rv.GetError() + ")"
	}
	if !rv.GetLgtm() {
		n = append(n, "an LGTM")
	}
	for _, o := range rv.GetOwnerSets() {
		if !o.GetSatisfied() {
			n = append(n, "Approval from "+ownerSetLabel(o))
		}
	}
	n = append(n, rv.GetIdentity()...)
	if u := len(rv.GetUnresolved()); u > 0 {
		n = append(n, fmt.Sprintf("%d unresolved thread%s resolved", u, plural(u)))
	}
	if len(n) == 0 {
		return "nothing more"
	}
	return strings.Join(n, ", ")
}

// reviewWaitsFor are the people review waits on: the reviewers for an LGTM, the owners of each set
// not yet approved, and the author for each unresolved thread.
func reviewWaitsFor(st *controlpb.CLStatus) []string {
	rv := st.GetReview()
	var who []string
	add := func(h string) {
		if h != "" && !slices.Contains(who, h) {
			who = append(who, h)
		}
	}
	if !rv.GetLgtm() {
		for _, r := range rv.GetReviewers() {
			add(r)
		}
	}
	for _, o := range rv.GetOwnerSets() {
		if o.GetSatisfied() {
			continue
		}
		for _, owner := range o.GetOwners() {
			if h, ok := strings.CutPrefix(owner, "user: "); ok {
				add(h)
			}
		}
	}
	if len(rv.GetUnresolved()) > 0 {
		add(st.GetCl().GetAuthor())
	}
	return who
}

func ownerSetLabel(o *controlpb.OwnerSet) string {
	if o.GetConfig() == "" {
		return "anyone (no OWNERS)"
	}
	label := "//" + o.GetConfig()
	if len(o.GetRules()) > 0 {
		label += " (" + strings.Join(o.GetRules(), ", ") + ")"
	}
	return label
}

func set(st *controlpb.CLStatus, stage controlpb.Stage, on controlpb.WaitingOn, who []string, summary string) {
	st.Stage, st.WaitingOn, st.WaitingFor, st.Summary = stage, on, who, summary
}

// Finished reports whether a CL will never change again by itself or by anyone's hand on it:
// deployed, or abandoned.
func Finished(st *controlpb.CLStatus) bool {
	return st.GetStage() == controlpb.Stage_STAGE_DEPLOYED || st.GetStage() == controlpb.Stage_STAGE_ABANDONED
}

func short(hex string) string {
	if len(hex) > 12 {
		return hex[:12]
	}
	return hex
}

func orElse(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
