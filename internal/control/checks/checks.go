// Package checks is how ok control reads ok automation's runs, wherever it shows them — the
// Checks tab and the ChangeLists service alike: what a job is called, when a run is over, and
// which of a submission's attempts speaks for its post.
package checks

import (
	"strings"

	automationpb "github.com/Cidan/ok/proto/ok/automation/v1"
)

// JobName splits a job's ID into the name a person reads and where it comes from: a workflow
// job's ID is //<workflow file>:<phase>:<name>; a built-in job's is its own name.
func JobName(id string) (name, source string) {
	if strings.HasPrefix(id, "//") {
		if i := strings.LastIndexByte(id, ':'); i > 0 {
			if j := strings.LastIndexByte(id[:i], ':'); j > 0 {
				return id[i+1:], id[:j]
			}
		}
	}
	return id, "Built In"
}

// Finished reports whether a run is over: passed, failed, canceled, or errored.
func Finished(r *automationpb.Run) bool {
	return r.GetStatus() >= automationpb.Run_STATUS_PASSED
}

// Counting is the attempt that speaks for a submission's post: the one testing, else the
// newest that landed or failed, else the newest. nil when it has none.
func Counting(sub *automationpb.Submission) *automationpb.Attempt {
	attempts := sub.GetAttempts()
	var counts *automationpb.Attempt
	for _, a := range attempts {
		switch {
		case a.GetStatus() == automationpb.Attempt_STATUS_ACTIVE:
			return a
		case counts == nil && (a.GetStatus() == automationpb.Attempt_STATUS_LANDED || a.GetStatus() == automationpb.Attempt_STATUS_FAILED):
			counts = a
		}
	}
	if counts == nil && len(attempts) > 0 {
		counts = attempts[0]
	}
	return counts
}

// Candidate is an attempt's candidate for CL number, or nil.
func Candidate(a *automationpb.Attempt, number int64) *automationpb.Attempt_Candidate {
	for _, c := range a.GetCandidates() {
		if c.GetClNumber() == number {
			return c
		}
	}
	return nil
}
