package db

import "time"

// Reviewer is one of a CL's reviewers.
type Reviewer struct {
	Principal string
	AddedBy   string
	AddedAt   time.Time
}

// Label is a review label; the values are stored.
type Label int16

const (
	LabelLGTM     Label = 1
	LabelApproval Label = 2
)

// Labels are the policy's labels, in order.
var Labels = []Label{LabelLGTM, LabelApproval}

// String is the label's name.
func (l Label) String() string {
	switch l {
	case LabelLGTM:
		return "LGTM"
	case LabelApproval:
		return "Approval"
	}
	return "unknown"
}

// ParseLabel reads a label by name.
func ParseLabel(s string) (Label, bool) {
	for _, l := range Labels {
		if l.String() == s {
			return l, true
		}
	}
	return 0, false
}

// Grant is a reviewer's current state for one label.
type Grant struct {
	Number    int64
	Principal string
	Label     Label
	Granted   bool
	Patchset  int32
	UpdatedAt time.Time
}

// Side is the diff side an inline comment anchors to; the values are stored.
type Side int16

const (
	SideNone     Side = 0
	SideBase     Side = 1
	SidePatchset Side = 2
)

// Comment is one comment. A thread is a root (ParentID 0) and its replies; the root carries the
// thread's anchor and resolution.
type Comment struct {
	ID        int64
	Number    int64
	Patchset  int32
	Author    string
	ParentID  int64
	FilePath  string // "": a CL-level comment
	Line      int32  // 0: file-level
	Side      Side
	Body      string
	Resolved  bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// EventKind is what happened to a CL; the values are stored.
type EventKind int16

const (
	EventUploaded        EventKind = 1  // a patchset (Patchset); the first created the CL
	EventReviewRequested EventKind = 2  // draft → open
	EventReturnedToDraft EventKind = 3  // open → draft
	EventAbandoned       EventKind = 4  // From → abandoned; Message is the reason
	EventRestored        EventKind = 5  // abandoned → To
	EventSubmitted       EventKind = 6  // landed on main as Commit
	EventReviewerAdded   EventKind = 7  // Subject was assigned
	EventReviewerRemoved EventKind = 8  // Subject stopped reviewing
	EventLabelGranted    EventKind = 9  // Label granted on Patchset
	EventLabelRevoked    EventKind = 10 // Label revoked on Patchset
	EventApproved        EventKind = 11 // automatic: every review requirement is met
	EventApprovalLost    EventKind = 12 // automatic: a review requirement is no longer met
	EventRolledBack      EventKind = 13 // Related, a rollback CL, was created to roll this one back
)

// Event is one entry of a CL's history. An empty Actor is ok itself.
type Event struct {
	ID       int64
	At       time.Time
	Actor    string
	Kind     EventKind
	From, To Status
	Subject  string
	Label    Label
	Patchset int32
	Commit   []byte
	Message  string
	Related  int64 // the other CL it names (EventRolledBack)
}
