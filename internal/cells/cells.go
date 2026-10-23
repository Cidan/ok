// Package cells is what ok's roles share about cells (docs/hosted.md, "Cells, as built"): how one
// refuses a request about a repository another cell holds, and how the edge reads the refusal.
package cells

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	okpb "github.com/Cidan/ok/proto/ok/v1"
)

// Header names the cell holding the repository on a page a role answered 421, Misdirected
// Request: the edge sends the request there.
const Header = "Ok-Cell"

// Moved is a role's answer to a request about a repository another cell holds: FAILED_PRECONDITION
// with an ok.v1.Moved detail naming the cell.
func Moved(repo int64, cell string) error {
	st, err := status.New(codes.FailedPrecondition, fmt.Sprintf("repository %d is in cell %s", repo, cell)).
		WithDetails(&okpb.Moved{RepositoryId: repo, Cell: cell})
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "repository %d is in cell %s", repo, cell)
	}
	return st.Err()
}

// MovedOf is the detail of a Moved answer err carries, if it carries one.
func MovedOf(err error) (*okpb.Moved, bool) {
	var se interface{ GRPCStatus() *status.Status }
	if !errors.As(err, &se) {
		return nil, false
	}
	st := se.GRPCStatus()
	if st.Code() != codes.FailedPrecondition {
		return nil, false
	}
	for _, d := range st.Details() {
		if m, ok := d.(*okpb.Moved); ok {
			return m, true
		}
	}
	return nil, false
}
