// Package scm is ok control's client of the SCM (docs/scm.md): its
// Repositories, ChangeLists, Browse, and Events services, with the conversions and ID
// formatting the UI shows.
package scm

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"google.golang.org/grpc"

	"github.com/Cidan/ok/internal/control/db"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Client is the SCM's services over one connection.
type Client struct {
	Repositories  scmpb.RepositoriesClient
	ChangeLists   scmpb.ChangeListsClient
	Review        scmpb.ReviewClient
	LandingChecks scmpb.LandingChecksClient
	Browse        scmpb.BrowseClient
	Events        scmpb.EventsClient
	Search        scmpb.SearchClient
}

// New builds a client over a connection.
func New(conn grpc.ClientConnInterface) *Client {
	return &Client{
		Repositories:  scmpb.NewRepositoriesClient(conn),
		ChangeLists:   scmpb.NewChangeListsClient(conn),
		Review:        scmpb.NewReviewClient(conn),
		LandingChecks: scmpb.NewLandingChecksClient(conn),
		Browse:        scmpb.NewBrowseClient(conn),
		Events:        scmpb.NewEventsClient(conn),
		Search:        scmpb.NewSearchClient(conn),
	}
}

// RootCommit is jj's root commit: 32 zero bytes. main starts there.
var RootCommit = make([]byte, 32)

// IsRoot reports whether a commit is the root commit.
func IsRoot(id []byte) bool {
	for _, b := range id {
		if b != 0 {
			return false
		}
	}
	return len(id) == 32
}

// Hex is an ID in hex, as commit IDs are shown and linked.
func Hex(id []byte) string { return hex.EncodeToString(id) }

// Short is a commit ID's first 12 hex digits, as jj shows it.
func Short(id []byte) string {
	h := Hex(id)
	return h[:min(12, len(h))]
}

// ParseHex reads a full commit ID from hex.
func ParseHex(s string) ([]byte, bool) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		return nil, false
	}
	return b, true
}

// ChangeID is a change ID as jj shows it: hex with each digit d written as the letter
// 'z' - d ("reverse hex"), so change IDs and commit IDs never look alike.
func ChangeID(id []byte) string {
	h := hex.EncodeToString(id)
	var b strings.Builder
	for _, c := range h {
		d := strings.IndexRune("0123456789abcdef", c)
		b.WriteByte(byte('z' - d))
	}
	return b.String()
}

// ShortChangeID is a change ID's first 12 letters.
func ShortChangeID(id []byte) string {
	s := ChangeID(id)
	return s[:min(12, len(s))]
}

// ChangeListOut converts the SCM's CL to its projection.
func ChangeListOut(c *scmpb.ChangeList) db.ChangeList {
	return db.ChangeList{
		Repo:            c.GetRepositoryId(),
		Number:          c.GetNumber(),
		ChangeID:        c.GetChangeId(),
		Author:          c.GetAuthorId(),
		Status:          db.Status(c.GetStatus()),
		ParentNumber:    c.GetParentNumber(),
		CurrentPatchset: c.GetCurrentPatchset(),
		Description:     c.GetDescription(),
		LandedCommit:    c.GetLandedCommit(),
		SubmittedBy:     c.GetSubmittedById(),
		CreatedAt:       c.GetCreatedAt().AsTime(),
		UpdatedAt:       c.GetUpdatedAt().AsTime(),
		RollsBack:       c.GetRollback().GetChangeLists(),
	}
}

// Patchset returns a CL's patchset n, or nil.
func Patchset(c *scmpb.ChangeList, n int32) *scmpb.Patchset {
	for _, p := range c.GetPatchsets() {
		if p.GetNumber() == n {
			return p
		}
	}
	return nil
}

// Tree is a commit's (resolved) root tree; the root commit's is the empty tree (nil).
func (c *Client) Tree(ctx context.Context, repo int64, commit []byte) ([]byte, error) {
	if IsRoot(commit) {
		return nil, nil
	}
	resp, err := c.Browse.GetCommits(ctx, &scmpb.GetCommitsRequest{RepositoryId: repo, CommitIds: [][]byte{commit}})
	if err != nil {
		return nil, err
	}
	cm := resp.GetCommits()[0]
	if cm.GetConflicted() {
		return nil, fmt.Errorf("commit %s has conflicts", Short(commit))
	}
	return cm.GetRootTree()[0], nil
}

// ReadFile reads a file at a commit, up to limit bytes (0: all of it). It returns the
// file's value (with its full size) and whether the file was cut short.
func (c *Client) ReadFile(ctx context.Context, repo int64, commit []byte, path string, limit uint64) ([]byte, *scmpb.TreeValue, bool, error) {
	stream, err := c.Browse.ReadFileAt(ctx, &scmpb.ReadFileAtRequest{RepositoryId: repo, CommitId: commit, Path: path, Length: limit})
	if err != nil {
		return nil, nil, false, err
	}
	var data []byte
	var value *scmpb.TreeValue
	for {
		f, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, false, err
		}
		if f.GetValue() != nil {
			value = f.GetValue()
		}
		data = append(data, f.GetData()...)
	}
	truncated := value != nil && limit != 0 && value.GetSize() > uint64(len(data))
	return data, value, truncated, nil
}

// FileAt is a path read with ReadFilesAt: its value (nil when the commit has nothing there) and
// up to the limit of a file's first bytes.
type FileAt struct {
	Value *scmpb.TreeValue
	Data  []byte
}

// readFilesAtBytes is how much one ReadFilesAt call may read in all (the SCM's bound).
const readFilesAtBytes = 64 << 20

// ReadFilesAt reads many paths of a commit at once, up to limit bytes of each file, by path: as
// few calls as the SCM's per-call bound allows (64 MiB in all; every path at once for limit 0).
func (c *Client) ReadFilesAt(ctx context.Context, repo int64, commit []byte, paths []string, limit uint64) (map[string]FileAt, error) {
	per := len(paths)
	if limit > 0 {
		per = max(1, int(readFilesAtBytes/limit))
	}
	out := make(map[string]FileAt, len(paths))
	for start := 0; start < len(paths); start += per {
		stream, err := c.Browse.ReadFilesAt(ctx, &scmpb.ReadFilesAtRequest{RepositoryId: repo, CommitId: commit, Paths: paths[start:min(start+per, len(paths))], Limit: limit})
		if err != nil {
			return nil, err
		}
		for {
			f, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
			out[f.GetPath()] = FileAt{Value: f.GetValue(), Data: f.GetData()}
		}
	}
	return out, nil
}

// PathEscape escapes each segment of a slash-separated path for a URL.
func PathEscape(p string) string {
	parts := strings.Split(p, "/")
	for i, s := range parts {
		parts[i] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}
