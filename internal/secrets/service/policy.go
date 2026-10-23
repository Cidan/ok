// Package service is ok secrets' services (docs/secrets.md, "The services"): people's Secrets,
// the SCM's Versions, the deployer's DeploySecrets, and workers' RunSecrets, each deciding who
// may from main — the folder's acl.yaml at main's head — and the directory's memberships, and
// auditing every decision.
package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/Cidan/ok/internal/identity"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/workflow"
	identitypb "github.com/Cidan/ok/proto/ok/identity/v1"
	scmpb "github.com/Cidan/ok/proto/ok/scm/v1"
)

// Main reads main: its head, and files of a commit. The SCM's, through Repositories and Browse.
type Main interface {
	Head(ctx context.Context, repo int64) ([]byte, error)
	// Files reads paths of a commit, up to limit bytes each: a path the commit has nothing at is
	// absent, and one longer than limit is an error.
	Files(ctx context.Context, repo int64, commit []byte, paths []string, limit int) (map[string][]byte, error)
	IsAncestor(ctx context.Context, repo int64, ancestor, descendant []byte) (bool, error)
}

// Members says whether a principal, by stable ID, is in a set: the directory's.
type Members interface {
	Member(ctx context.Context, principalID, set string) (bool, error)
}

// SCM reads main through the SCM's services.
type SCM struct {
	Repositories scmpb.RepositoriesClient
	Browse       scmpb.BrowseClient
}

func (s SCM) Head(ctx context.Context, repo int64) ([]byte, error) {
	r, err := s.Repositories.GetRepository(ctx, &scmpb.GetRepositoryRequest{Key: &scmpb.GetRepositoryRequest_Id{Id: repo}})
	if err != nil {
		return nil, err
	}
	return r.GetHead(), nil
}

// maxReadFilesAt is the most of a file the SCM's ReadFilesAt reads.
const maxReadFilesAt = 1 << 20

func (s SCM) Files(ctx context.Context, repo int64, commit []byte, paths []string, limit int) (map[string][]byte, error) {
	// A byte past the limit tells a larger file — but for one at the SCM's own most, which upload
	// keeps every configuration file within (maxConfigFile).
	stream, err := s.Browse.ReadFilesAt(ctx, &scmpb.ReadFilesAtRequest{RepositoryId: repo, CommitId: commit, Paths: paths,
		Limit: min(uint64(limit)+1, maxReadFilesAt)})
	if err != nil {
		return nil, err
	}
	out := map[string][]byte{}
	for {
		f, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if f.GetValue() == nil {
			continue
		}
		if len(f.GetData()) > limit {
			return nil, fmt.Errorf("%s is larger than %d bytes", f.GetPath(), limit)
		}
		out[f.GetPath()] = f.GetData()
	}
}

func (s SCM) IsAncestor(ctx context.Context, repo int64, ancestor, descendant []byte) (bool, error) {
	r, err := s.Browse.IsAncestor(ctx, &scmpb.IsAncestorRequest{RepositoryId: repo, Ancestor: ancestor, Descendant: descendant})
	return r.GetIsAncestor(), err
}

// Directory says who is in a set through ok identity's Directory.
type Directory struct{ Client identitypb.DirectoryClient }

func (d Directory) Member(ctx context.Context, principalID, set string) (bool, error) {
	r, err := d.Client.CheckMembership(ctx, &identitypb.CheckMembershipRequest{Set: set,
		Key: &identitypb.CheckMembershipRequest_PrincipalId{PrincipalId: principalID}})
	return r.GetMember(), err
}

// How long the policy keeps what it read: main's head briefly, so a change landing takes effect
// within seconds; what a commit holds for as long as it likes, as nothing changes it; and a
// membership as long as the design says a removal may take.
const (
	headTTL   = 2 * time.Second
	memberTTL = 10 * time.Second
	keptFiles = 4096
)

// Policy decides from main who may use a folder's secrets.
type Policy struct {
	main    Main
	members Members
	names   identity.Names

	headTTL, memberTTL time.Duration

	mu          sync.Mutex
	heads       map[int64]cachedHead
	files       map[fileKey]cachedFile
	memberships map[memberKey]cachedMember
}

type cachedHead struct {
	head  []byte
	until time.Time
}

type fileKey struct {
	repo   int64
	commit string
	path   string
}

type cachedFile struct {
	data    []byte
	present bool
}

type memberKey struct{ principal, set string }

type cachedMember struct {
	member bool
	until  time.Time
}

// NewPolicy decides from main, read through m, and memberships, asked of members.
func NewPolicy(m Main, members Members, names identity.Names) *Policy {
	return &Policy{main: m, members: members, names: names, headTTL: headTTL, memberTTL: memberTTL, heads: map[int64]cachedHead{},
		files: map[fileKey]cachedFile{}, memberships: map[memberKey]cachedMember{}}
}

// Folder is what main says of a folder.
type Folder struct {
	Path string // secret, secret/prod
	// Head is main's head it was read at.
	Head []byte
	// Settings are the repository's settings on main; Settings.Secrets nil for a repository that
	// keeps none.
	Settings *workflow.Settings
	// Present: main has the folder's acl.yaml. A folder main lacks is in review: anyone writes
	// there, as what is written is inert until a reviewed change lands it, and no one reads.
	Present bool
	Access  secrets.Access
}

// KeepsSecrets reports whether the repository keeps secrets: its settings on main say so.
func (f Folder) KeepsSecrets() bool { return f.Settings != nil && f.Settings.Secrets != nil }

// Store is the store the repository keeps its sealed values in.
func (f Folder) Store() string {
	if !f.KeepsSecrets() {
		return ""
	}
	return f.Settings.Secrets.Store
}

// Folder reads a folder's acl.yaml, and the repository's settings, at main's head.
func (p *Policy) Folder(ctx context.Context, repo int64, folder string) (Folder, error) {
	if !secrets.InTree(folder) {
		return Folder{}, fmt.Errorf("%q is not a folder of //%s", folder, secrets.Dir)
	}
	head, err := p.head(ctx, repo)
	if err != nil {
		return Folder{}, err
	}
	return p.FolderAt(ctx, repo, head, folder)
}

// FolderAt reads a folder's acl.yaml, and the repository's settings, at a commit.
func (p *Policy) FolderAt(ctx context.Context, repo int64, commit []byte, folder string) (Folder, error) {
	acl := path.Join(folder, secrets.ACLFile)
	files, err := p.read(ctx, repo, commit, []string{workflow.SettingsFile, acl})
	if err != nil {
		return Folder{}, err
	}
	f := Folder{Path: folder, Head: commit}
	settings, ok := files[workflow.SettingsFile]
	if f.Settings, err = workflow.ParseSettings(settings.data, ok && settings.present); err != nil {
		return Folder{}, fmt.Errorf("the repository's settings on main: %w", err)
	}
	if a := files[acl]; a.present {
		f.Present = true
		if f.Access, err = secrets.ParseACL(p.names, a.data); err != nil {
			// Upload refuses such a file, so main has none; one that is there grants nothing.
			f.Access = secrets.Access{}
		}
	}
	return f, nil
}

// Reader reports whether a principal is a member of one of a folder's readers.
func (p *Policy) Reader(ctx context.Context, f Folder, principal string) (bool, error) {
	return p.anyMember(ctx, principal, f.Access.Readers)
}

// Writer reports whether a principal may write a folder's secrets: a member of one of its
// writers, or anyone, while main lacks the folder.
func (p *Policy) Writer(ctx context.Context, f Folder, principal string) (bool, error) {
	if !f.Present {
		return true, nil
	}
	return p.anyMember(ctx, principal, f.Access.Writers)
}

func (p *Policy) anyMember(ctx context.Context, principal string, sets []string) (bool, error) {
	for _, s := range sets {
		ok, err := p.member(ctx, principal, s)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

func (p *Policy) member(ctx context.Context, principal, set string) (bool, error) {
	k := memberKey{principal, set}
	p.mu.Lock()
	c, ok := p.memberships[k]
	p.mu.Unlock()
	if ok && time.Now().Before(c.until) {
		return c.member, nil
	}
	member, err := p.members.Member(ctx, principal, set)
	if err != nil {
		return false, err
	}
	p.mu.Lock()
	if len(p.memberships) > keptFiles {
		clear(p.memberships)
	}
	p.memberships[k] = cachedMember{member: member, until: time.Now().Add(p.memberTTL)}
	p.mu.Unlock()
	return member, nil
}

func (p *Policy) head(ctx context.Context, repo int64) ([]byte, error) {
	p.mu.Lock()
	c, ok := p.heads[repo]
	p.mu.Unlock()
	if ok && time.Now().Before(c.until) {
		return c.head, nil
	}
	head, err := p.main.Head(ctx, repo)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if len(p.heads) > keptFiles {
		clear(p.heads)
	}
	p.heads[repo] = cachedHead{head: head, until: time.Now().Add(p.headTTL)}
	p.mu.Unlock()
	return head, nil
}

// read reads paths of a commit, each kept once read: a commit's files never change.
func (p *Policy) read(ctx context.Context, repo int64, commit []byte, paths []string) (map[string]cachedFile, error) {
	out := map[string]cachedFile{}
	var missing []string
	p.mu.Lock()
	for _, f := range paths {
		if c, ok := p.files[fileKey{repo, string(commit), f}]; ok {
			out[f] = c
		} else {
			missing = append(missing, f)
		}
	}
	p.mu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}
	got, err := p.main.Files(ctx, repo, commit, missing, secrets.MaxACLFile)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.files) > keptFiles {
		clear(p.files)
	}
	for _, f := range missing {
		data, ok := got[f]
		c := cachedFile{data: slices.Clone(data), present: ok}
		p.files[fileKey{repo, string(commit), f}] = c
		out[f] = c
	}
	return out, nil
}
