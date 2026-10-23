package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// Each workspace has its own operation log (docs/scm.md, "Decisions", 3): these tests
// hold the store to it — no workspace reads or changes another's log, by any path, and no log
// is ever made but by CreateWorkspace.

// viewIn is a view of heads whose working copies are wc (workspace name -> commit).
func (b *builder) viewIn(wc map[string]encoding.ID, heads ...encoding.ID) encoding.ID {
	if wc == nil {
		wc = map[string]encoding.ID{}
	}
	return b.add(&encoding.View{HeadIDs: heads, WCCommitIDs: wc})
}

// operationIn is an operation run in workspace (none when empty).
func (b *builder) operationIn(workspace string, view encoding.ID, desc string, parents ...encoding.ID) encoding.ID {
	b.seq++
	var ws *string
	if workspace != "" {
		ws = &workspace
	}
	return b.add(&encoding.Operation{
		ViewID:  view,
		Parents: parents,
		Metadata: encoding.OperationMetadata{
			Start:         encoding.Timestamp{Millis: int64(b.seq)},
			End:           encoding.Timestamp{Millis: int64(b.seq)},
			Description:   desc,
			Hostname:      "test",
			Username:      "t",
			WorkspaceName: ws,
		},
		CommitPredecessors: map[encoding.ID][]encoding.ID{},
	})
}

// publishIn runs a whole Publish exchange for everything in b, into workspace's log.
func publishIn(ctx context.Context, s *Store, repo int64, principal, workspace string, b *builder, heads *OpHeadsUpdate) ([]encoding.ID, error) {
	p, err := s.BeginPublish(ctx, repo, principal, workspace, b.order, b.manifests())
	if err != nil {
		return nil, err
	}
	objs, chunks := p.Missing()
	for _, r := range objs {
		if err := p.AddObject(EncodedObject{Kind: r.Kind, ID: r.ID, Body: b.body(r)}); err != nil {
			p.Close()
			return nil, err
		}
	}
	for _, c := range chunks {
		if err := p.AddChunk(c, b.chunk(c)); err != nil {
			p.Close()
			return nil, err
		}
	}
	return p.Commit(ctx, heads)
}

// workOf publishes, in workspace, a commit and an operation whose view has it as the
// workspace's working copy, and returns them.
func workOf(t *testing.T, s *Store, repo int64, principal, workspace string) (commit, op encoding.ID) {
	t.Helper()
	b := newBuilder(t)
	commit = b.commit(b.tree(fileEntry("f", b.file([]byte(workspace)))), workspace+"\n", encoding.RootCommitID)
	op = b.operationIn(workspace, b.viewIn(map[string]encoding.ID{workspace: commit}, commit), "work in "+workspace, encoding.RootOperationID)
	if _, err := publishIn(context.Background(), s, repo, principal, workspace, b, &OpHeadsUpdate{Old: []encoding.ID{encoding.RootOperationID}, New: op}); err != nil {
		t.Fatalf("publishing in %s: %v", workspace, err)
	}
	return commit, op
}

func TestWorkspacesHaveTheirOwnOperationLogs(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	const alice = "alice.human.example.com"
	for _, w := range []string{"laptop", "desk"} {
		if _, err := s.CreateWorkspace(ctx, alice, repo, w); err != nil {
			t.Fatal(err)
		}
	}
	commit, op := workOf(t, s, repo, alice, "laptop")

	if h, err := s.GetOpHeads(ctx, repo, alice, "laptop"); err != nil || len(h) != 1 || h[0] != op {
		t.Fatalf("laptop's heads: %v (%v)", h, err)
	}
	// desk sees none of it, by any way of reading a log.
	if h, err := s.GetOpHeads(ctx, repo, alice, "desk"); err != nil || len(h) != 1 || h[0] != encoding.RootOperationID {
		t.Errorf("desk's heads: %v (%v), want only the root operation", h, err)
	}
	if res, err := s.ReadOperations(ctx, repo, alice, "desk", []ObjectRef{{encoding.KindOperation, op}}); err != nil || res[0].Found {
		t.Errorf("desk reads laptop's operation: %+v (%v)", res, err)
	}
	var streamed []EncodedObject
	if err := s.PrefetchOperations(ctx, repo, alice, "desk", []encoding.ID{op}, nil, 0, func(o EncodedObject) error {
		streamed = append(streamed, o)
		return nil
	}); err != nil || len(streamed) != 0 {
		t.Errorf("desk prefetches laptop's operations: %d (%v)", len(streamed), err)
	}
	if r, _, err := s.ResolveOperationPrefix(ctx, repo, alice, "desk", op[:6], false); err != nil || r != NoMatch {
		t.Errorf("desk resolves laptop's operation: %v (%v)", r, err)
	}
	var commits []encoding.ID
	if err := s.PrefetchCommits(ctx, repo, alice, "desk", nil, nil, true, 0, func(o EncodedObject) error {
		commits = append(commits, o.ID)
		return nil
	}); err != nil || len(commits) != 0 {
		t.Errorf("desk's operation log references laptop's commits: %v (%v)", commits, err)
	}
	// desk collecting everything it holds leaves laptop's log as it was.
	if err := s.CollectOperations(ctx, repo, alice, "desk", nil, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if res, err := s.ReadOperations(ctx, repo, alice, "laptop", []ObjectRef{{encoding.KindOperation, op}}); err != nil || !res[0].Found {
		t.Errorf("laptop's operation after desk collected: %+v (%v)", res, err)
	}
	// laptop's log does reference its own commit.
	commits = nil
	if err := s.PrefetchCommits(ctx, repo, alice, "laptop", nil, nil, true, 0, func(o EncodedObject) error {
		commits = append(commits, o.ID)
		return nil
	}); err != nil || len(commits) != 1 || commits[0] != commit {
		t.Errorf("laptop's operation log's commits: %v (%v)", commits, err)
	}

	// desk's own work is just as private from laptop.
	_, deskOp := workOf(t, s, repo, alice, "desk")
	if res, err := s.ReadOperations(ctx, repo, alice, "laptop", []ObjectRef{{encoding.KindOperation, deskOp}}); err != nil || res[0].Found {
		t.Errorf("laptop reads desk's operation: %+v (%v)", res, err)
	}
}

func TestCreateWorkspace(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	const alice, bob = "alice.human.example.com", "bob.human.example.com"

	w, err := s.CreateWorkspace(ctx, alice, repo, "laptop-ada")
	if err != nil || w.Name != "laptop-ada" || !w.OwnLog {
		t.Fatalf("CreateWorkspace: %+v (%v)", w, err)
	}
	if h, err := s.GetOpHeads(ctx, repo, alice, "laptop-ada"); err != nil || len(h) != 1 || h[0] != encoding.RootOperationID {
		t.Errorf("a new workspace's heads: %v (%v), want only the root operation", h, err)
	}
	// A name is the principal's once: a second clone can never come to share the log.
	if _, err := s.CreateWorkspace(ctx, alice, repo, "laptop-ada"); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("creating it again: %v, want ErrAlreadyExists", err)
	}
	// Nor a workspace okfs recorded before workspaces had their own logs.
	if _, err := poolOf(s).Exec(ctx, `INSERT INTO workspace (repository_id, principal, name) VALUES ($1, $2, 'old-vfs')`, repo, alice); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateWorkspace(ctx, alice, repo, "old-vfs"); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("creating a recorded workspace: %v, want ErrAlreadyExists", err)
	}
	// Another principal's workspace of the same name is its own.
	if _, err := s.CreateWorkspace(ctx, bob, repo, "laptop-ada"); err != nil {
		t.Errorf("bob's laptop-ada: %v", err)
	}
	for _, name := range []string{"", strings.Repeat("x", maxWorkspaceName+1)} {
		if _, err := s.CreateWorkspace(ctx, alice, repo, name); !errors.Is(err, ErrInvalid) {
			t.Errorf("CreateWorkspace(%q...): %v, want ErrInvalid", name[:min(len(name), 8)], err)
		}
	}
	if _, err := s.CreateWorkspace(ctx, alice, repo+1_000_000, "w"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a workspace of a missing repository: %v, want ErrNotFound", err)
	}

	ws, err := s.ListWorkspaces(ctx, alice, repo)
	if err != nil {
		t.Fatal(err)
	}
	own := map[string]bool{}
	for _, w := range ws {
		own[w.Name] = w.OwnLog
	}
	if !own["laptop-ada"] || !own[testWorkspace] || own["old-vfs"] {
		t.Errorf("which workspaces have their own log: %v", own)
	}
}

// TestNoWorkspaceNoLog: a request naming a workspace the principal does not have is NotFound,
// and makes nothing.
func TestNoWorkspaceNoLog(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	const alice, ghost = "alice.human.example.com", "ghost"
	logs := func() (n int) {
		if err := poolOf(s).QueryRow(ctx, `SELECT count(*) FROM op_log WHERE repository_id = $1`, repo).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := logs()
	nop := func(EncodedObject) error { return nil }
	for name, err := range map[string]error{
		"GetOpHeads": func() error { _, err := s.GetOpHeads(ctx, repo, alice, ghost); return err }(),
		"ReadOperations": func() error {
			_, err := s.ReadOperations(ctx, repo, alice, ghost, []ObjectRef{{encoding.KindOperation, encoding.ID{1}}})
			return err
		}(),
		"PrefetchOperations": s.PrefetchOperations(ctx, repo, alice, ghost, nil, nil, 0, nop),
		"ResolveOperationPrefix": func() error {
			_, _, err := s.ResolveOperationPrefix(ctx, repo, alice, ghost, []byte{1}, false)
			return err
		}(),
		"CollectOperations": s.CollectOperations(ctx, repo, alice, ghost, nil, time.Now()),
		"PrefetchCommits":   s.PrefetchCommits(ctx, repo, alice, ghost, nil, nil, true, 0, nop),
		"BeginPublish":      func() error { _, err := s.BeginPublish(ctx, repo, alice, ghost, nil, nil); return err }(),
	} {
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("%s in a workspace alice does not have: %v, want ErrNotFound", name, err)
		}
	}
	// Another principal's workspace is no more reachable than a missing one.
	if _, err := s.GetOpHeads(ctx, repo, "bob.human.example.com", "nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("bob in a workspace he does not have: %v", err)
	}
	if got := logs(); got != before {
		t.Errorf("%d operation logs after, %d before: a request made one", got, before)
	}
}

// TestPublishStaysInItsWorkspace: a workspace's log takes only its own working copy and its own
// operations; the legacy log, every workspace's, takes any.
func TestPublishStaysInItsWorkspace(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	const alice = "alice.human.example.com"
	for _, w := range []string{"laptop", "desk"} {
		if _, err := s.CreateWorkspace(ctx, alice, repo, w); err != nil {
			t.Fatal(err)
		}
	}
	root := encoding.RootOperationID
	try := func(workspace string, build func(b *builder) encoding.ID) error {
		b := newBuilder(t)
		op := build(b)
		_, err := publishIn(ctx, s, repo, alice, workspace, b, &OpHeadsUpdate{Old: []encoding.ID{root}, New: op})
		return err
	}
	commit := func(b *builder) encoding.ID {
		return b.commit(b.tree(fileEntry("f", b.file([]byte("x")))), "x\n", encoding.RootCommitID)
	}

	if err := try("laptop", func(b *builder) encoding.ID {
		c := commit(b)
		return b.operationIn("laptop", b.viewIn(map[string]encoding.ID{"desk": c}, c), "names desk", root)
	}); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), `"desk"`) {
		t.Errorf("a view naming desk's working copy, in laptop: %v, want ErrInvalid", err)
	}
	if err := try("laptop", func(b *builder) encoding.ID {
		c := commit(b)
		return b.operationIn("laptop", b.viewIn(map[string]encoding.ID{"laptop": c, "desk": c}, c), "names both", root)
	}); !errors.Is(err, ErrInvalid) {
		t.Errorf("a view naming both working copies, in laptop: %v, want ErrInvalid", err)
	}
	if err := try("laptop", func(b *builder) encoding.ID {
		c := commit(b)
		return b.operationIn("desk", b.viewIn(map[string]encoding.ID{"laptop": c}, c), "ran in desk", root)
	}); !errors.Is(err, ErrInvalid) {
		t.Errorf("an operation run in desk, in laptop: %v, want ErrInvalid", err)
	}
	if h, _ := s.GetOpHeads(ctx, repo, alice, "laptop"); len(h) != 1 || h[0] != root {
		t.Errorf("laptop's heads after refusals: %v", h)
	}
	// Its own working copy, an operation run nowhere in particular (a clone's first), and a
	// view with no working copy (the workspace forgotten) are its own.
	for desc, build := range map[string]func(b *builder) encoding.ID{
		"its own": func(b *builder) encoding.ID {
			c := commit(b)
			return b.operationIn("laptop", b.viewIn(map[string]encoding.ID{"laptop": c}, c), "own", root)
		},
		"no workspace": func(b *builder) encoding.ID {
			return b.operationIn("", b.viewIn(nil), "unnamed", root)
		},
	} {
		if err := try("laptop", build); err != nil {
			t.Errorf("%s: %v", desc, err)
		}
	}
}

// TestNoWorkspaceNamed: a request naming no workspace — a clone from before each workspace had
// its own operation log — is refused by every way of reaching a log, and makes none. Objects
// are the repository's, so a publish of objects alone needs no workspace.
func TestNoWorkspaceNamed(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	const carol = "carol.example"
	nop := func(EncodedObject) error { return nil }
	for name, err := range map[string]error{
		"GetOpHeads": func() error { _, err := s.GetOpHeads(ctx, repo, carol, ""); return err }(),
		"ReadOperations": func() error {
			_, err := s.ReadOperations(ctx, repo, carol, "", []ObjectRef{{encoding.KindOperation, encoding.ID{1}}})
			return err
		}(),
		"PrefetchOperations": s.PrefetchOperations(ctx, repo, carol, "", nil, nil, 0, nop),
		"ResolveOperationPrefix": func() error {
			_, _, err := s.ResolveOperationPrefix(ctx, repo, carol, "", []byte{1}, false)
			return err
		}(),
		"CollectOperations": s.CollectOperations(ctx, repo, carol, "", nil, time.Now()),
		"PrefetchCommits":   s.PrefetchCommits(ctx, repo, carol, "", nil, nil, true, 0, nop),
		"BeginPublish": func() error {
			_, err := s.BeginPublish(ctx, repo, carol, "", []ObjectRef{{encoding.KindOperation, encoding.ID{1}}}, nil)
			return err
		}(),
	} {
		if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "jj clone makes a new workspace") {
			t.Errorf("%s naming no workspace: %v, want ErrInvalid", name, err)
		}
	}
	b := newBuilder(t)
	c := b.commit(b.tree(fileEntry("f", b.file([]byte("objects alone")))), "objects\n", encoding.RootCommitID)
	if heads, err := publishIn(ctx, s, repo, carol, "", b, nil); err != nil || len(heads) != 0 {
		t.Errorf("publishing objects alone, naming no workspace: %v (%v)", heads, err)
	}
	if res, err := s.ReadObjects(ctx, repo, []ObjectRef{{encoding.KindCommit, c}}); err != nil || !res[0].Found {
		t.Errorf("the published commit: %+v (%v)", res, err)
	}
	if _, err := publishIn(ctx, s, repo, carol, "", newBuilder(t), &OpHeadsUpdate{New: encoding.ID{7}}); !errors.Is(err, ErrInvalid) {
		t.Errorf("moving heads, naming no workspace: %v, want ErrInvalid", err)
	}
	var logs int
	if err := poolOf(s).QueryRow(ctx, `SELECT count(*) FROM op_log WHERE repository_id = $1 AND principal = $2`, repo, carol).Scan(&logs); err != nil || logs != 0 {
		t.Errorf("carol's operation logs: %d (%v), want none", logs, err)
	}
}

// TestGCDeletesLogsOfNoWorkspace: what was left from before each workspace had its own operation
// log — the shared logs, and the workspaces okfs recorded with none — is deleted whole by the
// next pass (a dry run only counts it), and every workspace jj clone made is left as it was.
func TestGCDeletesLogsOfNoWorkspace(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	repo := newRepo(t, s).ID
	const alice = "alice.human.example.com"
	if _, err := s.CreateWorkspace(ctx, alice, repo, "laptop"); err != nil {
		t.Fatal(err)
	}
	_, op := workOf(t, s, repo, alice, "laptop")
	var shared int64
	if err := poolOf(s).QueryRow(ctx, `INSERT INTO op_log (repository_id, principal, workspace) VALUES ($1, $2, '') RETURNING id`, repo, alice).Scan(&shared); err != nil {
		t.Fatal(err)
	}
	if _, err := poolOf(s).Exec(ctx, `INSERT INTO op_head (op_log_id, op_id) VALUES ($1, $2)`, shared, encoding.RootOperationID[:]); err != nil {
		t.Fatal(err)
	}
	// And a workspace okfs recorded then, with no log of its own, and its checkpoint.
	if _, err := poolOf(s).Exec(ctx, `INSERT INTO workspace (repository_id, principal, name) VALUES ($1, $2, 'old-vfs')`, repo, alice); err != nil {
		t.Fatal(err)
	}
	if _, err := poolOf(s).Exec(ctx, `INSERT INTO checkpoint (repository_id, principal, workspace, machine, tree_ids, base_commit_id, fork)
		VALUES ($1, $2, 'old-vfs', 'm1', ARRAY[$3::bytea], $4, false)`, repo, alice, encoding.EmptyTreeID[:], encoding.RootCommitID[:]); err != nil {
		t.Fatal(err)
	}
	logs := func() (n int) {
		if err := poolOf(s).QueryRow(ctx, `SELECT count(*) FROM op_log WHERE repository_id = $1 AND workspace = ''`, repo).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	workspaces := func() (n int) {
		if err := poolOf(s).QueryRow(ctx, `SELECT count(*) FROM workspace WHERE repository_id = $1 AND principal = $2 AND name = 'old-vfs'`, repo, alice).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	dry, err := s.GC(ctx, repo, time.Now(), GCOptions{DryRun: true})
	if err != nil || dry.OpLogsDeleted != 1 || dry.WorkspacesDeleted != 1 || logs() != 1 || workspaces() != 1 {
		t.Fatalf("a dry run: %d logs and %d workspaces counted, %d and %d left (%v)", dry.OpLogsDeleted, dry.WorkspacesDeleted, logs(), workspaces(), err)
	}
	r, err := s.GC(ctx, repo, time.Now(), GCOptions{})
	if err != nil || r.OpLogsDeleted != 1 || r.WorkspacesDeleted != 1 || logs() != 0 || workspaces() != 0 {
		t.Fatalf("a pass: %d logs and %d workspaces deleted, %d and %d left (%v)", r.OpLogsDeleted, r.WorkspacesDeleted, logs(), workspaces(), err)
	}
	if !strings.Contains(r.String(), "deleted 1 operation logs of no workspace and 1 workspaces with no log") {
		t.Errorf("the report: %s", r)
	}
	var checkpoints int
	if err := poolOf(s).QueryRow(ctx, `SELECT count(*) FROM checkpoint WHERE repository_id = $1 AND workspace = 'old-vfs'`, repo).Scan(&checkpoints); err != nil || checkpoints != 0 {
		t.Errorf("the deleted workspace's checkpoints: %d (%v)", checkpoints, err)
	}
	// Every workspace jj clone made is kept, with its log.
	ws, err := s.ListWorkspaces(ctx, alice, repo)
	if err != nil || len(ws) != 2 {
		t.Errorf("alice's workspaces after the pass: %+v (%v)", ws, err)
	}
	var heads int
	if err := poolOf(s).QueryRow(ctx, `SELECT count(*) FROM op_head WHERE op_log_id = $1`, shared).Scan(&heads); err != nil || heads != 0 {
		t.Errorf("the deleted log's heads: %d (%v)", heads, err)
	}
	if h, err := s.GetOpHeads(ctx, repo, alice, "laptop"); err != nil || len(h) != 1 || h[0] != op {
		t.Errorf("laptop's heads after the pass: %v (%v)", h, err)
	}
	if err := s.RecordGC(ctx, r, nil); err != nil {
		t.Fatal(err)
	}
	var recorded int64
	var recordedWorkspaces int64
	if err := poolOf(s).QueryRow(ctx, `SELECT op_logs_deleted, workspaces_deleted FROM gc_pass WHERE repository_id = $1 ORDER BY started_at DESC LIMIT 1`, repo).Scan(&recorded, &recordedWorkspaces); err != nil || recorded != 1 || recordedWorkspaces != 1 {
		t.Errorf("the recorded pass: %d logs, %d workspaces (%v)", recorded, recordedWorkspaces, err)
	}
}
