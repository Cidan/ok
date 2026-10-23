package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
	"github.com/Cidan/ok/internal/secrets"
	"github.com/Cidan/ok/internal/workflow"
)

// Repositories' secrets, as the SCM enforces them (docs/secrets.md, "The SCM"): whether a
// repository keeps them — its settings on main say so — upload's rules for a commit's //secret,
// publish's and checkpoints' refusal of anything under it but pointers, and the pointers a
// repository still holds, which ok secrets' collection keeps. The SCM keeps no value and decides
// no read: who may write a folder, and which versions exist, are ok secrets' (Secrets).

// Secrets is what upload asks ok secrets.
type Secrets interface {
	// Kept says of each pointer whether ok secrets keeps its version at its path.
	Kept(ctx context.Context, repo int64, pointers []SecretPointer) ([]bool, error)
	// Writers says of each folder whether principal (a stable ID) may write its secrets, as main
	// says: a folder main lacks is anyone's.
	Writers(ctx context.Context, repo int64, principal string, folders []string) ([]bool, error)
}

// SecretPointer is a pointer a tree holds: a secret's path, and its digest.
type SecretPointer struct {
	Path   string
	Digest secrets.Digest
}

// SetSecrets gives the store ok secrets, which upload asks; without it, a commit that changes a
// repository's secrets is refused.
func (s *Store) SetSecrets(sec Secrets) { s.secrets = sec }

// mainKeepsSecrets is whether a repository's main keeps secrets, at one head.
type mainKeepsSecrets struct {
	head  encoding.ID
	keeps bool
}

// KeepsSecrets reports whether a repository keeps secrets: its settings on main's head have a
// secrets stanza, or do not parse (which pre and upload refuse, so main has none unless a change
// predates them). Elsewhere //secret is an ordinary folder. It is kept per repository until main
// moves.
func (s *Store) KeepsSecrets(ctx context.Context, repo int64) (bool, error) {
	r, err := s.GetRepository(ctx, repo)
	if err != nil {
		return false, err
	}
	if v, ok := s.mainSecrets.Load(repo); ok && v.(mainKeepsSecrets).head == r.Head {
		return v.(mainKeepsSecrets).keeps, nil
	}
	keeps := false
	if r.Head != encoding.RootCommitID {
		c, err := s.readCommit(ctx, repo, r.Head)
		if err != nil {
			return false, err
		}
		if keeps, err = s.treeKeepsSecrets(ctx, repo, s.pendingTrees(ctx, repo), c.RootTree[0]); err != nil {
			return false, err
		}
	}
	s.mainSecrets.Store(repo, mainKeepsSecrets{head: r.Head, keeps: keeps})
	return keeps, nil
}

// treeKeepsSecrets reports whether a tree's settings have a secrets stanza; settings that do not
// parse count as having one.
func (s *Store) treeKeepsSecrets(ctx context.Context, repo int64, ts treeStore, root encoding.ID) (bool, error) {
	v, err := lookupPath(ts, root, workflow.SettingsFile)
	if err != nil || v == nil || v.Kind != encoding.TreeValueFile {
		return false, err
	}
	data, err := s.readSmallFile(ctx, repo, v.ID, maxConfigFile)
	if err != nil {
		return true, nil
	}
	st, err := workflow.ParseSettings(data, true)
	return err != nil || st.Secrets != nil, nil
}

// loadSecretTree reads and validates a tree's //secret. present is false where it has none; err
// is a *secrets.Error for one that does not validate, or a failure to read.
func (s *Store) loadSecretTree(ctx context.Context, repo int64, ts treeStore, root encoding.ID) (*secrets.Tree, bool, error) {
	v, err := lookupPath(ts, root, secrets.Dir)
	if err != nil || v == nil {
		return nil, false, err
	}
	entries, err := s.secretEntries(ctx, repo, ts, *v)
	if err != nil {
		return nil, true, err
	}
	t, err := secrets.Load(s.names, entries)
	return t, true, err
}

// secretEntries lists //secret's leaves, given its tree value, with the contents of what the
// rules read: every acl.yaml up to secrets.MaxACLFile, and every file a pointer's length (a file
// of any other length is no pointer, and is not read).
func (s *Store) secretEntries(ctx context.Context, repo int64, ts treeStore, v encoding.TreeValue) ([]secrets.Entry, error) {
	if v.Kind != encoding.TreeValueTree {
		return []secrets.Entry{{Path: secrets.Dir, Kind: entryKind(v)}}, nil
	}
	var out []secrets.Entry
	err := diffTrees(ts, encoding.EmptyTreeID, v.ID, secrets.Dir, func(ch leafChange) error {
		e := secrets.Entry{Path: ch.Path, Kind: entryKind(*ch.After)}
		// An OWNERS file is read by OWNERS validation, not the tree's rules.
		if ch.After.Kind == encoding.TreeValueFile && !secrets.IsOwners(ch.Path) {
			data, err := s.secretContents(ctx, repo, ch.After.ID, secrets.IsACL(ch.Path))
			if err != nil {
				return err
			}
			e.Contents = data
		}
		out = append(out, e)
		return nil
	})
	return out, err
}

// secretContents reads a //secret file the rules need: an acl.yaml (up to MaxACLFile; one larger
// reads as that many bytes and more, which the rules refuse), or a file a pointer's length.
func (s *Store) secretContents(ctx context.Context, repo int64, id encoding.ID, acl bool) ([]byte, error) {
	size, _, found, err := s.fileChunks(ctx, repo, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, notFound("file %s", id)
	}
	switch {
	case acl && size > secrets.MaxACLFile:
		return make([]byte, secrets.MaxACLFile+1), nil
	case !acl && size != uint64(secrets.PointerLen):
		return nil, nil
	}
	return s.readFile(ctx, repo, id)
}

func entryKind(v encoding.TreeValue) secrets.EntryKind {
	switch {
	case v.Kind == encoding.TreeValueSymlink:
		return secrets.Symlink
	case v.Executable:
		return secrets.Executable
	}
	return secrets.File
}

// validateSecretEdits checks a commit's //secret when the change from parentTree to tree touches
// it or the repository's settings, in a repository that keeps secrets or a commit that makes it
// keep them (docs/secrets.md, "The SCM"): the tree's rules, a settings stanza, every set an
// acl.yaml it changes names known to ok identity, every pointer it changes a version ok secrets
// keeps at its path, and every secret it adds, changes, or deletes in a folder main has changed by
// one of that folder's writers on main — author, who uploads it.
func (s *Store) validateSecretEdits(ctx context.Context, repo int64, ts treeStore, parentTree, tree encoding.ID, author string, known *knownHumans) ([]string, error) {
	from := parentTree
	if from == (encoding.ID{}) {
		from = encoding.EmptyTreeID
	}
	var before, after, settingsBefore, settingsAfter *encoding.TreeValue
	for _, l := range []struct {
		v    **encoding.TreeValue
		root encoding.ID
		path string
	}{{&before, from, secrets.Dir}, {&after, tree, secrets.Dir}, {&settingsBefore, from, workflow.SettingsFile}, {&settingsAfter, tree, workflow.SettingsFile}} {
		v, err := lookupPath(ts, l.root, l.path)
		if err != nil {
			return nil, err
		}
		*l.v = v
	}
	if sameValue(before, after) && sameValue(settingsBefore, settingsAfter) {
		return nil, nil
	}
	beforeID, afterID := encoding.EmptyTreeID, encoding.EmptyTreeID
	if before != nil && before.Kind == encoding.TreeValueTree {
		beforeID = before.ID
	}
	if after != nil && after.Kind == encoding.TreeValueTree {
		afterID = after.ID
	}
	keeps, err := s.treeKeepsSecrets(ctx, repo, ts, tree)
	if err != nil {
		return nil, err
	}
	mainKeeps, err := s.KeepsSecrets(ctx, repo)
	if err != nil {
		return nil, err
	}
	if !keeps && !mainKeeps {
		return nil, nil // neither main nor the commit keeps secrets: //secret is an ordinary folder
	}
	var problems []string
	var t *secrets.Tree
	if after != nil {
		if !keeps {
			problems = append(problems, fmt.Sprintf("//%s holds secrets, and %s keeps none (secrets: {}; docs/secrets.md)", secrets.Dir, workflow.SettingsFile))
		}
		if t, _, err = s.loadSecretTree(ctx, repo, ts, tree); err != nil {
			if e, ok := secrets.AsError(err); ok {
				return append(problems, e.Problems...), nil
			}
			return nil, err
		}
	}
	var changedACLs, touched []string
	var changed []SecretPointer
	if err := diffTrees(ts, beforeID, afterID, secrets.Dir, func(ch leafChange) error {
		switch {
		case secrets.IsACL(ch.Path):
			if ch.After != nil {
				changedACLs = append(changedACLs, ch.Path)
			}
		case secrets.IsOwners(ch.Path):
		default:
			touched = append(touched, ch.Path)
			if ch.After != nil {
				changed = append(changed, SecretPointer{Path: ch.Path, Digest: t.Secrets[ch.Path]})
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	for _, p := range changedACLs {
		a := t.Folders[secrets.Folder(p)]
		for _, set := range slices.Concat(a.Readers, a.Writers) {
			if !known.knownSet(ctx, set) {
				if known.err != nil {
					return nil, fmt.Errorf("check //%s's sets: %w", secrets.Dir, known.err)
				}
				problems = append(problems, fmt.Sprintf("%s: %s is no ACL or team ok identity knows (declare it in the groups-and-ACLs tree first)", p, set))
			}
		}
	}
	if len(touched) == 0 && len(changed) == 0 {
		slices.Sort(problems)
		return problems, nil
	}
	if s.secrets == nil {
		return append(problems, "this installation runs no ok secrets (services.secrets), which keeps secrets' values: no change to a secret can be uploaded"), nil
	}
	if len(touched) > 0 {
		var folders []string
		for _, p := range touched {
			if f := secrets.Folder(p); !slices.Contains(folders, f) {
				folders = append(folders, f)
			}
		}
		writers, err := s.secrets.Writers(ctx, repo, author, folders)
		if err != nil {
			return nil, fmt.Errorf("asking ok secrets who writes //%s: %w", secrets.Dir, err)
		}
		for _, p := range touched {
			if !writers[slices.Index(folders, secrets.Folder(p))] {
				problems = append(problems, fmt.Sprintf("%s: only a writer of %s changes its secrets, and %s is none (main's %s/%s)",
					p, secrets.Folder(p), s.handleOf(ctx, author), secrets.Folder(p), secrets.ACLFile))
			}
		}
	}
	if len(changed) > 0 {
		kept, err := s.secrets.Kept(ctx, repo, changed)
		if err != nil {
			return nil, fmt.Errorf("asking ok secrets for //%s's versions: %w", secrets.Dir, err)
		}
		for i, c := range changed {
			if !kept[i] {
				problems = append(problems, fmt.Sprintf("%s: names a version ok secrets does not keep at this path — never written there, or destroyed: write the secret in a virtual workspace, which keeps it in ok secrets (a pointer copied from elsewhere names nothing here)", c.Path))
			}
		}
	}
	slices.Sort(problems)
	return problems, nil
}

// knownSet reports whether a set's full name is an ACL or a team ok identity knows, asking once.
func (k *knownHumans) knownSet(ctx context.Context, name string) bool {
	if ok, done := k.sets[name]; done {
		return ok
	}
	if k.sets == nil {
		k.sets = map[string]bool{}
	}
	ok, err := k.people.KnownACL(ctx, name)
	if err != nil {
		if k.err == nil {
			k.err = err
		}
		return false
	}
	k.sets[name] = ok
	return ok
}

// RequireGuard refuses a publish from a client that does not promise to guard secrets, in a
// repository that keeps them (docs/secrets.md, "The SCM").
func (s *Store) RequireGuard(ctx context.Context, repo int64) error {
	keeps, err := s.KeepsSecrets(ctx, repo)
	if err != nil || !keeps {
		return err
	}
	return precondition("this repository keeps secrets in //%s, and this jj does not guard them — it could store a secret's value: install ok's jj from this release, then run the command again (docs/secrets.md)", secrets.Dir)
}

// fileReader reads a file's size and, for a size worth reading, its contents.
type fileReader func(id encoding.ID) (size uint64, read func() ([]byte, error), err error)

// storedFiles reads the repository's stored files.
func (s *Store) storedFiles(ctx context.Context, repo int64) fileReader {
	return func(id encoding.ID) (uint64, func() ([]byte, error), error) {
		size, _, found, err := s.fileChunks(ctx, repo, id)
		if err != nil {
			return 0, nil, err
		}
		if !found {
			return 0, nil, notFound("file %s", id)
		}
		return size, func() ([]byte, error) { return s.readFile(ctx, repo, id) }, nil
	}
}

// checkedSecretTrees remembers //secret trees found to hold only pointers, acl.yaml, and OWNERS
// files: a tree's contents never change, so neither does the answer.
type checkedSecretTrees struct {
	mu  sync.Mutex
	ids map[encoding.ID]bool
}

const maxCheckedSecretTrees = 100_000

func (c *checkedSecretTrees) has(id encoding.ID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ids[id]
}

func (c *checkedSecretTrees) add(id encoding.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ids == nil || len(c.ids) >= maxCheckedSecretTrees {
		c.ids = map[encoding.ID]bool{}
	}
	c.ids[id] = true
}

// checkSecretValues refuses roots whose //secret holds a file other than an acl.yaml or an OWNERS
// file that is not a pointer — a secret's value, which must never be stored — in a repository that
// keeps secrets, or in a root whose own settings have a secrets stanza (the change that makes the
// repository keep them). It checks only that much: a commit in progress may break the tree's
// other rules, which upload enforces.
func (s *Store) checkSecretValues(ctx context.Context, repo int64, ts treeStore, read fileReader, roots []encoding.ID) error {
	var problems []string
	mainKeeps := -1
	for _, root := range roots {
		v, err := lookupPath(ts, root, secrets.Dir)
		if err != nil {
			return err
		}
		if v == nil || v.Kind != encoding.TreeValueTree || s.checkedSecrets.has(v.ID) {
			continue
		}
		if mainKeeps < 0 {
			keeps, err := s.KeepsSecrets(ctx, repo)
			if err != nil {
				return err
			}
			mainKeeps = 0
			if keeps {
				mainKeeps = 1
			}
		}
		if mainKeeps == 0 {
			keeps, err := settingsKeepSecrets(ts, read, root)
			if err != nil {
				return err
			}
			if !keeps {
				continue
			}
		}
		found := len(problems)
		if err := diffTrees(ts, encoding.EmptyTreeID, v.ID, secrets.Dir, func(ch leafChange) error {
			if ch.After.Kind != encoding.TreeValueFile || !secrets.IsSecret(ch.Path) {
				return nil
			}
			size, contents, err := read(ch.After.ID)
			if err != nil {
				return err
			}
			if size == uint64(secrets.PointerLen) {
				data, err := contents()
				if err != nil {
					return err
				}
				if secrets.IsPointer(data) {
					return nil
				}
			}
			problems = append(problems, ch.Path)
			return nil
		}); err != nil {
			return err
		}
		if len(problems) == found {
			s.checkedSecrets.add(v.ID)
		}
	}
	if len(problems) == 0 {
		return nil
	}
	slices.Sort(problems)
	slog.Warn("scm: refused a secret's value", "repository", repo, "paths", problems)
	return precondition("%s: holds a value, not a pointer: ok never stores a secret's value (write secrets in a virtual workspace, which keeps them in ok secrets; docs/secrets.md)", strings.Join(problems, ", "))
}

// settingsKeepSecrets reports whether a root's settings have a secrets stanza, reading them through
// read. Settings that do not parse, or too large to read, count as having one.
func settingsKeepSecrets(ts treeStore, read fileReader, root encoding.ID) (bool, error) {
	v, err := lookupPath(ts, root, workflow.SettingsFile)
	if err != nil || v == nil || v.Kind != encoding.TreeValueFile {
		return false, err
	}
	size, contents, err := read(v.ID)
	if err != nil {
		return false, err
	}
	if size > maxConfigFile {
		return true, nil
	}
	data, err := contents()
	if err != nil {
		return false, err
	}
	st, err := workflow.ParseSettings(data, true)
	return err != nil || st.Secrets != nil, nil
}

// ─── References ───

// SecretReferences are the pointers a repository still holds under //secret: main's head's, every
// open or draft CL's current patchset's, and every workspace's latest checkpoint's — what ok
// secrets' collection keeps (docs/secrets.md, "The SCM"). A //secret subtree's pointers are read
// once: its contents never change. ErrNotFound for a repository the SCM holds no more.
func (s *Store) SecretReferences(ctx context.Context, repo int64) ([]SecretPointer, error) {
	r, err := s.GetRepository(ctx, repo)
	if err != nil {
		return nil, err
	}
	var commits []encoding.ID
	if r.Head != encoding.RootCommitID {
		commits = append(commits, r.Head)
	}
	for before := int64(0); ; {
		cls, next, err := s.ListChangeLists(ctx, repo, []Status{StatusDraft, StatusOpen}, "", 0, 1000, before)
		if err != nil {
			return nil, err
		}
		for i := range cls {
			commits = append(commits, cls[i].Current().Commit)
		}
		if next == 0 {
			break
		}
		before = next
	}
	var roots []encoding.ID
	for _, id := range commits {
		c, err := s.readCommit(ctx, repo, id)
		if err != nil {
			return nil, err
		}
		roots = append(roots, c.RootTree...)
	}
	var after backend.WorkspaceKey
	for {
		ws, err := s.ops(ctx).WorkspaceNames(ctx, repo, after, 500)
		if err != nil {
			return nil, err
		}
		for _, w := range ws {
			c, ok, err := s.ops(ctx).LatestCheckpoint(ctx, repo, w.Principal, w.Name)
			if err != nil {
				return nil, err
			}
			if ok {
				roots = append(roots, c.Trees...)
			}
		}
		if len(ws) < 500 {
			break
		}
		after = ws[len(ws)-1]
	}
	ts := s.pendingTrees(ctx, repo)
	seen := map[encoding.ID]bool{}
	held := map[SecretPointer]bool{}
	for _, root := range roots {
		v, err := lookupPath(ts, root, secrets.Dir)
		if err != nil {
			return nil, err
		}
		if v == nil || v.Kind != encoding.TreeValueTree || seen[v.ID] {
			continue
		}
		seen[v.ID] = true
		ps, err := s.subtreePointers(ctx, repo, ts, v.ID)
		if err != nil {
			return nil, err
		}
		for _, p := range ps {
			held[p] = true
		}
	}
	out := make([]SecretPointer, 0, len(held))
	for p := range held {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b SecretPointer) int {
		if c := strings.Compare(a.Path, b.Path); c != 0 {
			return c
		}
		return slices.Compare(a.Digest[:], b.Digest[:])
	})
	return out, nil
}

// subtreePointers are the pointers a //secret subtree holds, read once per subtree.
func (s *Store) subtreePointers(ctx context.Context, repo int64, ts treeStore, id encoding.ID) ([]SecretPointer, error) {
	if ps, ok := s.secretPointers.get(repo, id); ok {
		return ps, nil
	}
	var out []SecretPointer
	read := s.storedFiles(ctx, repo)
	err := diffTrees(ts, encoding.EmptyTreeID, id, secrets.Dir, func(ch leafChange) error {
		if ch.After.Kind != encoding.TreeValueFile || !secrets.IsSecret(ch.Path) {
			return nil
		}
		size, contents, err := read(ch.After.ID)
		if errors.Is(err, ErrNotFound) {
			return nil // a checkpoint's file collected since: it references nothing now
		}
		if err != nil || size != uint64(secrets.PointerLen) {
			return err
		}
		data, err := contents()
		if err != nil {
			return err
		}
		if d, err := secrets.ParsePointer(data); err == nil {
			out = append(out, SecretPointer{Path: ch.Path, Digest: d})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.secretPointers.put(repo, id, out)
	return out, nil
}

// pointerCache keeps the pointers //secret subtrees hold, by repository and subtree: a bounded
// number, all dropped at once past it.
type pointerCache struct {
	mu sync.Mutex
	m  map[pointerKey][]SecretPointer
}

type pointerKey struct {
	repo int64
	id   encoding.ID
}

const maxPointerSubtrees = 100_000

func (c *pointerCache) get(repo int64, id encoding.ID) ([]SecretPointer, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ps, ok := c.m[pointerKey{repo, id}]
	return ps, ok
}

func (c *pointerCache) put(repo int64, id encoding.ID, ps []SecretPointer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil || len(c.m) >= maxPointerSubtrees {
		c.m = map[pointerKey][]SecretPointer{}
	}
	c.m[pointerKey{repo, id}] = ps
}

// validateSecretStanzas checks the deploys' and post jobs' secrets: stanzas in every workflow file
// the change from parentTree to tree edits (docs/secrets.md, "Readers that are not people"): each
// secret one names is held at the commit, in a folder whose acl.yaml there gives that deploy
// (deploys:) or job (runs:) the folder's secrets — so a stanza names nothing it could not read.
// In a repository that keeps no secrets, a stanza names none.
func (s *Store) validateSecretStanzas(ctx context.Context, repo int64, ts treeStore, parentTree, tree encoding.ID) ([]string, error) {
	from := parentTree
	if from == (encoding.ID{}) {
		from = encoding.EmptyTreeID
	}
	var edited []leafChange
	if err := diffTrees(ts, from, tree, "", func(ch leafChange) error {
		if ch.After != nil && ch.After.Kind == encoding.TreeValueFile && workflow.IsWorkflowFile(ch.Path) {
			edited = append(edited, ch)
		}
		return nil
	}); err != nil || len(edited) == 0 {
		return nil, err
	}
	type named struct {
		file, what, id, grant string
		line                  int
		grants                func(secrets.Access) bool
		secrets               []workflow.Secret
	}
	var stanzas []named
	for _, ch := range edited {
		data, err := s.readSmallFile(ctx, repo, ch.After.ID, maxConfigFile)
		if err != nil {
			continue // validateWorkflowEdits reports it
		}
		f, _ := workflow.Parse(ch.Path, data)
		for _, d := range f.Deploys {
			if len(d.Secrets) > 0 {
				stanzas = append(stanzas, named{file: d.File, what: "deploy " + d.Name, id: d.ID(), grant: "deploys", line: d.Line,
					grants: func(a secrets.Access) bool { return a.GrantsDeploy(d.ID()) }, secrets: d.Secrets})
			}
		}
		for _, j := range f.Jobs {
			if len(j.Secrets) > 0 {
				stanzas = append(stanzas, named{file: j.File, what: "job " + j.Name, id: j.ID(), grant: "runs", line: j.Line,
					grants: func(a secrets.Access) bool { return a.GrantsRun(j.ID()) }, secrets: j.Secrets})
			}
		}
	}
	if len(stanzas) == 0 {
		return nil, nil
	}
	keeps, err := s.treeKeepsSecrets(ctx, repo, ts, tree)
	if err != nil {
		return nil, err
	}
	var t *secrets.Tree
	if keeps {
		if t, _, err = s.loadSecretTree(ctx, repo, ts, tree); err != nil {
			if _, ok := secrets.AsError(err); ok {
				return nil, nil // validateSecretEdits reports what is wrong with //secret
			}
			return nil, err
		}
	}
	var problems []string
	for _, st := range stanzas {
		for _, sec := range st.secrets {
			where := fmt.Sprintf("%s:%d: %s names //%s", st.file, sec.Line, st.what, sec.Path)
			folder := secrets.Folder(sec.Path)
			var access secrets.Access
			present := false
			if t != nil {
				access, present = t.Folders[folder]
			}
			held := false
			if t != nil {
				_, held = t.Secrets[sec.Path]
			}
			switch {
			case !keeps:
				problems = append(problems, fmt.Sprintf("%s, and the repository keeps no secrets (%s: secrets:)", where, workflow.SettingsFile))
			case !held:
				problems = append(problems, fmt.Sprintf("%s, which the commit holds no secret at", where))
			case !present || !st.grants(access):
				problems = append(problems, fmt.Sprintf("%s, and %s/%s does not give it the folder's secrets (%s: [\"%s\"])", where, folder, secrets.ACLFile, st.grant, st.id))
			}
		}
	}
	slices.Sort(problems)
	return problems, nil
}
