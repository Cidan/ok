package store

import (
	"context"
	"errors"
	"path"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/scm/encoding"
)

// Browse serves code to ok control and automation as structured values rather than jj
// objects (docs/scm.md, "Browse"). It works on resolved trees, which is all main
// and patchsets ever hold: a conflicted commit is refused.

// CommitInfo is a commit and its ID.
type CommitInfo struct {
	ID     encoding.ID
	Commit *encoding.Commit
}

// GetCommits reads commits by ID, in the order asked. The root commit is virtual and
// has no metadata to serve.
func (s *Store) GetCommits(ctx context.Context, repo int64, ids []encoding.ID) ([]CommitInfo, error) {
	refs := make([]ObjectRef, len(ids))
	for i, id := range ids {
		if id == encoding.RootCommitID {
			return nil, invalid("the root commit is virtual and has no metadata")
		}
		refs[i] = ObjectRef{Kind: encoding.KindCommit, ID: id}
	}
	res, err := s.ReadObjects(ctx, repo, refs)
	if err != nil {
		return nil, err
	}
	out := make([]CommitInfo, len(ids))
	for i, r := range res {
		if !r.Found {
			return nil, notFound("commit %s", ids[i])
		}
		o, err := decodeStored(encoding.KindCommit, ids[i], r.Body)
		if err != nil {
			return nil, err
		}
		out[i] = CommitInfo{ID: ids[i], Commit: o.(*encoding.Commit)}
	}
	return out, nil
}

// resolvedTree is a commit's root tree, which Browse requires to be resolved. The root
// commit's tree is the empty tree.
func (s *Store) resolvedTree(ctx context.Context, repo int64, id encoding.ID) (encoding.ID, error) {
	if id == encoding.RootCommitID {
		return encoding.EmptyTreeID, nil
	}
	c, err := s.readCommit(ctx, repo, id)
	if err != nil {
		return encoding.ID{}, err
	}
	if len(c.RootTree) != 1 {
		return encoding.ID{}, invalid("commit %s has conflicts: Browse serves resolved trees", id)
	}
	return c.RootTree[0], nil
}

// maxLogPage bounds one page of Log.
const maxLogPage = 1000

// LogPage is one page of main's history, newest first. Older and Newer continue it: the
// generation to page from in that direction, or 0 when nothing is left that way.
type LogPage struct {
	Commits      []CommitInfo
	Older, Newer int64
}

// Log returns a page of main's history, newest first, up to commit from (main's head when
// nil), optionally only the commits that changed one of paths — a file, or a directory
// and anything in it (commit_path indexes every changed path's directories too). A page
// continues another: before > 0 pages to commits older than that generation, after > 0 to
// commits newer than it. Uploads refuse merge commits, so main is a chain of single-parent
// commits and the commit head_log records at seq n has generation n: every page is an
// index range scan, on head_log or on commit_path.
func (s *Store) Log(ctx context.Context, repo int64, from *encoding.ID, paths []string, pageSize int, before, after int64) (LogPage, error) {
	paths, err := cleanPrefixes(paths)
	if err != nil {
		return LogPage{}, err
	}
	if before > 0 && after > 0 {
		return LogPage{}, invalid("a page continues either older or newer, not both")
	}
	if pageSize <= 0 || pageSize > maxLogPage {
		pageSize = maxLogPage
	}
	top, err := s.mainGeneration(ctx, repo, from)
	if err != nil || top == 0 {
		return LogPage{}, err
	}
	// scan lists the matching commits with generations in [lo, hi], newest first unless
	// ascending, at most limit of them.
	scan := func(lo, hi int64, ascending bool, limit int) ([]encoding.ID, []int64, error) {
		if lo > hi {
			return nil, nil, nil
		}
		order := "DESC"
		if ascending {
			order = "ASC"
		}
		var rows pgx.Rows
		var err error
		if len(paths) == 0 {
			rows, err = s.pool.Query(ctx, `SELECT new_head, seq FROM head_log
				WHERE repository_id = $1 AND seq BETWEEN $2 AND $3 ORDER BY seq `+order+` LIMIT $4`, repo, lo, hi, limit)
		} else {
			rows, err = s.pool.Query(ctx, `SELECT commit_id, generation FROM commit_path
				WHERE repository_id = $1 AND path = ANY($2::text[]) AND generation BETWEEN $3 AND $4
				GROUP BY commit_id, generation ORDER BY generation `+order+` LIMIT $5`, repo, paths, lo, hi, limit)
		}
		if err != nil {
			return nil, nil, err
		}
		defer rows.Close()
		var out []encoding.ID
		var gens []int64
		for rows.Next() {
			var b []byte
			var g int64
			if err := rows.Scan(&b, &g); err != nil {
				return nil, nil, err
			}
			id, err := toID(b)
			if err != nil {
				return nil, nil, err
			}
			out = append(out, id)
			gens = append(gens, g)
		}
		return out, gens, rows.Err()
	}
	var ids []encoding.ID
	var gens []int64
	if after > 0 {
		ids, gens, err = scan(after+1, top, true, pageSize)
		slices.Reverse(ids)
		slices.Reverse(gens)
	} else {
		hi := top
		if before > 0 {
			hi = min(hi, before-1)
		}
		ids, gens, err = scan(1, hi, false, pageSize)
	}
	if err != nil || len(ids) == 0 {
		return LogPage{}, err
	}
	var page LogPage
	newest, oldest := gens[0], gens[len(gens)-1]
	if more, _, err := scan(1, oldest-1, false, 1); err != nil {
		return LogPage{}, err
	} else if len(more) > 0 {
		page.Older = oldest
	}
	if more, _, err := scan(newest+1, top, true, 1); err != nil {
		return LogPage{}, err
	} else if len(more) > 0 {
		page.Newer = newest
	}
	if page.Commits, err = s.GetCommits(ctx, repo, ids); err != nil {
		return LogPage{}, err
	}
	return page, nil
}

// mainGeneration is the generation of a commit on main (main's head when nil): 0 for the
// root commit. A commit off main is refused.
func (s *Store) mainGeneration(ctx context.Context, repo int64, commit *encoding.ID) (int64, error) {
	var at encoding.ID
	if commit != nil {
		at = *commit
	} else {
		r, err := s.GetRepository(ctx, repo)
		if err != nil {
			return 0, err
		}
		at = r.Head
	}
	if at == encoding.RootCommitID {
		return 0, nil
	}
	var gen int64
	err := s.pool.QueryRow(ctx, `SELECT seq FROM head_log WHERE repository_id = $1 AND new_head = $2`, repo, at[:]).Scan(&gen)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := s.repositoryExists(ctx, s.pool, repo); err != nil {
			return 0, err
		}
		return 0, invalid("commit %s is not on main", at)
	}
	return gen, err
}

// BrowseValue is a tree entry's value as Browse serves it: the stored value, with a
// file's size and a symlink's target filled in.
type BrowseValue struct {
	encoding.TreeValue
	Size          uint64
	SymlinkTarget string
}

// BrowseEntry is one entry of a directory listing. LastCommit, when asked for, is the
// newest commit on main up to the listed commit that changed the entry.
type BrowseEntry struct {
	Name       string
	Value      BrowseValue
	LastCommit *encoding.ID
}

// cleanPath normalizes a path in a tree: "" is the root.
func cleanPath(p string) (string, error) {
	p = strings.Trim(p, "/")
	if p == "" {
		return "", nil
	}
	if path.Clean(p) != p || p == ".." || strings.HasPrefix(p, "../") {
		return "", invalid("path %q is not a clean relative path", p)
	}
	return p, nil
}

// treeReader reads a repository's stored trees for the tree walkers.
type treeReader struct {
	ctx  context.Context
	s    *Store
	repo int64
}

func (t treeReader) readTree(id encoding.ID) (*encoding.Tree, error) {
	if id == encoding.EmptyTreeID {
		return &encoding.Tree{}, nil
	}
	return t.s.readTree(t.ctx, t.repo, id)
}

func (t treeReader) readTrees(ids []encoding.ID) (map[encoding.ID]*encoding.Tree, error) {
	return t.s.readTrees(t.ctx, t.repo, ids)
}

func (t treeReader) writeTree(*encoding.Tree) (encoding.ID, error) {
	return encoding.ID{}, errors.New("browse never writes trees")
}

// fill completes values with file sizes and symlink targets, in two queries.
func (s *Store) fill(ctx context.Context, repo int64, vals []*BrowseValue) error {
	var files, links []encoding.ID
	for _, v := range vals {
		switch v.Kind {
		case encoding.TreeValueFile:
			files = append(files, v.ID)
		case encoding.TreeValueSymlink:
			links = append(links, v.ID)
		}
	}
	sizes := map[encoding.ID]uint64{}
	if len(files) > 0 {
		var err error
		if sizes, err = s.fileSizes(ctx, repo, files); err != nil {
			return err
		}
	}
	targets := map[encoding.ID]string{}
	if len(links) > 0 {
		refs := make([]ObjectRef, len(links))
		for i, id := range links {
			refs[i] = ObjectRef{Kind: encoding.KindSymlink, ID: id}
		}
		res, err := s.ReadObjects(ctx, repo, refs)
		if err != nil {
			return err
		}
		for i, r := range res {
			if !r.Found {
				return notFound("symlink %s", links[i])
			}
			o, err := decodeStored(encoding.KindSymlink, links[i], r.Body)
			if err != nil {
				return err
			}
			targets[links[i]] = o.(*encoding.Symlink).Target
		}
	}
	for _, v := range vals {
		switch v.Kind {
		case encoding.TreeValueFile:
			n, ok := sizes[v.ID]
			if !ok {
				return notFound("file %s", v.ID)
			}
			v.Size = n
		case encoding.TreeValueSymlink:
			v.SymlinkTarget = targets[v.ID]
		}
	}
	return nil
}

// ReadTree lists the directory at a path of a commit's tree, in name order. With
// lastCommits, and a commit on main, each entry also names the newest commit up to it that
// changed the entry; one index probe per entry.
func (s *Store) ReadTree(ctx context.Context, repo int64, commit encoding.ID, dir string, lastCommits bool) (encoding.ID, []BrowseEntry, error) {
	dir, err := cleanPath(dir)
	if err != nil {
		return encoding.ID{}, nil, err
	}
	root, err := s.resolvedTree(ctx, repo, commit)
	if err != nil {
		return encoding.ID{}, nil, err
	}
	tr := treeReader{ctx: ctx, s: s, repo: repo}
	treeID := root
	if dir != "" {
		v, err := lookupPath(tr, root, dir)
		if err != nil {
			return encoding.ID{}, nil, err
		}
		if v == nil || v.Kind != encoding.TreeValueTree {
			return encoding.ID{}, nil, notFound("directory %s at commit %s", dir, commit)
		}
		treeID = v.ID
	}
	t, err := tr.readTree(treeID)
	if err != nil {
		return encoding.ID{}, nil, err
	}
	out := make([]BrowseEntry, len(t.Entries))
	vals := make([]*BrowseValue, len(t.Entries))
	for i, e := range t.Entries {
		out[i] = BrowseEntry{Name: e.Name, Value: BrowseValue{TreeValue: e.Value}}
		vals[i] = &out[i].Value
	}
	if err := s.fill(ctx, repo, vals); err != nil {
		return encoding.ID{}, nil, err
	}
	if lastCommits && len(out) > 0 {
		if err := s.lastCommits(ctx, repo, commit, dir, out); err != nil {
			return encoding.ID{}, nil, err
		}
	}
	return treeID, out, nil
}

// lastCommits fills each entry's LastCommit when commit is on main; off main it leaves
// them nil.
func (s *Store) lastCommits(ctx context.Context, repo int64, commit encoding.ID, dir string, entries []BrowseEntry) error {
	gen, err := s.mainGeneration(ctx, repo, &commit)
	if errors.Is(err, ErrInvalid) {
		return nil
	}
	if err != nil || gen == 0 {
		return err
	}
	paths := make([]string, len(entries))
	for i, e := range entries {
		paths[i] = joinPath(dir, e.Name)
	}
	rows, err := s.pool.Query(ctx, `SELECT u.i, (SELECT cp.commit_id FROM commit_path cp
			WHERE cp.repository_id = $1 AND cp.path = u.path AND cp.generation <= $2
			ORDER BY cp.generation DESC LIMIT 1)
		FROM unnest($3::text[]) WITH ORDINALITY AS u(path, i)`, repo, gen, paths)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var i int64
		var b []byte
		if err := rows.Scan(&i, &b); err != nil {
			return err
		}
		if b == nil {
			continue
		}
		id, err := toID(b)
		if err != nil {
			return err
		}
		entries[i-1].LastCommit = &id
	}
	return rows.Err()
}

// lookup resolves a path of a commit's tree to its value, with size or target filled
// in; the root path is the root tree.
func (s *Store) lookup(ctx context.Context, repo int64, commit encoding.ID, p string) (*BrowseValue, error) {
	p, err := cleanPath(p)
	if err != nil {
		return nil, err
	}
	root, err := s.resolvedTree(ctx, repo, commit)
	if err != nil {
		return nil, err
	}
	if p == "" {
		return &BrowseValue{TreeValue: encoding.TreeValue{Kind: encoding.TreeValueTree, ID: root}}, nil
	}
	v, err := lookupPath(treeReader{ctx: ctx, s: s, repo: repo}, root, p)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, notFound("%s at commit %s", p, commit)
	}
	bv := &BrowseValue{TreeValue: *v}
	if err := s.fill(ctx, repo, []*BrowseValue{bv}); err != nil {
		return nil, err
	}
	return bv, nil
}

// ReadFileAt streams a file (or a byte range of it: length bytes from offset, fewer at the
// file's end, and all of it for length 0) at a path of a commit's tree: its value first,
// then its bytes in order. A symlink is its value alone.
func (s *Store) ReadFileAt(ctx context.Context, repo int64, commit encoding.ID, p string, offset, length uint64, emit func(v *BrowseValue, data []byte) error) error {
	v, err := s.lookup(ctx, repo, commit, p)
	if err != nil {
		return err
	}
	switch v.Kind {
	case encoding.TreeValueTree:
		return invalid("%s at commit %s is a directory", p, commit)
	case encoding.TreeValueSymlink:
		return emit(v, nil)
	case encoding.TreeValueFile:
	default:
		return invalid("%s at commit %s is not a file", p, commit)
	}
	first := true
	if offset > v.Size {
		return invalid("%s: offset %d is past its %d bytes", p, offset, v.Size)
	}
	end := v.Size
	if length != 0 && length < v.Size-offset {
		end = offset + length
	}
	if offset == end {
		return emit(v, nil)
	}
	return s.ReadFiles(ctx, repo, []FileRange{{FileID: v.ID, Offset: offset, Length: end - offset}}, RawChunks, func(f FileFrame) error {
		if f.NotFound {
			return notFound("file %s", v.ID)
		}
		// Frames are whole chunks: trim each to the range.
		lo := max(offset, f.Offset)
		hi := min(end, f.Offset+uint64(len(f.Data)))
		var data []byte
		if hi > lo {
			data = f.Data[lo-f.Offset : hi-f.Offset]
		}
		if first {
			first = false
			return emit(v, data)
		}
		if len(data) == 0 {
			return nil
		}
		return emit(nil, data)
	})
}

// MaxReadFilesAtLimit bounds how much of each file ReadFilesAt reads, and MaxReadFilesAtBytes
// how much one call may read in all (its limit times its paths): the heads are held until sent.
const (
	MaxReadFilesAtLimit = 1 << 20
	MaxReadFilesAtBytes = 64 << 20
)

// FileAt is a path ReadFilesAt read: its value, nil when the tree has nothing there, and up to the
// limit of a file's first bytes.
type FileAt struct {
	Path  string
	Value *BrowseValue
	Data  []byte
}

// ReadFilesAt reads many paths of a commit's tree at once, in their order: each one's value, and
// up to limit bytes of each file's head. The trees along the paths are read a level of them at a
// time, every file's size with one query, and their heads together (ReadFiles). A path that
// passes through a file, or names nothing, has no value.
func (s *Store) ReadFilesAt(ctx context.Context, repo int64, commit encoding.ID, paths []string, limit uint64) ([]FileAt, error) {
	if limit > MaxReadFilesAtLimit {
		return nil, invalid("limit %d is over %d", limit, MaxReadFilesAtLimit)
	}
	if limit*uint64(len(paths)) > MaxReadFilesAtBytes {
		return nil, invalid("%d paths of up to %d bytes each is over %d bytes in all", len(paths), limit, MaxReadFilesAtBytes)
	}
	cleaned := make([]string, len(paths))
	along := map[string]bool{"": true}
	for i, p := range paths {
		c, err := cleanPath(p)
		if err != nil {
			return nil, err
		}
		cleaned[i] = c
		for d := path.Dir(c); d != "." && !along[d]; d = path.Dir(d) {
			along[d] = true
		}
	}
	root, err := s.resolvedTree(ctx, repo, commit)
	if err != nil {
		return nil, err
	}
	tr := treeReader{ctx: ctx, s: s, repo: repo}
	trees, err := prefetchWalk(tr, root, func(dir string) bool { return along[dir] })
	if err != nil {
		return nil, err
	}
	ts := prefetched{treeStore: tr, trees: trees}
	out := make([]FileAt, len(paths))
	var vals []*BrowseValue
	for i, p := range cleaned {
		out[i].Path = paths[i]
		if p == "" {
			out[i].Value = &BrowseValue{TreeValue: encoding.TreeValue{Kind: encoding.TreeValueTree, ID: root}}
			continue
		}
		v, err := lookupPath(ts, root, p)
		if err != nil {
			return nil, err
		}
		if v != nil {
			out[i].Value = &BrowseValue{TreeValue: *v}
			vals = append(vals, out[i].Value)
		}
	}
	if err := s.fill(ctx, repo, vals); err != nil {
		return nil, err
	}
	if limit == 0 {
		return out, nil
	}
	var ranges []FileRange
	var owner []int
	for i, f := range out {
		if v := f.Value; v != nil && v.Kind == encoding.TreeValueFile && v.Size > 0 {
			ranges = append(ranges, FileRange{FileID: v.ID, Length: min(limit, v.Size)})
			owner = append(owner, i)
		}
	}
	err = s.ReadFiles(ctx, repo, ranges, RawChunks, func(f FileFrame) error {
		if f.NotFound {
			return notFound("file %s", f.FileID)
		}
		// Frames are whole chunks: keep what lies within the limit.
		if f.Offset >= limit {
			return nil
		}
		data := f.Data[:min(uint64(len(f.Data)), limit-f.Offset)]
		out[owner[f.Range]].Data = append(out[owner[f.Range]].Data, data...)
		return nil
	})
	return out, err
}

// ReadFile reads a whole file's contents by ID.
func (s *Store) ReadFile(ctx context.Context, repo int64, id encoding.ID) ([]byte, error) {
	return s.readFile(ctx, repo, id)
}

// IsAncestor reports whether ancestor is descendant or one of its ancestors. It walks
// descendant's parents, never below ancestor's generation.
func (s *Store) IsAncestor(ctx context.Context, repo int64, ancestor, descendant encoding.ID) (bool, error) {
	if ancestor == descendant || ancestor == encoding.RootCommitID {
		return true, nil
	}
	if descendant == encoding.RootCommitID {
		return false, nil
	}
	gen := map[encoding.ID]int64{}
	if err := s.loadGenerations(ctx, repo, []encoding.ID{ancestor, descendant}, gen); err != nil {
		return false, err
	}
	ga, okA := gen[ancestor]
	gd, okD := gen[descendant]
	if !okA {
		return false, notFound("commit %s", ancestor)
	}
	if !okD {
		return false, notFound("commit %s", descendant)
	}
	if ga >= gd {
		return false, nil
	}
	edges := map[encoding.ID][]encoding.ID{}
	loaded := map[encoding.ID]bool{}
	if err := s.loadEdges(ctx, repo, []encoding.ID{descendant}, ga, gen, edges, loaded); err != nil {
		return false, err
	}
	seen := map[encoding.ID]bool{descendant: true}
	stack := []encoding.ID{descendant}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, p := range edges[id] {
			if p == ancestor {
				return true, nil
			}
			if !seen[p] && gen[p] > ga {
				seen[p] = true
				stack = append(stack, p)
			}
		}
	}
	return false, nil
}

// ExportEntry is one leaf of an export: its path and value.
type ExportEntry struct {
	Path  string
	Value BrowseValue
}

// Export streams a commit's checkout under path prefixes (all of it when none), in path
// order: each leaf's entry, then — with contents — a file's bytes.
func (s *Store) Export(ctx context.Context, repo int64, commit encoding.ID, prefixes []string, contents bool, emit func(e *ExportEntry, data []byte) error) error {
	prefixes, err := cleanPrefixes(prefixes)
	if err != nil {
		return err
	}
	root, err := s.resolvedTree(ctx, repo, commit)
	if err != nil {
		return err
	}
	tr := treeReader{ctx: ctx, s: s, repo: repo}
	within := func(p string) (inside, along bool) {
		if len(prefixes) == 0 {
			return true, true
		}
		for _, pre := range prefixes {
			if p == pre || strings.HasPrefix(p, pre+"/") {
				return true, true
			}
			if strings.HasPrefix(pre, p+"/") {
				along = true
			}
		}
		return false, along
	}
	trees, err := prefetchWalk(tr, root, func(dir string) bool {
		inside, along := within(dir)
		return inside || along
	})
	if err != nil {
		return err
	}
	ts := prefetched{treeStore: tr, trees: trees}
	// Leaves are filled (sizes, symlink targets) a batch at a time, across directories, then
	// emitted in path order.
	var batch []*ExportEntry
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		vals := make([]*BrowseValue, len(batch))
		for i, entry := range batch {
			vals[i] = &entry.Value
		}
		if err := s.fill(ctx, repo, vals); err != nil {
			return err
		}
		// The batch's files are read together (ReadFiles), each entry emitted just before its
		// file's first bytes, so the stream stays in path order.
		var ranges []FileRange
		var owner []int // the batch index of each range's entry
		if contents {
			for i, entry := range batch {
				if v := entry.Value; v.Kind == encoding.TreeValueFile && v.Size > 0 {
					ranges = append(ranges, FileRange{FileID: v.ID})
					owner = append(owner, i)
				}
			}
		}
		next := 0
		through := func(i int) error {
			for ; next <= i; next++ {
				if err := emit(batch[next], nil); err != nil {
					return err
				}
			}
			return nil
		}
		if err := s.ReadFiles(ctx, repo, ranges, RawChunks, func(f FileFrame) error {
			if f.NotFound {
				return notFound("file %s", f.FileID)
			}
			if err := through(owner[f.Range]); err != nil {
				return err
			}
			if len(f.Data) == 0 {
				return nil
			}
			return emit(nil, f.Data)
		}); err != nil {
			return err
		}
		if err := through(len(batch) - 1); err != nil {
			return err
		}
		batch = nil
		return nil
	}
	var walk func(id encoding.ID, dir string) error
	walk = func(id encoding.ID, dir string) error {
		t, err := ts.readTree(id)
		if err != nil {
			return err
		}
		for _, e := range t.Entries {
			p := joinPath(dir, e.Name)
			inside, along := within(p)
			if e.Value.Kind == encoding.TreeValueTree {
				if inside || along {
					if err := walk(e.Value.ID, p); err != nil {
						return err
					}
				}
				continue
			}
			if !inside {
				continue
			}
			batch = append(batch, &ExportEntry{Path: p, Value: BrowseValue{TreeValue: e.Value}})
			if len(batch) == exportFillBatch {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(root, ""); err != nil {
		return err
	}
	return flush()
}

// exportFillBatch is how many leaves Export fills to a query.
const exportFillBatch = 2000

// prefetchWalk reads every tree a walk from root reads — root and each directory descend
// admits, by its path — a level of the trees to one read.
func prefetchWalk(bt treeBatcher, root encoding.ID, descend func(dir string) bool) (map[encoding.ID]*encoding.Tree, error) {
	type dirAt struct {
		id   encoding.ID
		path string
	}
	trees := map[encoding.ID]*encoding.Tree{encoding.EmptyTreeID: {}}
	level := []dirAt{{root, ""}}
	for len(level) > 0 {
		var want []encoding.ID
		asked := map[encoding.ID]bool{}
		for _, d := range level {
			if _, ok := trees[d.id]; !ok && !asked[d.id] {
				asked[d.id] = true
				want = append(want, d.id)
			}
		}
		read, err := bt.readTrees(want)
		if err != nil {
			return nil, err
		}
		for id, t := range read {
			trees[id] = t
		}
		var next []dirAt
		for _, d := range level {
			for _, e := range trees[d.id].Entries {
				if e.Value.Kind != encoding.TreeValueTree {
					continue
				}
				if p := joinPath(d.path, e.Name); descend(p) {
					next = append(next, dirAt{e.Value.ID, p})
				}
			}
		}
		level = next
	}
	return trees, nil
}
