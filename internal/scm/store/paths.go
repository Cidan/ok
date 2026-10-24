package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strings"

	"github.com/Cidan/ok/internal/scm/encoding"
	"github.com/Cidan/ok/internal/scm/store/backend"
)

// Main's path history — for each commit of main, the paths it changed and the directories above
// them — is in the tail as it lands, then in path segments (segments.go): every read of it looks in
// the tail, then the segments, the manifest read after the tail. A landing that changed more paths
// than one transaction writes leaves them to finishPathIndexes (path_index), and until they are
// written every read answers its commit from its trees (pendingPaths), read before the tail: a
// landing's mark goes only once its rows are all in.

// pathMark is a landing whose path history is yet to be written, with its tree and its parent's.
type pathMark struct {
	backend.PathIndex
	tree, parentTree encoding.ID
}

// pendingPathIndex are a repository's landings whose path history is yet to be written, oldest
// first, over its trees.
type pendingPathIndex struct {
	ts    treeStore
	marks []pathMark
}

// pendingPaths are a repository's landings whose path history is yet to be written: nil for none.
func (s *Store) pendingPaths(ctx context.Context, repo int64) (*pendingPathIndex, error) {
	pending, err := s.ops(ctx).PathIndexes(ctx, repo)
	if err != nil || len(pending) == 0 {
		return nil, err
	}
	out := &pendingPathIndex{ts: s.pendingTrees(ctx, repo)}
	for _, p := range pending {
		m := pathMark{PathIndex: p}
		if m.tree, err = s.rootTreeOf(ctx, repo, p.Commit); err != nil {
			return nil, err
		}
		if m.parentTree, err = s.rootTreeOf(ctx, repo, p.Parent); err != nil {
			return nil, err
		}
		out.marks = append(out.marks, m)
	}
	return out, nil
}

// rootTreeOf is a commit's first root tree, as main's path history diffs it: the empty tree for
// the root commit.
func (s *Store) rootTreeOf(ctx context.Context, repo int64, id encoding.ID) (encoding.ID, error) {
	if id == encoding.RootCommitID {
		return encoding.EmptyTreeID, nil
	}
	c, err := s.readCommit(ctx, repo, id)
	if err != nil {
		return encoding.ID{}, err
	}
	return c.RootTree[0], nil
}

// errChanged stops a diff at its first change.
var errChanged = errors.New("changed")

// changed reports whether a landing's path history records p: whether the diff of its tree from
// its parent's (changedPaths) reports a leaf at p or below it. It reads the trees along p, and
// below p only to the first change.
func (pp *pendingPathIndex) changed(m pathMark, p string) (bool, error) {
	if p == "" || p == "." || strings.IndexByte(p, 0) >= 0 {
		return false, nil
	}
	a, b := m.parentTree, m.tree
	parts := strings.Split(p, "/")
	for i, name := range parts {
		if a == b {
			return false, nil
		}
		ta, err := pp.ts.readTree(a)
		if err != nil {
			return false, err
		}
		tb, err := pp.ts.readTree(b)
		if err != nil {
			return false, err
		}
		va, vb := lookupEntry(ta, name), lookupEntry(tb, name)
		if sameValue(va, vb) {
			return false, nil
		}
		la, sa := split(va)
		lb, sb := split(vb)
		if i == len(parts)-1 {
			if !sameValue(la, lb) {
				return true, nil
			}
			err := walkDiff(pp.ts, sa, sb, p, func(leafChange) error { return errChanged })
			if errors.Is(err, errChanged) {
				return true, nil
			}
			return false, err
		}
		a, b = sa, sb
	}
	return false, nil
}

// lookupEntry is a tree's entry of a name, or nil: a tree's entries are in name order.
func lookupEntry(t *encoding.Tree, name string) *encoding.TreeValue {
	i, ok := slices.BinarySearchFunc(t.Entries, name, func(e encoding.TreeEntry, n string) int { return strings.Compare(e.Name, n) })
	if !ok {
		return nil
	}
	return &t.Entries[i].Value
}

// pathPrefix is a path's records' prefix in a path segment: the path and a NUL, which no path
// holds. A path holding a NUL has no records, and no prefix (ok false).
func pathPrefix(p string) ([]byte, bool) {
	if strings.IndexByte(p, 0) >= 0 {
		return nil, false
	}
	return append([]byte(p), 0), true
}

// pathAt is a path's first key in a path segment at generation gen or later, from its prefix.
func pathAt(prefix []byte, gen int64) []byte {
	return binary.BigEndian.AppendUint64(bytes.Clone(prefix), uint64(gen))
}

// pathAfter is the first key in a path segment past a path's records at generation gen, from its
// prefix.
func pathAfter(prefix []byte, gen int64) []byte {
	if gen == math.MaxInt64 {
		return prefixEnd(prefix)
	}
	return pathAt(prefix, gen+1)
}

// pathRecord is a path segment's record, decoded: its commit and generation.
func pathRecord(key []byte, prefixLen int) (backend.GenCommit, bool) {
	rest := key[prefixLen:]
	if len(rest) != 8+encoding.IDLen {
		return backend.GenCommit{}, false
	}
	return backend.GenCommit{Generation: int64(binary.BigEndian.Uint64(rest)), Commit: encoding.ID(rest[8:])}, true
}

// pathKey is a row of main's path history as a path segment's key.
func pathKey(r backend.PathRow) []byte {
	prefix, _ := pathPrefix(r.Path)
	return append(pathAt(prefix, r.Generation), r.Commit[:]...)
}

// byGeneration sorts commits by generation, then ID, ascending or descending.
func byGeneration(cs []backend.GenCommit, ascending bool) {
	slices.SortFunc(cs, func(a, b backend.GenCommit) int {
		c := 0
		switch {
		case a.Generation < b.Generation:
			c = -1
		case a.Generation > b.Generation:
			c = 1
		default:
			c = compareIDs(a.Commit, b.Commit)
		}
		if !ascending {
			c = -c
		}
		return c
	})
}

// pathCommits are up to limit of the commits of main that changed any of paths, at generations from
// to to, once each, newest first unless ascending.
func (s *Store) pathCommits(ctx context.Context, repo int64, paths []string, from, to int64, ascending bool, limit int) ([]backend.GenCommit, error) {
	pending, err := s.pendingPaths(ctx, repo)
	if err != nil {
		return nil, err
	}
	tail, err := s.ops(ctx).PathCommits(ctx, repo, paths, from, to, ascending, limit)
	if err != nil {
		return nil, err
	}
	readers, err := s.segmentReaders(ctx, repoScope(repo), segPaths, true)
	if err != nil || (len(readers) == 0 && pending == nil) {
		return tail, err
	}
	seen := map[encoding.ID]bool{}
	out := slices.Clone(tail)
	for _, c := range tail {
		seen[c.Commit] = true
	}
	if pending != nil {
		for _, m := range pending.marks {
			if m.Generation < from || m.Generation > to || seen[m.Commit] {
				continue
			}
			for _, p := range paths {
				ok, err := pending.changed(m, p)
				if err != nil {
					return nil, err
				}
				if ok {
					seen[m.Commit] = true
					out = append(out, backend.GenCommit{Commit: m.Commit, Generation: m.Generation})
					break
				}
			}
		}
	}
	for _, p := range paths {
		if len(readers) == 0 {
			break
		}
		prefix, ok := pathPrefix(p)
		if !ok {
			continue
		}
		// A path's records are distinct commits, so its first limit in order are all a page can use.
		n := 0
		err := scanSegments(ctx, readers, prefix, pathAt(prefix, from), pathAfter(prefix, to), !ascending, func(k, _ []byte) bool {
			if c, ok := pathRecord(k, len(prefix)); ok && !seen[c.Commit] {
				seen[c.Commit] = true
				out = append(out, c)
			}
			n++
			return n < limit
		})
		if err != nil {
			return nil, err
		}
	}
	byGeneration(out, ascending)
	return out[:min(limit, len(out))], nil
}

// pathCommit is the newest commit of main at or below a generation that changed a path, or none.
type pathCommit struct {
	commit encoding.ID
	found  bool
}

// lastPathCommits are, for each of paths, the newest commit of main at or below generation gen that
// changed it (absent: none), by its index in paths. gen is a commit's of main: main's history up to
// it never changes, so each answer is kept in the replica's cache, which a repository's listings
// at the same commit of main — its people's browsing at main's head — read again and again. Within
// a transaction, which may itself be landing gen, nothing is kept.
func (s *Store) lastPathCommits(ctx context.Context, repo, gen int64, paths []string) (map[int]encoding.ID, error) {
	keep := !inTxContext(ctx)
	out := make(map[int]encoding.ID, len(paths))
	var ask []string
	var at []int
	for i, p := range paths {
		if keep {
			if c, ok := s.newestOfPaths.get(pathCommitKey(repo, gen, p)); ok {
				if c.found {
					out[i] = c.commit
				}
				continue
			}
		}
		ask, at = append(ask, p), append(at, i)
	}
	if len(ask) == 0 {
		return out, nil
	}
	got, err := s.readLastPathCommits(ctx, repo, gen, ask)
	if err != nil {
		return nil, err
	}
	for j, p := range ask {
		c, found := got[j]
		if found {
			out[at[j]] = c
		}
		if keep {
			key := pathCommitKey(repo, gen, p)
			s.newestOfPaths.put(key, pathCommit{commit: c, found: found}, len(key)+encoding.IDLen+16)
		}
	}
	return out, nil
}

// pathCommitKey is a path's key in newestOfPaths at a generation of a repository's main.
func pathCommitKey(repo, gen int64, path string) string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(repo))
	binary.BigEndian.PutUint64(b[8:], uint64(gen))
	return string(b[:]) + path
}

// readLastPathCommits is lastPathCommits read: the tail, the segments, and what is landed but not
// yet in either.
func (s *Store) readLastPathCommits(ctx context.Context, repo, gen int64, paths []string) (map[int]encoding.ID, error) {
	pending, err := s.pendingPaths(ctx, repo)
	if err != nil {
		return nil, err
	}
	tail, err := s.ops(ctx).LastPathCommits(ctx, repo, gen, paths)
	if err != nil {
		return nil, err
	}
	readers, err := s.segmentReaders(ctx, repoScope(repo), segPaths, true)
	if err != nil {
		return nil, err
	}
	out := make(map[int]encoding.ID, len(paths))
	for i, p := range paths {
		last, found := tail[i]
		if prefix, ok := pathPrefix(p); ok && len(readers) > 0 {
			err := scanSegments(ctx, readers, prefix, prefix, pathAfter(prefix, gen), true, func(k, _ []byte) bool {
				if c, ok := pathRecord(k, len(prefix)); ok && (!found || c.Generation > last.Generation) {
					last, found = c, true
				}
				return false
			})
			if err != nil {
				return nil, err
			}
		}
		if pending != nil {
			// The newest landing at or below gen that changed p, if newer than what the index holds.
			for j := len(pending.marks) - 1; j >= 0; j-- {
				m := pending.marks[j]
				if m.Generation > gen {
					continue
				}
				if found && m.Generation <= last.Generation {
					break
				}
				ok, err := pending.changed(m, p)
				if err != nil {
					return nil, err
				}
				if ok {
					last, found = backend.GenCommit{Commit: m.Commit, Generation: m.Generation}, true
					break
				}
			}
		}
		if found {
			out[i] = last.Commit
		}
	}
	return out, nil
}

// pathHistory is the commits of main at or below generation gen that changed path, newest first.
func (s *Store) pathHistory(ctx context.Context, repo int64, path string, gen int64) ([]encoding.ID, error) {
	pending, err := s.pendingPaths(ctx, repo)
	if err != nil {
		return nil, err
	}
	all, err := s.ops(ctx).PathHistory(ctx, repo, path, gen)
	if err != nil {
		return nil, err
	}
	readers, err := s.segmentReaders(ctx, repoScope(repo), segPaths, true)
	if err != nil {
		return nil, err
	}
	seen := map[encoding.ID]bool{}
	for _, c := range all {
		seen[c.Commit] = true
	}
	added := false
	if prefix, ok := pathPrefix(path); ok && len(readers) > 0 {
		err := scanSegments(ctx, readers, prefix, prefix, pathAfter(prefix, gen), true, func(k, _ []byte) bool {
			if c, ok := pathRecord(k, len(prefix)); ok && !seen[c.Commit] {
				seen[c.Commit] = true
				all, added = append(all, c), true
			}
			return true
		})
		if err != nil {
			return nil, err
		}
	}
	if pending != nil {
		for _, m := range pending.marks {
			if m.Generation > gen || seen[m.Commit] {
				continue
			}
			ok, err := pending.changed(m, path)
			if err != nil {
				return nil, err
			}
			if ok {
				seen[m.Commit] = true
				all, added = append(all, backend.GenCommit{Commit: m.Commit, Generation: m.Generation}), true
			}
		}
	}
	if added {
		byGeneration(all, false)
	}
	out := make([]encoding.ID, len(all))
	for i, c := range all {
		out[i] = c.Commit
	}
	return out, nil
}
